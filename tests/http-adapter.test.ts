import http from "node:http";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { afterEach, expect, test } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  HTTPAdapter,
  HTTPTransportError,
  COMMAND_OPCODES,
  httpCommandDisposition
} from "../src/index.js";
import { decodeHTTPEnvelope, encodeHTTPCommands } from "../src/http-envelope.js";
import { normalizeHTTPOptions } from "../src/http-options.js";
import { HTTPTransport } from "../src/http-transport.js";
import { durableMutationMayHaveCommitted } from "../src/client-durable-step.js";

const servers: (http.Server | http2.Http2Server)[] = [];
const serverSessions = new Set<http2.ServerHttp2Session>();

afterEach(async () => {
  for (const session of serverSessions) session.destroy();
  serverSessions.clear();
  await Promise.all(servers.splice(0).map(async (server) => {
    if (!server.listening) return;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }));
});

test("HTTP JSON decoding preserves integers outside JavaScript's safe range", () => {
  const envelope = decodeHTTPEnvelope(Buffer.from(
    '{"results":[{"status":"ok","value":9007199254740993},{"status":"ok","value":-9007199254740993}]}'
  ));
  expect(envelope.results).toEqual([
    { status: "ok", value: 9_007_199_254_740_993n },
    { status: "ok", value: -9_007_199_254_740_993n }
  ]);
});

test("HTTP transport preserves binary values and the command API", async () => {
  const bytes = Buffer.from([0, 1, 255]);
  const server = await startHttpServer(async (request, response) => {
    const envelope = JSON.parse(await body(request)) as Envelope;
    expect(envelope.commands).toEqual([
      ["ECHO", { $ferricstore_bytes: bytes.toString("base64") }]
    ]);
    json(response, 200, success({ $ferricstore_bytes: bytes.toString("base64") }));
  });
  const client = await FerricStoreClient.fromUrl(url(server), { reconnect: false });
  try {
    await expect(client.echo(bytes)).resolves.toEqual(bytes);
  } finally {
    await client.close();
  }
});

test("HTTP transport accepts the same binary command names as native TCP", async () => {
  const server = await startHttpServer(async (request, response) => {
    const envelope = decodeTestValue(JSON.parse(await body(request))) as Envelope;
    expect(envelope.commands).toEqual([
      { command: "PING", opcode: COMMAND_OPCODES.PING, payload: {} }
    ]);
    json(response, 200, success("PONG"));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await expect(adapter.executeCommand(Buffer.from("PING"))).resolves.toBe("PONG");
  } finally {
    await adapter.close();
  }
});

test("HTTP transport uses structured native descriptors when the command has a typed payload", async () => {
  const server = await startHttpServer(async (request, response) => {
    const envelope = decodeTestValue(JSON.parse(await body(request))) as { commands: unknown[] };
    expect(envelope.commands).toEqual([
      {
        command: "FLOW.START_AND_CLAIM",
        opcode: COMMAND_OPCODES["FLOW.START_AND_CLAIM"],
        payload: {
          id: "flow-1",
          initial_state: "queued",
          now_ms: 1,
          partition_key: "partition-1",
          type: "jobs",
          worker: "worker-1"
        }
      }
    ]);
    json(response, 200, success("claimed"));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await expect(adapter.executeCommand(
      "FLOW.START_AND_CLAIM",
      "flow-1",
      "TYPE", "jobs",
      "INITIAL_STATE", "queued",
      "WORKER", "worker-1",
      "NOW", 1,
      "PARTITION", "partition-1"
    )).resolves.toBe("claimed");
  } finally {
    await adapter.close();
  }
});

test("one SDK pipeline is one HTTP request and preserves ordered item errors", async () => {
  let requests = 0;
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    const envelope = JSON.parse(await body(request)) as Envelope;
    expect(envelope.commands).toHaveLength(3);
    json(response, 200, {
      encoding: "ferricstore-json-v1",
      results: [
        { status: "ok", value: "PONG" },
        { status: "error", error: { code: "noperm", message: "denied" } },
        { status: "ok", value: 3 }
      ]
    });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const results = await adapter.executePipeline?.(
      [["PING"], ["GET", "secret"], ["INCR", "counter"]],
      { throwOnItemError: false }
    );
    expect(results?.[0]).toBe("PONG");
    expect(results?.[1]).toBeInstanceOf(FerricStoreError);
    expect(results?.[2]).toBe(3);
    expect(requests).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("redirects retain caller authentication and custom headers across origins", async () => {
  const target = await startHttpServer(async (request, response) => {
    expect(request.headers.authorization).toBe("Bearer secret");
    expect(request.headers["x-trace"]).toBe("trace-1");
    json(response, 200, success("PONG"));
  });
  const redirect = await startHttpServer(async (_request, response) => {
    response.writeHead(307, { location: `${url(target)}/redirected` });
    response.end();
  });
  const adapter = await HTTPAdapter.fromUrl(url(redirect), {
    bearerToken: "secret",
    headers: { "x-trace": "trace-1" }
  });
  try {
    await expect(adapter.executeCommand("PING")).resolves.toBe("PONG");
  } finally {
    await adapter.close();
  }
});

test("authenticated HTTP requests revalidate once after a stale-session 401", async () => {
  let requests = 0;
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    expect(request.headers.authorization).toBe("Bearer secret");
    await body(request);
    if (requests === 1) {
      json(response, 401, { error: { code: "unauthenticated" } });
    } else {
      json(response, 200, success("PONG"));
    }
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { bearerToken: "secret" });
  try {
    await expect(adapter.executeCommand("PING")).resolves.toBe("PONG");
    expect(requests).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("authenticated HTTP requests retry a persistent 401 at most once", async () => {
  let requests = 0;
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    await body(request);
    json(response, 401, { error: { code: "unauthenticated" } });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { bearerToken: "invalid" });
  try {
    await expect(adapter.executeCommand("PING")).rejects.toMatchObject({ statusCode: 401 });
    expect(requests).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("anonymous HTTP requests do not retry a 401", async () => {
  let requests = 0;
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    await body(request);
    json(response, 401, { error: { code: "unauthenticated" } });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await expect(adapter.executeCommand("PING")).rejects.toMatchObject({ statusCode: 401 });
    expect(requests).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 follows cross-origin redirects without dropping the request body or credentials", async () => {
  let targetRequest: { authorization?: string; body?: string; method?: string; path?: string } = {};
  const target = startHttp2Server();
  target.on("stream", (stream, headers) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk: Uint8Array) => chunks.push(Buffer.from(chunk)));
    stream.on("end", () => {
      targetRequest = {
        authorization: typeof headers.authorization === "string" ? headers.authorization : undefined,
        body: Buffer.concat(chunks).toString("utf8"),
        method: typeof headers[":method"] === "string" ? headers[":method"] : undefined,
        path: typeof headers[":path"] === "string" ? headers[":path"] : undefined
      };
      stream.respond({ ":status": 200, "content-type": "application/json" });
      stream.end(JSON.stringify(success("PONG")));
    });
  });
  await listen(target);

  const redirect = startHttp2Server();
  redirect.on("stream", (stream) => {
    stream.on("data", () => undefined);
    stream.on("end", () => {
      stream.respond({ ":status": 307, location: `${url(target)}/redirected` });
      stream.end();
    });
  });
  await listen(redirect);

  const adapter = await HTTPAdapter.fromUrl(url(redirect), { bearerToken: "secret", http2: true });
  try {
    await expect(adapter.executeCommand("PING")).resolves.toBe("PONG");
    expect(targetRequest).toMatchObject({
      authorization: "Bearer secret",
      method: "POST",
      path: "/redirected"
    });
    expect(targetRequest.body).toContain('"commands"');
  } finally {
    await adapter.close();
  }
});

test("HTTP options reject malformed URLs, headers, and ambiguous credentials before I/O", async () => {
  await expect(HTTPAdapter.fromUrl("http://user:secret@127.0.0.1")).rejects.toThrow(/userinfo/i);
  await expect(HTTPAdapter.fromUrl("http://127.0.0.1?query=1")).rejects.toThrow(/query/i);
  await expect(HTTPAdapter.fromUrl("http://127.0.0.1", {
    headers: { "bad header": "value" }
  })).rejects.toThrow(/invalid HTTP header/i);
  await expect(HTTPAdapter.fromUrl("http://127.0.0.1", {
    bearerToken: "bad\0token"
  })).rejects.toThrow(/bearer token/i);
  await expect(HTTPAdapter.fromUrl("http://127.0.0.1", {
    bearerToken: ""
  })).rejects.toThrow(/bearer token/i);
  await expect(HTTPAdapter.fromUrl("http://127.0.0.1", {
    password: "secret"
  })).rejects.toThrow(/HTTPS/i);
  await expect(HTTPAdapter.fromUrl("https://127.0.0.1", {
    bearerToken: "token",
    password: "secret"
  })).rejects.toThrow(/mutually exclusive/i);
});

test("valid header names that match Object prototype properties are preserved", async () => {
  let received: string | undefined;
  const server = await startHttpServer(async (request, response) => {
    const index = request.rawHeaders.findIndex((value) => value.toLowerCase() === "__proto__");
    received = index >= 0 ? request.rawHeaders[index + 1] : undefined;
    json(response, 200, success("PONG"));
  });
  const headers = Object.fromEntries([["__proto__", "trace-value"]]);
  const adapter = await HTTPAdapter.fromUrl(url(server), { headers });
  try {
    await expect(adapter.executeCommand("PING")).resolves.toBe("PONG");
    expect(received).toBe("trace-value");
  } finally {
    await adapter.close();
  }
});

test("HTTP/1.1 keep-alive reuses a connection", async () => {
  let connections = 0;
  const server = await startHttpServer(async (_request, response) => json(response, 200, success("PONG")));
  server.on("connection", () => connections += 1);
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await adapter.executeCommand("PING");
    await adapter.executeCommand("PING");
    expect(connections).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 multiplexes concurrent commands on one session", async () => {
  let sessions = 0;
  let active = 0;
  let maximumActive = 0;
  const server = startHttp2Server();
  server.on("session", () => sessions += 1);
  server.on("stream", (stream) => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    stream.on("data", () => undefined);
    stream.on("end", () => {
      setTimeout(() => {
        stream.respond({ ":status": 200, "content-type": "application/json" });
        stream.end(JSON.stringify(success("PONG")));
        active -= 1;
      }, 10);
    });
  });
  await listen(server);
  const adapter = await HTTPAdapter.fromUrl(url(server), { http2: true });
  try {
    await Promise.all(Array.from({ length: 20 }, async () => await adapter.executeCommand("PING")));
    expect(sessions).toBe(1);
    expect(maximumActive).toBeGreaterThan(1);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 queues streams behind the peer and client concurrency limits", async () => {
  let sessions = 0;
  let active = 0;
  let maximumActive = 0;
  const server = startHttp2Server({ settings: { maxConcurrentStreams: 2 } });
  server.on("session", () => sessions += 1);
  server.on("stream", (stream) => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    stream.on("data", () => undefined);
    stream.on("end", () => {
      setTimeout(() => {
        stream.respond({ ":status": 200, "content-type": "application/json" });
        stream.end(JSON.stringify(success("PONG")));
        active -= 1;
      }, 5);
    });
  });
  await listen(server);
  const adapter = await HTTPAdapter.fromUrl(url(server), { http2: true, maxConnections: 1 });
  try {
    const results = await Promise.all(
      Array.from({ length: 20 }, async () => await adapter.executeCommand("PING"))
    );
    expect(results).toEqual(Array.from({ length: 20 }, () => "PONG"));
    expect(sessions).toBe(1);
    expect(maximumActive).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 replaces a session after GOAWAY without losing the replacement", async () => {
  let sessions = 0;
  let requests = 0;
  const server = startHttp2Server();
  server.on("session", () => sessions += 1);
  server.on("stream", (stream) => {
    stream.on("data", () => undefined);
    stream.on("end", () => {
      requests += 1;
      stream.respond({ ":status": 200, "content-type": "application/json" });
      stream.end(JSON.stringify(success("PONG")));
      if (requests === 1) stream.session?.goaway(http2.constants.NGHTTP2_NO_ERROR, stream.id);
    });
  });
  await listen(server);
  const adapter = await HTTPAdapter.fromUrl(url(server), { http2: true });
  try {
    await adapter.executeCommand("PING");
    await delay(20);
    await adapter.executeCommand("PING");
    await delay(20);
    await adapter.executeCommand("PING");
    expect(requests).toBe(3);
    expect(sessions).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 retries a REFUSED_STREAM because the peer guarantees it was not processed", async () => {
  let requests = 0;
  const server = startHttp2Server();
  server.on("stream", (stream) => {
    requests += 1;
    stream.on("error", () => undefined);
    stream.on("data", () => undefined);
    stream.on("end", () => {
      if (requests === 1) {
        stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
        return;
      }
      stream.respond({ ":status": 200, "content-type": "application/json" });
      stream.end(JSON.stringify(success("OK")));
    });
  });
  await listen(server);
  const adapter = await HTTPAdapter.fromUrl(url(server), { http2: true });
  try {
    await expect(adapter.executeCommand("SET", "key", "value")).resolves.toBe("OK");
    expect(requests).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("close aborts active and HTTP/1.1 pool-queued commands immediately", async () => {
  let firstRequest: (() => void) | undefined;
  const started = new Promise<void>((resolve) => { firstRequest = resolve; });
  const server = await startHttpServer(async () => {
    firstRequest?.();
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), {
    maxConnections: 1,
    timeoutMs: 10_000
  });
  const first = adapter.executeCommand("PING");
  const second = adapter.executeCommand("PING");
  const settled = Promise.allSettled([first, second]);
  await started;
  await adapter.close();
  const results = await Promise.race([
    settled,
    delay(250).then(() => { throw new Error("HTTP commands did not settle after close"); })
  ]);
  expect(results).toHaveLength(2);
  for (const result of results) {
    expect(result.status).toBe("rejected");
    if (result.status === "rejected") expect(result.reason).toMatchObject({ message: /closed/i });
  }
});

test("whole-request deadlines support values above Node's single-timer limit", async () => {
  const server = await startHttpServer(async (_request, response) => {
    await delay(10);
    json(response, 200, success("PONG"));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 2_147_483_648 });
  try {
    await expect(adapter.executeCommand("PING")).resolves.toBe("PONG");
  } finally {
    await adapter.close();
  }
});

test("request, response, batch and whole-request deadlines are bounded", async () => {
  const server = await startHttpServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.write('{"encoding":"ferricstore-json-v1","results":[');
    setTimeout(() => response.end('{"status":"ok","value":"late"}]}'), 100);
  });
  await expect(HTTPAdapter.fromUrl(url(server), { maxBatchItems: 1 }).then(
    async (adapter) => await adapter.executePipeline?.([["PING"], ["PING"]])
  )).rejects.toThrow(/batch/i);
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 20, maxResponseBytes: 32 });
  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow(/timed out|maxResponseBytes/i);
  } finally {
    await adapter.close();
  }
});

test("request bounds stop nested traversal before allocating an oversized JSON envelope", async () => {
  let distantItemRead = false;
  const nested = Array.from<Buffer>({ length: 100 }).fill(Buffer.alloc(0));
  Object.defineProperty(nested, 99, {
    enumerable: true,
    get: () => {
      distantItemRead = true;
      return Buffer.alloc(0);
    }
  });
  const adapter = await HTTPAdapter.fromUrl("http://127.0.0.1:1", { maxRequestBytes: 128 });
  try {
    await expect(adapter.executeCommand("ECHO", nested)).rejects.toThrow(/maxRequestBytes/i);
    expect(distantItemRead).toBe(false);
  } finally {
    await adapter.close();
  }
});

test("local HTTP request failures are marked unsent and make no network exchange", async () => {
  let requests = 0;
  const server = await startHttpServer(async (_request, response) => {
    requests += 1;
    json(response, 200, success("unexpected"));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), {
    maxBatchItems: 1,
    maxRequestBytes: 128
  });
  try {
    for (const operation of [
      async () => await adapter.executePipeline?.([["PING"], ["PING"]]),
      async () => await adapter.executeCommand(
        "ECHO",
        Array.from({ length: 100 }, () => Buffer.alloc(0))
      ),
      async () => await adapter.executeCommand("AUTH", "default", "secret")
    ]) {
      const error = await rejected(operation);
      expect(error).toBeInstanceOf(HTTPTransportError);
      expect(error).toMatchObject({
        requestDisposition: "unsent",
        retryable: false,
        safeToRetry: true
      });
      expect(durableMutationMayHaveCommitted(error)).toBe(false);
    }
    expect(requests).toBe(0);
  } finally {
    await adapter.close();
  }

  const closedError = await rejected(async () => await adapter.executeCommand("PING"));
  expect(closedError).toMatchObject({ requestDisposition: "unsent", safeToRetry: true });
  expect(requests).toBe(0);
});

test("HTTP 408 remains uncertain after the server received the mutation", async () => {
  let requests = 0;
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    await body(request);
    json(response, 408, {
      error: { message: "upstream deadline elapsed", safe_to_retry: true }
    });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const error = await rejected(async () => await adapter.executeCommand("SET", "key", "value"));
    expect(error).toMatchObject({
      requestDisposition: "possibly_sent",
      retryable: true,
      safeToRetry: false,
      statusCode: 408
    });
    expect(durableMutationMayHaveCommitted(error)).toBe(true);
    expect(requests).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("definite HTTP rejections and explicit retry guarantees do not become unknown outcomes", async () => {
  const responses = [
    { status: 401, envelope: { error: { message: "unauthenticated" } } },
    {
      status: 503,
      envelope: {
        error: {
          code: "overloaded",
          message: "admission rejected the command",
          safe_to_retry: true
        }
      }
    }
  ];
  const server = await startHttpServer(async (request, response) => {
    await body(request);
    const next = responses.shift();
    if (next == null) throw new Error("unexpected request");
    json(response, next.status, next.envelope);
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    for (const expectedStatus of [401, 503]) {
      const error = await rejected(async () => await adapter.executeCommand("SET", "key", "value"));
      expect(error).toMatchObject({
        requestDisposition: "possibly_sent",
        safeToRetry: true,
        statusCode: expectedStatus
      });
      expect(durableMutationMayHaveCommitted(error)).toBe(false);
    }
  } finally {
    await adapter.close();
  }
});

test("HTTP/1.1 truncated response after receipt stays uncertain and is not replayed", async () => {
  let effects = 0;
  const server = await startHttpServer(async (request, response) => {
    await body(request);
    effects += 1;
    response.writeHead(200, {
      "content-length": "1000",
      "content-type": "application/json"
    });
    response.write('{"encoding":"ferricstore-json-v1","results":[');
    response.socket?.destroy();
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const error = await rejected(async () => await adapter.executeCommand("SET", "key", "value"));
    expect(error).toMatchObject({ retryable: true, safeToRetry: false });
    expect(effects).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("HTTP/2 reset after receipt stays uncertain and is not replayed", async () => {
  let effects = 0;
  const server = startHttp2Server();
  server.on("stream", (stream) => {
    stream.on("error", () => undefined);
    stream.on("data", () => undefined);
    stream.on("end", () => {
      effects += 1;
      stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
    });
  });
  await listen(server);
  const adapter = await HTTPAdapter.fromUrl(url(server), { http2: true });
  try {
    const error = await rejected(async () => await adapter.executeCommand("SET", "key", "value"));
    expect(error).toMatchObject({ retryable: true, safeToRetry: false });
    expect(effects).toBe(1);
  } finally {
    await adapter.close();
  }
});

test("request encoding budget never rejects a body at its exact encoded size", () => {
  const commands = [Array.from({ length: 100 }, () => new Map())];
  const body = encodeHTTPCommands(commands);
  expect(encodeHTTPCommands(commands, body.byteLength)).toEqual(body);
});

test("every native command has an explicit HTTP disposition and session commands fail locally", async () => {
  const dispositions = Object.keys(COMMAND_OPCODES).map((name) => httpCommandDisposition(name));
  expect(dispositions).toHaveLength(Object.keys(COMMAND_OPCODES).length);
  expect(dispositions.every((value) => value === "supported" || value === "native_only")).toBe(true);
  expect(httpCommandDisposition("SET")).toBe("supported");
  expect(httpCommandDisposition("PIPELINE")).toBe("native_only");
  for (const command of [
    "BLMOVE", "BLMPOP", "BLPOP", "BRPOP", "BRPOPLPUSH", "BZMPOP", "BZPOPMAX", "BZPOPMIN",
    "XREAD", "XREADGROUP"
  ]) {
    expect(httpCommandDisposition(command)).toBe("supported");
  }

  const adapter = await HTTPAdapter.fromUrl("http://127.0.0.1:1");
  try {
    for (const command of Object.keys(COMMAND_OPCODES)) {
      if (httpCommandDisposition(command) === "native_only") {
        await expect(adapter.executeCommand(command)).rejects.toThrow(/native TCP/i);
      }
    }
    for (const command of [
      "ASKING", "AUTH", "CLIENT", "DISCARD", "EXEC", "HELLO", "MONITOR", "MULTI",
      "FETCH_OR_COMPUTE", "FETCH_OR_COMPUTE_ERROR", "FETCH_OR_COMPUTE_RESULT",
      "PSUBSCRIBE", "PSYNC", "PUNSUBSCRIBE", "QUIT", "READONLY", "READWRITE", "REPLCONF",
      "RESET", "SANDBOX", "SELECT", "SSUBSCRIBE", "SUBSCRIBE", "SUNSUBSCRIBE", "SYNC",
      "UNSUBSCRIBE", "UNWATCH", "WATCH"
    ]) {
      expect(httpCommandDisposition(command)).toBe("native_only");
      await expect(adapter.executeCommand(command)).rejects.toThrow(/native TCP/i);
    }
  } finally {
    await adapter.close();
  }
});

test("blocking list and stream commands pass the HTTP policy and reach the adapter", async () => {
  const commands = [
    "BLMOVE", "BLMPOP", "BLPOP", "BRPOP", "BRPOPLPUSH", "BZMPOP", "BZPOPMAX", "BZPOPMIN",
    "XREAD", "XREADGROUP"
  ];
  let observed: string[] = [];
  const server = await startHttpServer(async (request, response) => {
    const envelope = decodeTestValue(JSON.parse(await body(request))) as Envelope;
    observed = envelope.commands.map(encodedCommandName);
    json(response, 200, {
      encoding: "ferricstore-json-v1",
      results: envelope.commands.map(() => ({ status: "ok", value: null }))
    });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await expect(adapter.executePipeline(commands.map((command) => [command]))).resolves.toHaveLength(commands.length);
    expect(observed).toEqual(commands);
  } finally {
    await adapter.close();
  }
});

test("finite blocking HTTP commands add their server wait to the whole-request deadline", async () => {
  const commands: readonly (readonly (string | number)[])[] = [
    ["BLMOVE", "source", "target", "LEFT", "RIGHT", 0.2],
    ["BLMPOP", 0.2, 1, "jobs", "LEFT"],
    ["BLPOP", "jobs", 0.2],
    ["BRPOP", "jobs", 0.2],
    ["BRPOPLPUSH", "source", "target", 0.2],
    ["BZMPOP", 0.2, 1, "scores", "MIN"],
    ["BZPOPMAX", "scores", 0.2],
    ["BZPOPMIN", "scores", 0.2],
    ["XREAD", "BLOCK", 200, "STREAMS", "events", "$"],
    ["XREADGROUP", "GROUP", "workers", "worker-1", "BLOCK", 200, "STREAMS", "events", ">"]
  ];
  const server = await startHttpServer(async (_request, response) => {
    await delay(80);
    json(response, 200, success(null));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 40 });
  try {
    for (const command of commands) {
      await expect(adapter.executeCommand(...command)).resolves.toBeNull();
    }
  } finally {
    await adapter.close();
  }
});

test("blocking zero disables the HTTP response deadline until close", async () => {
  const commands: readonly (readonly (string | number)[])[] = [
    ["BLPOP", "jobs", 0],
    ["BZMPOP", 0, 1, "scores", "MIN"],
    ["BZPOPMIN", "scores", 0],
    ["XREAD", "BLOCK", 0, "STREAMS", "events", "$"],
    ["XREADGROUP", "GROUP", "workers", "worker-1", "BLOCK", 0, "STREAMS", "events", ">"]
  ];
  const server = await startHttpServer(async (_request, response) => {
    await delay(80);
    json(response, 200, success(null));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 30 });
  try {
    for (const command of commands) {
      await expect(adapter.executeCommand(...command)).resolves.toBeNull();
    }
  } finally {
    await adapter.close();
  }
});

test("finite blocking HTTP timeout errors report the extended response deadline", async () => {
  const server = await startHttpServer(async () => undefined);
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 50 });
  try {
    await expect(adapter.executeCommand("BLPOP", "jobs", 0.01)).rejects.toMatchObject({
      code: "request_timeout",
      timeoutMs: 60
    });
  } finally {
    await adapter.close();
  }
});

test("one mixed HTTP pipeline remains ordered and budgets finite blocking waits cumulatively", async () => {
  let requests = 0;
  let observed: string[] = [];
  const server = await startHttpServer(async (request, response) => {
    requests += 1;
    const envelope = decodeTestValue(JSON.parse(await body(request))) as Envelope;
    observed = envelope.commands.map(encodedCommandName);
    await delay(100);
    json(response, 200, {
      encoding: "ferricstore-json-v1",
      results: [
        { status: "ok", value: "value" },
        { status: "ok", value: null },
        { status: "ok", value: null },
        { status: "ok", value: "OK" }
      ]
    });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server), { timeoutMs: 40 });
  try {
    await expect(adapter.executePipeline([
      ["GET", "before"],
      ["BLPOP", "jobs", 0.1],
      ["XREAD", "BLOCK", 100, "STREAMS", "events", "$"],
      ["SET", "after", "value"]
    ])).resolves.toEqual(["value", null, null, "OK"]);
    expect(requests).toBe(1);
    expect(observed).toEqual(["GET", "BLPOP", "XREAD", "SET"]);
  } finally {
    await adapter.close();
  }
});

test("every supported native opcode crosses the HTTP adapter with its canonical command name", async () => {
  const supported = Object.keys(COMMAND_OPCODES)
    .filter((command) => httpCommandDisposition(command) === "supported")
    .sort();
  let observed: string[] = [];
  const server = await startHttpServer(async (request, response) => {
    const envelope = decodeTestValue(JSON.parse(await body(request))) as Envelope;
    observed = envelope.commands.map(encodedCommandName);
    json(response, 200, {
      encoding: "ferricstore-json-v1",
      results: envelope.commands.map(() => ({ status: "ok", value: "OK" }))
    });
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const results = await adapter.executePipeline(supported.map(minimalNativeCommand));
    expect(results).toHaveLength(supported.length);
    expect(observed.sort()).toEqual(supported);
  } finally {
    await adapter.close();
  }
});

test("malformed byte markers and response items are rejected", async () => {
  const responses = [
    success({ $ferricstore_bytes: "not-base64" }),
    { encoding: "ferricstore-json-v1", results: [{ status: "unknown", value: "PONG" }] },
    { encoding: "ferricstore-json-v1", results: [{ status: "error", error: "bad shape" }] }
  ];
  const server = await startHttpServer(async (_request, response) => {
    json(response, 200, responses.shift());
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow(/bytes marker/i);
    await expect(adapter.executeCommand("PING")).rejects.toThrow(/response/i);
    await expect(adapter.executeCommand("PING")).rejects.toThrow(/response/i);
  } finally {
    await adapter.close();
  }
});

test("plain response records preserve an own __proto__ key without prototype mutation", async () => {
  const server = await startHttpServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(
      '{"encoding":"ferricstore-json-v1","results":[{"status":"ok","value":' +
      '{"__proto__":{"polluted":true},"safe":1}}]}'
    );
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const value = await adapter.executeCommand("PING") as Record<string, unknown>;
    expect(Object.getPrototypeOf(value)).toBe(Object.prototype);
    expect(Object.hasOwn(value, "__proto__")).toBe(true);
    expect(value.__proto__).toEqual({ polluted: true });
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  } finally {
    await adapter.close();
  }
});

test("HTTP response headers preserve prototype-like names as ordinary data", async () => {
  const server = startHttp2Server();
  server.on("stream", (stream) => {
    stream.on("data", () => undefined);
    stream.on("end", () => {
      const headers: http2.OutgoingHttpHeaders = { ":status": 200 };
      const prototypeLikeHeader = ["__", "proto", "__"].join("");
      Object.defineProperty(headers, prototypeLikeHeader, {
        enumerable: true,
        value: "trace-value",
      });
      stream.respond(headers);
      stream.end("ok");
    });
  });
  await listen(server);
  const transport = new HTTPTransport(normalizeHTTPOptions(url(server), { http2: true }));
  try {
    const response = await transport.post(Buffer.from("request"), 30_000);
    expect(Object.getPrototypeOf(response.headers)).toBeNull();
    expect(Object.hasOwn(response.headers, "__proto__")).toBe(true);
    expect(response.headers.__proto__).toBe("trace-value");
  } finally {
    await transport.close();
  }
});

test("oversized Retry-After metadata is ignored instead of exposing Infinity", async () => {
  const server = await startHttpServer(async (_request, response) => {
    response.writeHead(429, {
      "content-type": "application/json",
      "retry-after": "999999999999999999999999"
    });
    response.end(JSON.stringify({ error: { message: "busy" } }));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const error = await adapter.executeCommand("PING").catch((reason: unknown) => reason);
    expect(error).toMatchObject({ retryAfterMs: undefined, statusCode: 429 });
  } finally {
    await adapter.close();
  }
});

test("Retry-After HTTP dates are exposed as bounded retry metadata", async () => {
  const deadline = new Date(Date.now() + 60_000).toUTCString();
  const server = await startHttpServer(async (_request, response) => {
    response.writeHead(503, {
      "content-type": "application/json",
      "retry-after": deadline
    });
    response.end(JSON.stringify({ error: { message: "unavailable" } }));
  });
  const adapter = await HTTPAdapter.fromUrl(url(server));
  try {
    const error = await adapter.executeCommand("PING").catch((reason: unknown) => reason) as {
      readonly retryAfterMs?: number;
    };
    expect(error.retryAfterMs).toBeGreaterThanOrEqual(58_000);
    expect(error.retryAfterMs).toBeLessThanOrEqual(60_000);
  } finally {
    await adapter.close();
  }
});

interface Envelope {
  readonly commands: unknown[];
}

function success(value: unknown): unknown {
  return { encoding: "ferricstore-json-v1", results: [{ status: "ok", value }] };
}

async function startHttpServer(
  handler: (request: http.IncomingMessage, response: http.ServerResponse) => Promise<void>
): Promise<http.Server> {
  const server = http.createServer((request, response) => void handler(request, response));
  servers.push(server);
  await listen(server);
  return server;
}

function startHttp2Server(options: http2.ServerOptions = {}): http2.Http2Server {
  const server = http2.createServer(options);
  servers.push(server);
  server.on("session", (session) => {
    serverSessions.add(session);
    session.once("close", () => serverSessions.delete(session));
  });
  return server;
}

async function listen(server: http.Server | http2.Http2Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function url(server: http.Server | http2.Http2Server): string {
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

async function body(request: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(bodyChunk(chunk));
  return Buffer.concat(chunks).toString("utf8");
}

function bodyChunk(value: unknown): Buffer {
  if (typeof value === "string" || value instanceof Uint8Array) return Buffer.from(value);
  throw new TypeError("HTTP request yielded an invalid body chunk");
}

function json(response: http.ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}

async function delay(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function rejected(action: () => Promise<unknown>): Promise<Error> {
  try {
    await action();
  } catch (error) {
    if (error instanceof Error) return error;
    throw error;
  }
  throw new Error("expected operation to reject");
}

function decodeTestValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(decodeTestValue);
  if (typeof value !== "object" || value == null) return value;
  const record = value as Record<string, unknown>;
  if (Array.isArray(record.$ferricstore_map)) {
    return Object.fromEntries(record.$ferricstore_map.map((pair) => {
      if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string") {
        throw new TypeError("invalid test map marker");
      }
      return [pair[0], decodeTestValue(pair[1])];
    }));
  }
  return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, decodeTestValue(item)]));
}

function encodedCommandName(value: unknown): string {
  if (Array.isArray(value) && typeof value[0] === "string") return value[0];
  if (typeof value === "object" && value != null) {
    const command = (value as { readonly command?: unknown }).command;
    if (typeof command === "string") return command;
  }
  throw new TypeError("test HTTP command has no canonical name");
}

function minimalNativeCommand(command: string): readonly (string | number)[] {
  switch (command) {
    case "FETCH_OR_COMPUTE_ERROR":
      return [command, "key", "token", "message"];
    case "FETCH_OR_COMPUTE_RESULT":
      return [command, "key", "token", "value", 1];
    case "FLOW.QUERY":
      return [command, "FQL1", "FROM runs WHERE type = @type LIMIT 1 RETURN COUNT", "type", "test"];
    default:
      return [command];
  }
}
