import http from "node:http";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { afterEach, expect, test } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  HTTPAdapter,
  COMMAND_OPCODES,
  httpCommandDisposition
} from "../src/index.js";

const servers: (http.Server | http2.Http2Server)[] = [];

afterEach(async () => {
  await Promise.all(servers.map(async (server) => await new Promise<void>((resolve) => server.close(() => resolve()))));
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
  const server = http2.createServer();
  servers.push(server);
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

test("every native command has an explicit HTTP disposition and session commands fail locally", async () => {
  const dispositions = Object.keys(COMMAND_OPCODES).map((name) => httpCommandDisposition(name));
  expect(dispositions).toHaveLength(Object.keys(COMMAND_OPCODES).length);
  expect(dispositions.every((value) => value === "supported" || value === "native_only")).toBe(true);
  expect(httpCommandDisposition("SET")).toBe("supported");
  expect(httpCommandDisposition("PIPELINE")).toBe("native_only");
  expect(httpCommandDisposition("XREADGROUP")).toBe("native_only");

  const adapter = await HTTPAdapter.fromUrl("http://127.0.0.1:1");
  try {
    for (const command of ["AUTH", "CLIENT", "MULTI", "SUBSCRIBE", "BLPOP", "XREADGROUP"]) {
      await expect(adapter.executeCommand(command)).rejects.toThrow(/native TCP session/i);
    }
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
