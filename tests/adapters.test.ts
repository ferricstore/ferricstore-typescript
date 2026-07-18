import net from "node:net";
import tls from "node:tls";
import type { AddressInfo, Socket } from "node:net";
import { expect, test, vi } from "vitest";
import {
  NativeAdapter,
  executeCommandsIndividually
} from "../src/adapters.js";
import { ReconnectingExecutor } from "../src/reconnecting-executor.js";
import { FerricStoreClient } from "../src/client.js";
import { ConnectionClosedError, FerricStoreError } from "../src/errors.js";
import type { Command } from "../src/internal.js";
import {
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
} from "../src/protocol.js";
import { RoutingTopology, type RoutingRoute } from "../src/topology.js";
import {
  type TestRequest,
  NO_RESPONSE,
  activeConnections,
  commandExecName,
  listen,
  responseFrame,
  responseFrameFromBody,
  servers,
  startCountingServer,
  startFragmentingServer,
  validTopologyPayload,
  waitFor,
  writeIncrementalFragments
} from "./adapter-test-support.js";

test("NativeAdapter handles fragmented response frames", async () => {
  const server = await startFragmentingServer();
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 4
  });

  try {
    const response = await adapter.executeCommand("PING");
    expect(Buffer.isBuffer(response)).toBe(true);
    expect((response as Buffer).toString("utf8")).toBe("PONG");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter rejects client and topology options instead of silently ignoring them", async () => {
  await expect(NativeAdapter.fromUrl("ferric://127.0.0.1:1", {
    haRouting: true
  } as never)).rejects.toThrow(/FerricStoreClient.*haRouting/i);
  await expect(NativeAdapter.fromUrl("ferric://127.0.0.1:1", {
    endpointPolicy: "any"
  } as never)).rejects.toThrow(/TopologyNativeAdapterPool.*endpointPolicy/i);

  const assertDirectOptionsAreNarrow = (): void => {
    // @ts-expect-error HA selection belongs to FerricStoreClient.fromUrl.
    void NativeAdapter.fromUrl("ferric://localhost:6388", { haRouting: true });
    // @ts-expect-error Endpoint trust belongs to TopologyNativeAdapterPool.
    void NativeAdapter.fromUrl("ferric://localhost:6388", { endpointPolicy: "any" });
  };
  void assertDirectOptionsAreNarrow;
});

test("NativeAdapter connects to bracketed IPv6 FerricStore URLs", async () => {
  const server = await startCountingServer(() => undefined, {
    fragmentResponses: false,
    host: "::1"
  });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://[::1]:${address.port}`);

  try {
    await expect(adapter.executeCommand("PING")).resolves.toEqual(Buffer.from("PONG"));
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter copies fragmented receive data linearly", async () => {
  const payload = Buffer.alloc(32 * 1024, 0x61);
  const server = await startCountingServer((request, socket) => {
    if (request.opcode !== OPCODES.ping) return undefined;
    writeIncrementalFragments(
      socket,
      responseFrame(request.opcode, request.laneId, request.requestId, payload),
      256
    );
    return NO_RESPONSE;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);
  const originalConcat = Buffer.concat.bind(Buffer);
  let copiedBytes = 0;
  const concat = vi.spyOn(Buffer, "concat").mockImplementation((chunks: readonly Uint8Array[], totalLength?: number) => {
    copiedBytes += chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
    return originalConcat(chunks, totalLength);
  });

  try {
    await expect(adapter.executeCommand("PING")).resolves.toEqual(payload);
    expect(copiedBytes).toBeLessThan(payload.byteLength * 6);
  } finally {
    concat.mockRestore();
    await adapter.close();
  }
});

test("NativeAdapter closes its socket when startup fails", async () => {
  const server = await startCountingServer(
    (request) => (request.opcode === OPCODES.startup ? NO_RESPONSE : undefined),
    { fragmentResponses: false }
  );
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  const address = server.address() as AddressInfo;

  await expect(
    NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, { timeoutMs: 20 })
  ).rejects.toThrow("timed out");
  try {
    await waitFor(async () => (await activeConnections(server)) === 0);
  } finally {
    for (const socket of sockets) socket.destroy();
  }
});

test("NativeAdapter rejects startup when authentication is required but credentials are missing", async () => {
  let authRequests = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) return { auth_required: true };
    if (request.opcode === OPCODES.auth) authRequests += 1;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;

  let unexpectedlyOpened: NativeAdapter | undefined;
  const opening = NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`).then((adapter) => {
    unexpectedlyOpened = adapter;
    throw new Error("startup unexpectedly resolved");
  });
  try {
    await expect(opening).rejects.toThrow(/requires authentication.*no password/i);
    expect(authRequests).toBe(0);
  } finally {
    await unexpectedlyOpened?.close();
  }
  await waitFor(async () => (await activeConnections(server)) === 0);
});

test("NativeAdapter applies connectTimeoutMs through the TLS handshake", async () => {
  const sockets = new Set<Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
    socket.on("data", () => undefined);
  });
  servers.push(server);
  await listen(server);
  const address = server.address() as AddressInfo;
  const startedAt = Date.now();

  try {
    await expect(
      NativeAdapter.fromUrl(`ferrics://127.0.0.1:${address.port}`, {
        connectTimeoutMs: 30,
        timeoutMs: 500,
        tlsOptions: { rejectUnauthorized: false }
      })
    ).rejects.toThrow("connection timed out after 30ms");

    expect(Date.now() - startedAt).toBeLessThan(300);
  } finally {
    for (const socket of sockets) socket.destroy();
  }
});

test("NativeAdapter derives TLS SNI from DNS URLs without overriding explicit TLS options", async () => {
  const captures: tls.ConnectionOptions[] = [];
  const connect = vi.spyOn(tls, "connect").mockImplementation(((options: tls.ConnectionOptions) => {
    captures.push(options);
    throw new Error("stop after capturing TLS options");
  }) as typeof tls.connect);

  try {
    await expect(NativeAdapter.fromUrl("ferrics://cache.example.test:6389", {
      tlsOptions: { host: "wrong.example.test", port: 1 }
    })).rejects.toThrow("stop after capturing");
    await expect(NativeAdapter.fromUrl("ferrics://cache.example.test:6389", {
      tlsOptions: { servername: "certificate.example.test" }
    })).rejects.toThrow("stop after capturing");
    await expect(NativeAdapter.fromUrl("ferrics://127.0.0.1:6389")).rejects.toThrow("stop after capturing");

    expect(captures[0]).toMatchObject({
      host: "cache.example.test",
      port: 6389,
      servername: "cache.example.test"
    });
    expect(captures[1]).toMatchObject({
      host: "cache.example.test",
      port: 6389,
      servername: "certificate.example.test"
    });
    expect(captures[2]).toMatchObject({ host: "127.0.0.1", port: 6389 });
    expect(captures[2]).not.toHaveProperty("servername");
  } finally {
    connect.mockRestore();
  }
});

test("FerricStoreClient.fromUrl waits for initial startup with reconnect enabled", async () => {
  let releaseStartup: (() => void) | undefined;
  let startupSeen = false;
  const startupGate = new Promise<void>((resolve) => {
    releaseStartup = resolve;
  });
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode !== OPCODES.startup) return undefined;
      startupSeen = true;
      void startupGate.then(() => {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "OK"));
      });
      return NO_RESPONSE;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const opening = FerricStoreClient.fromUrl(`ferric://127.0.0.1:${address.port}`);
  let settled = false;
  void opening.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    }
  );

  await waitFor(() => startupSeen);
  const settledBeforeStartup = settled;
  releaseStartup?.();
  const client = await opening;
  await client.ping();
  await client.close();
  expect(settledBeforeStartup).toBe(false);
});

test("FerricStoreClient.fromUrls waits for initial topology with reconnect enabled", async () => {
  let releaseTopology: (() => void) | undefined;
  let topologySeen = false;
  let nativePort = 0;
  const topologyGate = new Promise<void>((resolve) => {
    releaseTopology = resolve;
  });
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode !== OPCODES.shards) return undefined;
      topologySeen = true;
      void topologyGate.then(() => {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, {
          ranges: [
            {
              endpoint: { host: "127.0.0.1", native_port: nativePort, node: "seed@local" },
              first_slot: 0,
              lane_id: 1,
              last_slot: 1023,
              shard: 0
            }
          ],
          route_epoch: 1,
          shard_count: 1
        }));
      });
      return NO_RESPONSE;
    },
    { fragmentResponses: false }
  );
  nativePort = (server.address() as AddressInfo).port;
  const opening = FerricStoreClient.fromUrls([`ferric://127.0.0.1:${nativePort}`]);
  let settled = false;
  void opening.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    }
  );

  await waitFor(() => topologySeen);
  const settledBeforeTopology = settled;
  releaseTopology?.();
  const client = await opening;
  await client.ping();
  await client.close();
  expect(settledBeforeTopology).toBe(false);
});

test("NativeAdapter pipelines compact-capable commands with typed bodies", async () => {
  const requests: TestRequest[] = [];
  const server = await startCountingServer(
    (request) => {
      requests.push(request);
      if (request.opcode === OPCODES.pipeline) {
        return [
          ["ok", [Buffer.from("value-1")]],
          ["ok", [Buffer.from("value-2")]]
        ];
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(
      adapter.executePipeline([
        ["MGET", "a", "b"],
        ["MGET", "c", "d"]
      ])
    ).resolves.toEqual([[Buffer.from("value-1")], [Buffer.from("value-2")]]);
    expect(requests.filter((request) => request.opcode === OPCODES.pipeline)).toHaveLength(1);
    const pipeline = requests.find((request) => request.opcode === OPCODES.pipeline)?.payload as {
      commands?: { body?: { keys?: unknown }; opcode?: unknown }[];
    };
    expect(pipeline.commands).toHaveLength(2);
    expect(pipeline.commands?.map((command) => command.opcode)).toEqual([
      OPCODES.mget,
      OPCODES.mget
    ]);
    expect(pipeline.commands?.map((command) => command.body?.keys)).toEqual([
      [Buffer.from("a"), Buffer.from("b")],
      [Buffer.from("c"), Buffer.from("d")]
    ]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter executes control-command pipeline items individually", async () => {
  const requests: TestRequest[] = [];
  const server = await startCountingServer((request) => {
    requests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["PING", "one"],
      ["PING", "two"]
    ])).resolves.toEqual([Buffer.from("PONG"), Buffer.from("PONG")]);
    expect(requests.filter((request) => request.opcode === OPCODES.pipeline)).toHaveLength(0);
    expect(requests.filter((request) => request.opcode === OPCODES.ping)).toHaveLength(2);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter correlates compact claim response modes with their requests", async () => {
  const sized = (value: string): Buffer => {
    const bytes = Buffer.from(value);
    const encoded = Buffer.allocUnsafe(4 + bytes.byteLength);
    encoded.writeUInt32BE(bytes.byteLength, 0);
    bytes.copy(encoded, 4);
    return encoded;
  };
  const count = Buffer.allocUnsafe(4);
  count.writeUInt32BE(1, 0);
  const fencing = Buffer.allocUnsafe(8);
  fencing.writeBigInt64BE(9n, 0);
  const stateResponseBody = Buffer.concat([
    Buffer.from([0, 0, 0x80]),
    count,
    sized("flow-1"),
    sized("p1"),
    sized("lease-token"),
    fencing,
    sized("running:step")
  ]);
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return {
        capabilities: {
          response_codecs: {
            compact_response_opcodes: { flow_claim_jobs_v1: [OPCODES.flowClaimDue] }
          }
        }
      };
    }
    if (request.opcode !== OPCODES.flowClaimDue) return undefined;
    socket.write(responseFrameFromBody(
      request.opcode,
      request.laneId,
      request.requestId,
      stateResponseBody,
      FLAG_CUSTOM_PAYLOAD
    ));
    return NO_RESPONSE;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);
  const claim = (returnMode: string): Promise<unknown> => adapter.executeCommand(
    "FLOW.CLAIM_DUE",
    "order",
    "STATE",
    "queued",
    "WORKER",
    "worker-1",
    "LEASE_MS",
    30_000,
    "LIMIT",
    1,
    "RETURN",
    returnMode
  );

  try {
    await expect(claim("JOBS_COMPACT")).rejects.toThrow("expected base");
    await expect(claim("JOBS_COMPACT_STATE")).resolves.toEqual([[
      Buffer.from("flow-1"),
      Buffer.from("p1"),
      Buffer.from("lease-token"),
      9,
      Buffer.from("running:step")
    ]]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter preserves blocking semantics outside native pipeline frames", async () => {
  let blockingFinished = false;
  const requestOrder: string[] = [];
  const server = await startCountingServer((request, socket) => {
    if (commandExecName(request) === "BLPOP") {
      requestOrder.push("BLPOP");
      setTimeout(() => {
        blockingFinished = true;
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, null));
      }, 10);
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.get) {
      requestOrder.push("GET");
      return Buffer.from(blockingFinished ? "after-block" : "before-block");
    }
    if (request.opcode === OPCODES.pipeline) {
      requestOrder.push("PIPELINE");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["BLPOP", "queue", 0.01],
      ["GET", "after"]
    ])).resolves.toEqual([null, Buffer.from("after-block")]);
    expect(requestOrder).toEqual(["BLPOP", "GET"]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter orders connection-state pipeline fallbacks before data commands", async () => {
  let authenticated = false;
  const requestOrder: string[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.auth) {
      requestOrder.push("AUTH");
      setImmediate(() => {
        authenticated = true;
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "OK"));
      });
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.get) {
      requestOrder.push("GET");
      return Buffer.from(authenticated ? "after-auth" : "before-auth");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["AUTH", "default", "secret"],
      ["GET", "protected-key"]
    ])).resolves.toEqual([Buffer.from("OK"), Buffer.from("after-auth")]);
    expect(requestOrder).toEqual(["AUTH", "GET"]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter orders wrapped connection-state fallbacks before data commands", async () => {
  let resetFinished = false;
  const requestOrder: string[] = [];
  const server = await startCountingServer((request, socket) => {
    if (commandExecName(request) === "RESET") {
      requestOrder.push("RESET");
      setImmediate(() => {
        resetFinished = true;
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "OK"));
      });
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.get) {
      requestOrder.push("GET");
      return Buffer.from(resetFinished ? "after-reset" : "before-reset");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["COMMAND_EXEC", "RESET"],
      ["GET", "key"]
    ])).resolves.toEqual([Buffer.from("OK"), Buffer.from("after-reset")]);
    expect(requestOrder).toEqual(["RESET", "GET"]);
  } finally {
    await adapter.close();
  }
});

test("concurrent individual pipeline fallback waits for every started command before rejecting", async () => {
  let releaseSlow: (() => void) | undefined;
  let slowFinished = false;
  const slowGate = new Promise<void>((resolve) => {
    releaseSlow = resolve;
  });
  const failure = new Error("first command failed");
  let rejected = false;
  const operation = executeCommandsIndividually(async (name) => {
    if (name === "FAIL") throw failure;
    await slowGate;
    slowFinished = true;
    return "slow result";
  }, [["FAIL"], ["SLOW"]]).catch((error: unknown) => {
    rejected = true;
    return error;
  });

  await new Promise((resolve) => setImmediate(resolve));
  expect(rejected).toBe(false);
  expect(slowFinished).toBe(false);

  releaseSlow?.();
  await expect(operation).resolves.toBe(failure);
  expect(slowFinished).toBe(true);
});

test("individual pipeline fallback preserves non-Error rejection reasons", async () => {
  const rejection = { code: "custom_rejection" };

  await expect(executeCommandsIndividually(
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- plain JS may reject with any value
    async () => await Promise.reject(rejection),
    [["CUSTOM"]]
  )).rejects.toBe(rejection);
});

test("NativeAdapter rejects connection-pinned commands without writing them", async () => {
  const requests: TestRequest[] = [];
  const server = await startCountingServer((request) => {
    requests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    const requestsAfterStartup = requests.length;
    await expect(adapter.executeCommand("MULTI")).rejects.toThrow(/MULTI.*pinned connection/i);
    await expect(adapter.executePipeline([
      ["MULTI"],
      ["SET", "transaction:key", "value"],
      ["EXEC"]
    ])).rejects.toThrow(/MULTI.*pinned connection/i);
    expect(requests).toHaveLength(requestsAfterStartup);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter rejects malformed pipeline response shapes", async () => {
  const server = await startCountingServer(
    (request) => request.opcode === OPCODES.pipeline ? "OK" : undefined,
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["GET", "a"],
      ["GET", "b"]
    ])).rejects.toThrow("invalid response");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter defaults to latency-first eight protocol lanes", async () => {
  const server = await startFragmentingServer();
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    expect((adapter as unknown as { protocolLanes: number }).protocolLanes).toBe(8);
    expect((adapter as unknown as { heartbeatIntervalMs: number }).heartbeatIntervalMs).toBe(60_000);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter caps automatic lanes to the STARTUP multiplexing limit", async () => {
  const lanes: number[] = [];
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { multiplexing: { max_lanes_per_connection: 2 } };
    }
    if (request.opcode === OPCODES.get) {
      lanes.push(request.laneId);
      return Buffer.from("value");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 8
  });

  try {
    for (let index = 0; index < 4; index += 1) {
      await adapter.executeCommand("GET", `key-${index}`);
    }
    expect(lanes).toEqual([1, 2, 1, 2]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter fallback honors auto-batch dependencies without serializing independent keys", async () => {
  let value = Buffer.from("before-set");
  let pendingSet: { request: TestRequest; socket: Socket } | undefined;
  let dependentReadSeen = false;
  let independentReadSeen = false;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_pipeline_commands: 0 } };
    }
    if (request.opcode === OPCODES.set) {
      pendingSet = { request, socket };
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.get) {
      const payload = request.payload as { readonly key?: unknown };
      const key = Buffer.isBuffer(payload.key) ? payload.key.toString("utf8") : String(payload.key);
      if (key === "dependent") {
        dependentReadSeen = true;
        return value;
      }
      independentReadSeen = true;
      return Buffer.from("independent");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);
  const client = new FerricStoreClient(adapter, { autoBatch: true });

  try {
    const resultsPromise = Promise.all([
      client.command("SET", "dependent", "after-set"),
      client.command("GET", "dependent"),
      client.command("GET", "independent")
    ]);
    await waitFor(() => pendingSet != null && independentReadSeen);
    expect(dependentReadSeen).toBe(false);

    const captured = pendingSet;
    if (captured == null) throw new Error("SET request was not captured");
    value = Buffer.from("after-set");
    captured.socket.write(responseFrame(
      captured.request.opcode,
      captured.request.laneId,
      captured.request.requestId,
      "OK"
    ));

    await expect(resultsPromise).resolves.toEqual([
      Buffer.from("OK"),
      Buffer.from("after-set"),
      Buffer.from("independent")
    ]);
  } finally {
    await client.close();
  }
});

test("NativeAdapter chunks pipelines to the STARTUP command limit", async () => {
  const pipelineSizes: number[] = [];
  let responseIndex = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_pipeline_commands: 2 } };
    }
    if (request.opcode === OPCODES.pipeline) {
      const payload = request.payload as { readonly commands?: readonly unknown[] };
      const size = payload.commands?.length ?? 0;
      pipelineSizes.push(size);
      return Array.from({ length: size }, () => Buffer.from(`value-${responseIndex++}`));
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    const commands = Array.from({ length: 5 }, (_, index) => ["CUSTOM.GET", `key-${index}`] as const);
    await expect(adapter.executePipeline(commands)).resolves.toEqual(
      Array.from({ length: 5 }, (_, index) => Buffer.from(`value-${index}`))
    );
    expect(pipelineSizes).toEqual([2, 2, 1]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter declines fused execution before sending when a pipeline requires chunks", async () => {
  let pipelineRequests = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_pipeline_commands: 1 } };
    }
    if (request.opcode === OPCODES.pipeline) pipelineRequests += 1;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executeFusedPipeline([
      ["GET", "first"],
      ["GET", "second"]
    ])).resolves.toBeUndefined();
    expect(pipelineRequests).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter declines an oversized fused pipeline before sending", async () => {
  let pipelineRequests = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_frame_bytes: 64, max_pipeline_commands: 1_024 } };
    }
    if (request.opcode === OPCODES.pipeline) pipelineRequests += 1;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executeFusedPipeline([
      ["SET", "key", Buffer.alloc(256)]
    ])).resolves.toBeUndefined();
    expect(pipelineRequests).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter sends every pipeline chunk before surfacing an item error", async () => {
  let pipelineRequests = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_pipeline_commands: 1 } };
    }
    if (request.opcode === OPCODES.pipeline) {
      pipelineRequests += 1;
      return pipelineRequests === 1
        ? [[Buffer.from("error"), Buffer.from("ERR first failed")]]
        : [Buffer.from("second-result")];
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["CUSTOM.GET", "first"],
      ["CUSTOM.GET", "second"]
    ])).rejects.toThrow("ERR first failed");
    expect(pipelineRequests).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter rejects outbound frames above the STARTUP frame limit", async () => {
  let setSeen = false;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_frame_bytes: 64 } };
    }
    if (request.opcode === OPCODES.set) {
      setSeen = true;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executeCommand("SET", "key", Buffer.alloc(256))).rejects.toThrow(
      "server-advertised 64-byte frame limit"
    );
    expect(setSeen).toBe(false);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter splits pipelines whose commands fit individually but not together", async () => {
  let pipelineRequests = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return { limits: { max_frame_bytes: 148, max_pipeline_commands: 1_024 } };
    }
    if (request.opcode === OPCODES.pipeline) {
      pipelineRequests += 1;
      return [Buffer.from("OK")];
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executePipeline([
      ["SET", "a", Buffer.alloc(80)],
      ["SET", "b", Buffer.alloc(80)]
    ])).resolves.toEqual([Buffer.from("OK"), Buffer.from("OK")]);
    expect(pipelineRequests).toBe(2);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter falls back safely for non-finite scheduling options", async () => {
  const server = await startFragmentingServer();
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: Number.NaN,
    protocolLanes: Number.NaN
  });

  try {
    expect((adapter as unknown as { protocolLanes: number }).protocolLanes).toBe(8);
    expect((adapter as unknown as { heartbeatIntervalMs: number }).heartbeatIntervalMs).toBe(60_000);
  } finally {
    await adapter.close();
  }
});

test("ReconnectingExecutor bounds non-finite retry configuration", async () => {
  const executor = new ReconnectingExecutor(async () => ({
    async executeCommand(): Promise<unknown> {
      return "OK";
    }
  }), { maxRetries: Number.NaN });

  try {
    expect((executor as unknown as { maxRetries: number }).maxRetries).toBe(1);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor rejects connection-local mutations before dispatch", async () => {
  const calls: Command[] = [];
  const executor = new ReconnectingExecutor(async () => ({
    async executeCommand(...args): Promise<unknown> {
      calls.push(args.slice());
      return Buffer.from("OK");
    },
    async executePipeline(commands): Promise<unknown[]> {
      calls.push(...commands.map((command) => command.slice()));
      return commands.map(() => Buffer.from("OK"));
    }
  }));
  await executor.ready();

  try {
    await expect(executor.executeCommand("AUTH", "default", "secret")).rejects.toThrow(
      /AUTH.*stable single connection/i
    );
    await expect(executor.executeCommand("CLIENT", "SETNAME", "worker-1")).rejects.toThrow(
      /CLIENT SETNAME.*stable single connection/i
    );
    await expect(executor.executeCommand("QUIT")).rejects.toThrow(
      /QUIT.*stable single connection/i
    );
    await expect(executor.executePipeline([
      ["PING"],
      ["RESET"]
    ])).rejects.toThrow(/RESET.*stable single connection/i);
    expect(calls).toEqual([]);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor reconnects but never replays an uncertain native pipeline", async () => {
  let createCount = 0;
  let durableEffects = 0;
  const executor = new ReconnectingExecutor(async () => {
    createCount += 1;
    if (createCount === 1) {
      return {
        async executeCommand(): Promise<unknown> {
          return "OK";
        },
        async executePipeline(): Promise<unknown[]> {
          durableEffects += 1;
          throw new ConnectionClosedError("unsent");
        }
      };
    }
    return {
      async executeCommand(): Promise<unknown> {
        return "OK";
      },
      async executePipeline(): Promise<unknown[]> {
        durableEffects += 1;
        return [Buffer.from("OK")];
      }
    };
  });

  try {
    await expect(executor.executePipeline([["INCR", "counter"]])).rejects.toThrow(
      "FerricStore connection is closed"
    );
    expect(createCount).toBe(2);
    expect(durableEffects).toBe(1);

    await expect(executor.executePipeline([["INCR", "counter"]])).resolves.toEqual([Buffer.from("OK")]);
    expect(durableEffects).toBe(2);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor rejects pipelines after close without dispatch", async () => {
  let pipelineCalls = 0;
  const executor = new ReconnectingExecutor(async () => ({
    async executeCommand(): Promise<unknown> {
      return "OK";
    },
    async executePipeline(): Promise<unknown[]> {
      pipelineCalls += 1;
      return [Buffer.from("OK")];
    }
  }));
  await executor.ready();
  await executor.close();

  await expect(executor.executePipeline([["GET", "key"]])).rejects.toThrow("client is closed");
  expect(pipelineCalls).toBe(0);
});

test("ReconnectingExecutor retries a read-only topology refresh after an idle close", async () => {
  let createCount = 0;
  const expected = RoutingTopology.build(validTopologyPayload());
  const executor = new ReconnectingExecutor(async () => {
    createCount += 1;
    return {
      async executeCommand(): Promise<unknown> {
        return "OK";
      },
      async refreshTopology(): Promise<RoutingTopology> {
        if (createCount === 1) throw new ConnectionClosedError("unsent");
        return expected;
      }
    };
  });

  try {
    await expect(executor.refreshTopology()).resolves.toBe(expected);
    expect(createCount).toBe(2);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor retries a read-only topology refresh after a wrapped idle close", async () => {
  let createCount = 0;
  const expected = RoutingTopology.build(validTopologyPayload());
  const executor = new ReconnectingExecutor(async () => {
    const generation = ++createCount;
    return {
      async executeCommand(): Promise<unknown> {
        return "OK";
      },
      async refreshTopology(): Promise<RoutingTopology> {
        if (generation === 1) {
          throw new FerricStoreError("no FerricStore topology endpoint reachable", {
            raw: new ConnectionClosedError("unsent")
          });
        }
        return expected;
      }
    };
  });

  try {
    await expect(executor.refreshTopology()).resolves.toBe(expected);
    expect(createCount).toBe(2);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor retries a read-only route lookup after an idle close", async () => {
  let createCount = 0;
  const expected = RoutingTopology.build(validTopologyPayload()).routeKey("tenant-key");
  const executor = new ReconnectingExecutor(async () => {
    createCount += 1;
    return {
      async executeCommand(): Promise<unknown> {
        return "OK";
      },
      route(): RoutingRoute {
        if (createCount === 1) throw new ConnectionClosedError("unsent");
        return expected;
      }
    };
  });

  try {
    await expect(executor.route("tenant-key")).resolves.toBe(expected);
    expect(createCount).toBe(2);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor uses the remaining retry budget after a transient redial failure", async () => {
  let createCount = 0;
  let operationCalls = 0;
  const timeout = vi.spyOn(globalThis, "setTimeout");
  const executor = new ReconnectingExecutor(async () => {
    createCount += 1;
    if (createCount === 2) throw new Error("transient dial failure");
    return {
      async executeCommand(): Promise<unknown> {
        operationCalls += 1;
        if (createCount === 1) throw new ConnectionClosedError("unsent");
        return Buffer.from("OK");
      }
    };
  }, {
    baseDelayMs: 5,
    jitterPct: 0,
    maxDelayMs: 5,
    maxRetries: 2
  });

  try {
    await expect(executor.executeCommand("SET", "key", "value")).resolves.toEqual(Buffer.from("OK"));
    expect(createCount).toBe(3);
    expect(operationCalls).toBe(2);
    expect(timeout.mock.calls.some((call) => call[1] === 5)).toBe(true);
  } finally {
    timeout.mockRestore();
    await executor.close();
  }
});

test("ReconnectingExecutor preserves per-item errors for command-only pipeline fallbacks", async () => {
  const executor = new ReconnectingExecutor(async () => ({
    async executeCommand(...args): Promise<unknown> {
      if (args[1] === "bad") throw new Error("ERR item failed");
      return Buffer.from("OK");
    }
  }));

  try {
    const results = await executor.executePipeline(
      [
        ["SET", "good", "1"],
        ["SET", "bad", "2"]
      ],
      { throwOnItemError: false }
    );
    expect(results[0]).toEqual(Buffer.from("OK"));
    expect(results[1]).toBeInstanceOf(Error);
  } finally {
    await executor.close();
  }
});
