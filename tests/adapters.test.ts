import net from "node:net";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, expect, test } from "vitest";
import { NativeAdapter, ReconnectingExecutor } from "../src/adapters.js";
import { HEADER_SIZE, MAGIC, OPCODES, RESPONSE_VERSION, decodeValue, encodeValue } from "../src/protocol.js";
import { RoutingTopology, TopologyNativeAdapterPool } from "../src/topology.js";

const servers: net.Server[] = [];

afterEach(async () => {
  await Promise.all(servers.map((server) => closeServer(server)));
  servers.length = 0;
});

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

test("NativeAdapter cleans chunk buffers when a chunked request times out", async () => {
  const server = await startFragmentingServer({ chunkOnlyPing: true });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 4,
    timeoutMs: 20
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("timed out");
    expect((adapter as unknown as { chunks: Map<string, Buffer[]> }).chunks.size).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter rejects oversized chunked responses", async () => {
  const server = await startFragmentingServer({ chunkOnlyPing: true });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxChunkBytes: 4,
    protocolLanes: 4,
    timeoutMs: 100
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("chunked response exceeded");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter can keep idle sockets active with heartbeat pings", async () => {
  let pingCount = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.ping) {
      pingCount++;
    }
  });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 10
  });

  try {
    await waitFor(() => pingCount > 0);
    expect(pingCount).toBeGreaterThan(0);
  } finally {
    await adapter.close();
  }
});

test("ReconnectingExecutor reconnects when the native adapter was closed while idle", async () => {
  const server = await startStartupClosingServer();
  const address = server.address() as AddressInfo;
  const executor = new ReconnectingExecutor(async () => await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`));

  try {
    await waitFor(() => server.connectionCount > 0);
    const response = await executor.executeCommand("PING");
    expect(Buffer.isBuffer(response)).toBe(true);
    expect((response as Buffer).toString("utf8")).toBe("PONG");
    expect(server.connectionCount).toBeGreaterThan(1);
  } finally {
    await executor.close();
  }
});

test("RoutingTopology builds 1024-slot hash-tag aware routes", () => {
  const topology = RoutingTopology.build({
    route_epoch: 7,
    shard_count: 2,
    ranges: [
      {
        endpoint: { host: "node-a.local", native_port: 6388, node: "a@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 511,
        shard: 0
      },
      {
        endpoint: { host: "node-b.local", native_port: 6389, node: "b@cluster" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      }
    ]
  });

  const shard0Key = Array.from({ length: 10_000 }, (_, index) => `slot-a-${index}`).find(
    (key) => RoutingTopology.slotForKey(key) < 512
  );
  const shard1Key = Array.from({ length: 10_000 }, (_, index) => `slot-b-${index}`).find(
    (key) => RoutingTopology.slotForKey(key) >= 512
  );

  expect(shard0Key).toBeDefined();
  expect(shard1Key).toBeDefined();
  expect(topology.routeEpoch).toBe(7);
  expect(RoutingTopology.slotForKey("{tenant}:one")).toBe(RoutingTopology.slotForKey("{tenant}:two"));
  expect(topology.routeKey(shard0Key ?? "slot-a-0")).toMatchObject({ shard: 0, endpoint: { host: "node-a.local" } });
  expect(topology.routeKey(shard1Key ?? "slot-b-0")).toMatchObject({ laneId: 2, endpointKey: "node-b.local:6389" });
});

test("TopologyNativeAdapterPool routes keyed commands to learned shard leader lane", async () => {
  const leaderRequests: TestRequest[] = [];
  const leader = await startCountingServer((request) => {
    leaderRequests.push(request);
    return undefined;
  });
  const leaderAddress = leader.address() as AddressInfo;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: {
            host: "127.0.0.1",
            native_port: leaderAddress.port,
            node: "leader@local"
          },
          first_slot: 0,
          lane_id: 3,
          last_slot: 1023,
          shard: 0
        }
      ],
      route_epoch: 1,
      shard_count: 1
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    await pool.executeCommand("SET", "tenant-key", "value");

    const routedSet = leaderRequests.find((request) => request.opcode === OPCODES.set);
    expect(routedSet).toMatchObject({ laneId: 3, opcode: OPCODES.set });
    expect(pool.route("tenant-key")).toMatchObject({
      endpoint: { host: "127.0.0.1", nativePort: leaderAddress.port },
      leaderNode: "leader@local"
    });
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool keeps unpartitioned Flow queries on the control path", async () => {
  const leaderRequests: TestRequest[] = [];
  const leader = await startCountingServer((request) => {
    leaderRequests.push(request);
    return [];
  });
  const leaderAddress = leader.address() as AddressInfo;
  const seedRequests: TestRequest[] = [];
  const seed = await startCountingServer((request) => {
    seedRequests.push(request);
    if (request.opcode !== OPCODES.shards) return [];
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: leaderAddress.port, node: "leader@local" },
          first_slot: 0,
          lane_id: 4,
          last_slot: 1023,
          shard: 0
        }
      ],
      route_epoch: 1,
      shard_count: 1
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    await pool.executeCommand("FLOW.SEARCH", "order");
    await pool.executeCommand("FLOW.SEARCH", "order", "PARTITION", "tenant-a");

    expect(seedRequests.some((request) => request.opcode === OPCODES.flowSearch)).toBe(true);
    expect(leaderRequests.some((request) => request.opcode === OPCODES.flowSearch && request.laneId === 4)).toBe(true);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool routes command_exec commands by real key positions only", async () => {
  const operationSlot = RoutingTopology.slotForKey("OR");
  const streamTokenSlot = RoutingTopology.slotForKey("STREAMS");
  const bitopTag = keyForSlot((slot) => (operationSlot < 512 ? slot >= 512 : slot < 512), "bitmap");
  const streamTag = keyForSlot((slot) => (streamTokenSlot < 512 ? slot >= 512 : slot < 512), "stream");
  const bitopSlot = RoutingTopology.slotForKey(bitopTag);
  const streamSlot = RoutingTopology.slotForKey(streamTag);
  const leftKey = keyForSlot((slot) => slot < 512, "rename-left");
  const rightKey = keyForSlot((slot) => slot >= 512, "rename-right");

  const lowRequests: TestRequest[] = [];
  const low = await startCountingServer((request) => {
    lowRequests.push(request);
    return "OK";
  });
  const lowAddress = low.address() as AddressInfo;
  const highRequests: TestRequest[] = [];
  const high = await startCountingServer((request) => {
    highRequests.push(request);
    return "OK";
  });
  const highAddress = high.address() as AddressInfo;
  const seedRequests: TestRequest[] = [];
  const seed = await startCountingServer((request) => {
    seedRequests.push(request);
    if (request.opcode !== OPCODES.shards) return "OK";
    return twoShardTopology(lowAddress.port, highAddress.port);
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    endpointPolicy: "any"
  });

  try {
    await pool.executeCommand("BITOP", "OR", `{${bitopTag}}:out`, `{${bitopTag}}:in`);
    await pool.executeCommand("XREAD", "STREAMS", `{${streamTag}}:events`, "0-0");
    await pool.executeCommand("RENAME", leftKey, rightKey);

    const bitopTarget = bitopSlot < 512 ? lowRequests : highRequests;
    const bitopWrong = bitopSlot < 512 ? highRequests : lowRequests;
    expect(bitopTarget.some((request) => commandExecName(request) === "BITOP")).toBe(true);
    expect(bitopWrong.some((request) => commandExecName(request) === "BITOP")).toBe(false);

    const streamTarget = streamSlot < 512 ? lowRequests : highRequests;
    const streamWrong = streamSlot < 512 ? highRequests : lowRequests;
    expect(streamTarget.some((request) => commandExecName(request) === "XREAD")).toBe(true);
    expect(streamWrong.some((request) => commandExecName(request) === "XREAD")).toBe(false);

    expect(seedRequests.filter((request) => commandExecName(request) === "RENAME")).toHaveLength(1);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects untrusted learned endpoints by default", async () => {
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: { host: "other.local", native_port: 6388, node: "other@cluster" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`]);

  try {
    await expect(pool.executeCommand("GET", "tenant-key")).rejects.toThrow("unsafe learned endpoint");
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects learned seed host ports unless explicitly trusted", async () => {
  const learned = await startCountingServer(() => "OK");
  const learnedAddress = learned.address() as AddressInfo;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return "OK";
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: learnedAddress.port, node: "other-port@local" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const seedUrl = `ferric://127.0.0.1:${seedAddress.port}`;
  const strictPool = await TopologyNativeAdapterPool.fromUrls([seedUrl]);

  try {
    await expect(strictPool.executeCommand("GET", "tenant-key")).rejects.toThrow("unsafe learned endpoint");
  } finally {
    await strictPool.close();
  }

  const trustedPool = await TopologyNativeAdapterPool.fromUrls([seedUrl], { trustedHosts: ["127.0.0.1"] });
  try {
    await expect(trustedPool.executeCommand("GET", "tenant-key")).resolves.toBeDefined();
  } finally {
    await trustedPool.close();
  }
});

async function startFragmentingServer(options: { chunkOnlyPing?: boolean } = {}): Promise<net.Server> {
  const server = net.createServer((socket) => handleSocket(socket, options));
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  return server;
}

function twoShardTopology(lowPort: number, highPort: number): unknown {
  return {
    ranges: [
      {
        endpoint: { host: "127.0.0.1", native_port: lowPort, node: "low@local" },
        first_slot: 0,
        lane_id: 2,
        last_slot: 511,
        shard: 0
      },
      {
        endpoint: { host: "127.0.0.1", native_port: highPort, node: "high@local" },
        first_slot: 512,
        lane_id: 3,
        last_slot: 1023,
        shard: 1
      }
    ],
    route_epoch: 1,
    shard_count: 2
  };
}

function keyForSlot(predicate: (slot: number) => boolean, prefix: string): string {
  const key = Array.from({ length: 20_000 }, (_, index) => `${prefix}-${index}`).find((candidate) =>
    predicate(RoutingTopology.slotForKey(candidate))
  );
  if (key == null) {
    throw new Error(`no key found for ${prefix}`);
  }
  return key;
}

async function startCountingServer(onRequest: (request: TestRequest) => void): Promise<net.Server> {
  const server = net.createServer((socket) => handleSocket(socket, { onRequest }));
  servers.push(server);
  await listen(server);
  return server;
}

async function startStartupClosingServer(): Promise<net.Server & { connectionCount: number }> {
  const server = net.createServer() as net.Server & {
    connectionCount: number;
  };
  server.on("connection", (socket) => handleStartupClosingSocket(socket, server));
  server.connectionCount = 0;
  servers.push(server);
  await listen(server);
  return server;
}

async function listen(server: net.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

interface TestRequest {
  readonly laneId: number;
  readonly opcode: number;
  readonly payload: unknown;
  readonly requestId: bigint;
}

function handleSocket(
  socket: Socket,
  options: { chunkOnlyPing?: boolean; onRequest?: (request: TestRequest) => unknown }
): void {
  let input: Buffer = Buffer.alloc(0);
  socket.on("data", (chunk: Buffer) => {
    input = Buffer.concat([input, chunk]);
    for (;;) {
      const request = readRequest(input);
      if (request == null) {
        return;
      }
      const override = options.onRequest?.(request);
      input = request.rest;
      const value = override ?? (request.opcode === OPCODES.ping ? "PONG" : "OK");
      if (options.chunkOnlyPing === true && request.opcode === OPCODES.ping) {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "partial-response", 0x20));
      } else {
        writeFragmented(socket, responseFrame(request.opcode, request.laneId, request.requestId, value));
      }
    }
  });
}

function handleStartupClosingSocket(socket: Socket, server: net.Server & { connectionCount: number }): void {
  server.connectionCount++;
  const connectionNumber = server.connectionCount;
  let input: Buffer = Buffer.alloc(0);
  socket.on("data", (chunk: Buffer) => {
    input = Buffer.concat([input, chunk]);
    for (;;) {
      const request = readRequest(input);
      if (request == null) {
        return;
      }
      input = request.rest;
      const frame = responseFrame(request.opcode, request.laneId, request.requestId, request.opcode === OPCODES.ping ? "PONG" : "OK");
      if (connectionNumber === 1 && request.opcode === OPCODES.startup) {
        socket.write(frame, () => socket.end());
      } else {
        socket.write(frame);
      }
    }
  });
}

function readRequest(input: Buffer): (TestRequest & { readonly rest: Buffer }) | null {
  if (input.byteLength < HEADER_SIZE) {
    return null;
  }
  if (input.toString("ascii", 0, 4) !== MAGIC) {
    throw new Error("bad test request magic");
  }
  const bodyLength = input.readUInt32BE(20);
  const frameLength = HEADER_SIZE + bodyLength;
  if (input.byteLength < frameLength) {
    return null;
  }
  return {
    laneId: input.readUInt32BE(6),
    opcode: input.readUInt16BE(10),
    payload: decodeValue(input.subarray(HEADER_SIZE, frameLength)).value,
    requestId: input.readBigUInt64BE(12),
    rest: input.subarray(frameLength)
  };
}

function commandExecName(request: TestRequest): string | undefined {
  if (request.opcode !== OPCODES.commandExec) {
    return undefined;
  }
  const payload = request.payload;
  if (payload instanceof Map) {
    const command = (payload as Map<unknown, unknown>).get("command");
    return typeof command === "string" ? command : Buffer.isBuffer(command) ? command.toString("utf8") : undefined;
  }
  if (typeof payload === "object" && payload != null && "command" in payload) {
    const command = (payload as { readonly command?: unknown }).command;
    return typeof command === "string" ? command : Buffer.isBuffer(command) ? command.toString("utf8") : undefined;
  }
  return undefined;
}

function responseFrame(opcode: number, laneId: number, requestId: bigint, value: unknown, flags = 0): Buffer {
  const encoded = encodeValue(value);
  const body = Buffer.allocUnsafe(2 + encoded.byteLength);
  body.writeUInt16BE(0, 0);
  encoded.copy(body, 2);

  const frame = Buffer.allocUnsafe(HEADER_SIZE + body.byteLength);
  frame.write(MAGIC, 0, "ascii");
  frame.writeUInt8(RESPONSE_VERSION, 4);
  frame.writeUInt8(flags, 5);
  frame.writeUInt32BE(laneId, 6);
  frame.writeUInt16BE(opcode, 10);
  frame.writeBigUInt64BE(requestId, 12);
  frame.writeUInt32BE(body.byteLength, 20);
  body.copy(frame, HEADER_SIZE);
  return frame;
}

function writeFragmented(socket: Socket, frame: Buffer): void {
  socket.write(frame.subarray(0, 3));
  socket.write(frame.subarray(3, 17));
  setImmediate(() => {
    socket.write(frame.subarray(17, HEADER_SIZE + 1));
    socket.write(frame.subarray(HEADER_SIZE + 1));
  });
}

async function closeServer(server: net.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error != null) reject(error);
      else resolve();
    });
  });
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 1_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("condition was not met before timeout");
}
