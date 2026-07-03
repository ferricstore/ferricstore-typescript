import net from "node:net";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, expect, test } from "vitest";
import { NativeAdapter, ReconnectingExecutor } from "../src/adapters.js";
import { HEADER_SIZE, MAGIC, OPCODES, RESPONSE_VERSION, encodeValue } from "../src/protocol.js";

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
  readonly requestId: bigint;
}

function handleSocket(socket: Socket, options: { chunkOnlyPing?: boolean; onRequest?: (request: TestRequest) => void }): void {
  let input: Buffer = Buffer.alloc(0);
  socket.on("data", (chunk: Buffer) => {
    input = Buffer.concat([input, chunk]);
    for (;;) {
      const request = readRequest(input);
      if (request == null) {
        return;
      }
      options.onRequest?.(request);
      input = request.rest;
      const value = request.opcode === OPCODES.ping ? "PONG" : "OK";
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
    requestId: input.readBigUInt64BE(12),
    rest: input.subarray(frameLength)
  };
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
