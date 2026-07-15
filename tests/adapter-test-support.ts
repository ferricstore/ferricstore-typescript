import net from "node:net";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import type { Socket } from "node:net";
import { afterEach } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import { RoutingTopology } from "../src/topology.js";
import {
  HEADER_SIZE,
  MAGIC,
  OPCODES,
  RESPONSE_VERSION,
  decodeValue,
  encodeValue
} from "../src/protocol.js";

export const servers: net.Server[] = [];
export const NO_RESPONSE = Symbol("NO_RESPONSE");

export function chunkAssemblerInternals(adapter: NativeAdapter): {
  assembler: NativeChunkAssembler;
  chunks: Map<string, Buffer[]>;
  keysByRequest: Map<bigint, Set<string>>;
} {
  const assembler = (adapter as unknown as { chunkAssembler: NativeChunkAssembler }).chunkAssembler;
  const internals = assembler as unknown as {
    chunks: Map<string, Buffer[]>;
    keysByRequest: Map<bigint, Set<string>>;
  };
  return { assembler, chunks: internals.chunks, keysByRequest: internals.keysByRequest };
}

afterEach(async () => {
  await Promise.all(servers.map((server) => closeServer(server)));
  servers.length = 0;
});

export async function startFragmentingServer(options: { chunkOnlyPing?: boolean } = {}): Promise<net.Server> {
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

export function twoShardTopology(lowPort: number, highPort: number): unknown {
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

export function keyForSlot(predicate: (slot: number) => boolean, prefix: string): string {
  const key = Array.from({ length: 20_000 }, (_, index) => `${prefix}-${index}`).find((candidate) =>
    predicate(RoutingTopology.slotForKey(candidate))
  );
  if (key == null) {
    throw new Error(`no key found for ${prefix}`);
  }
  return key;
}

export function flowPartitionForSlot(predicate: (slot: number) => boolean, prefix: string): string {
  const partition = Array.from({ length: 20_000 }, (_, index) => `${prefix}-${index}`).find((candidate) => {
    const digest = createHash("sha256").update(candidate).digest("base64url");
    return predicate(RoutingTopology.slotForKey(`f:{f:${digest}}:route`));
  });
  if (partition == null) {
    throw new Error(`no Flow partition found for ${prefix}`);
  }
  return partition;
}

export async function startCountingServer(
  onRequest: (request: TestRequest, socket: Socket) => unknown,
  options: { fragmentResponses?: boolean; host?: string } = {}
): Promise<net.Server> {
  const server = net.createServer((socket) => handleSocket(socket, {
    fragmentResponses: options.fragmentResponses,
    onRequest
  }));
  servers.push(server);
  await listen(server, options.host);
  return server;
}

export async function startStartupClosingServer(): Promise<net.Server & { connectionCount: number }> {
  const server = net.createServer() as net.Server & {
    connectionCount: number;
  };
  server.on("connection", (socket) => handleStartupClosingSocket(socket, server));
  server.connectionCount = 0;
  servers.push(server);
  await listen(server);
  return server;
}

export async function listen(server: net.Server, host = "127.0.0.1"): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

export interface TestRequest {
  readonly flags: number;
  readonly laneId: number;
  readonly opcode: number;
  readonly payload: unknown;
  readonly requestId: bigint;
}

export function handleSocket(
  socket: Socket,
  options: {
    chunkOnlyPing?: boolean;
    fragmentResponses?: boolean;
    onRequest?: (request: TestRequest, socket: Socket) => unknown;
  }
): void {
  let input: Buffer = Buffer.alloc(0);
  socket.on("error", () => undefined);
  socket.on("data", (chunk: Buffer) => {
    input = Buffer.concat([input, chunk]);
    for (;;) {
      const request = readRequest(input);
      if (request == null) {
        return;
      }
      const override = options.onRequest?.(request, socket);
      input = request.rest;
      if (override === NO_RESPONSE) {
        continue;
      }
      const value = override ?? (request.opcode === OPCODES.ping ? "PONG" : "OK");
      if (options.chunkOnlyPing === true && request.opcode === OPCODES.ping) {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "partial-response", 0x20));
      } else if (options.fragmentResponses === false) {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, value));
      } else {
        writeFragmented(socket, responseFrame(request.opcode, request.laneId, request.requestId, value));
      }
    }
  });
}

export function handleStartupClosingSocket(socket: Socket, server: net.Server & { connectionCount: number }): void {
  server.connectionCount++;
  socket.on("error", () => undefined);
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

export function readRequest(input: Buffer): (TestRequest & { readonly rest: Buffer }) | null {
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
    flags: input.readUInt8(5),
    laneId: input.readUInt32BE(6),
    opcode: input.readUInt16BE(10),
    payload:
      (input.readUInt8(5) & 0x02) !== 0
        ? input.subarray(HEADER_SIZE, frameLength)
        : decodeValue(input.subarray(HEADER_SIZE, frameLength)).value,
    requestId: input.readBigUInt64BE(12),
    rest: input.subarray(frameLength)
  };
}

export function commandExecName(request: TestRequest): string | undefined {
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

export function responseFrame(opcode: number, laneId: number, requestId: bigint, value: unknown, flags = 0): Buffer {
  return responseFrameFromBody(opcode, laneId, requestId, encodedResponseBody(value), flags);
}

export function encodedResponseBody(value: unknown): Buffer {
  const encoded = encodeValue(value);
  const body = Buffer.allocUnsafe(2 + encoded.byteLength);
  body.writeUInt16BE(0, 0);
  encoded.copy(body, 2);
  return body;
}

export function responseFrameFromBody(
  opcode: number,
  laneId: number,
  requestId: bigint,
  body: Buffer,
  flags = 0
): Buffer {
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

export function writeFragmented(socket: Socket, frame: Buffer): void {
  socket.write(frame.subarray(0, 3));
  socket.write(frame.subarray(3, 17));
  setImmediate(() => {
    socket.write(frame.subarray(17, HEADER_SIZE + 1));
    socket.write(frame.subarray(HEADER_SIZE + 1));
  });
}

export function writeIncrementalFragments(socket: Socket, frame: Buffer, chunkBytes: number): void {
  let offset = 0;
  const writeNext = (): void => {
    const end = Math.min(frame.byteLength, offset + chunkBytes);
    socket.write(frame.subarray(offset, end));
    offset = end;
    if (offset < frame.byteLength) setImmediate(writeNext);
  };
  writeNext();
}

export function validTopologyRange(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    endpoint: { host: "node-a.local", native_port: 6388, node: "a@cluster" },
    first_slot: 0,
    lane_id: 1,
    last_slot: 1023,
    shard: 0,
    ...overrides
  };
}

export function validTopologyPayload(
  rangeOverrides: Record<string, unknown> = {},
  payloadOverrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    ranges: [validTopologyRange(rangeOverrides)],
    route_epoch: 1,
    shard_count: 1,
    slots: 1024,
    ...payloadOverrides
  };
}

export class BackpressureSocket extends EventEmitter {
  readonly writes: Buffer[] = [];

  write(frame: Buffer): boolean {
    this.writes.push(frame);
    return this.writes.length !== 1;
  }

  end(): this {
    this.emit("close");
    return this;
  }

  destroy(): this {
    this.emit("close");
    return this;
  }
}

export function directNativeAdapter(
  socket: BackpressureSocket,
  maxQueuedWriteBytes: number,
  timeoutMs = 20,
  maxChunkBytes = 64 * 1024 * 1024
): NativeAdapter {
  const AdapterConstructor = NativeAdapter as unknown as new (
    socket: Socket,
    timeoutMs: number,
    protocolLanes: number,
    maxChunkBytes: number,
    maxChunkFrames: number,
    maxFrameBytes: number,
    maxResponseBytes: number,
    maxPendingControlRequests: number,
    maxQueuedRequests: number,
    heartbeatIntervalMs: number,
    onEvent: undefined,
    maxQueuedWriteBytes: number
  ) => NativeAdapter;
  return new AdapterConstructor(
    socket as unknown as Socket,
    timeoutMs,
    1,
    maxChunkBytes,
    65_536,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
    4_096,
    65_536,
    0,
    undefined,
    maxQueuedWriteBytes
  );
}

export async function closeServer(server: net.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error != null) reject(error);
      else resolve();
    });
  });
}

export async function activeConnections(server: net.Server): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    server.getConnections((error, count) => {
      if (error != null) reject(error);
      else resolve(count);
    });
  });
}

export async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 1_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("condition was not met before timeout");
}
