import net from "node:net";
import tls from "node:tls";
import { Buffer } from "node:buffer";
import { FerricStoreError, classifyServerError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import {
  FLAG_MORE_CHUNKS,
  OPCODES,
  buildProtocolCommand,
  decodeResponse,
  encodeRequest,
  pipelineCommand,
  tryDecodeFrame,
  unwrapPipelineResponse,
  type ProtocolCommand,
  type ResponseFrame
} from "./protocol.js";

export interface CommandExecutor {
  executeCommand(...args: CommandArgument[]): Promise<unknown>;
  executePipeline?(commands: readonly Command[], options?: ExecutePipelineOptions): Promise<unknown[]>;
  close?(): Promise<void> | void;
}

export interface ExecutePipelineOptions {
  throwOnItemError?: boolean;
}

export interface NativeAdapterOptions {
  autoReconnect?: boolean | ReconnectOptions;
  clientName?: string;
  connectTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  keepAlive?: boolean;
  keepAliveInitialDelayMs?: number;
  maxChunkBytes?: number;
  protocolLanes?: number;
  timeoutMs?: number;
  username?: string;
  password?: string;
  tlsOptions?: tls.ConnectionOptions;
}

export interface ReconnectOptions {
  maxRetries?: number;
}

interface PendingRequest {
  readonly opcode: number;
  readonly reject: (reason: unknown) => void;
  readonly resolve: (value: unknown) => void;
}

interface ParsedUrl {
  readonly host: string;
  readonly password?: string;
  readonly port: number;
  readonly tls: boolean;
  readonly username?: string;
}

export class NativeAdapter implements CommandExecutor {
  private readonly socket: net.Socket | tls.TLSSocket;
  private readonly pending = new Map<bigint, PendingRequest>();
  private readonly chunks = new Map<string, Buffer[]>();
  private readonly chunkBytes = new Map<string, number>();
  private buffer: Buffer = Buffer.alloc(0);
  private closed = false;
  private dataLane = 0;
  private readonly maxChunkBytes: number;
  private requestId = 0n;
  private readonly protocolLanes: number;
  private readonly heartbeatIntervalMs?: number;
  private heartbeatInFlight = false;
  private heartbeatTimer?: NodeJS.Timeout;
  private lastActivityMs = Date.now();
  private readonly timeoutMs: number;

  private constructor(
    socket: net.Socket | tls.TLSSocket,
    timeoutMs: number,
    protocolLanes: number,
    maxChunkBytes: number,
    heartbeatIntervalMs?: number
  ) {
    this.socket = socket;
    this.maxChunkBytes = Math.max(1, Math.trunc(maxChunkBytes));
    this.timeoutMs = timeoutMs;
    this.protocolLanes = Math.max(1, Math.trunc(protocolLanes));
    this.heartbeatIntervalMs = normalizeHeartbeatInterval(heartbeatIntervalMs);
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error) => this.failAll(error));
    this.socket.on("close", () => this.failAll(new FerricStoreError("FerricStore connection closed")));
    this.startHeartbeat();
  }

  static async fromUrl(url: string, options: NativeAdapterOptions = {}): Promise<NativeAdapter> {
    const parsed = parseFerricUrl(url);
    const socket = await connect(parsed, options);
    const adapter = new NativeAdapter(
      socket,
      options.timeoutMs ?? 30_000,
      options.protocolLanes ?? 8,
      options.maxChunkBytes ?? 64 * 1024 * 1024,
      options.heartbeatIntervalMs
    );
    await adapter.startup(options.clientName);
    const password = options.password ?? parsed.password;
    if (password != null && password !== "") {
      await adapter.auth(options.username ?? parsed.username ?? "default", password);
    }
    return adapter;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    const command = buildProtocolCommand(args);
    return await this.request(command);
  }

  async executePipeline(commands: readonly Command[], options: ExecutePipelineOptions = {}): Promise<unknown[]> {
    if (commands.length === 0) {
      return [];
    }
    return unwrapPipelineResponse(await this.request(pipelineCommand(commands)), options);
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.stopHeartbeat();
    this.socket.end();
  }

  private async startup(clientName?: string): Promise<void> {
    await this.request({
      laneId: 0,
      opcode: OPCODES.startup,
      payload: {
        client_name: clientName ?? "ferricstore-typescript",
        compact_flow_responses: true,
        compression: "none",
        driver_name: clientName ?? "ferricstore-typescript"
      }
    });
  }

  private async auth(username: string, password: string): Promise<void> {
    await this.request({ laneId: 0, opcode: OPCODES.auth, payload: { password, username } });
  }

  private async request(command: ProtocolCommand): Promise<unknown> {
    if (this.closed) {
      throw new FerricStoreError("FerricStore connection is closed");
    }
    this.lastActivityMs = Date.now();
    const requestId = this.nextRequestId();
    const frame = encodeRequest(this.assignLane(command), requestId);

    return await new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        this.cleanupChunksForRequest(requestId);
        reject(new FerricStoreError(`FerricStore request timed out after ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      timer.unref?.();

      this.pending.set(requestId, {
        opcode: command.opcode,
        reject: (reason: unknown) => {
          clearTimeout(timer);
          this.lastActivityMs = Date.now();
          reject(reason instanceof Error ? reason : classifyServerError(String(reason), reason));
        },
        resolve: (value: unknown) => {
          clearTimeout(timer);
          this.lastActivityMs = Date.now();
          resolve(value);
        }
      });

      this.socket.write(frame, (error) => {
        if (error != null) {
          this.pending.delete(requestId);
          this.cleanupChunksForRequest(requestId);
          clearTimeout(timer);
          this.lastActivityMs = Date.now();
          reject(error);
        }
      });
    });
  }

  private startHeartbeat(): void {
    if (this.heartbeatIntervalMs == null) {
      return;
    }
    this.heartbeatTimer = setInterval(() => {
      void this.sendHeartbeat();
    }, this.heartbeatIntervalMs);
    this.heartbeatTimer.unref?.();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer == null) {
      return;
    }
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = undefined;
  }

  private async sendHeartbeat(): Promise<void> {
    if (this.closed || this.heartbeatInFlight || this.pending.size > 0 || this.heartbeatIntervalMs == null) {
      return;
    }
    if (Date.now() - this.lastActivityMs < this.heartbeatIntervalMs) {
      return;
    }
    this.heartbeatInFlight = true;
    try {
      await this.request(buildProtocolCommand(["PING"]));
    } catch {
      // Socket close/error handlers own adapter shutdown and pending request rejection.
    } finally {
      this.heartbeatInFlight = false;
    }
  }

  private onData(chunk: Buffer): void {
    try {
      this.buffer = this.buffer.byteLength === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
      for (;;) {
        const parsed = tryDecodeFrame(this.buffer);
        if (parsed == null) {
          return;
        }
        this.buffer = parsed.rest;
        this.handleFrame(parsed.frame);
      }
    } catch (error) {
      this.failAll(error);
      this.socket.destroy(error instanceof Error ? error : undefined);
    }
  }

  private handleFrame(frame: ResponseFrame): void {
    if (frame.requestId === 0n) {
      return;
    }
    if ((frame.flags & FLAG_MORE_CHUNKS) !== 0) {
      const key = `${frame.requestId}:${frame.opcode}:${frame.laneId}`;
      const bytes = (this.chunkBytes.get(key) ?? 0) + frame.body.byteLength;
      if (bytes > this.maxChunkBytes) {
        throw new FerricStoreError(`native protocol chunked response exceeded ${this.maxChunkBytes} bytes`);
      }
      this.chunkBytes.set(key, bytes);
      this.chunks.set(key, [...(this.chunks.get(key) ?? []), frame.body]);
      return;
    }

    let completeFrame = frame;
    if (this.chunks.size > 0) {
      const key = `${frame.requestId}:${frame.opcode}:${frame.laneId}`;
      const previous = this.chunks.get(key);
      this.chunks.delete(key);
      this.chunkBytes.delete(key);
      if (previous != null) {
        const body = Buffer.concat([...previous, frame.body]);
        completeFrame = { ...frame, body, bodyLength: body.byteLength };
      }
    }

    const pending = this.pending.get(frame.requestId);
    if (pending == null) {
      return;
    }
    this.pending.delete(frame.requestId);
    this.cleanupChunksForRequest(frame.requestId);

    try {
      pending.resolve(decodeResponse(completeFrame, pending.opcode));
    } catch (error) {
      pending.reject(error);
    }
  }

  private failAll(reason: unknown): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.stopHeartbeat();
    const error = reason instanceof Error ? reason : classifyServerError(String(reason), reason);
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
    this.chunks.clear();
    this.chunkBytes.clear();
  }

  private cleanupChunksForRequest(requestId: bigint): void {
    const prefix = `${requestId}:`;
    for (const key of this.chunks.keys()) {
      if (key.startsWith(prefix)) {
        this.chunks.delete(key);
        this.chunkBytes.delete(key);
      }
    }
  }

  private nextRequestId(): bigint {
    this.requestId = this.requestId === 0xffff_ffff_ffff_ffffn ? 1n : this.requestId + 1n;
    return this.requestId;
  }

  private assignLane(command: ProtocolCommand): ProtocolCommand {
    if (command.laneId != null) {
      return command;
    }
    if (command.opcode < 0x0100 && command.opcode !== OPCODES.pipeline) {
      return { ...command, laneId: 0 };
    }
    this.dataLane = (this.dataLane % this.protocolLanes) + 1;
    return { ...command, laneId: this.dataLane };
  }
}

export class ReconnectingExecutor implements CommandExecutor {
  private closed = false;
  private readonly maxRetries: number;
  private executorPromise: Promise<CommandExecutor>;
  private reconnectPromise?: Promise<CommandExecutor>;

  constructor(
    private readonly createExecutor: () => Promise<CommandExecutor>,
    options: ReconnectOptions = {}
  ) {
    this.maxRetries = Math.max(0, Math.trunc(options.maxRetries ?? 1));
    this.executorPromise = createExecutor();
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.withReconnect((executor) => executor.executeCommand(...args));
  }

  async executePipeline(commands: readonly Command[], options: ExecutePipelineOptions = {}): Promise<unknown[]> {
    return await this.withReconnect(async (executor) => {
      if (executor.executePipeline != null) {
        return await executor.executePipeline(commands, options);
      }
      return await Promise.all(commands.map((command) => executor.executeCommand(...command)));
    });
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    const executor = await this.executorPromise.catch(() => undefined);
    await Promise.resolve(executor?.close?.());
  }

  private async withReconnect<T>(operation: (executor: CommandExecutor) => Promise<T>): Promise<T> {
    let executor = await this.executorPromise;
    for (let attempt = 0; ; attempt++) {
      try {
        return await operation(executor);
      } catch (error) {
        if (this.closed || attempt >= this.maxRetries || !isReconnectableClosedConnectionError(error)) {
          throw error;
        }
        executor = await this.reconnect(executor);
      }
    }
  }

  private async reconnect(staleExecutor: CommandExecutor): Promise<CommandExecutor> {
    if (this.closed) {
      throw new FerricStoreError("FerricStore client is closed");
    }
    if (this.reconnectPromise != null) {
      return await this.reconnectPromise;
    }
    const currentExecutor = await this.executorPromise.catch(() => undefined);
    if (currentExecutor != null && currentExecutor !== staleExecutor) {
      return currentExecutor;
    }
    this.reconnectPromise = (async () => {
      await Promise.resolve(staleExecutor.close?.()).catch(() => undefined);
      const nextExecutor = await this.createExecutor();
      this.executorPromise = Promise.resolve(nextExecutor);
      return nextExecutor;
    })().finally(() => {
      this.reconnectPromise = undefined;
    });
    return await this.reconnectPromise;
  }
}

export function isReconnectableClosedConnectionError(error: unknown): boolean {
  return error instanceof Error && error.message === "FerricStore connection is closed";
}

function parseFerricUrl(value: string): ParsedUrl {
  const url = new URL(value);
  if (url.protocol !== "ferric:" && url.protocol !== "ferrics:") {
    throw new FerricStoreError(`unsupported FerricStore URL scheme: ${url.protocol}`);
  }
  return {
    host: url.hostname || "127.0.0.1",
    ...(url.password === "" ? {} : { password: decodeURIComponent(url.password) }),
    port: Number(url.port || (url.protocol === "ferrics:" ? 6389 : 6388)),
    tls: url.protocol === "ferrics:",
    ...(url.username === "" ? {} : { username: decodeURIComponent(url.username) })
  };
}

async function connect(parsed: ParsedUrl, options: NativeAdapterOptions): Promise<net.Socket | tls.TLSSocket> {
  const timeoutMs = options.connectTimeoutMs ?? options.timeoutMs ?? 30_000;
  return await new Promise((resolve, reject) => {
    const socket = parsed.tls
      ? tls.connect({ host: parsed.host, port: parsed.port, ...options.tlsOptions })
      : net.createConnection({ host: parsed.host, port: parsed.port });
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new FerricStoreError(`FerricStore connection timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref?.();

    socket.setNoDelay(true);
    socket.setKeepAlive(options.keepAlive ?? true, normalizeKeepAliveInitialDelay(options.keepAliveInitialDelayMs));
    socket.once("connect", () => {
      clearTimeout(timer);
      resolve(socket);
    });
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error instanceof Error ? error : classifyServerError(String(error), error));
    });
  });
}

function normalizeHeartbeatInterval(value: number | undefined): number | undefined {
  if (value != null && value <= 0) {
    return undefined;
  }
  return Math.trunc(value ?? 60_000);
}

function normalizeKeepAliveInitialDelay(value: number | undefined): number {
  return Math.max(0, Math.trunc(value ?? 30_000));
}
