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
  executePipeline?(commands: readonly Command[]): Promise<unknown[]>;
  close?(): Promise<void> | void;
}

export interface NativeAdapterOptions {
  clientName?: string;
  connectTimeoutMs?: number;
  protocolLanes?: number;
  timeoutMs?: number;
  username?: string;
  password?: string;
  tlsOptions?: tls.ConnectionOptions;
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
  private buffer: Buffer = Buffer.alloc(0);
  private closed = false;
  private dataLane = 0;
  private requestId = 0n;
  private readonly protocolLanes: number;
  private readonly timeoutMs: number;

  private constructor(socket: net.Socket | tls.TLSSocket, timeoutMs: number, protocolLanes: number) {
    this.socket = socket;
    this.timeoutMs = timeoutMs;
    this.protocolLanes = Math.max(1, Math.trunc(protocolLanes));
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error) => this.failAll(error));
    this.socket.on("close", () => this.failAll(new FerricStoreError("FerricStore connection closed")));
  }

  static async fromUrl(url: string, options: NativeAdapterOptions = {}): Promise<NativeAdapter> {
    const parsed = parseFerricUrl(url);
    const socket = await connect(parsed, options);
    const adapter = new NativeAdapter(socket, options.timeoutMs ?? 30_000, options.protocolLanes ?? 64);
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

  async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
    if (commands.length === 0) {
      return [];
    }
    return unwrapPipelineResponse(await this.request(pipelineCommand(commands)));
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
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
    const requestId = this.nextRequestId();
    const frame = encodeRequest(this.assignLane(command), requestId);

    return await new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new FerricStoreError(`FerricStore request timed out after ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      timer.unref?.();

      this.pending.set(requestId, {
        opcode: command.opcode,
        reject: (reason: unknown) => {
          clearTimeout(timer);
          reject(reason instanceof Error ? reason : classifyServerError(String(reason), reason));
        },
        resolve: (value: unknown) => {
          clearTimeout(timer);
          resolve(value);
        }
      });

      this.socket.write(frame, (error) => {
        if (error != null) {
          this.pending.delete(requestId);
          clearTimeout(timer);
          reject(error);
        }
      });
    });
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    for (;;) {
      const parsed = tryDecodeFrame(this.buffer);
      if (parsed == null) {
        return;
      }
      this.buffer = parsed.rest;
      this.handleFrame(parsed.frame);
    }
  }

  private handleFrame(frame: ResponseFrame): void {
    if (frame.requestId === 0n) {
      return;
    }
    const key = `${frame.requestId}:${frame.opcode}:${frame.laneId}`;
    if ((frame.flags & FLAG_MORE_CHUNKS) !== 0) {
      this.chunks.set(key, [...(this.chunks.get(key) ?? []), frame.body]);
      return;
    }

    const previous = this.chunks.get(key);
    this.chunks.delete(key);
    const body = previous == null ? frame.body : Buffer.concat([...previous, frame.body]);
    const completeFrame: ResponseFrame = { ...frame, body, bodyLength: body.byteLength };
    const pending = this.pending.get(frame.requestId);
    if (pending == null) {
      return;
    }
    this.pending.delete(frame.requestId);

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
    const error = reason instanceof Error ? reason : classifyServerError(String(reason), reason);
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
    this.chunks.clear();
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
