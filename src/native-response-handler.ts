import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { NativeProtocolEvent } from "./adapter-types.js";
import { NativeChunkAssembler } from "./native-chunk-assembler.js";
import { NativeHeartbeat } from "./native-heartbeat.js";
import type { PendingRequest } from "./native-pending-request.js";
import {
  EMPTY_COMPACT_RESPONSE_OPCODES,
  type CompactResponseOpcodes
} from "./native-negotiation.js";
import { NativeReceiveBuffer } from "./native-receive-buffer.js";
import {
  FLAG_MORE_CHUNKS,
  MAX_FRAMES_PER_DECODE,
  OPCODES,
  decodeResponse,
  type ResponseFrame
} from "./protocol.js";

interface NativeResponseHandlerOptions {
  readonly applyFlowControlLimits: (value: unknown) => void;
  readonly beginDraining: () => void;
  readonly chunkAssembler: NativeChunkAssembler;
  readonly compactResponseOpcodes?: () => CompactResponseOpcodes;
  readonly destroy: (error?: Error) => void;
  readonly failAll: (reason: unknown, connectionClosed: boolean, message?: string) => void;
  readonly heartbeat: NativeHeartbeat;
  readonly maxChunkBytes: number;
  readonly maxChunkFrames: number;
  readonly maxFrameBytes: number;
  readonly maxResponseBytes: number;
  readonly onEvent?: (event: NativeProtocolEvent) => unknown;
  readonly pause: () => void;
  readonly pending: ReadonlyMap<bigint, PendingRequest>;
  readonly resume: () => void;
  readonly takePending: (requestId: bigint) => PendingRequest | undefined;
}

/** Correlate, decode, and bound inbound native protocol frames. */
export class NativeResponseHandler {
  private readonly receiveBuffer = new NativeReceiveBuffer();
  private drainScheduled = false;
  private paused = false;
  private stopped = false;
  private maxResponseBytes: number;

  constructor(private readonly options: NativeResponseHandlerOptions) {
    this.maxResponseBytes = options.maxResponseBytes;
  }

  updateMaxResponseBytes(limit: number): void {
    this.maxResponseBytes = Math.min(this.maxResponseBytes, limit);
    this.options.chunkAssembler.updateMaxResponseBytes(this.maxResponseBytes);
  }

  onData(chunk: Buffer): void {
    if (this.stopped) return;
    try {
      this.receiveBuffer.append(chunk);
    } catch (error) {
      this.fail(error);
      return;
    }
    if (!this.drainScheduled) this.drainFrames();
  }

  stop(): void {
    this.stopped = true;
    this.receiveBuffer.clear();
  }

  private drainFrames(): void {
    if (this.stopped) return;
    try {
      for (let count = 0; count < MAX_FRAMES_PER_DECODE; count += 1) {
        const frame = this.receiveBuffer.nextFrame(this.options.maxFrameBytes);
        if (frame == null) {
          this.resume();
          return;
        }
        this.handleFrame(frame);
        if (this.stopped) return;
      }
      this.scheduleDrain();
    } catch (error) {
      this.fail(error);
    }
  }

  private scheduleDrain(): void {
    if (this.drainScheduled || this.stopped) return;
    this.drainScheduled = true;
    if (!this.paused) {
      this.paused = true;
      this.options.pause();
    }
    setImmediate(() => {
      this.drainScheduled = false;
      this.drainFrames();
    });
  }

  private resume(): void {
    if (!this.paused) return;
    this.paused = false;
    this.options.resume();
  }

  private fail(error: unknown): void {
    this.stop();
    this.options.failAll(error, true, error instanceof Error ? error.message : String(error));
    this.options.destroy(error instanceof Error ? error : undefined);
  }

  private handleFrame(frame: ResponseFrame): void {
    this.options.heartbeat.recordInbound();
    if (frame.requestId === 0n) {
      if (frame.laneId !== 0) {
        throw new FerricStoreError("native protocol management frame used non-control lane", {
          raw: frame
        });
      }
      if (
        frame.opcode !== OPCODES.event
        && frame.opcode !== OPCODES.goaway
        && frame.opcode !== OPCODES.windowUpdate
      ) {
        throw new FerricStoreError(`native protocol unsupported management opcode ${frame.opcode}`, {
          raw: frame
        });
      }
      const completeFrame = this.options.chunkAssembler.assemble(frame);
      if (completeFrame != null) this.handleManagementFrame(completeFrame);
      return;
    }
    const pending = this.options.pending.get(frame.requestId);
    if (pending == null) return;
    if (frame.opcode !== pending.opcode || frame.laneId !== pending.laneId) {
      throw new FerricStoreError("native protocol response correlation mismatch", { raw: frame });
    }
    if (pending.timedOut) {
      this.discardTimedOutFrame(frame, pending);
      return;
    }
    const completeFrame = this.options.chunkAssembler.assemble(frame);
    if (completeFrame == null) return;
    if (completeFrame.body.byteLength > this.maxResponseBytes) {
      throw new FerricStoreError(
        `native protocol response exceeded ${this.maxResponseBytes} bytes`
      );
    }

    let value: unknown;
    let responseError: unknown;
    try {
      value = decodeResponse(completeFrame, pending.opcode, {
        ...pending,
        compactResponseOpcodes: this.options.compactResponseOpcodes?.() ?? EMPTY_COMPACT_RESPONSE_OPCODES
      });
      if (pending.opcode === OPCODES.windowUpdate) this.options.applyFlowControlLimits(value);
    } catch (error) {
      responseError = error;
    }
    const completed = this.options.takePending(frame.requestId);
    if (completed == null) return;
    if (responseError == null) completed.resolve(value);
    else completed.reject(responseError);
  }

  private discardTimedOutFrame(frame: ResponseFrame, pending: PendingRequest): void {
    const discardedResponseBytes = (pending.discardedResponseBytes ?? 0) + frame.body.byteLength;
    const discardedResponseFrames = (pending.discardedResponseFrames ?? 0) + 1;
    pending.discardedResponseBytes = discardedResponseBytes;
    pending.discardedResponseFrames = discardedResponseFrames;
    const chunked = discardedResponseFrames > 1 || (frame.flags & FLAG_MORE_CHUNKS) !== 0;
    if (
      discardedResponseBytes > this.maxResponseBytes ||
      (chunked && discardedResponseBytes > this.options.maxChunkBytes)
    ) {
      throw new FerricStoreError("native protocol discarded response exceeded configured byte limits");
    }
    if (discardedResponseFrames > this.options.maxChunkFrames) {
      throw new FerricStoreError("native protocol discarded response exceeded configured frame limits");
    }
    if ((frame.flags & FLAG_MORE_CHUNKS) === 0) this.options.takePending(frame.requestId);
  }

  private handleManagementFrame(frame: ResponseFrame): void {
    if (frame.body.byteLength > this.maxResponseBytes) {
      throw new FerricStoreError(
        `native protocol response exceeded ${this.maxResponseBytes} bytes`
      );
    }
    const value = decodeResponse(frame, frame.opcode, {
      compactResponseOpcodes: this.options.compactResponseOpcodes?.() ?? EMPTY_COMPACT_RESPONSE_OPCODES
    });
    if (frame.opcode === OPCODES.windowUpdate) this.options.applyFlowControlLimits(value);
    const event: NativeProtocolEvent = {
      flags: frame.flags,
      laneId: frame.laneId,
      opcode: frame.opcode,
      value
    };
    if (frame.opcode === OPCODES.goaway) this.options.beginDraining();
    try {
      const callback = this.options.onEvent?.(event);
      if (callback != null) void Promise.resolve(callback).catch(() => undefined);
    } catch {
      // User event handlers must not corrupt transport state.
    }
  }
}
