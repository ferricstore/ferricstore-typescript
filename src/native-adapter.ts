import net from "node:net";
import tls from "node:tls";
import { Buffer } from "node:buffer";
import { FerricStoreError, OverloadedError, RequestTimeoutError, classifyServerError } from "./errors.js";
import { possiblySentConnectionClosedError, unsentConnectionClosedError } from "./adapter-connection-errors.js";
import {
  booleanResponse,
  field,
  setLongTimeout,
  type Command,
  type CommandArgument
} from "./internal.js";
import {
  flowControlLimits,
  normalizeHeartbeatInterval,
  normalizeNonNegativeInteger,
  normalizePositiveLimit,
  startupLimits
} from "./native-connection.js";
import { NativeFlowControl } from "./native-flow-control.js";
import { NativeHeartbeat } from "./native-heartbeat.js";
import {
  DEFAULT_MAX_FRAME_BYTES,
  OPCODES,
  buildProtocolCommand,
  encodeRequest,
  type ProtocolCommand
} from "./protocol.js";
import type { ExecutePipelineOptions } from "./pipeline-execution.js";
import type { CommandExecutor, NativeAdapterOptions, NativeProtocolEvent } from "./adapter-types.js";
import {
  DEFAULT_MAX_CHUNK_BYTES,
  DEFAULT_MAX_CHUNK_FRAMES,
  DEFAULT_MAX_PENDING_CONTROL_REQUESTS,
  DEFAULT_MAX_QUEUED_REQUESTS,
  DEFAULT_MAX_QUEUED_WRITE_BYTES,
  DEFAULT_MAX_RESPONSE_BYTES
} from "./native-adapter-config.js";
import { bootstrapNativeAdapter } from "./native-adapter-bootstrap.js";
import { executeNativeFusedPipeline, executeNativePipeline } from "./native-pipeline-execution.js";
import type { PendingRequest } from "./native-pending-request.js";
import { nativeResponseTimeoutMs, timeoutNativePendingRequest } from "./native-pending-timeout.js";
import { NativeChunkAssembler } from "./native-chunk-assembler.js";
import { NativeResponseHandler } from "./native-response-handler.js";
import { NativeRequestScheduler } from "./native-request-scheduler.js";
import { NativeWriteQueue } from "./native-write-queue.js";
export class NativeAdapter implements CommandExecutor {
  private readonly socket: net.Socket | tls.TLSSocket;
  private readonly pending = new Map<bigint, PendingRequest>();
  private readonly flowControl: NativeFlowControl;
  private readonly closedWaiters = new Set<() => void>();
  private readonly chunkAssembler: NativeChunkAssembler;
  private closed = false;
  private protocolLanes: number;
  private maxPipelineCommands = Number.MAX_SAFE_INTEGER;
  private maxRequestFrameBytes: number;
  private pendingControlRequests = 0;
  private readonly maxPendingControlRequests: number;
  private readonly heartbeat: NativeHeartbeat;
  private readonly heartbeatIntervalMs: number | undefined;
  private readonly responseHandler: NativeResponseHandler;
  private readonly requestScheduler = new NativeRequestScheduler();
  private readonly timeoutMs: number;
  private draining = false;
  private readonly writeQueue: NativeWriteQueue;

  private constructor(
    socket: net.Socket | tls.TLSSocket,
    timeoutMs: number,
    protocolLanes: number,
    maxChunkBytes: number,
    maxChunkFrames: number,
    maxFrameBytes: number,
    maxResponseBytes: number,
    maxPendingControlRequests: number,
    maxQueuedRequests: number,
    heartbeatIntervalMs?: number,
    onEvent?: (event: NativeProtocolEvent) => unknown,
    maxQueuedWriteBytes = DEFAULT_MAX_QUEUED_WRITE_BYTES
  ) {
    this.socket = socket;
    const normalizedMaxChunkBytes = normalizePositiveLimit(maxChunkBytes, DEFAULT_MAX_CHUNK_BYTES);
    const normalizedMaxChunkFrames = normalizePositiveLimit(maxChunkFrames, DEFAULT_MAX_CHUNK_FRAMES);
    const normalizedMaxFrameBytes = normalizePositiveLimit(maxFrameBytes, DEFAULT_MAX_FRAME_BYTES);
    this.maxRequestFrameBytes = normalizedMaxFrameBytes;
    const normalizedMaxResponseBytes = normalizePositiveLimit(maxResponseBytes, DEFAULT_MAX_RESPONSE_BYTES);
    this.chunkAssembler = new NativeChunkAssembler(
      normalizedMaxChunkBytes,
      normalizedMaxChunkFrames,
      normalizedMaxResponseBytes
    );
    this.maxPendingControlRequests = normalizePositiveLimit(
      maxPendingControlRequests,
      DEFAULT_MAX_PENDING_CONTROL_REQUESTS
    );
    this.flowControl = new NativeFlowControl(
      normalizeNonNegativeInteger(maxQueuedRequests, DEFAULT_MAX_QUEUED_REQUESTS)
    );
    this.writeQueue = new NativeWriteQueue(
      socket,
      normalizeNonNegativeInteger(maxQueuedWriteBytes, DEFAULT_MAX_QUEUED_WRITE_BYTES),
      () => this.closed,
      (requestId) => this.pending.has(requestId),
      (requestId, error) => this.takePending(requestId)?.reject(error)
    );
    this.timeoutMs = normalizePositiveLimit(timeoutMs, 30_000);
    this.protocolLanes = normalizePositiveLimit(protocolLanes, 8);
    this.heartbeatIntervalMs = normalizeHeartbeatInterval(heartbeatIntervalMs);
    this.heartbeat = new NativeHeartbeat(
      this.heartbeatIntervalMs,
      async () => { await this.request(buildProtocolCommand(["PING"], this.maxRequestFrameBytes), true); },
      (error) => {
        this.failAll(new FerricStoreError("FerricStore protocol heartbeat failed", { cause: error }), true);
        this.socket.destroy();
      }
    );
    this.responseHandler = new NativeResponseHandler({
      applyFlowControlLimits: (value) => this.applyFlowControlLimits(value),
      beginDraining: () => this.beginDraining(),
      chunkAssembler: this.chunkAssembler,
      destroy: (error) => this.socket.destroy(error),
      failAll: (reason, connectionClosed, message) => this.failAll(reason, connectionClosed, message),
      heartbeat: this.heartbeat,
      maxChunkBytes: normalizedMaxChunkBytes,
      maxChunkFrames: normalizedMaxChunkFrames,
      maxFrameBytes: normalizedMaxFrameBytes,
      maxResponseBytes: normalizedMaxResponseBytes,
      onEvent,
      pause: () => this.socket.pause(),
      pending: this.pending,
      resume: () => this.socket.resume(),
      takePending: (requestId) => this.takePending(requestId)
    });
    this.socket.on("data", (chunk: Buffer) => this.responseHandler.onData(chunk));
    this.socket.on("error", (error) => this.failAll(error, true));
    this.socket.on("close", () => this.failAll(new FerricStoreError("FerricStore connection closed"), true));
    this.socket.on("drain", () => this.writeQueue.flush());
  }

  static async fromUrl(url: string, options: NativeAdapterOptions = {}): Promise<NativeAdapter> {
    return await bootstrapNativeAdapter(url, options, (args) => {
      const adapter = new NativeAdapter(...args);
      return {
        adapter,
        auth: async (username, password) => {
          await adapter.request({ laneId: 0, opcode: OPCODES.auth, payload: { password, username } });
        },
        close: async () => { await adapter.close(); },
        startHeartbeat: () => adapter.heartbeat.start(),
        startup: async (clientName, events) => await adapter.startup(clientName, events)
      };
    });
  }

  /** @internal Closed and GOAWAY-draining adapters must not receive new work. */
  get isUnavailable(): boolean {
    return this.closed || this.draining;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    const command = buildProtocolCommand(args, this.maxRequestFrameBytes);
    return await this.request(command);
  }

  /** @internal Execute raw command arguments on a topology-selected lane. */
  async executeCommandOnLane(
    args: readonly CommandArgument[],
    laneId: number
  ): Promise<unknown> {
    return await this.executeProtocolCommand(
      buildProtocolCommand(args, this.maxRequestFrameBytes),
      laneId
    );
  }

  async executeProtocolCommand(command: ProtocolCommand, laneId?: number): Promise<unknown> {
    return await this.request(laneId == null ? command : { ...command, laneId });
  }

  async executePipeline(commands: readonly Command[], options: ExecutePipelineOptions = {}): Promise<unknown[]> {
    return await this.executePipelineOnLane(commands, undefined, options);
  }

  /** @internal Attempt one physical pipeline request without splitting or fallback. */
  async executeFusedPipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[] | undefined> {
    return await this.executeFusedPipelineOnLane(commands, undefined, options);
  }

  /** @internal Attempt one physical pipeline request on a topology-selected lane. */
  async executeFusedPipelineOnLane(
    commands: readonly Command[],
    laneId: number | undefined,
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[] | undefined> {
    return await executeNativeFusedPipeline(
      this, commands, laneId, options, this.maxPipelineCommands, this.maxRequestFrameBytes
    );
  }

  /** @internal Execute an ordered pipeline on a topology-selected lane. */
  async executePipelineOnLane(
    commands: readonly Command[],
    laneId: number | undefined,
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[]> {
    return await executeNativePipeline(
      this, commands, laneId, options, this.maxPipelineCommands, this.maxRequestFrameBytes
    );
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.failAll(new FerricStoreError("FerricStore connection closed"), true);
    this.socket.destroy();
  }

  private async startup(clientName?: string, events?: readonly string[]): Promise<boolean> {
    const response = await this.request({
      laneId: 0,
      opcode: OPCODES.startup,
      payload: {
        client_name: clientName ?? "ferricstore-typescript",
        compact_flow_responses: true,
        compression: "none",
        driver_name: clientName ?? "ferricstore-typescript",
        ...(events == null || events.length === 0 ? {} : { events: [...events] })
      }
    });
    this.applyStartupLimits(response);
    const authRequired = field(response, "auth_required");
    return authRequired == null ? false : booleanResponse(authRequired);
  }

  private request(command: ProtocolCommand, allowWhileDraining = false): Promise<unknown> {
    if (this.closed || (this.draining && !allowWhileDraining)) {
      return Promise.reject(unsentConnectionClosedError());
    }
    const assigned = this.requestScheduler.assignLane(command, this.protocolLanes);
    const laneId = assigned.laneId ?? 0;
    const responseTimeoutMs = nativeResponseTimeoutMs(assigned, this.timeoutMs);
    // FerricStore enforces advertised credits on data opcodes. Control opcodes
    // bypass the server counters, which also lets WINDOW_UPDATE reopen zero.
    if (command.opcode < OPCODES.commandExec && command.opcode !== OPCODES.pipeline) {
      return this.sendAcquiredRequest(assigned, laneId, responseTimeoutMs, false);
    }
    if (this.flowControl.tryAcquire(laneId)) {
      return this.sendAcquiredRequest(assigned, laneId, responseTimeoutMs);
    }
    return this.waitForCreditAndSend(assigned, laneId, Date.now(), responseTimeoutMs);
  }

  private async waitForCreditAndSend(
    command: ProtocolCommand,
    laneId: number,
    startedAtMs: number,
    responseTimeoutMs: number | undefined
  ): Promise<unknown> {
    await this.flowControl.wait(laneId, Math.min(this.timeoutMs, responseTimeoutMs ?? this.timeoutMs));
    if (this.closed || this.draining) {
      this.flowControl.release(laneId);
      throw unsentConnectionClosedError();
    }
    const remainingMs = responseTimeoutMs == null
      ? undefined
      : responseTimeoutMs - (Date.now() - startedAtMs);
    if (remainingMs != null && remainingMs <= 0) {
      this.flowControl.release(laneId);
      throw new RequestTimeoutError(responseTimeoutMs ?? this.timeoutMs, "unsent");
    }
    return await this.sendAcquiredRequest(command, laneId, remainingMs);
  }

  private sendAcquiredRequest(
    command: ProtocolCommand,
    laneId: number,
    timeoutMs: number | undefined,
    hasFlowControlCredit = true
  ): Promise<unknown> {
    if (!hasFlowControlCredit && this.pendingControlRequests >= this.maxPendingControlRequests) {
      return Promise.reject(new OverloadedError(
        "FerricStore client control request limit is full",
        { reason: "client_control_requests_full" }
      ));
    }
    let requestId: bigint;
    let frame: Buffer;
    try {
      requestId = this.requestScheduler.nextRequestId();
      frame = encodeRequest(command, requestId, this.maxRequestFrameBytes);
    } catch (error) {
      if (hasFlowControlCredit) this.flowControl.release(laneId);
      return Promise.reject(error instanceof Error ? error : new FerricStoreError(String(error), { raw: error }));
    }

    return new Promise<unknown>((resolve, reject) => {
      const timer = timeoutMs == null ? undefined : setLongTimeout(() => {
        this.timeoutPending(requestId, timeoutMs);
      }, timeoutMs);
      timer?.unref();

      if (!hasFlowControlCredit) this.pendingControlRequests += 1;
      this.pending.set(requestId, {
        ...(command.compactClaimMode == null ? {} : { compactClaimMode: command.compactClaimMode }),
        hasFlowControlCredit,
        indefinite: timeoutMs == null,
        laneId,
        opcode: command.opcode,
        ...(command.pipelineClaimModes == null ? {} : { pipelineClaimModes: command.pipelineClaimModes }),
        reject: (reason: unknown) => {
          timer?.cancel();
          reject(reason instanceof Error ? reason : classifyServerError(String(reason), reason));
        },
        resolve: (value: unknown) => {
          timer?.cancel();
          resolve(value);
        }
      });

      this.writeQueue.write(requestId, frame);
    });
  }

  private timeoutPending(requestId: bigint, timeoutMs: number): void {
    timeoutNativePendingRequest({
      discardRequest: (id) => this.chunkAssembler.discardRequest(id),
      getPending: (id) => this.pending.get(id),
      isWriteQueued: (id) => this.writeQueue.has(id),
      retireConnection: (error) => {
        this.failAll(error, true, error.message);
        this.socket.destroy(error);
      },
      takePending: (id) => this.takePending(id)
    }, requestId, timeoutMs);
  }

  private finishDrainIfIdle(): void {
    if (!this.draining || this.pending.size > 0 || this.closed) {
      return;
    }
    this.closed = true;
    this.responseHandler.stop();
    this.heartbeat.stop();
    this.flowControl.close(unsentConnectionClosedError());
    this.socket.destroy();
    this.resolveClosedWaiters();
  }

  private failAll(reason: unknown, connectionClosed = false, dispatchedMessage?: string): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.responseHandler.stop();
    this.resolveClosedWaiters();
    this.heartbeat.stop();
    const cause = reason instanceof Error ? reason : classifyServerError(String(reason), reason);
    const unsentError = unsentConnectionClosedError(cause);
    const dispatchedError = connectionClosed
      ? possiblySentConnectionClosedError(cause, dispatchedMessage)
      : cause;
    this.flowControl.close(unsentError);
    for (const [requestId, pending] of this.pending) {
      pending.lateResponseTimer?.cancel();
      if (pending.hasFlowControlCredit) this.flowControl.release(pending.laneId);
      pending.reject(this.writeQueue.has(requestId) ? unsentError : dispatchedError);
    }
    this.pending.clear();
    this.pendingControlRequests = 0;
    this.writeQueue.clear();
    this.chunkAssembler.clear();
  }

  /** @internal Gracefully stop new requests and close after correlated requests settle. */
  async retire(): Promise<void> {
    if (!this.closed) {
      this.beginDraining();
    }
    if (this.closed) {
      return;
    }
    await new Promise<void>((resolve) => this.closedWaiters.add(resolve));
  }

  private beginDraining(): void {
    this.draining = true;
    const unsentError = unsentConnectionClosedError();
    const possiblySentError = possiblySentConnectionClosedError();
    this.flowControl.cancelQueued(unsentError);
    this.writeQueue.rejectQueued(unsentError);
    // Reject only infinite server blocks; finite requests retain graceful drain semantics.
    for (const [requestId, pending] of [...this.pending]) {
      if (pending.indefinite) {
        const error = this.writeQueue.has(requestId) ? unsentError : possiblySentError;
        this.takePending(requestId)?.reject(error);
      }
    }
    this.finishDrainIfIdle();
  }

  private resolveClosedWaiters(): void {
    for (const resolve of this.closedWaiters) resolve();
    this.closedWaiters.clear();
  }

  private takePending(requestId: bigint): PendingRequest | undefined {
    const pending = this.pending.get(requestId);
    if (pending == null) return undefined;
    pending.lateResponseTimer?.cancel();
    this.pending.delete(requestId);
    this.writeQueue.cancel(requestId);
    this.chunkAssembler.discardRequest(requestId);
    if (pending.hasFlowControlCredit) this.flowControl.release(pending.laneId);
    else this.pendingControlRequests = Math.max(0, this.pendingControlRequests - 1);
    this.finishDrainIfIdle();
    return pending;
  }

  private applyFlowControlLimits(value: unknown): void {
    const limits = flowControlLimits(value);
    if (limits.connection == null && limits.lane == null) return;
    this.flowControl.updateLimits(limits.connection, limits.lane);
  }

  private applyStartupLimits(value: unknown): void {
    this.applyFlowControlLimits(value);
    const limits = startupLimits(value);
    if (limits.laneQueue != null) {
      this.flowControl.updateLaneQueueLimit(limits.laneQueue);
    }
    if (limits.lanes != null) {
      this.protocolLanes = Math.min(this.protocolLanes, limits.lanes);
    }
    if (limits.pipelineCommands != null) {
      this.maxPipelineCommands = limits.pipelineCommands;
    }
    if (limits.frameBytes != null) {
      this.maxRequestFrameBytes = Math.min(this.maxRequestFrameBytes, limits.frameBytes);
    }
  }
}
