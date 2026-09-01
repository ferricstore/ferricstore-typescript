import { Buffer } from "node:buffer";
import {
  executeCommandArgs,
  type CommandExecutor,
  type ExecutePipelineOptions,
  type ReconnectOptions
} from "./adapters.js";
import { ConnectionClosedError, FerricStoreError, RequestNotSentError } from "./errors.js";
import { sleep, type Command, type CommandArgument } from "./internal.js";
import { snapshotCommandArguments } from "./command-snapshot.js";
import {
  executeCommandArraysIndividually,
  pipelineFallbackOptions,
  snapshotPipelineCommands,
  snapshotPipelineOptions
} from "./pipeline-execution.js";
import { assertCommandHasStableConnectionState } from "./protocol.js";
import type { RoutingRoute, RoutingTopology } from "./topology.js";
import { isCasMutation } from "./command-retry-policy.js";

export class ReconnectingExecutor implements CommandExecutor {
  private readonly baseDelayMs: number;
  private closed = false;
  private closePromise?: Promise<void>;
  private readonly jitterPct: number;
  private readonly maxDelayMs: number;
  private readonly maxRetries: number;
  private readonly retryAbortController = new AbortController();
  private executorPromise: Promise<CommandExecutor>;
  private reconnectPromise?: Promise<CommandExecutor>;

  constructor(
    private readonly createExecutor: (signal?: AbortSignal) => Promise<CommandExecutor>,
    options: ReconnectOptions = {}
  ) {
    const captured = { ...options };
    this.baseDelayMs = normalizeNonNegativeInteger(captured.baseDelayMs ?? 25, 25);
    this.maxDelayMs = Math.max(
      this.baseDelayMs,
      normalizeNonNegativeInteger(captured.maxDelayMs ?? 1_000, 1_000)
    );
    this.jitterPct = Math.min(100, normalizeNonNegativeInteger(captured.jitterPct ?? 20, 20));
    this.maxRetries = normalizeNonNegativeInteger(captured.maxRetries ?? 1, 1);
    this.executorPromise = createExecutor(this.retryAbortController.signal);
    void this.executorPromise.catch(() => undefined);
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    const snapshot = snapshotCommandArguments(args);
    assertCommandHasStableConnectionState(snapshot);
    const casMutation = isCasMutation(snapshot);
    return await this.withReconnect(
      (executor) => executeCommandArgs(executor, snapshot),
      casMutation ? isClosedConnectionError : isReconnectableClosedConnectionError,
      !casMutation
    );
  }

  async ready(): Promise<void> {
    await this.executorPromise;
  }

  async executePipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[]> {
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options) ?? {};
    for (const command of snapshot) assertCommandHasStableConnectionState(command);
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    const initialExecutor = await this.executorPromise;
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    if (initialExecutor.executePipeline == null) {
      return await executeCommandArraysIndividually(
        async (command) => await this.executeCommandArgs(command),
        snapshot,
        pipelineFallbackOptions(snapshot, snapshotOptions)
      );
    }
    try {
      return await initialExecutor.executePipeline(snapshot, snapshotOptions);
    } catch (error) {
      // A native pipeline can span requests; never replay possibly confirmed effects.
      if (!this.closed && this.maxRetries > 0 && isReconnectableClosedConnectionError(error)) {
        await this.reconnect(initialExecutor).catch(() => undefined);
      }
      throw error;
    }
  }

  async executeFusedPipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[] | undefined> {
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options) ?? {};
    for (const command of snapshot) assertCommandHasStableConnectionState(command);
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    const initialExecutor = await this.executorPromise;
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    if (initialExecutor.executeFusedPipeline == null) return undefined;
    try {
      return await initialExecutor.executeFusedPipeline(snapshot, snapshotOptions);
    } catch (error) {
      if (!this.closed && this.maxRetries > 0 && isReconnectableClosedConnectionError(error)) {
        await this.reconnect(initialExecutor).catch(() => undefined);
      }
      throw error;
    }
  }

  async refreshTopology(): Promise<RoutingTopology> {
    return await this.withReconnect(async (executor) => {
      if (executor.refreshTopology == null) {
        throw new FerricStoreError("topology refresh requires a topology-aware native executor");
      }
      return await executor.refreshTopology();
    }, isReadOnlyReconnectableClosedConnectionError);
  }

  async route(key: string | Buffer): Promise<RoutingRoute> {
    return await this.withReconnect(async (executor) => {
      if (executor.route == null) {
        throw new FerricStoreError("route lookup requires a topology-aware native executor");
      }
      return await executor.route(key);
    }, isReadOnlyReconnectableClosedConnectionError);
  }

  async close(): Promise<void> {
    if (this.closePromise != null) {
      await this.closePromise;
      return;
    }
    this.closed = true;
    this.retryAbortController.abort(new RequestNotSentError("FerricStore client is closed"));
    const executorPromise = this.executorPromise;
    const reconnectPromise = this.reconnectPromise;
    this.closePromise = (async () => {
      let closeError: unknown;
      let closeFailed = false;
      const executor = await executorPromise.catch(() => undefined);
      try {
        await Promise.resolve(executor?.close?.());
      } catch (error) {
        closeFailed = true;
        closeError = error;
      }
      await reconnectPromise?.catch(() => undefined);
      if (closeFailed) throw closeError;
    })();
    await this.closePromise;
  }

  private async withReconnect<T>(
    operation: (executor: CommandExecutor) => Promise<T>,
    reconnectable: (error: unknown) => boolean = isReconnectableClosedConnectionError,
    replayAllowed = true
  ): Promise<T> {
    let executor = await this.executorPromise;
    let retries = 0;
    for (;;) {
      if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
      try {
        return await operation(executor);
      } catch (error) {
        if (this.closed || retries >= this.maxRetries || !reconnectable(error)) throw error;
        if (!replayAllowed) {
          await this.reconnect(executor).catch(() => undefined);
          throw error;
        }
        for (;;) {
          retries += 1;
          try {
            executor = await this.reconnect(executor);
            break;
          } catch (reconnectError) {
            if (this.closed || retries >= this.maxRetries) throw reconnectError;
            await this.waitBeforeReconnect(retries);
          }
        }
      }
    }
  }

  private async waitBeforeReconnect(failedAttempts: number): Promise<void> {
    const exponent = Math.min(30, Math.max(0, failedAttempts - 1));
    const base = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** exponent);
    const jitter = base * (this.jitterPct / 100) * Math.random();
    try {
      await sleep(Math.min(this.maxDelayMs, base + jitter), this.retryAbortController.signal);
    } catch (error) {
      if (this.closed) throw new RequestNotSentError("FerricStore client is closed", { cause: error });
      throw error;
    }
  }

  private async reconnect(staleExecutor: CommandExecutor): Promise<CommandExecutor> {
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    const competingReconnect = this.reconnectPromise;
    if (competingReconnect != null) return await competingReconnect;
    const observedExecutorPromise = this.executorPromise;
    const currentExecutor = await observedExecutorPromise.catch(() => undefined);
    if (this.closed) throw new RequestNotSentError("FerricStore client is closed");
    if (this.reconnectPromise != null) return await this.reconnectPromise;
    if (this.executorPromise !== observedExecutorPromise) return await this.executorPromise;
    if (currentExecutor != null && currentExecutor !== staleExecutor) return currentExecutor;
    this.reconnectPromise = (async () => {
      await Promise.resolve(staleExecutor.close?.()).catch(() => undefined);
      const nextExecutor = await this.createExecutor(this.retryAbortController.signal);
      if (this.closed) {
        await Promise.resolve(nextExecutor.close?.()).catch(() => undefined);
        throw new RequestNotSentError("FerricStore client is closed");
      }
      this.executorPromise = Promise.resolve(nextExecutor);
      return nextExecutor;
    })().finally(() => {
      this.reconnectPromise = undefined;
    });
    return await this.reconnectPromise;
  }
}

export function isReconnectableClosedConnectionError(error: unknown): boolean {
  return error instanceof ConnectionClosedError && error.requestDisposition === "unsent";
}

function isClosedConnectionError(error: unknown): boolean {
  return error instanceof ConnectionClosedError;
}

function isReadOnlyReconnectableClosedConnectionError(error: unknown): boolean {
  const pending: unknown[] = [error];
  const seen = new Set<unknown>();
  for (let visited = 0; visited < 16 && pending.length > 0; visited += 1) {
    const current = pending.pop();
    if (isReconnectableClosedConnectionError(current)) return true;
    if (
      current == null ||
      (typeof current !== "object" && typeof current !== "function") ||
      seen.has(current)
    ) continue;
    seen.add(current);
    if (current instanceof Error && current.cause != null) pending.push(current.cause);
    if (current instanceof FerricStoreError && current.raw != null) pending.push(current.raw);
  }
  return false;
}

function normalizeNonNegativeInteger(value: number, fallback: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : fallback;
}
