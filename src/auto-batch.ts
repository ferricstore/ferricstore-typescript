import { Buffer } from "node:buffer";
import {
  executeCommandArraysIndividually,
  executeCommandArgs,
  pipelineFallbackOptions,
  pipelineItemRejectionFlags,
  snapshotCommandArguments,
  snapshotPipelineCommands,
  snapshotPipelineOptions,
  type CommandExecutor,
  type ExecutePipelineOptions
} from "./adapters.js";
import type { AutoBatchOptions } from "./client-options.js";
import {
  neverAutoBatchCommandPrefixes,
  neverAutoBatchCommands,
  safeAutoBatchCommands
} from "./command-metadata.js";
import { autoBatchCommandName, autoBatchOrderingPlan } from "./auto-batch-ordering.js";
import { RequestNotSentError, mapException } from "./errors.js";
import { setLongTimeout, type Command, type CommandArgument, type LongTimer } from "./internal.js";
import { commandHasServerBlock, commandRequiresDedicatedConnection } from "./protocol.js";
import type { RoutingRoute, RoutingTopology } from "./topology.js";

interface NormalizedAutoBatchOptions {
  readonly maxCommands: number;
  readonly maxDelayMs: number;
  readonly mode: "safe" | "all";
}

interface AutoBatchItem {
  readonly command: Command;
  readonly reject: (reason: unknown) => void;
  readonly resolve: (value: unknown) => void;
}

interface AutoBatchKeyState {
  lastWrite?: Promise<void>;
  readonly reads: Set<Promise<void>>;
}

class AutoBatchExecutor implements CommandExecutor {
  private globalBatchTail: Promise<void> = Promise.resolve();
  private readonly batchKeyStates = new Map<string, AutoBatchKeyState>();
  private closed = false;
  private closePromise?: Promise<void>;
  private readonly inFlightBatches = new Set<Promise<void>>();
  private readonly inFlightHelpers = new Set<Promise<unknown>>();
  private orderingBarrier: Promise<void> | undefined;
  private readonly pending: AutoBatchItem[] = [];
  private scheduled = false;
  private timer: LongTimer | undefined;

  constructor(
    private readonly executor: CommandExecutor,
    private readonly options: NormalizedAutoBatchOptions
  ) {}

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    const command = snapshotCommandArguments(args);
    if (this.executor.executePipeline == null) {
      return await executeCommandArgs(this.executor, command);
    }
    if (!autoBatchCommandAllowed(command, this.options.mode)) {
      if (commandHasServerBlock(command)) {
        return await this.executeBlockingAfterPendingBatches(
          async () => await executeCommandArgs(this.executor, command)
        );
      }
      return await this.executeAfterPendingBatches(
        async () => await executeCommandArgs(this.executor, command)
      );
    }
    const orderingBarrier = this.orderingBarrier;
    if (orderingBarrier != null) {
      await orderingBarrier;
      if (this.closed) {
        throw new RequestNotSentError("FerricStore client is closed");
      }
    }

    return await new Promise<unknown>((resolve, reject) => {
      this.pending.push({ command, reject, resolve });

      if (this.pending.length >= this.options.maxCommands) {
        void this.flushNow();
      } else {
        this.scheduleFlush();
      }
    });
  }

  async executePipeline(commands: readonly Command[], options?: ExecutePipelineOptions): Promise<unknown[]> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options);
    return await this.executeAfterPendingBatches(async () => {
      if (this.executor.executePipeline != null) {
        return await this.executor.executePipeline(snapshot, snapshotOptions);
      }

      return await executeCommandArraysIndividually(
        async (command) => await executeCommandArgs(this.executor, command),
        snapshot,
        pipelineFallbackOptions(snapshot, snapshotOptions)
      );
    });
  }

  async executeFusedPipeline(
    commands: readonly Command[],
    options?: ExecutePipelineOptions
  ): Promise<unknown[] | undefined> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    if (this.executor.executeFusedPipeline == null) return undefined;
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options);
    return await this.executeAfterPendingBatches(
      async () => await this.executor.executeFusedPipeline?.(snapshot, snapshotOptions)
    );
  }

  async refreshTopology(): Promise<RoutingTopology> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    const refreshTopology = this.executor.refreshTopology?.bind(this.executor);
    if (refreshTopology == null) {
      throw new Error("topology refresh requires a topology-aware native executor");
    }
    return await this.executeHelper(async () => await refreshTopology());
  }

  async route(key: string | Buffer): Promise<RoutingRoute> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    const route = this.executor.route?.bind(this.executor);
    if (route == null) {
      throw new Error("route lookup requires a topology-aware native executor");
    }
    return await this.executeHelper(async () => await route(key));
  }

  async close(): Promise<void> {
    if (this.closePromise != null) {
      await this.closePromise;
      return;
    }
    this.closed = true;
    this.closePromise = (async () => {
      this.scheduled = false;
      if (this.timer != null) {
        this.timer.cancel();
        this.timer = undefined;
      }
      this.failPending(new RequestNotSentError("FerricStore client closed before auto-batch flush"));
      await this.orderingBarrier;
      await this.waitForInFlightBatches();
      await Promise.allSettled([...this.inFlightHelpers]);
      await this.executor.close?.();
    })();
    await this.closePromise;
  }

  private async executeAfterPendingBatches<T>(operation: () => Promise<T>): Promise<T> {
    if (this.closed) {
      throw new RequestNotSentError("FerricStore client is closed");
    }
    const previousBarrier = this.orderingBarrier;
    let releaseBarrier: (() => void) | undefined;
    const barrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
    this.orderingBarrier = barrier;

    try {
      await previousBarrier;
      await this.flushNow();
      return await operation();
    } finally {
      releaseBarrier?.();
      if (this.orderingBarrier === barrier) {
        this.orderingBarrier = undefined;
      }
    }
  }

  private executeHelper<T>(operation: () => Promise<T>): Promise<T> {
    if (this.closed) {
      return Promise.reject(new RequestNotSentError("FerricStore client is closed"));
    }
    const task = Promise.resolve().then(operation);
    this.inFlightHelpers.add(task);
    void task.then(
      () => this.inFlightHelpers.delete(task),
      () => this.inFlightHelpers.delete(task)
    );
    return task;
  }

  private async executeBlockingAfterPendingBatches<T>(operation: () => Promise<T>): Promise<T> {
    const previousBarrier = this.orderingBarrier;
    let releaseBarrier: (() => void) | undefined;
    const barrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
    this.orderingBarrier = barrier;
    let dispatched: Promise<T>;

    try {
      await previousBarrier;
      await this.flushNow();
      if (this.closed) {
        throw new RequestNotSentError("FerricStore client is closed");
      }
      dispatched = operation();
    } finally {
      releaseBarrier?.();
      if (this.orderingBarrier === barrier) {
        this.orderingBarrier = undefined;
      }
    }

    return await dispatched;
  }

  private scheduleFlush(): void {
    if (this.scheduled) {
      return;
    }

    this.scheduled = true;

    if (this.options.maxDelayMs <= 0) {
      queueMicrotask(() => {
        this.scheduled = false;
        void this.flushNow();
      });
      return;
    }

    this.timer = setLongTimeout(() => {
      this.scheduled = false;
      this.timer = undefined;
      void this.flushNow();
    }, this.options.maxDelayMs);
  }

  private async flushNow(): Promise<void> {
    this.scheduled = false;

    if (this.timer != null) {
      this.timer.cancel();
      this.timer = undefined;
    }

    if (this.pending.length === 0) {
      await this.waitForInFlightBatches();
      return;
    }

    const batch = this.pending.splice(0, this.options.maxCommands);
    if (this.pending.length > 0) {
      this.scheduleFlush();
    }

    const orderingPlan = autoBatchOrderingPlan(batch);
    const accesses = orderingPlan?.accesses;
    const keyStates = new Map<string, AutoBatchKeyState>();
    const dependencies: Promise<void>[] = accesses == null
      ? [...this.inFlightBatches]
      : [this.globalBatchTail];
    if (accesses != null) {
      for (const [key, access] of accesses) {
        let state = this.batchKeyStates.get(key);
        if (state == null) {
          state = { reads: new Set() };
          this.batchKeyStates.set(key, state);
        }
        keyStates.set(key, state);
        if (state.lastWrite != null) dependencies.push(state.lastWrite);
        if (access === "write") {
          for (const read of state.reads) dependencies.push(read);
        }
      }
    }
    const operation = Promise.all([...new Set(dependencies)].map(async (dependency) => {
      await dependency.catch(() => undefined);
    })).then(async () => await this.sendBatch(batch, orderingPlan?.fallbackDependencies)).finally(() => {
      this.inFlightBatches.delete(operation);
      if (accesses == null) {
        if (this.globalBatchTail === operation) this.globalBatchTail = Promise.resolve();
      } else {
        for (const [key, access] of accesses) {
          const state = keyStates.get(key);
          if (state == null) continue;
          if (access === "read") {
            state.reads.delete(operation);
          } else if (state.lastWrite === operation) {
            state.lastWrite = undefined;
          }
          if (state.lastWrite == null && state.reads.size === 0 && this.batchKeyStates.get(key) === state) {
            this.batchKeyStates.delete(key);
          }
        }
      }
    });
    if (accesses == null) {
      this.globalBatchTail = operation;
    } else {
      for (const [key, access] of accesses) {
        const state = keyStates.get(key);
        if (state == null) continue;
        if (access === "read") {
          state.reads.add(operation);
        } else {
          state.reads.clear();
          state.lastWrite = operation;
        }
      }
    }
    this.inFlightBatches.add(operation);
    await operation;
    await this.waitForInFlightBatches();
  }

  private async waitForInFlightBatches(): Promise<void> {
    while (this.inFlightBatches.size > 0) {
      await Promise.all([...this.inFlightBatches]);
    }
  }

  private async sendBatch(
    batch: readonly AutoBatchItem[],
    fallbackDependencies: readonly (readonly number[])[] | undefined
  ): Promise<void> {
    try {
      const results = await this.executor.executePipeline?.(
        batch.map((item) => item.command),
        fallbackDependencies == null
          ? { ordered: true, throwOnItemError: false }
          : { fallbackDependencies, throwOnItemError: false }
      );

      if (!Array.isArray(results) || results.length !== batch.length) {
        const error = new Error("FerricStore auto-batch response length mismatch");
        for (const item of batch) item.reject(error);
        return;
      }

      const rejected = pipelineItemRejectionFlags(results);
      for (let index = 0; index < batch.length; index += 1) {
        const item = batch[index];
        const result = results[index];
        if (item == null) continue;
        if (!Object.hasOwn(results, index)) {
          item.reject(new Error(`FerricStore auto-batch response item ${index} is missing`));
          continue;
        }
        if (result instanceof Error) {
          item.reject(autoBatchErrorFromUnknown(result));
        } else if (rejected?.[index] === true) {
          item.reject(result);
        } else {
          item.resolve(result);
        }
      }
    } catch (error) {
      for (const item of batch) item.reject(error);
    }
  }

  private failPending(error: Error): void {
    const pending = this.pending.splice(0);
    for (const item of pending) item.reject(error);
  }
}

export function maybeAutoBatchExecutor(
  executor: CommandExecutor,
  options: boolean | AutoBatchOptions | undefined
): CommandExecutor {
  const normalized = normalizeAutoBatchOptions(options);
  return normalized == null ? executor : new AutoBatchExecutor(executor, normalized);
}

function normalizeAutoBatchOptions(
  options: boolean | AutoBatchOptions | undefined
): NormalizedAutoBatchOptions | null {
  if (options == null || options === false) {
    return null;
  }

  if (options === true) {
    return { maxCommands: 512, maxDelayMs: 0, mode: "safe" };
  }

  if (options.enabled === false) {
    return null;
  }

  return {
    maxCommands: finitePositiveInteger(options.maxCommands, 512),
    maxDelayMs: finiteNonNegativeInteger(options.maxDelayMs, 0),
    mode: options.mode === "all" ? "all" : "safe"
  };
}
function autoBatchCommandAllowed(command: Command, mode: "safe" | "all"): boolean {
  const name = autoBatchCommandName(command);
  if (
    name == null
    || neverAutoBatchCommands.has(name)
    || commandHasServerBlock(command)
    || commandRequiresDedicatedConnection(command, name)
  ) {
    return false;
  }

  const prefix = name.includes(".") ? name.slice(0, name.indexOf(".")) : name;
  if (neverAutoBatchCommandPrefixes.has(prefix)) {
    return false;
  }

  return mode === "all" || safeAutoBatchCommands.has(name);
}

export function finitePositiveInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value)
    ? fallback
    : Math.max(1, Math.trunc(value));
}
function finiteNonNegativeInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value)
    ? fallback
    : Math.max(0, Math.trunc(value));
}

function autoBatchErrorFromUnknown(error: unknown): Error {
  const mapped = mapException(error);
  return mapped instanceof Error ? mapped : new Error(String(error));
}
