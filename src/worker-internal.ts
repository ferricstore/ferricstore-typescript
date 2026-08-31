import { RawCodec, type Codec } from "./codecs.js";
import {
  normalizeExceptionPolicy,
  type WorkerConfig
} from "./types.js";
import { setLongTimeout, sleep, type LongTimer } from "./internal.js";
import { snapshotWorkerConfig } from "./worker-config.js";
export { LeaseRenewalError, LeaseRenewalGuard } from "./worker-lease-guard.js";

const DEFAULT_FLOW_MANY_BATCH_LIMIT = 1_000;

export function workerConcurrency(options: WorkerConfig): number {
  return positiveInteger(options.concurrency ?? options.workers, 1);
}

export function workerRefillStrategy(options: WorkerConfig): "continuous" | "wave" {
  return options.refillStrategy === "wave" ? "wave" : "continuous";
}

export function workerSignalAborted(signal?: AbortSignal): boolean {
  return signal?.aborted === true;
}

export function workerErrorPayload(error: unknown, options: WorkerConfig, codec: Codec): unknown {
  const payload = error instanceof Error ? {
    message: error.message,
    name: error.name,
    ...(options.includeErrorStack === true && error.stack != null ? { stack: error.stack } : {})
  } : error;
  if (!(codec instanceof RawCodec)) return payload;
  if (
    payload == null ||
    typeof payload === "string" ||
    Buffer.isBuffer(payload) ||
    payload instanceof Uint8Array
  ) {
    return payload;
  }
  try {
    const encoded = JSON.stringify(payload);
    if (encoded != null) return encoded;
  } catch {
    // Fall through to a safe textual representation for circular values and BigInts.
  }
  switch (typeof payload) {
    case "bigint":
    case "boolean":
    case "number":
      return String(payload);
    case "symbol":
      return payload.description ?? "Worker handler threw a Symbol";
    case "function":
      return payload.name === "" ? "Worker handler threw a function" : payload.name;
    case "string":
      return payload;
    case "object":
      return "Unserializable worker error";
    case "undefined":
      return "Worker handler threw undefined";
  }
  return "Unknown worker error";
}

export function workerBatchSize(
  options: WorkerConfig,
  flowManyBatchLimit = DEFAULT_FLOW_MANY_BATCH_LIMIT
): number {
  return Math.min(
    positiveInteger(options.batchSize, 10),
    positiveInteger(flowManyBatchLimit, DEFAULT_FLOW_MANY_BATCH_LIMIT)
  );
}

export function workerClaimLimit(
  options: WorkerConfig,
  flowManyBatchLimit = DEFAULT_FLOW_MANY_BATCH_LIMIT
): number {
  return Math.min(workerBatchSize(options, flowManyBatchLimit), workerConcurrency(options));
}

export function workerDrainBatches(options: WorkerConfig): number {
  return positiveInteger(options.claimDrainBatches, 1);
}

export function workerLeaseMs(options: WorkerConfig, fallback = 30_000): number {
  return positiveInteger(options.leaseMs, positiveInteger(fallback, 30_000));
}

export function workerIdleSleepMs(options: WorkerConfig): number {
  return nonNegativeDuration(options.idleSleepMs, 250);
}

/**
 * A native blocking claim cannot be abandoned safely: it may lease work just
 * before its late response arrives. Bound each server wait instead, so abort
 * is observed at a response boundary without losing a claimed job.
 */
export function workerClaimBlockMs(options: WorkerConfig, useBlocking: boolean): number | undefined {
  if (!useBlocking || options.blockMs == null) {
    return undefined;
  }
  if (options.signal == null || !Number.isFinite(options.blockMs) || options.blockMs < 0) {
    return options.blockMs;
  }
  const abortPollMs = positiveInteger(options.abortPollMs, 1_000);
  return options.blockMs === 0 ? abortPollMs : Math.min(Math.trunc(options.blockMs), abortPollMs);
}

export function workerMaxIdleSleepMs(options: WorkerConfig, idleSleepMs = workerIdleSleepMs(options)): number {
  return Math.max(idleSleepMs, nonNegativeDuration(options.maxIdleSleepMs, 5_000));
}

/** Advance exponential idle backoff without trapping a zero initial delay at zero. */
export function nextWorkerIdleSleepMs(currentIdleSleepMs: number, maxIdleSleepMs: number): number {
  if (maxIdleSleepMs === 0) return 0;
  return Math.min(maxIdleSleepMs, Math.max(1, currentIdleSleepMs * 2));
}

interface ContinuousWorkerPoolOptions<T> {
  claim: (limit: number, useBlocking: boolean) => Promise<readonly T[]>;
  concurrency: number;
  handle: (item: T) => Promise<void | ContinuousWorkerHandleResult<T>>;
  idleSleepMs?: number;
  maxClaimSize: number;
  maxIdleSleepMs?: number;
  onFailure?: () => void;
  refillPartialClaims?: boolean;
  refillDelayMs?: number;
  signal?: AbortSignal;
}

export interface ContinuousWorkerHandleResult<T> {
  readonly error?: unknown;
  readonly items?: readonly T[];
}

/** @internal */
export async function runContinuousWorkerPool<T>(options: ContinuousWorkerPoolOptions<T>): Promise<void> {
  const concurrency = positiveInteger(options.concurrency, 1);
  const maxClaimSize = positiveInteger(options.maxClaimSize, concurrency);
  const initialIdleSleepMs = nonNegativeDuration(options.idleSleepMs, 250);
  const maxIdleSleepMs = Math.max(
    initialIdleSleepMs,
    nonNegativeDuration(options.maxIdleSleepMs, 5_000)
  );
  const refillDelayMs = nonNegativeInteger(options.refillDelayMs, 0);
  const active = new Set<Promise<void>>();
  const pending: T[] = [];
  let pendingIndex = 0;
  const notifier = new WorkerNotifier();
  let failure: { error: unknown } | undefined;
  let currentIdleSleepMs = initialIdleSleepMs;
  let coalesceBeforeClaim = false;

  const recordFailure = (error: unknown): void => {
    if (failure == null) {
      failure = { error };
      try {
        options.onFailure?.();
      } catch {
        // Failure notification is advisory; preserve the originating error.
      }
    }
    notifier.notify();
  };
  const start = (item: T): void => {
    const task = Promise.resolve()
      .then(async () => {
        const result = await options.handle(item);
        if (result != null) {
          enqueue(result.items ?? []);
          if (Object.hasOwn(result, "error")) {
            recordFailure(result.error);
          }
        }
      })
      .catch(recordFailure)
      .finally(() => {
        active.delete(task);
        notifier.notify();
      });
    active.add(task);
  };
  const enqueue = (items: readonly T[]): void => {
    for (const item of items) pending.push(item);
  };
  const startAvailable = (): void => {
    while (active.size < concurrency && pendingIndex < pending.length) {
      const item = pending[pendingIndex] as T;
      pendingIndex += 1;
      start(item);
    }
    if (pendingIndex === pending.length) {
      pending.length = 0;
      pendingIndex = 0;
    }
  };
  const onAbort = (): void => notifier.notify();
  options.signal?.addEventListener("abort", onAbort);

  try {
    while (!workerSignalAborted(options.signal) && failure == null) {
      startAvailable();
      if (active.size >= concurrency) {
        await notifier.wait();
        coalesceBeforeClaim = true;
        continue;
      }

      if (coalesceBeforeClaim && active.size > 0) {
        await waitForRefillWindow(refillDelayMs, options.signal);
        coalesceBeforeClaim = false;
        if (workerSignalAborted(options.signal) || failure != null) {
          continue;
        }
      }

      const limit = Math.min(maxClaimSize, concurrency - active.size);
      let items: readonly T[];
      try {
        items = await options.claim(limit, active.size === 0);
      } catch (error) {
        recordFailure(error);
        break;
      }

      // A server may over-return. Retain every leased job, but never exceed handler concurrency.
      enqueue(items);
      startAvailable();
      if (items.length > 0) {
        currentIdleSleepMs = initialIdleSleepMs;
      }
      if (workerSignalAborted(options.signal) || failure != null) {
        break;
      }
      if (
        items.length >= limit ||
        (items.length > 0 && options.refillPartialClaims === true)
      ) {
        continue;
      }

      const waitResult = await notifier.wait(
        active.size >= concurrency ? undefined : currentIdleSleepMs
      );
      if (waitResult === "timeout") {
        currentIdleSleepMs = nextWorkerIdleSleepMs(currentIdleSleepMs, maxIdleSleepMs);
      } else {
        coalesceBeforeClaim = true;
      }
    }
  } catch (error) {
    recordFailure(error);
  } finally {
    options.signal?.removeEventListener("abort", onAbort);
    // Claims already received must still be finalized after shutdown or another
    // handler failure. An active handler may return replacement leases while
    // this drain is already in progress, so wait for the pool to become fully
    // quiescent instead of taking a one-time snapshot of pending work.
    while (true) {
      startAvailable();
      if (pendingIndex >= pending.length && active.size === 0) break;
      await notifier.wait();
    }
  }

  if (failure != null) {
    throw failure.error;
  }
}

export function normalizeWorkflowWorkerConfig<T extends WorkerConfig & { states?: string[] }>(options: T): T {
  const captured = snapshotWorkerConfig(options);
  if (captured.exceptionPolicy != null) normalizeExceptionPolicy(captured.exceptionPolicy);
  if (captured.profile !== "throughput") return captured;
  return Object.freeze({
    batchSize: 500,
    claimPayload: false,
    ...captured
  });
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(1, Math.trunc(value));
}

function nonNegativeInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(0, Math.trunc(value));
}

function nonNegativeDuration(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) || value < 0
    ? fallback
    : Math.trunc(value);
}

class WorkerNotifier {
  private readonly listeners = new Set<() => void>();

  notify(): void {
    const listeners = [...this.listeners];
    this.listeners.clear();
    for (const listener of listeners) {
      listener();
    }
  }

  async wait(timeoutMs?: number): Promise<"notified" | "timeout"> {
    return await new Promise((resolve) => {
      let timer: LongTimer | undefined;
      const listener = (): void => {
        timer?.cancel();
        resolve("notified");
      };
      this.listeners.add(listener);
      if (timeoutMs != null) {
        timer = setLongTimeout(() => {
          this.listeners.delete(listener);
          resolve("timeout");
        }, timeoutMs);
      }
    });
  }
}

async function waitForRefillWindow(delayMs: number, signal?: AbortSignal): Promise<void> {
  if (workerSignalAborted(signal)) {
    return;
  }
  if (delayMs === 0) {
    await new Promise<void>((resolve) => setImmediate(resolve));
    return;
  }
  try {
    await sleep(delayMs, signal);
  } catch (error) {
    if (!workerSignalAborted(signal)) {
      throw error;
    }
  }
}
