import { completeJobsResultError, type ClaimDueOptions } from "./client.js";
import type { Queue, QueueJob } from "./queue.js";
import type { WorkerConfig } from "./types.js";
import {
  LeaseRenewalGuard,
  type ContinuousWorkerHandleResult,
  workerBatchSize,
  workerClaimBlockMs,
  workerLeaseMs,
  workerSignalAborted
} from "./worker-internal.js";

interface ContinuousQueueCompletion {
  guard: LeaseRenewalGuard;
  job: QueueJob;
  reject: (error: unknown) => void;
  resolve: (result?: ContinuousWorkerHandleResult<QueueJob>) => void;
}

export function queueClaimOptions(
  queue: Queue,
  options: WorkerConfig,
  overrides: {
    readonly limit?: number;
    readonly partitionKey?: string;
    readonly partitionKeys?: string[];
    readonly useBlocking?: boolean;
  } = {}
): ClaimDueOptions {
  const compact = options.claimPayload === false;
  return {
    blockMs: workerClaimBlockMs(options, overrides.useBlocking !== false),
    includeState: false,
    includeAttributes: options.claimAttributes,
    jobOnly: compact,
    leaseMs: workerLeaseMs(options),
    limit: overrides.limit ?? workerBatchSize(options, queue.client.flowManyBatchLimit),
    nowMs: options.nowMs,
    partitionKey: overrides.partitionKey ?? options.partitionKey,
    partitionKeys: overrides.partitionKeys ?? options.partitionKeys,
    payload: compact ? false : true,
    priority: options.priority,
    reclaimExpired: options.reclaimExpired,
    reclaimRatio: options.reclaimRatio,
    state: queue.state,
    valueMaxBytes: options.valueMaxBytes,
    values: options.claimValues,
    worker: options.worker ?? queue.defaultWorker
  };
}

export class QueueCompletionBatcher {
  private readonly batchSize: number;
  private readonly maxDepth: number;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly pending: ContinuousQueueCompletion[] = [];
  private closed = false;
  private refillEnabled: boolean;
  private scheduled?: NodeJS.Immediate;

  constructor(
    private readonly queue: Queue,
    private readonly options: WorkerConfig
  ) {
    this.batchSize = workerBatchSize(options, queue.client.flowManyBatchLimit);
    this.maxDepth = finitePositiveInteger(options.completeAsyncDepth ?? 1, 1);
    this.refillEnabled = options.fuseCompleteClaim !== false;
  }

  async complete(
    job: QueueJob,
    guard: LeaseRenewalGuard
  ): Promise<void | ContinuousWorkerHandleResult<QueueJob>> {
    if (this.closed) throw new Error("Queue completion batcher is closed");
    const completion = new Promise<void | ContinuousWorkerHandleResult<QueueJob>>((resolve, reject) => {
      this.pending.push({ guard, job, reject, resolve });
    });
    this.pump(false);
    this.schedulePartialFlush();
    return await completion;
  }

  disableRefill(): void {
    this.refillEnabled = false;
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.scheduled != null) {
      clearImmediate(this.scheduled);
      this.scheduled = undefined;
    }
    while (this.pending.length > 0 || this.inFlight.size > 0) {
      this.pump(true);
      await Promise.all([...this.inFlight]);
    }
  }

  private schedulePartialFlush(): void {
    if (this.closed || this.pending.length === 0 || this.scheduled != null) return;
    this.scheduled = setImmediate(() => {
      this.scheduled = undefined;
      this.pump(true);
    });
  }

  private pump(forcePartial: boolean): void {
    while (
      this.inFlight.size < this.maxDepth &&
      this.pending.length > 0 &&
      (forcePartial || this.pending.length >= this.batchSize)
    ) {
      const entries = this.pending.splice(0, this.batchSize);
      const operation = this.flushBatch(entries).finally(() => {
        this.inFlight.delete(operation);
        this.pump(false);
        this.schedulePartialFlush();
      });
      this.inFlight.add(operation);
    }
  }

  private async flushBatch(entries: ContinuousQueueCompletion[]): Promise<void> {
    const stopped = await Promise.allSettled(entries.map(async (entry) => await entry.guard.stop()));
    const ready: ContinuousQueueCompletion[] = [];
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index];
      const result = stopped[index];
      if (entry == null || result == null) continue;
      if (result.status === "rejected") entry.reject(result.reason);
      else ready.push(entry);
    }
    if (ready.length === 0) return;

    let claimed: QueueJob[] = [];
    let operationError: Error | undefined;
    try {
      if (this.refillEnabled && !this.closed && !workerSignalAborted(this.options.signal)) {
        const result = await this.queue.client.completeJobsAndClaimJobs(
          ready.map((entry) => entry.job),
          this.queue.type,
          queueClaimOptions(this.queue, this.options, { limit: ready.length, useBlocking: false }),
          { independent: this.options.completeIndependent ?? true }
        );
        claimed = result.claimed;
        operationError = result.completionError ?? result.claimError;
      } else {
        const response = await this.queue.client.completeJobs(ready.map((entry) => entry.job), {
          independent: this.options.completeIndependent ?? true,
          returnOkOnSuccess: true
        });
        operationError = completeJobsResultError(response, ready.length);
      }
    } catch (error) {
      this.disableRefill();
      for (const entry of ready) entry.reject(error);
      return;
    }

    if (operationError != null) this.disableRefill();
    for (let index = 0; index < ready.length; index += 1) {
      const entry = ready[index];
      if (entry == null) continue;
      if (index === 0 && (claimed.length > 0 || operationError != null)) {
        entry.resolve({
          ...(claimed.length === 0 ? {} : { items: claimed }),
          ...(operationError == null ? {} : { error: operationError })
        });
      } else if (operationError != null) entry.resolve({ error: operationError });
      else entry.resolve();
    }
  }
}

function finitePositiveInteger(value: number, fallback: number): number {
  return Number.isFinite(value) ? Math.max(1, Math.trunc(value)) : fallback;
}
