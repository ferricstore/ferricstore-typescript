import { randomUUID } from "node:crypto";
import type { FerricStoreClient } from "./client.js";
import { complete, fail, isOutcome, retry, type CompleteOutcome, type FailOutcome, type Outcome, type RetryOutcome } from "./outcomes.js";
import {
  normalizeExceptionPolicy,
  type ClaimedItem,
  type CreateItem,
  type ExceptionPolicy,
  type FlowRecord,
  type StateMeta,
  type WorkerConfig
} from "./types.js";
import { sleep } from "./internal.js";

export type QueueJob = FlowRecord | ClaimedItem;
export type QueueHandler = (job: QueueJob) => Promise<unknown> | unknown;
export type QueueBatchHandler = (jobs: QueueJob[]) => Promise<unknown> | unknown;

export interface QueueOptions {
  type: string;
  state?: string;
  worker?: string;
}

export interface QueueWorkerResult {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
}

interface PendingCompletion {
  done: boolean;
  error?: unknown;
  promise: Promise<number>;
  value: number;
}

export class QueueClient {
  readonly flow: FerricStoreClient;

  constructor(flow: FerricStoreClient) {
    this.flow = flow;
  }

  queue(options: QueueOptions | string): Queue {
    return new Queue(this.flow, typeof options === "string" ? { type: options } : options);
  }
}

export class Queue {
  readonly client: FerricStoreClient;
  readonly type: string;
  readonly state: string;
  readonly defaultWorker: string;

  constructor(client: FerricStoreClient, options: QueueOptions) {
    this.client = client;
    this.type = options.type;
    this.state = options.state ?? "queued";
    this.defaultWorker = options.worker ?? `${this.type}-${randomUUID()}`;
  }

  async enqueue(id: string, options: {
    payload?: unknown;
    partitionKey?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    retentionTtlMs?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
    returnRecord?: boolean;
  } = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.client.enqueue(id, {
      ...options,
      state: this.state,
      type: this.type
    });
  }

  async enqueueMany(items: CreateItem[], options: {
    partitionKey?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    independent?: boolean;
    retentionTtlMs?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
  } = {}): Promise<unknown[] | unknown> {
    return await this.client.enqueueMany(items, {
      ...options,
      state: this.state,
      type: this.type
    });
  }

  worker(options: WorkerConfig = {}): QueueWorker {
    return new QueueWorker(this, options);
  }
}

export class QueueWorker {
  readonly queue: Queue;
  readonly options: WorkerConfig;
  private readonly pendingCompletions: PendingCompletion[] = [];

  constructor(queue: Queue, options: WorkerConfig) {
    this.queue = queue;
    this.options = normalizeWorkerConfig(options);
  }

  async runOnce(handler: QueueHandler): Promise<QueueWorkerResult> {
    const completed = await this.drainPendingCompletions(false);
    const jobs = await this.claimJobs();
    const result: QueueWorkerResult = { claimed: jobs.length, completed, failed: 0, retried: 0 };
    if (jobs.length === 0) {
      return result;
    }
    const completions: ClaimedItem[] = [];
    const concurrency = Math.max(1, this.options.concurrency ?? 1);
    let cursor = 0;

    const runNext = async (): Promise<void> => {
      while (cursor < jobs.length) {
        const job = jobs[cursor];
        cursor += 1;
        if (job == null) continue;
        await this.applyJob(job, handler, result, completions);
      }
    };

    await Promise.all(Array.from({ length: Math.min(concurrency, jobs.length) }, () => runNext()));
    await this.flushCompletions(completions, result);
    return result;
  }

  async runBatchOnce(handler: QueueBatchHandler): Promise<QueueWorkerResult> {
    return await this.runBatchOnceWithClaim(handler);
  }

  async runBatchOnceForPartitionKeys(
    handler: QueueBatchHandler,
    partitionKeys: readonly string[],
    options: { readonly claimCredit?: number } = {}
  ): Promise<QueueWorkerResult> {
    if (partitionKeys.length === 0) {
      return { claimed: 0, completed: await this.drainPendingCompletions(false), failed: 0, retried: 0 };
    }
    return await this.runBatchOnceWithClaim(handler, {
      limit: options.claimCredit == null ? undefined : Math.min(this.options.batchSize ?? 10, Math.max(1, options.claimCredit)),
      partitionKey: partitionKeys.length === 1 ? partitionKeys[0] : undefined,
      partitionKeys: partitionKeys.length === 1 ? undefined : [...partitionKeys]
    });
  }

  async flush(): Promise<number> {
    return await this.drainPendingCompletions(true);
  }

  private async runBatchOnceWithClaim(
    handler: QueueBatchHandler,
    claimOptions: {
      readonly limit?: number;
      readonly partitionKey?: string;
      readonly partitionKeys?: string[];
    } = {}
  ): Promise<QueueWorkerResult> {
    const completed = await this.drainPendingCompletions(false);
    const jobs = await this.claimJobs(claimOptions);
    const result: QueueWorkerResult = { claimed: jobs.length, completed, failed: 0, retried: 0 };
    if (jobs.length === 0) {
      return result;
    }

    let batchComplete = false;
    let outcome: CompleteOutcome | RetryOutcome | FailOutcome | undefined;
    try {
      const value = await handler(jobs);
      if (value === undefined) {
        batchComplete = true;
      } else {
        const nextOutcome: Outcome = isOutcome(value) ? value : complete({ result: value });
        if (nextOutcome.kind === "transition") {
          throw new Error("Queue batch handlers cannot return transition(); use Workflow for state transitions");
        }
        if (isBatchableComplete(nextOutcome)) {
          batchComplete = true;
        } else {
          outcome = nextOutcome;
        }
      }
    } catch (error) {
      for (const job of jobs) {
        await this.applyException(job, error);
      }
      if (normalizeExceptionPolicy(this.options.exceptionPolicy) === "fail") {
        result.failed += jobs.length;
      } else {
        result.retried += jobs.length;
      }
      return result;
    }

    if (batchComplete) {
      await this.flushCompletions(jobs, result);
      return result;
    }
    if (outcome == null) {
      throw new Error("Queue batch handler did not produce a valid outcome");
    }

    for (const job of jobs) {
      await this.applyOutcome(job, outcome);
    }
    if (outcome.kind === "retry") {
      result.retried += jobs.length;
    } else if (outcome.kind === "fail") {
      result.failed += jobs.length;
    } else {
      result.completed += jobs.length;
    }
    return result;
  }

  async run(handler: QueueHandler): Promise<void> {
    const idleSleepMs = this.options.idleSleepMs ?? 250;
    let currentIdleSleepMs = idleSleepMs;
    const maxIdleSleepMs = this.options.maxIdleSleepMs ?? 5_000;

    try {
      while (this.options.signal?.aborted !== true) {
        const result = await this.runOnce(handler);
        if (result.claimed === 0) {
          await sleep(currentIdleSleepMs, this.options.signal);
          currentIdleSleepMs = Math.min(maxIdleSleepMs, currentIdleSleepMs * 2);
        } else {
          currentIdleSleepMs = idleSleepMs;
        }
      }
    } finally {
      await this.flush();
    }
  }

  private async claimJobs(
    options: {
      readonly limit?: number;
      readonly partitionKey?: string;
      readonly partitionKeys?: string[];
    } = {}
  ): Promise<QueueJob[]> {
    const compact = this.options.claimPayload === false;
    return await this.queue.client.claimDue(this.queue.type, {
      blockMs: this.options.blockMs,
      includeState: false,
      jobOnly: compact,
      leaseMs: this.options.leaseMs ?? 30_000,
      limit: options.limit ?? this.options.batchSize ?? 10,
      nowMs: this.options.nowMs,
      partitionKey: options.partitionKey ?? this.options.partitionKey,
      partitionKeys: options.partitionKeys ?? this.options.partitionKeys,
      payload: compact ? false : true,
      priority: this.options.priority,
      reclaimExpired: this.options.reclaimExpired,
      reclaimRatio: this.options.reclaimRatio,
      state: this.queue.state,
      valueMaxBytes: this.options.valueMaxBytes,
      values: this.options.claimValues,
      worker: this.options.worker ?? this.queue.defaultWorker
    });
  }

  private async applyJob(
    job: QueueJob,
    handler: QueueHandler,
    result: QueueWorkerResult,
    completions: ClaimedItem[]
  ): Promise<void> {
    let batchComplete = false;
    let outcome: CompleteOutcome | RetryOutcome | FailOutcome | undefined;
    try {
      const value = await handler(job);
      if (value === undefined) {
        batchComplete = true;
      } else {
        const nextOutcome: Outcome = isOutcome(value) ? value : complete({ result: value });
        if (nextOutcome.kind === "transition") {
          throw new Error("Queue handlers cannot return transition(); use Workflow for state transitions");
        }
        if (isBatchableComplete(nextOutcome)) {
          batchComplete = true;
        } else {
          outcome = nextOutcome;
        }
      }
    } catch (error) {
      await this.applyException(job, error);
      if (normalizeExceptionPolicy(this.options.exceptionPolicy) === "fail") {
        result.failed += 1;
      } else {
        result.retried += 1;
      }
      return;
    }

    if (batchComplete) {
      completions.push(job);
      return;
    }
    if (outcome == null) {
      throw new Error("Queue handler did not produce a valid outcome");
    }

    await this.applyOutcome(job, outcome);
    if (outcome.kind === "retry") {
      result.retried += 1;
    } else if (outcome.kind === "fail") {
      result.failed += 1;
    } else {
      result.completed += 1;
    }
  }

  private async flushCompletions(jobs: ClaimedItem[], result: QueueWorkerResult): Promise<void> {
    if (jobs.length === 0) {
      return;
    }
    const depth = Math.max(0, this.options.completeAsyncDepth ?? 0);
    const batchSize = Math.max(1, this.options.batchSize ?? jobs.length);
    if (depth > 0) {
      for (let index = 0; index < jobs.length; index += batchSize) {
        while (this.pendingCompletions.length >= depth) {
          result.completed += await this.drainPendingCompletions(true);
        }
        this.enqueueCompletion(jobs.slice(index, index + batchSize));
      }
      return;
    }

    if (jobs.length <= batchSize) {
      await this.queue.client.completeJobs(jobs, {
        independent: this.options.completeIndependent ?? true,
        returnOkOnSuccess: true
      });
      result.completed += jobs.length;
      return;
    }

    const syncDepth = 1;
    const pending = new Set<Promise<void>>();
    for (let index = 0; index < jobs.length; index += batchSize) {
      const batch = jobs.slice(index, index + batchSize);
      const promise = this.queue.client.completeJobs(batch, {
        independent: this.options.completeIndependent ?? true,
        returnOkOnSuccess: true
      }).then(() => {
        result.completed += batch.length;
      });
      pending.add(promise.finally(() => pending.delete(promise)));
      if (pending.size >= syncDepth) {
        await Promise.race(pending);
      }
    }
    await Promise.all(pending);
  }

  private enqueueCompletion(jobs: ClaimedItem[]): void {
    const pending: PendingCompletion = {
      done: false,
      promise: Promise.resolve(0),
      value: 0
    };
    pending.promise = this.queue.client.completeJobs(jobs, {
      independent: this.options.completeIndependent ?? true,
      returnOkOnSuccess: true
    }).then(
      () => {
        pending.done = true;
        pending.value = jobs.length;
        return jobs.length;
      },
      (error: unknown) => {
        pending.done = true;
        pending.error = error;
        return 0;
      }
    );
    this.pendingCompletions.push(pending);
  }

  private async drainPendingCompletions(block: boolean): Promise<number> {
    let completed = 0;
    while (true) {
      let drained = false;
      for (let index = 0; index < this.pendingCompletions.length; ) {
        const pending = this.pendingCompletions[index];
        if (pending == null) {
          break;
        }
        if (!pending.done) {
          index += 1;
          continue;
        }
        this.pendingCompletions.splice(index, 1);
        drained = true;
        if (pending.error != null) {
          if (pending.error instanceof Error) {
            throw pending.error;
          }
          if (typeof pending.error === "string") {
            throw new Error(pending.error);
          }
          throw new Error("Queue completion failed", { cause: pending.error });
        }
        completed += pending.value;
      }
      if (!block || this.pendingCompletions.length === 0) {
        return completed;
      }
      if (!drained) {
        await Promise.race(this.pendingCompletions.map((pending) => pending.promise));
      }
    }
  }

  private async applyOutcome(job: QueueJob, outcome: CompleteOutcome | RetryOutcome | FailOutcome): Promise<void> {
    if (outcome.kind === "retry") {
      await this.queue.client.retry(job.id, {
        error: outcome.error,
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        payload: outcome.payload,
        runAtMs: outcome.runAtMs,
        stateMeta: outcome.stateMeta,
        valueRefs: outcome.valueRefs,
        values: outcome.values
      });
      return;
    }
    if (outcome.kind === "fail") {
      await this.queue.client.fail(job.id, {
        error: outcome.error,
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        payload: outcome.payload,
        stateMeta: outcome.stateMeta,
        ttlMs: outcome.ttlMs,
        valueRefs: outcome.valueRefs,
        values: outcome.values
      });
      return;
    }
    await this.queue.client.complete(job.id, {
      fencingToken: job.fencingToken,
      leaseToken: job.leaseToken,
      partitionKey: job.partitionKey,
      payload: outcome.payload,
      result: outcome.result,
      stateMeta: outcome.stateMeta,
      ttlMs: outcome.ttlMs,
      valueRefs: outcome.valueRefs,
      values: outcome.values
    });
  }

  private async applyException(job: QueueJob, error: unknown): Promise<void> {
    const policy: ExceptionPolicy = normalizeExceptionPolicy(this.options.exceptionPolicy);
    if (policy === "raise") {
      throw error;
    }
    const payload = error instanceof Error ? { message: error.message, name: error.name, stack: error.stack } : error;
    if (policy === "fail") {
      await this.applyOutcome(job, fail({ error: payload }));
      return;
    }
    await this.applyOutcome(job, retry({ error: payload }));
  }
}

function isBatchableComplete(outcome: CompleteOutcome | RetryOutcome | FailOutcome): outcome is CompleteOutcome {
  return (
    outcome.kind === "complete" &&
    outcome.payload === undefined &&
    outcome.result === undefined &&
    outcome.ttlMs === undefined &&
    outcome.values === undefined &&
    outcome.valueRefs === undefined &&
    outcome.dropValues === undefined &&
    outcome.overrideValues === undefined &&
    outcome.stateMeta === undefined
  );
}

function normalizeWorkerConfig(options: WorkerConfig): WorkerConfig {
  if (options.profile !== "throughput") {
    return options;
  }
  return {
    batchSize: 500,
    claimPayload: false,
    completeAsyncDepth: 8,
    completeIndependent: true,
    ...options
  };
}
