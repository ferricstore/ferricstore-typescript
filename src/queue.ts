import { randomUUID } from "node:crypto";
import type { FlowClient } from "./client.js";
import { complete, fail, isOutcome, retry, type CompleteOutcome, type FailOutcome, type RetryOutcome } from "./outcomes.js";
import { normalizeExceptionPolicy, type CreateItem, type ExceptionPolicy, type FlowRecord, type WorkerConfig } from "./types.js";
import { sleep } from "./internal.js";

export type QueueHandler = (job: FlowRecord) => Promise<unknown> | unknown;

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

export class QueueClient {
  readonly flow: FlowClient;

  constructor(flow: FlowClient) {
    this.flow = flow;
  }

  queue(options: QueueOptions | string): Queue {
    return new Queue(this.flow, typeof options === "string" ? { type: options } : options);
  }
}

export class Queue {
  readonly client: FlowClient;
  readonly type: string;
  readonly state: string;
  readonly defaultWorker: string;

  constructor(client: FlowClient, options: QueueOptions) {
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

  constructor(queue: Queue, options: WorkerConfig) {
    this.queue = queue;
    this.options = options;
  }

  async runOnce(handler: QueueHandler): Promise<QueueWorkerResult> {
    const jobs = (await this.queue.client.claimDue(this.queue.type, {
      blockMs: this.options.blockMs,
      leaseMs: this.options.leaseMs ?? 30_000,
      limit: this.options.batchSize ?? 10,
      payload: true,
      priority: this.options.priority,
      reclaimExpired: this.options.reclaimExpired,
      reclaimRatio: this.options.reclaimRatio,
      state: this.queue.state,
      valueMaxBytes: this.options.valueMaxBytes,
      values: this.options.claimValues,
      worker: this.options.worker ?? this.queue.defaultWorker
    })) as FlowRecord[];

    const result: QueueWorkerResult = { claimed: jobs.length, completed: 0, failed: 0, retried: 0 };
    for (const job of jobs) {
      try {
        const value = await handler(job);
        const outcome = isOutcome(value) ? value : complete({ result: value });
        if (outcome.kind === "transition") {
          throw new Error("Queue handlers cannot return transition(); use Workflow for state transitions");
        }
        await this.applyOutcome(job, outcome);
        if (outcome.kind === "retry") {
          result.retried += 1;
        } else if (outcome.kind === "fail") {
          result.failed += 1;
        } else {
          result.completed += 1;
        }
      } catch (error) {
        await this.applyException(job, error);
        if (normalizeExceptionPolicy(this.options.exceptionPolicy) === "fail") {
          result.failed += 1;
        } else {
          result.retried += 1;
        }
      }
    }
    return result;
  }

  async run(handler: QueueHandler): Promise<void> {
    const idleSleepMs = this.options.idleSleepMs ?? 250;
    let currentIdleSleepMs = idleSleepMs;
    const maxIdleSleepMs = this.options.maxIdleSleepMs ?? 5_000;

    while (this.options.signal?.aborted !== true) {
      const result = await this.runOnce(handler);
      if (result.claimed === 0) {
        await sleep(currentIdleSleepMs, this.options.signal);
        currentIdleSleepMs = Math.min(maxIdleSleepMs, currentIdleSleepMs * 2);
      } else {
        currentIdleSleepMs = idleSleepMs;
      }
    }
  }

  private async applyOutcome(job: FlowRecord, outcome: CompleteOutcome | RetryOutcome | FailOutcome): Promise<void> {
    if (outcome.kind === "retry") {
      await this.queue.client.retry(job.id, {
        error: outcome.error,
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        payload: outcome.payload,
        runAtMs: outcome.runAtMs,
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
      ttlMs: outcome.ttlMs,
      valueRefs: outcome.valueRefs,
      values: outcome.values
    });
  }

  private async applyException(job: FlowRecord, error: unknown): Promise<void> {
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
