import { complete, fail, isOutcome, retry, type CompleteOutcome, type FailOutcome, type Outcome, type RetryOutcome } from "./outcomes.js";
import { normalizeExceptionPolicy, type ClaimedItem, type WorkerConfig } from "./types.js";
import { QueueCompletionBatcher, queueClaimOptions } from "./queue-completion-batcher.js";
import { flushQueueCompletions } from "./queue-completion-flush.js";
import type { Queue, QueueBatchHandler, QueueHandler, QueueJob, QueueWorkerResult } from "./queue.js";
import {
  finiteNonNegativeInteger,
  isBatchableComplete,
  normalizeWorkerConfig,
  stopQueueBatchGuards
} from "./queue-worker-utilities.js";
import {
  LeaseRenewalGuard,
  runContinuousWorkerPool,
  type ContinuousWorkerHandleResult,
  workerBatchSize,
  workerClaimLimit,
  workerConcurrency,
  workerDrainBatches,
  workerErrorPayload,
  workerLeaseMs,
  workerRefillStrategy
} from "./worker-internal.js";
import { applyQueueBatchOutcome, applyQueueException, applyQueueOutcome } from "./queue-worker-outcomes.js";
import { QueuePendingCompletions } from "./queue-pending-completions.js";
import { runQueueWorkerInWaves } from "./queue-worker-run-loop.js";

export class QueueWorker {
  readonly queue: Queue;
  readonly options: WorkerConfig;
  private readonly pendingCompletions: QueuePendingCompletions;

  constructor(queue: Queue, options: WorkerConfig) {
    this.queue = queue;
    this.options = normalizeWorkerConfig(options);
    this.pendingCompletions = new QueuePendingCompletions(queue, this.options);
  }

  async runOnce(handler: QueueHandler): Promise<QueueWorkerResult> {
    const result: QueueWorkerResult = {
      claimed: 0,
      completed: await this.pendingCompletions.drain(false),
      failed: 0,
      retried: 0
    };
    for (let batch = 0; batch < workerDrainBatches(this.options); batch += 1) {
      const next = await this.runHandlerClaimOnce(handler, batch === 0);
      result.claimed += next.claimed;
      result.completed += next.completed;
      result.failed += next.failed;
      result.retried += next.retried;
      if (next.claimed === 0) {
        break;
      }
    }
    return result;
  }

  private async runHandlerClaimOnce(handler: QueueHandler, useBlocking: boolean): Promise<QueueWorkerResult> {
    const leaseMs = workerLeaseMs(this.options);
    const jobs = await this.claimJobs({
      limit: workerClaimLimit(this.options, this.queue.client.flowManyBatchLimit),
      useBlocking
    });
    const result: QueueWorkerResult = { claimed: jobs.length, completed: 0, failed: 0, retried: 0 };
    if (jobs.length === 0) {
      return result;
    }
    const completions: LeaseRenewalGuard[] = [];
    const concurrency = workerConcurrency(this.options);
    const guards = new Map(
      jobs.map((job) => [job, new LeaseRenewalGuard(this.queue.client, job, leaseMs, this.options)] as const)
    );
    const activeGuards = new Set(guards.values());
    let cursor = 0;

    const runNext = async (): Promise<void> => {
      while (cursor < jobs.length) {
        const job = jobs[cursor];
        cursor += 1;
        if (job == null) continue;
        const guard = guards.get(job);
        if (guard == null) continue;
        const awaitsBatchCompletion = await this.applyJob(job, handler, result, completions, guard);
        if (!awaitsBatchCompletion) {
          activeGuards.delete(guard);
          await guard.stop();
        }
      }
    };

    try {
      const handlers = await Promise.allSettled(
        Array.from({ length: Math.min(concurrency, jobs.length) }, () => runNext())
      );
      const completionJobs: ClaimedItem[] = [];
      const completionGuards: LeaseRenewalGuard[] = [];
      for (const guard of completions) {
        activeGuards.delete(guard);
        completionJobs.push(guard.job);
        completionGuards.push(guard);
      }
      const stopped = await stopQueueBatchGuards(completionJobs, completionGuards);
      await this.flushCompletions(stopped.jobs, result);
      const failed = handlers.find((handler): handler is PromiseRejectedResult => handler.status === "rejected");
      if (failed != null) {
        throw failed.reason;
      }
      if (stopped.error != null) throw stopped.error;
      return result;
    } finally {
      await Promise.all([...activeGuards].map(async (guard) => await guard.stop()));
    }
  }

  async runBatchOnce(handler: QueueBatchHandler): Promise<QueueWorkerResult> {
    return await this.runBatchOnceWithClaim(handler);
  }

  async runBatchOnceForPartitionKeys(
    handler: QueueBatchHandler,
    partitionKeys: readonly string[],
    options: { readonly claimCredit?: number } = {}
  ): Promise<QueueWorkerResult> {
    const claimCredit = options.claimCredit == null
      ? undefined
      : finiteNonNegativeInteger(options.claimCredit, 0);
    if (partitionKeys.length === 0 || claimCredit === 0) {
      return { claimed: 0, completed: await this.pendingCompletions.drain(false), failed: 0, retried: 0 };
    }
    return await this.runBatchOnceWithClaim(handler, {
      limit: claimCredit == null
        ? undefined
        : Math.min(workerBatchSize(this.options, this.queue.client.flowManyBatchLimit), claimCredit),
      partitionKey: partitionKeys.length === 1 ? partitionKeys[0] : undefined,
      partitionKeys: partitionKeys.length === 1 ? undefined : [...partitionKeys]
    });
  }

  async flush(): Promise<number> {
    return await this.pendingCompletions.drain(true);
  }

  private async runBatchOnceWithClaim(
    handler: QueueBatchHandler,
    claimOptions: {
      readonly limit?: number;
      readonly partitionKey?: string;
      readonly partitionKeys?: string[];
    } = {}
  ): Promise<QueueWorkerResult> {
    const completed = await this.pendingCompletions.drain(false);
    const jobs = await this.claimJobs(claimOptions);
    const result: QueueWorkerResult = { claimed: jobs.length, completed, failed: 0, retried: 0 };
    if (jobs.length === 0) {
      return result;
    }
    const leaseMs = workerLeaseMs(this.options);
    const guards = jobs.map((job) => new LeaseRenewalGuard(this.queue.client, job, leaseMs, this.options));

    try {
      let batchComplete = false;
      let outcome: CompleteOutcome | RetryOutcome | FailOutcome | undefined;
      let handlerError: unknown;
      let handlerFailed = false;
      try {
        const value = await handler([...jobs]);
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
        handlerFailed = true;
        handlerError = error;
        const policy = normalizeExceptionPolicy(this.options.exceptionPolicy);
        if (policy !== "raise") {
          const payload = workerErrorPayload(error, this.options, this.queue.client.codec);
          outcome = policy === "fail" ? fail({ error: payload }) : retry({ error: payload });
        }
      }

      const stopped = await stopQueueBatchGuards(guards.map((guard) => guard.job), guards);
      if (handlerFailed && normalizeExceptionPolicy(this.options.exceptionPolicy) === "raise") {
        throw handlerError;
      }
      if (batchComplete) {
        await this.flushCompletions(stopped.jobs, result);
      } else {
        if (outcome == null) {
          throw new Error("Queue batch handler did not produce a valid outcome");
        }
        await applyQueueBatchOutcome(this.queue, this.options, stopped.jobs, outcome);
        if (outcome.kind === "retry") {
          result.retried += stopped.jobs.length;
        } else if (outcome.kind === "fail") {
          result.failed += stopped.jobs.length;
        } else {
          result.completed += stopped.jobs.length;
        }
      }
      if (stopped.error != null) throw stopped.error;
      return result;
    } finally {
      await Promise.allSettled(guards.map(async (guard) => await guard.stop()));
    }
  }

  async run(handler: QueueHandler): Promise<void> {
    if (workerRefillStrategy(this.options) === "continuous") {
      await this.runContinuously(handler);
      return;
    }

    await runQueueWorkerInWaves(this, handler, this.options);
  }

  private async runContinuously(handler: QueueHandler): Promise<void> {
    await this.flush();
    const completionBatcher = new QueueCompletionBatcher(this.queue, this.options);
    try {
      await runContinuousWorkerPool({
        claim: async (limit, useBlocking) => await this.claimJobs({ limit, useBlocking }),
        concurrency: workerConcurrency(this.options),
        handle: async (job) => await this.applyContinuousJob(job, handler, completionBatcher),
        idleSleepMs: this.options.idleSleepMs,
        maxClaimSize: workerBatchSize(this.options, this.queue.client.flowManyBatchLimit),
        maxIdleSleepMs: this.options.maxIdleSleepMs,
        onFailure: () => completionBatcher.disableRefill(),
        refillDelayMs: this.options.refillDelayMs,
        signal: this.options.signal
      });
    } finally {
      await completionBatcher.close();
      await this.flush();
    }
  }

  private async claimJobs(
    options: {
      readonly useBlocking?: boolean;
      readonly limit?: number;
      readonly partitionKey?: string;
      readonly partitionKeys?: string[];
    } = {}
  ): Promise<QueueJob[]> {
    return await this.queue.client.claimDue(
      this.queue.type,
      queueClaimOptions(this.queue, this.options, {
        limit: options.limit,
        partitionKey: options.partitionKey,
        partitionKeys: options.partitionKeys,
        useBlocking: options.useBlocking
      })
    );
  }

  private async applyJob(
    job: QueueJob,
    handler: QueueHandler,
    result: QueueWorkerResult,
    completions: LeaseRenewalGuard[],
    guard: LeaseRenewalGuard
  ): Promise<boolean> {
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
      await guard.stop();
      await applyQueueException(this.queue, this.options, guard.job, error);
      if (normalizeExceptionPolicy(this.options.exceptionPolicy) === "fail") {
        result.failed += 1;
      } else {
        result.retried += 1;
      }
      return false;
    }

    if (batchComplete) {
      completions.push(guard);
      return true;
    }
    if (outcome == null) {
      throw new Error("Queue handler did not produce a valid outcome");
    }

    await guard.stop();
    await applyQueueOutcome(this.queue, guard.job, outcome);
    if (outcome.kind === "retry") {
      result.retried += 1;
    } else if (outcome.kind === "fail") {
      result.failed += 1;
    } else {
      result.completed += 1;
    }
    return false;
  }

  private async applyContinuousJob(
    job: QueueJob,
    handler: QueueHandler,
    completionBatcher: QueueCompletionBatcher
  ): Promise<void | ContinuousWorkerHandleResult<QueueJob>> {
    const guard = new LeaseRenewalGuard(
      this.queue.client,
      job,
      workerLeaseMs(this.options),
      this.options
    );
    let guardOwned = true;
    const stopGuard = async (): Promise<void> => {
      guardOwned = false;
      await guard.stop();
    };

    try {
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
        await stopGuard();
        await applyQueueException(this.queue, this.options, guard.job, error);
        return;
      }

      if (batchComplete) {
        guardOwned = false;
        return await completionBatcher.complete(guard.job, guard);
      }
      if (outcome == null) {
        throw new Error("Queue handler did not produce a valid outcome");
      }

      await stopGuard();
      await applyQueueOutcome(this.queue, guard.job, outcome);
    } finally {
      if (guardOwned) {
        await guard.stop();
      }
    }
  }

  private async flushCompletions(jobs: ClaimedItem[], result: QueueWorkerResult): Promise<void> {
    await flushQueueCompletions(this.queue, this.options, this.pendingCompletions, jobs, result);
  }

}
