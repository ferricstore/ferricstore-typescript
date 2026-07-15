import { fail, retry, type CompleteOutcome, type FailOutcome, type RetryOutcome } from "./outcomes.js";
import type { Queue, QueueJob } from "./queue.js";
import { normalizeExceptionPolicy, type WorkerConfig } from "./types.js";
import {
  assertQueueCompletionSuccess,
  assertQueueManyMutationSuccess,
  commonQueuePartition
} from "./queue-worker-utilities.js";
import { workerBatchSize, workerErrorPayload } from "./worker-internal.js";

type QueueOutcome = CompleteOutcome | RetryOutcome | FailOutcome;

export async function applyQueueOutcome(queue: Queue, job: QueueJob, outcome: QueueOutcome): Promise<void> {
  if (outcome.kind === "retry") {
    await queue.client.retry(job.id, {
      attributesDelete: outcome.attributesDelete,
      attributesMerge: outcome.attributesMerge,
      dropValues: outcome.dropValues,
      error: outcome.error,
      fencingToken: job.fencingToken,
      leaseToken: job.leaseToken,
      overrideValues: outcome.overrideValues,
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
    await queue.client.fail(job.id, {
      attributesDelete: outcome.attributesDelete,
      attributesMerge: outcome.attributesMerge,
      dropValues: outcome.dropValues,
      error: outcome.error,
      fencingToken: job.fencingToken,
      leaseToken: job.leaseToken,
      overrideValues: outcome.overrideValues,
      partitionKey: job.partitionKey,
      payload: outcome.payload,
      stateMeta: outcome.stateMeta,
      ttlMs: outcome.ttlMs,
      valueRefs: outcome.valueRefs,
      values: outcome.values
    });
    return;
  }
  await queue.client.complete(job.id, {
    attributesDelete: outcome.attributesDelete,
    attributesMerge: outcome.attributesMerge,
    dropValues: outcome.dropValues,
    fencingToken: job.fencingToken,
    leaseToken: job.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: job.partitionKey,
    payload: outcome.payload,
    result: outcome.result,
    stateMeta: outcome.stateMeta,
    ttlMs: outcome.ttlMs,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

export async function applyQueueBatchOutcome(
  queue: Queue,
  options: WorkerConfig,
  jobs: QueueJob[],
  outcome: QueueOutcome
): Promise<void> {
  if (jobs.length === 0) return;
  const independent = options.completeIndependent ?? true;
  const batchSize = Math.min(workerBatchSize(options, queue.client.flowManyBatchLimit), jobs.length);
  let firstFailure: { readonly error: unknown } | undefined;
  for (let index = 0; index < jobs.length; index += batchSize) {
    const batch = jobs.slice(index, index + batchSize);
    try {
      if (outcome.kind === "retry") {
        const response = await queue.client.retryMany(commonQueuePartition(batch), batch, {
          attributesDelete: outcome.attributesDelete,
          attributesMerge: outcome.attributesMerge,
          dropValues: outcome.dropValues,
          error: outcome.error,
          independent,
          overrideValues: outcome.overrideValues,
          payload: outcome.payload,
          returnOkOnSuccess: true,
          runAtMs: outcome.runAtMs,
          stateMeta: outcome.stateMeta,
          valueRefs: outcome.valueRefs,
          values: outcome.values
        });
        assertQueueManyMutationSuccess(response, batch.length, "FLOW.RETRY_MANY");
      } else if (outcome.kind === "fail") {
        const response = await queue.client.failMany(commonQueuePartition(batch), batch, {
          attributesDelete: outcome.attributesDelete,
          attributesMerge: outcome.attributesMerge,
          dropValues: outcome.dropValues,
          error: outcome.error,
          independent,
          overrideValues: outcome.overrideValues,
          payload: outcome.payload,
          returnOkOnSuccess: true,
          stateMeta: outcome.stateMeta,
          ttlMs: outcome.ttlMs,
          valueRefs: outcome.valueRefs,
          values: outcome.values
        });
        assertQueueManyMutationSuccess(response, batch.length, "FLOW.FAIL_MANY");
      } else {
        const response = await queue.client.completeJobs(batch, {
          attributesDelete: outcome.attributesDelete,
          attributesMerge: outcome.attributesMerge,
          dropValues: outcome.dropValues,
          independent,
          overrideValues: outcome.overrideValues,
          payload: outcome.payload,
          result: outcome.result,
          returnOkOnSuccess: true,
          stateMeta: outcome.stateMeta,
          ttlMs: outcome.ttlMs,
          valueRefs: outcome.valueRefs,
          values: outcome.values
        });
        assertQueueCompletionSuccess(response, batch.length);
      }
    } catch (error) {
      firstFailure ??= { error };
    }
  }
  if (firstFailure != null) throw firstFailure.error;
}

export async function applyQueueException(
  queue: Queue,
  options: WorkerConfig,
  job: QueueJob,
  error: unknown
): Promise<void> {
  const policy = normalizeExceptionPolicy(options.exceptionPolicy);
  if (policy === "raise") throw error;
  const payload = workerErrorPayload(error, options, queue.client.codec);
  await applyQueueOutcome(queue, job, policy === "fail" ? fail({ error: payload }) : retry({ error: payload }));
}
