import { FerricStoreError } from "./errors.js";
import { completeJobsResultError } from "./client.js";
import type { CompleteOutcome, FailOutcome, RetryOutcome } from "./outcomes.js";
import { normalizeExceptionPolicy, type WorkerConfig } from "./types.js";
import { unwrapPipelineResponse } from "./protocol.js";
import { LeaseRenewalGuard } from "./worker-internal.js";
import { snapshotWorkerConfig } from "./worker-config.js";
import type { QueueJob } from "./queue.js";

export async function stopQueueBatchGuards(
  jobs: readonly QueueJob[],
  guards: readonly LeaseRenewalGuard[]
): Promise<{ readonly error?: Error; readonly jobs: QueueJob[] }> {
  const stopped = await Promise.allSettled(guards.map(async (guard) => await guard.stop()));
  const ready: QueueJob[] = [];
  let firstFailure: { readonly error: Error } | undefined;
  for (let index = 0; index < stopped.length; index += 1) {
    const result = stopped[index];
    const job = jobs[index];
    if (result == null || job == null) continue;
    if (result.status === "rejected") {
      firstFailure ??= {
        error: result.reason instanceof Error
          ? result.reason
          : new Error("Queue lease guard failed", { cause: result.reason })
      };
    } else {
      ready.push(job);
    }
  }
  return firstFailure == null ? { jobs: ready } : { error: firstFailure.error, jobs: ready };
}

export function commonQueuePartition(jobs: readonly QueueJob[]): string | undefined {
  const first = jobs[0]?.partitionKey;
  return first != null && jobs.every((job) => job.partitionKey === first) ? first : undefined;
}

export function assertQueueManyMutationSuccess(response: unknown, expectedCount: number, command: string): void {
  if (response instanceof Error) throw response;
  if (queueOkLike(response)) return;
  let results: unknown[];
  try {
    results = unwrapPipelineResponse(response, { throwOnItemError: false }, expectedCount);
  } catch (error) {
    throw new FerricStoreError(`${command} returned an unexpected response`, {
      cause: error,
      raw: response
    });
  }
  for (const result of results) {
    if (result instanceof Error) throw result;
    if (!queueOkLike(result)) {
      throw new FerricStoreError(`${command} returned an unexpected per-item result`, {
        raw: result
      });
    }
  }
}

function queueOkLike(value: unknown): boolean {
  if (value === true || value === 1) return true;
  if (typeof value === "string") return value.toUpperCase() === "OK";
  return (Buffer.isBuffer(value) || value instanceof Uint8Array) &&
    Buffer.from(value).toString("utf8").toUpperCase() === "OK";
}

export function assertQueueCompletionSuccess(response: unknown, expectedCount: number): void {
  const error = completeJobsResultError(response, expectedCount);
  if (error != null) throw error;
}

export function isBatchableComplete(outcome: CompleteOutcome | RetryOutcome | FailOutcome): outcome is CompleteOutcome {
  return (
    outcome.kind === "complete" &&
    outcome.payload === undefined &&
    outcome.result === undefined &&
    outcome.ttlMs === undefined &&
    outcome.values === undefined &&
    outcome.valueRefs === undefined &&
    outcome.dropValues === undefined &&
    outcome.overrideValues === undefined &&
    outcome.attributesMerge === undefined &&
    outcome.attributesDelete === undefined &&
    outcome.stateMeta === undefined
  );
}

export function normalizeWorkerConfig(options: WorkerConfig): WorkerConfig {
  const captured = snapshotWorkerConfig(options);
  if (captured.exceptionPolicy != null) normalizeExceptionPolicy(captured.exceptionPolicy);
  if (captured.profile !== "throughput") return captured;
  return Object.freeze({
    batchSize: 500,
    claimPayload: false,
    completeAsyncDepth: 8,
    completeIndependent: true,
    ...captured
  });
}

export function finiteNonNegativeInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(0, Math.trunc(value));
}
