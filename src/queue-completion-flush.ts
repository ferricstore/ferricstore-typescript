import type { Queue, QueueWorkerResult } from "./queue.js";
import { QueueCompletionError } from "./queue-completion-error.js";
import type { QueuePendingCompletions } from "./queue-pending-completions.js";
import { assertQueueCompletionSuccess, finiteNonNegativeInteger } from "./queue-worker-utilities.js";
import type { ClaimedItem, WorkerConfig } from "./types.js";
import { workerBatchSize } from "./worker-internal.js";

export async function flushQueueCompletions(
  queue: Queue,
  options: WorkerConfig,
  pending: QueuePendingCompletions,
  jobs: ClaimedItem[],
  result: QueueWorkerResult
): Promise<void> {
  if (jobs.length === 0) return;
  const depth = finiteNonNegativeInteger(options.completeAsyncDepth, 0);
  const batchSize = Math.min(workerBatchSize(options, queue.client.flowManyBatchLimit), jobs.length);
  if (depth > 0) {
    let completed = 0;
    let firstFailure: { readonly error: unknown } | undefined;
    const drainPending = async (all: boolean): Promise<void> => {
      try {
        const count = all ? await pending.drain(true) : await pending.drainOne();
        completed += count;
        result.completed += count;
      } catch (error) {
        if (error instanceof QueueCompletionError) {
          completed += error.completed;
          result.completed += error.completed;
          firstFailure ??= { error: error.cause ?? error };
        } else {
          firstFailure ??= { error };
        }
      }
    };
    for (let index = 0; index < jobs.length; index += batchSize) {
      while (pending.size >= depth) await drainPending(false);
      pending.enqueue(jobs.slice(index, index + batchSize));
    }
    if (firstFailure != null) {
      await drainPending(true);
      throw new QueueCompletionError(firstFailure.error, completed);
    }
    return;
  }

  if (jobs.length <= batchSize) {
    const response = await queue.client.completeJobs(jobs, {
      independent: options.completeIndependent ?? true,
      returnOkOnSuccess: true
    });
    assertQueueCompletionSuccess(response, jobs.length);
    result.completed += jobs.length;
    return;
  }

  let completed = 0;
  let firstFailure: { readonly error: unknown } | undefined;
  for (let index = 0; index < jobs.length; index += batchSize) {
    const batch = jobs.slice(index, index + batchSize);
    try {
      const response = await queue.client.completeJobs(batch, {
        independent: options.completeIndependent ?? true,
        returnOkOnSuccess: true
      });
      assertQueueCompletionSuccess(response, batch.length);
      result.completed += batch.length;
      completed += batch.length;
    } catch (error) {
      firstFailure ??= { error };
    }
  }
  if (firstFailure != null) throw new QueueCompletionError(firstFailure.error, completed);
}
