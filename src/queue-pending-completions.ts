import type { Queue } from "./queue.js";
import { QueueCompletionError } from "./queue-completion-error.js";
import { assertQueueCompletionSuccess } from "./queue-worker-utilities.js";
import type { ClaimedItem, WorkerConfig } from "./types.js";

interface PendingCompletion {
  done: boolean;
  error?: unknown;
  failed: boolean;
  promise: Promise<number>;
  value: number;
}

export class QueuePendingCompletions {
  private readonly pending: PendingCompletion[] = [];
  private readonly settledWaiters = new Set<() => void>();

  constructor(
    private readonly queue: Queue,
    private readonly options: WorkerConfig
  ) {}

  get size(): number {
    return this.pending.length;
  }

  enqueue(jobs: ClaimedItem[]): void {
    const pending: PendingCompletion = {
      done: false,
      failed: false,
      promise: Promise.resolve(0),
      value: 0
    };
    pending.promise = this.queue.client.completeJobs(jobs, {
      independent: this.options.completeIndependent ?? true,
      returnOkOnSuccess: true
    }).then(
      (response) => {
        try {
          assertQueueCompletionSuccess(response, jobs.length);
          pending.done = true;
          pending.value = jobs.length;
          return jobs.length;
        } catch (error) {
          pending.done = true;
          pending.error = error;
          pending.failed = true;
          return 0;
        }
      },
      (error: unknown) => {
        pending.done = true;
        pending.error = error;
        pending.failed = true;
        return 0;
      }
    ).finally(() => this.notifySettled());
    this.pending.push(pending);
  }

  async drain(block: boolean): Promise<number> {
    if (block && this.pending.length > 0) {
      await Promise.all(this.pending.map((pending) => pending.promise));
    }
    return this.collectSettled();
  }

  async drainOne(): Promise<number> {
    const initialSize = this.pending.length;
    const completed = this.collectSettled();
    if (this.pending.length < initialSize || this.pending.length === 0) return completed;
    await new Promise<void>((resolve) => this.settledWaiters.add(resolve));
    return this.collectSettled();
  }

  private collectSettled(): number {
    let completed = 0;
    let firstFailure: { readonly error: unknown } | undefined;
    let remaining = 0;
    for (const pending of this.pending) {
      if (!pending.done) {
        this.pending[remaining] = pending;
        remaining += 1;
        continue;
      }
      if (pending.failed) firstFailure ??= { error: pending.error };
      else completed += pending.value;
    }
    this.pending.length = remaining;
    if (firstFailure != null) throw new QueueCompletionError(firstFailure.error, completed);
    return completed;
  }

  private notifySettled(): void {
    const waiters = [...this.settledWaiters];
    this.settledWaiters.clear();
    for (const resolve of waiters) resolve();
  }
}
