/** Async terminal-write failure with the number of durable successes drained first. */
export class QueueCompletionError extends Error {
  readonly completed: number;

  constructor(cause: unknown, completed: number) {
    const detail = cause instanceof Error
      ? cause.message
      : typeof cause === "string"
        ? cause
        : "Queue completion failed";
    super(
      completed === 0 ? detail : `${detail} after ${completed} successful completion(s)`,
      { cause }
    );
    this.name = "QueueCompletionError";
    this.completed = completed;
  }
}
