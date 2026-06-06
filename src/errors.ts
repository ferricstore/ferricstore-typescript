export class FerricStoreError extends Error {
  readonly code: string = "ferricstore_error";
  readonly raw: unknown;

  constructor(message: string, options: { raw?: unknown; cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = new.target.name;
    this.raw = options.raw;
  }
}

export class FlowNotFoundError extends FerricStoreError {
  override readonly code = "flow_not_found";
}

export class FlowWrongStateError extends FerricStoreError {
  override readonly code = "flow_wrong_state";
}

export class StaleLeaseError extends FerricStoreError {
  override readonly code = "stale_lease";
}

export class FlowAlreadyExistsError extends FerricStoreError {
  override readonly code = "flow_already_exists";
}

export class LockHeldError extends FerricStoreError {
  override readonly code = "lock_held";
}

export class LockNotOwnedError extends FerricStoreError {
  override readonly code = "lock_not_owned";
}

export class InvalidCommandError extends FerricStoreError {
  override readonly code = "invalid_command";
}

export class OverloadedError extends FerricStoreError {
  override readonly code = "overloaded";
  readonly retryAfterMs: number | undefined;
  readonly reason: string | undefined;

  constructor(
    message: string,
    options: { raw?: unknown; cause?: unknown; retryAfterMs?: number; reason?: string } = {}
  ) {
    super(message, options);
    this.retryAfterMs = options.retryAfterMs;
    this.reason = options.reason;
  }
}

export function classifyServerError(message: string, raw?: unknown, cause?: unknown): FerricStoreError {
  const lower = message.toLowerCase();

  if (lower.includes("overloaded") || lower.includes("busy")) {
    return new OverloadedError(message, {
      cause,
      raw,
      reason: stringField(lower, "reason"),
      retryAfterMs: intField(lower, "retry_after_ms")
    });
  }
  if (lower.includes("already exists")) {
    return new FlowAlreadyExistsError(message, { cause, raw });
  }
  if (lower.includes("wrong state")) {
    return new FlowWrongStateError(message, { cause, raw });
  }
  if (lower.includes("stale flow lease") || lower.includes("stale lease") || lower.includes("stale token")) {
    return new StaleLeaseError(message, { cause, raw });
  }
  if (lower.includes("not found") || lower.includes("does not exist")) {
    return new FlowNotFoundError(message, { cause, raw });
  }
  if (lower.includes("lock is held") || lower.includes("held by another owner")) {
    return new LockHeldError(message, { cause, raw });
  }
  if (lower.includes("not the lock owner") || lower.includes("caller is not the lock owner")) {
    return new LockNotOwnedError(message, { cause, raw });
  }
  if (lower.includes("wrong number of arguments") || lower.includes("syntax error")) {
    return new InvalidCommandError(message, { cause, raw });
  }

  return new FerricStoreError(message, { cause, raw });
}

export function mapException(error: unknown): unknown {
  if (error instanceof FerricStoreError) {
    return error;
  }

  if (!(error instanceof Error)) {
    return error;
  }

  const message = error.message;
  const serverLike =
    error.name === "ResponseError" ||
    message.startsWith("ERR ") ||
    message.startsWith("WRONGTYPE ") ||
    message.startsWith("DISTLOCK ");

  if (!serverLike) {
    return error;
  }

  return classifyServerError(message, error, error);
}

function intField(message: string, name: string): number | undefined {
  const match = new RegExp(`\\b${name}=([0-9]+)\\b`).exec(message);
  return match?.[1] == null ? undefined : Number.parseInt(match[1], 10);
}

function stringField(message: string, name: string): string | undefined {
  const match = new RegExp(`\\b${name}=([a-z0-9_:-]+)\\b`).exec(message);
  return match?.[1];
}
