export class FerricStoreError extends Error {
  readonly code: string = "ferricstore_error";
  readonly raw: unknown;
  readonly retryable: boolean | undefined;
  readonly safeToRetry: boolean | undefined;
  readonly retryAfterMs: number | undefined;

  constructor(message: string, options: {
    raw?: unknown;
    cause?: unknown;
    retryable?: boolean;
    safeToRetry?: boolean;
    retryAfterMs?: number;
  } = {}) {
    super(message, { cause: options.cause });
    this.name = new.target.name;
    this.raw = options.raw;
    this.retryable = options.retryable ?? structuredBooleanField(options.raw, "retryable");
    this.safeToRetry = options.safeToRetry ?? structuredBooleanField(options.raw, "safe_to_retry");
    this.retryAfterMs = options.retryAfterMs ?? structuredIntegerField(options.raw, "retry_after_ms");
  }
}

export type RequestDisposition = "unsent" | "possibly_sent";
/** @deprecated Use RequestDisposition; retained for source compatibility. */
export type ConnectionRequestDisposition = RequestDisposition;

/** Connection closure annotated with whether the current request may have reached the server. */
export class ConnectionClosedError extends FerricStoreError {
  override readonly code = "connection_closed";
  readonly requestDisposition: RequestDisposition;

  constructor(
    requestDisposition: RequestDisposition,
    options: { raw?: unknown; cause?: unknown; message?: string } = {}
  ) {
    super(
      options.message ?? (requestDisposition === "unsent"
        ? "FerricStore connection is closed"
        : "FerricStore connection closed"),
      options
    );
    this.requestDisposition = requestDisposition;
  }
}

/** Request timeout annotated with whether the request may have reached the server. */
export class RequestTimeoutError extends FerricStoreError {
  override readonly code = "request_timeout";
  readonly requestDisposition: RequestDisposition;
  readonly timeoutMs: number;

  constructor(
    timeoutMs: number,
    requestDisposition: RequestDisposition,
    options: { raw?: unknown; cause?: unknown } = {}
  ) {
    super(`FerricStore request timed out after ${timeoutMs}ms`, options);
    this.requestDisposition = requestDisposition;
    this.timeoutMs = timeoutMs;
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

/** FLOW.POLICY.SET expected_generation did not match the stored generation. */
export class StalePolicyGenerationError extends FerricStoreError {
  override readonly code = "stale_policy_generation";
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
  readonly reason: string | undefined;

  constructor(
    message: string,
    options: {
      raw?: unknown;
      cause?: unknown;
      retryAfterMs?: number;
      reason?: string;
      retryable?: boolean;
      safeToRetry?: boolean;
    } = {}
  ) {
    const local = options.raw == null;
    super(message, {
      ...options,
      retryable: options.retryable ?? (local ? true : undefined),
      safeToRetry: options.safeToRetry ?? (local ? true : undefined)
    });
    this.reason = options.reason;
  }
}

/** The contacted endpoint cannot serve this route and topology should be refreshed. */
export class RerouteError extends FerricStoreError {
  override readonly code = "reroute";
}

const OVERLOAD_CODES = new Set([
  "backpressure",
  "busy",
  "flow_control_window_exhausted",
  "lane_queue_full",
  "overloaded"
]);

export function classifyServerError(
  message: string,
  raw?: unknown,
  cause?: unknown,
  status?: number | string
): FerricStoreError {
  const lower = message.toLowerCase();
  const structuredCode = structuredStringField(raw, "code");
  const code = structuredCode?.toLowerCase();
  const retry = {
    retryable: structuredBooleanField(raw, "retryable"),
    safeToRetry: structuredBooleanField(raw, "safe_to_retry"),
    retryAfterMs: structuredIntegerField(raw, "retry_after_ms") ?? intField(lower, "retry_after_ms")
  };

  if (isRerouteStatus(status) || code === "reroute") {
    return new RerouteError(message, { cause, raw, ...definedRetryMetadata(retry) });
  }
  if (isBusyStatus(status) || isOverloadCode(structuredCode) || overloadMessage(lower)) {
    return new OverloadedError(message, {
      cause,
      raw,
      ...definedRetryMetadata(retry),
      reason: structuredStringField(raw, "reason") ?? structuredCode ?? stringField(lower, "reason"),
      retryAfterMs: retry.retryAfterMs
    });
  }
  if (code === "flow_already_exists" || (lower.includes("flow") && lower.includes("already exists"))) {
    return new FlowAlreadyExistsError(message, { cause, raw });
  }
  if (lower.includes("flow wrong state") || code === "flow_wrong_state") {
    return new FlowWrongStateError(message, { cause, raw });
  }
  if (
    code === "stale_lease"
    || code === "stale_flow_lease"
    || lower.includes("stale flow lease")
    || lower.includes("stale lease")
    || lower.includes("stale token")
  ) {
    return new StaleLeaseError(message, { cause, raw });
  }
  if (
    code === "stale_generation"
    || code === "stale_policy_generation"
    || code === "stale_flow_policy_generation"
    || lower.includes("stale flow policy generation")
    || lower.includes("stale policy generation")
  ) {
    return new StalePolicyGenerationError(message, { cause, raw });
  }
  if (
    code === "flow_not_found"
    || (lower.includes("flow") && (lower.includes("not found") || lower.includes("does not exist")))
  ) {
    return new FlowNotFoundError(message, { cause, raw });
  }
  if (code === "lock_held" || lower.includes("lock is held") || lower.includes("held by another owner")) {
    return new LockHeldError(message, { cause, raw });
  }
  if (code === "lock_not_owned" || lower.includes("not the lock owner") || lower.includes("caller is not the lock owner")) {
    return new LockNotOwnedError(message, { cause, raw });
  }
  if (code === "invalid_command" || lower.includes("wrong number of arguments") || lower.includes("syntax error")) {
    return new InvalidCommandError(message, { cause, raw });
  }

  return new FerricStoreError(message, { cause, raw, ...definedRetryMetadata(retry) });
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
  return match?.[1] == null ? undefined : nonNegativeSafeIntegerText(match[1]);
}

function isBusyStatus(status: number | string | undefined): boolean {
  return status === 4 || (typeof status === "string" && (status === "4" || status.toLowerCase() === "busy"));
}

function isRerouteStatus(status: number | string | undefined): boolean {
  return status === 5 || (typeof status === "string" && (status === "5" || status.toLowerCase() === "reroute"));
}

function isOverloadCode(code: string | undefined): boolean {
  return code != null && OVERLOAD_CODES.has(code.toLowerCase());
}

function overloadMessage(message: string): boolean {
  return /\boverloaded\b/u.test(message) || /(?:^|\s)busy(?:\s|:|$)/u.test(message);
}

function structuredIntegerField(raw: unknown, name: string): number | undefined {
  const value = structuredField(raw, name);
  if (typeof value === "number") {
    return Number.isSafeInteger(value) && value >= 0 ? value : undefined;
  }
  if (typeof value === "bigint") {
    return value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : undefined;
  }
  const text = binaryText(value);
  return text == null ? undefined : nonNegativeSafeIntegerText(text);
}

function structuredBooleanField(raw: unknown, name: string): boolean | undefined {
  const value = structuredField(raw, name);
  if (typeof value === "boolean") return value;
  const text = binaryText(value)?.toLowerCase();
  if (text === "true" || text === "1") return true;
  if (text === "false" || text === "0") return false;
  return undefined;
}

function definedRetryMetadata(metadata: {
  readonly retryable?: boolean;
  readonly safeToRetry?: boolean;
  readonly retryAfterMs?: number;
}): { retryable?: boolean; safeToRetry?: boolean; retryAfterMs?: number } {
  return {
    ...(metadata.retryable == null ? {} : { retryable: metadata.retryable }),
    ...(metadata.safeToRetry == null ? {} : { safeToRetry: metadata.safeToRetry }),
    ...(metadata.retryAfterMs == null ? {} : { retryAfterMs: metadata.retryAfterMs })
  };
}

function nonNegativeSafeIntegerText(value: string): number | undefined {
  if (!/^[0-9]+$/u.test(value)) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function structuredStringField(raw: unknown, name: string): string | undefined {
  return binaryText(structuredField(raw, name));
}

function structuredField(raw: unknown, name: string): unknown {
  if (raw instanceof Map) {
    if (raw.has(name)) return raw.get(name);
    for (const [key, value] of raw.entries()) {
      if (binaryText(key) === name) return value;
    }
    return undefined;
  }
  if (typeof raw === "object" && raw != null && Object.hasOwn(raw, name)) {
    return (raw as Record<string, unknown>)[name];
  }
  return undefined;
}

function binaryText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
  return undefined;
}

function stringField(message: string, name: string): string | undefined {
  const match = new RegExp(`\\b${name}=([a-z0-9_:-]+)\\b`).exec(message);
  return match?.[1];
}
