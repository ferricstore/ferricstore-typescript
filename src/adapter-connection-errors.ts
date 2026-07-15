import { ConnectionClosedError } from "./errors.js";

export function unsentConnectionClosedError(cause?: unknown): ConnectionClosedError {
  return new ConnectionClosedError("unsent", cause == null ? {} : { cause, raw: cause });
}

export function possiblySentConnectionClosedError(cause?: unknown, message?: string): ConnectionClosedError {
  return new ConnectionClosedError("possibly_sent", cause == null
    ? { message }
    : { cause, message, raw: cause });
}
