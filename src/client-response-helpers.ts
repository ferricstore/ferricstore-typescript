import { FerricStoreError, mapException } from "./errors.js";
import { unwrapPipelineResponse } from "./protocol.js";

export function isRecordLike(value: unknown): boolean {
  return typeof value === "object" && value != null && !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array) && !Array.isArray(value);
}

export function requiredArrayResponse(value: unknown, command: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new FerricStoreError(`${command} returned an invalid response`, { raw: value });
  }
  return value;
}

export function isCompactClaimTuple(value: unknown): value is unknown[] {
  if (!Array.isArray(value) || value.length < 4) return false;
  const tuple = value as unknown[];
  const id = tuple[0];
  const leaseToken = tuple[2];
  const fencingToken = tuple[3];
  return (
    (typeof id === "string" || Buffer.isBuffer(id) || id instanceof Uint8Array) &&
    (typeof leaseToken === "string" || Buffer.isBuffer(leaseToken) || leaseToken instanceof Uint8Array) &&
    (typeof fencingToken === "number" || typeof fencingToken === "bigint" || typeof fencingToken === "string")
  );
}

/** @internal */
export function completeJobsResultError(value: unknown, expectedCount: number): Error | undefined {
  if (value instanceof Error) return errorFromUnknown(value);
  if (completionOk(value)) return undefined;
  if (!Array.isArray(value)) {
    return new FerricStoreError("FLOW.COMPLETE_MANY returned an unexpected response", { raw: value });
  }

  let results: unknown[];
  try {
    results = unwrapPipelineResponse(value, { throwOnItemError: false });
  } catch (error) {
    return errorFromUnknown(error);
  }
  if (results.length !== expectedCount) {
    return new FerricStoreError(
      `FLOW.COMPLETE_MANY returned ${results.length} items; expected ${expectedCount}`,
      { raw: value }
    );
  }
  for (const result of results) {
    if (result instanceof Error) return errorFromUnknown(result);
    if (!completionOk(result)) {
      return new FerricStoreError(
        "FLOW.COMPLETE_MANY returned an unexpected per-item result",
        { raw: result }
      );
    }
  }
  return undefined;
}

export function completionOk(value: unknown): boolean {
  if (value === true || value === 1) return true;
  if (typeof value === "string") return value.toUpperCase() === "OK";
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase() === "OK";
  }
  return false;
}

export function errorFromUnknown(error: unknown): Error {
  const mapped = mapException(error);
  return mapped instanceof Error ? mapped : new Error(String(error));
}

export function throwMapped(error: unknown): never {
  const mapped = mapException(error);
  throw mapped instanceof Error ? mapped : error;
}
