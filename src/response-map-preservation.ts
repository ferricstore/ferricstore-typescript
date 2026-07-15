import { Buffer } from "node:buffer";
import { setOwnValue, text } from "./internal.js";
import { mapDenseResponseArray } from "./response-array-normalization.js";

/** Normalize response-map keys and containers without coercing opaque binary leaves. */
export function toStringKeyMapPreservingValues(value: unknown): Record<string, unknown> | undefined {
  if (value == null) return undefined;
  const result: Record<string, unknown> = {};
  if (value instanceof Map) {
    for (const [key, item] of value.entries()) {
      setOwnValue(result, text(key), normalizeMapStructurePreservingBytes(item));
    }
    return result;
  }
  if (
    typeof value === "object" &&
    !Array.isArray(value) &&
    !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array)
  ) {
    for (const [key, item] of Object.entries(value)) {
      setOwnValue(result, text(key), normalizeMapStructurePreservingBytes(item));
    }
    return result;
  }
  return undefined;
}

function normalizeMapStructurePreservingBytes(value: unknown): unknown {
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value;
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      setOwnValue(result, text(key), normalizeMapStructurePreservingBytes(item));
    }
    return result;
  }
  if (Array.isArray(value)) {
    return mapDenseResponseArray(value, normalizeMapStructurePreservingBytes);
  }
  if (typeof value === "object" && value != null) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      setOwnValue(result, text(key), normalizeMapStructurePreservingBytes(item));
    }
    return result;
  }
  return value;
}
