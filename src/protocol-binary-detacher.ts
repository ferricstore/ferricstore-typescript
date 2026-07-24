import { Buffer } from "node:buffer";
import { setOwnValue } from "./protocol-core.js";

const MIN_PINNED_BACKING_BYTES = 64;

/**
 * Copy small decoded slices that would otherwise retain a much larger response.
 * Large values remain zero-copy, preserving throughput for payload-heavy pages.
 */
export function detachDecodedBinary(value: unknown, backingBytes: number): unknown {
  if (Buffer.isBuffer(value)) return detachBuffer(value, backingBytes);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      value[index] = detachDecodedBinary(value[index], backingBytes);
    }
    return value;
  }
  if (!isDecodedRecord(value)) return value;
  for (const key of Object.keys(value)) {
    setOwnValue(value, key, detachDecodedBinary(value[key], backingBytes));
  }
  return value;
}

function detachBuffer(value: Buffer, backingBytes: number): Buffer {
  if (backingBytes <= Math.max(MIN_PINNED_BACKING_BYTES, value.byteLength * 2)) {
    return value;
  }
  if (value.byteLength === 0) return Buffer.alloc(0);
  const detached = Buffer.allocUnsafeSlow(value.byteLength);
  value.copy(detached);
  return detached;
}

function isDecodedRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value == null) return false;
  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
}
