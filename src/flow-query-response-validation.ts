import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { field } from "./internal.js";
import type { FlowQueryInteger } from "./flow-query-types.js";

export function requiredMap(
  value: unknown,
  context: string,
): Map<unknown, unknown> | Record<string, unknown> {
  if (value instanceof Map) return value;
  if (
    typeof value !== "object" ||
    value == null ||
    Array.isArray(value) ||
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array
  ) {
    throw decodeError(`${context} must be a map`, value);
  }
  return value as Record<string, unknown>;
}

export function freezeMap(
  value: Map<unknown, unknown> | Record<string, unknown>,
): Readonly<Record<string, unknown>> {
  const output = Object.create(null) as Record<string, unknown>;
  const entries =
    value instanceof Map ? value.entries() : Object.entries(value);
  for (const [rawKey, item] of entries) {
    const key = strictText(rawKey);
    if (key == null || Object.hasOwn(output, key)) {
      throw decodeError(
        "server response map contains an invalid or duplicate key",
        value,
      );
    }
    output[key] = item;
  }
  return Object.freeze(output);
}

export function requireContract(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  expected: string,
  context: string,
): void {
  const actual = requiredText(mapping, name, context);
  if (actual !== expected) {
    throw decodeError(
      `${context} has unsupported contract ${JSON.stringify(actual)}`,
      mapping,
    );
  }
}

export function requiredText(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  context: string,
): string {
  const value = strictText(field(mapping, name));
  if (value == null || value.length === 0) {
    throw decodeError(`${context} ${name} must be non-empty text`, mapping);
  }
  return value;
}

export function optionalText(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  context: string,
): string | undefined {
  const raw = field(mapping, name);
  if (raw == null) return undefined;
  const value = strictText(raw);
  if (value == null)
    throw decodeError(`${context} ${name} must be text`, mapping);
  return value;
}

export function requiredBoundedText(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  context: string,
  maximumBytes: number,
): string {
  const value = requiredText(mapping, name, context);
  if (Buffer.byteLength(value, "utf8") > maximumBytes) {
    throw decodeError(
      `${context} ${name} exceeds ${maximumBytes} bytes`,
      mapping,
    );
  }
  return value;
}

export function requiredBoolean(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  context: string,
): boolean {
  const value = field(mapping, name);
  if (typeof value !== "boolean") {
    throw decodeError(`${context} ${name} must be boolean`, mapping);
  }
  return value;
}

export function nonNegativeInteger(value: unknown, context: string): number {
  if (typeof value === "bigint") {
    if (value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER))
      return Number(value);
  } else if (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0
  ) {
    return value;
  }
  throw decodeError(`${context} must be a non-negative safe integer`, value);
}

export function positiveInteger(value: unknown, context: string): number {
  const parsed = nonNegativeInteger(value, context);
  if (parsed === 0) throw decodeError(`${context} must be positive`, value);
  return parsed;
}

export function boundedInteger(
  value: unknown,
  maximum: bigint,
  context: string,
): FlowQueryInteger {
  let parsed: bigint;
  if (typeof value === "bigint") {
    parsed = value;
  } else if (typeof value === "number" && Number.isSafeInteger(value)) {
    parsed = BigInt(value);
  } else {
    throw decodeError(
      `${context} must be a lossless non-negative integer`,
      value,
    );
  }
  if (parsed < 0n || parsed > maximum) {
    throw decodeError(
      `${context} is outside its supported integer range`,
      value,
    );
  }
  return parsed <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(parsed) : parsed;
}

export function positiveBoundedInteger(
  value: unknown,
  maximum: bigint,
  context: string,
): FlowQueryInteger {
  const parsed = boundedInteger(value, maximum, context);
  if (parsed === 0) throw decodeError(`${context} must be positive`, value);
  return parsed;
}

export function hasKey(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
): boolean {
  if (!(mapping instanceof Map)) return Object.hasOwn(mapping, name);
  if (mapping.has(name)) return true;
  const binaryName = Buffer.from(name);
  for (const key of mapping.keys()) {
    if (Buffer.isBuffer(key) && key.equals(binaryName)) return true;
    if (key instanceof Uint8Array && Buffer.from(key).equals(binaryName))
      return true;
  }
  return false;
}

export function decodeError(message: string, raw: unknown): FerricStoreError {
  return new FerricStoreError(`invalid server response: ${message}`, { raw });
}

function strictText(value: unknown): string | undefined {
  if (typeof value === "string")
    return value.isWellFormed() ? value : undefined;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(value);
    } catch {
      return undefined;
    }
  }
  return undefined;
}
