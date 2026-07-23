import { Buffer } from "node:buffer";
import type { SearchOptions } from "./client-options.js";

const MAX_FLOW_QUERY_METADATA_KEY_BYTES = 64;

export function normalizeStateMeta(
  value: SearchOptions["stateMeta"],
  state: string | undefined,
): Readonly<Record<string, Readonly<Record<string, unknown>>>> {
  if (value == null) return {};
  const entries = objectEntries(value, "stateMeta");
  if (entries.length === 0) return {};
  const nested = entries.map(([, item]) => isPlainRecord(item));
  if (nested.every(Boolean)) {
    return value as Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  }
  if (nested.some(Boolean)) {
    throw new TypeError(
      "stateMeta must be entirely flat or entirely nested by state",
    );
  }
  if (state == null || state === "" || state === "any") {
    throw new TypeError(
      "FLOW.QUERY stateMeta filters require state; a concrete state is required",
    );
  }
  return { [state]: value };
}

export function metadataEntries(
  value: Readonly<Record<string, unknown>> | undefined,
  context: string,
): readonly (readonly [string, unknown])[] {
  const entries = objectEntries(value ?? {}, context).map(([rawName, item]) => {
    const name = rawName.trim();
    const size = Buffer.byteLength(name, "utf8");
    if (
      size === 0 ||
      size > MAX_FLOW_QUERY_METADATA_KEY_BYTES ||
      name.startsWith("__")
    ) {
      throw new TypeError(`${context} key is invalid or reserved`);
    }
    return [name, item] as const;
  });
  entries.sort(([left], [right]) => compareUnicodeScalars(left, right));
  for (let index = 1; index < entries.length; index += 1) {
    if (entries[index - 1]?.[0] === entries[index]?.[0]) {
      throw new TypeError(`${context} key is duplicated after normalization`);
    }
  }
  return entries;
}

export function objectEntries(
  value: Readonly<Record<string, unknown>>,
  context: string,
): readonly (readonly [string, unknown])[] {
  if (!isPlainRecord(value))
    throw new TypeError(`${context} must be a plain object`);
  const keys = Reflect.ownKeys(value);
  if (keys.some((key) => typeof key !== "string")) {
    throw new TypeError(`${context} keys must be strings`);
  }
  return (keys as string[]).map((key) => [key, value[key]] as const);
}

export function isPlainRecord(
  value: unknown,
): value is Readonly<Record<string, unknown>> {
  if (
    typeof value !== "object" ||
    value == null ||
    Array.isArray(value) ||
    Buffer.isBuffer(value)
  ) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value) as unknown;
  return prototype === null || prototype === Object.prototype;
}

export function metadataSelector(root: string, ...names: string[]): string {
  return (
    root + names.map((name) => `['${name.replaceAll("'", "''")}']`).join("")
  );
}

export function compareUnicodeScalars(left: string, right: string): number {
  let leftOffset = 0;
  let rightOffset = 0;
  while (leftOffset < left.length && rightOffset < right.length) {
    const leftCodePoint = left.codePointAt(leftOffset);
    const rightCodePoint = right.codePointAt(rightOffset);
    if (leftCodePoint == null || rightCodePoint == null) break;
    if (leftCodePoint !== rightCodePoint)
      return leftCodePoint < rightCodePoint ? -1 : 1;
    leftOffset += leftCodePoint > 0xffff ? 2 : 1;
    rightOffset += rightCodePoint > 0xffff ? 2 : 1;
  }
  return left.length === right.length ? 0 : leftOffset === left.length ? -1 : 1;
}
