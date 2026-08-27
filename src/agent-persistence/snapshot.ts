import { createHash } from "node:crypto";

type EncodedSnapshot =
  | ["array", EncodedSnapshot[]]
  | ["binary", string]
  | ["boolean", boolean]
  | ["null"]
  | ["number", number | "NaN" | "+Infinity" | "-Infinity" | "-0"]
  | ["object", [string, EncodedSnapshot][]]
  | ["string", string]
  | ["undefined"];

export function encodeSnapshot(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(snapshot(value, new WeakSet())), "utf8");
}

export function decodeSnapshot<T>(value: unknown, name: string): T {
  const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(asBytes(value, name));
  let encoded: unknown;
  try {
    encoded = JSON.parse(bytes.toString("utf8"));
  } catch (error) {
    throw new Error(`FerricStore returned an invalid ${name}`, { cause: error });
  }
  return restore(encoded, name) as T;
}

export function snapshotDigest(value: unknown): string {
  return createHash("sha256").update(encodeSnapshot(value)).digest("hex");
}

export function cloneSnapshot<T>(value: T): T {
  return decodeSnapshot<T>(encodeSnapshot(value), "snapshot");
}

export function snapshotsEqual(left: unknown, right: unknown): boolean {
  return encodeSnapshot(left).equals(encodeSnapshot(right));
}

function snapshot(value: unknown, ancestors: WeakSet<object>): EncodedSnapshot {
  if (value === null) return ["null"];
  if (value === undefined) return ["undefined"];
  if (typeof value === "string") return ["string", value];
  if (typeof value === "boolean") return ["boolean", value];
  if (typeof value === "number") {
    if (Object.is(value, -0)) return ["number", "-0"];
    if (Number.isNaN(value)) return ["number", "NaN"];
    if (value === Infinity) return ["number", "+Infinity"];
    if (value === -Infinity) return ["number", "-Infinity"];
    return ["number", value];
  }
  if (typeof value !== "object") throw new TypeError("session history contains unsupported data");
  if (value instanceof Uint8Array) {
    const keys = Reflect.ownKeys(value);
    if (
      keys.length !== value.length ||
      keys.some((key) => typeof key !== "string" || !isArrayIndex(key, value.length))
    ) {
      throw new TypeError("session history binary data contains custom properties");
    }
    return ["binary", Buffer.from(value).toString("base64")];
  }
  if (ancestors.has(value)) throw new TypeError("session history contains cyclic data");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const keys = Reflect.ownKeys(value);
      if (
        keys.length !== value.length + 1 ||
        keys.some((key) => typeof key !== "string" || key !== "length" && !isArrayIndex(key, value.length))
      ) {
        throw new TypeError("session history contains a sparse or customized array");
      }
      const items: EncodedSnapshot[] = [];
      for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (descriptor == null || !descriptor.enumerable || !("value" in descriptor)) {
          throw new TypeError("session history contains an unsupported array item");
        }
        items.push(snapshot(descriptor.value as unknown, ancestors));
      }
      return ["array", items];
    }
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("session history contains an unsupported object");
    }
    const keys = Reflect.ownKeys(value);
    if (keys.some((key) => typeof key !== "string")) {
      throw new TypeError("session history contains a symbol property");
    }
    const entries: [string, EncodedSnapshot][] = [];
    for (const key of (keys as string[]).sort((left, right) => left.localeCompare(right))) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor == null || !descriptor.enumerable || !("value" in descriptor)) {
        throw new TypeError("session history contains an unsupported property");
      }
      entries.push([key, snapshot(descriptor.value as unknown, ancestors)]);
    }
    return ["object", entries];
  } finally {
    ancestors.delete(value);
  }
}

function restore(value: unknown, name: string): unknown {
  if (!Array.isArray(value) || typeof value[0] !== "string") throw new TypeError(`invalid ${name}`);
  switch (value[0]) {
    case "null": return null;
    case "undefined": return undefined;
    case "string": return requireValueType(value[1], "string", name);
    case "boolean": return requireValueType(value[1], "boolean", name);
    case "binary": return Buffer.from(requireValueType(value[1], "string", name), "base64");
    case "number": return restoreNumber(value[1], name);
    case "array": {
      if (!Array.isArray(value[1])) throw new TypeError(`invalid ${name}`);
      return value[1].map((item) => restore(item, name));
    }
    case "object": {
      if (!Array.isArray(value[1])) throw new TypeError(`invalid ${name}`);
      const result: Record<string, unknown> = {};
      for (const entry of value[1]) {
        if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
          throw new TypeError(`invalid ${name}`);
        }
        Object.defineProperty(result, entry[0], {
          configurable: true,
          enumerable: true,
          value: restore(entry[1], name),
          writable: true
        });
      }
      return result;
    }
    default: throw new TypeError(`invalid ${name}`);
  }
}

function restoreNumber(value: unknown, name: string): number {
  if (typeof value === "number") return value;
  if (value === "NaN") return Number.NaN;
  if (value === "+Infinity") return Infinity;
  if (value === "-Infinity") return -Infinity;
  if (value === "-0") return -0;
  throw new TypeError(`invalid ${name}`);
}

function requireValueType<T extends "boolean" | "string">(
  value: unknown,
  type: T,
  name: string
): T extends "string" ? string : boolean {
  if (typeof value !== type) throw new TypeError(`invalid ${name}`);
  return value as T extends "string" ? string : boolean;
}

function asBytes(value: unknown, name: string): Uint8Array {
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value;
  throw new TypeError(`FerricStore returned a non-binary ${name}`);
}

function isArrayIndex(key: string, length: number): boolean {
  if (!/^(?:0|[1-9]\d*)$/u.test(key)) return false;
  const index = Number(key);
  return Number.isSafeInteger(index) && index >= 0 && index < length;
}
