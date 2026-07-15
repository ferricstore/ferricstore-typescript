import type { Codec } from "./codecs.js";
import { setLongTimeout } from "./internal-timers.js";
import { mapDenseResponseArray } from "./response-array-normalization.js";
export * from "./auto-partition.js";
export * from "./flow-argument-helpers.js";
export * from "./internal-array-responses.js";
export * from "./internal-timers.js";

export type CommandArgument =
  | string
  | Buffer
  | number
  | bigint
  | boolean
  | readonly CommandArgument[]
  | Record<string, unknown>
  | null
  | undefined;
export type Command = readonly CommandArgument[];
export type RespMap = Map<unknown, unknown> | Record<PropertyKey, unknown>;

export function nowMs(): number {
  return Date.now();
}

/** Decode a typed textual reply without coercing arbitrary server values. */
export function textResponse(value: unknown, context = "server"): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  throw new TypeError(`${context} returned an invalid text response`);
}

export function append(args: CommandArgument[], name: string, value: unknown): void {
  if (value != null) {
    args.push(name, value as CommandArgument);
  }
}

export function appendBool(args: CommandArgument[], name: string, value: boolean | null | undefined): void {
  if (value != null) {
    args.push(name, value ? "true" : "false");
  }
}

export function appendEncoded(
  args: CommandArgument[],
  name: string,
  codec: Codec,
  value: unknown
): void {
  if (value != null) {
    args.push(name, codec.encode(value));
  }
}

export function okResponse(value: unknown): boolean {
  if (value === true) return true;
  const source = Buffer.isBuffer(value) || value instanceof Uint8Array
    ? Buffer.from(value).toString("utf8")
    : value;
  if (source === "OK") return true;
  throw new TypeError("server returned an invalid OK response");
}

export function binaryBooleanResponse(value: unknown): boolean {
  const parsed = integer(value, Number.NaN);
  if (parsed === 1) return true;
  if (parsed === 0) return false;
  throw new TypeError("server returned an invalid binary boolean response");
}

export function booleanResponse(value: unknown): boolean {
  if (value === true || value === 1 || value === 1n) return true;
  if (value == null || value === false || value === 0 || value === 0n) return false;
  const source = Buffer.isBuffer(value) || value instanceof Uint8Array
    ? Buffer.from(value).toString("utf8")
    : value;
  if (typeof source === "string") {
    if (source === "1" || source.toLowerCase() === "true") return true;
    if (source === "0" || source.toLowerCase() === "false") return false;
  }
  throw new TypeError("server returned an invalid boolean response");
}

export function text(value: unknown): string {
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  return String(value);
}

export function optionalString(value: unknown): string | undefined {
  if (
    value == null ||
    value === "" ||
    ((Buffer.isBuffer(value) || value instanceof Uint8Array) && value.byteLength === 0)
  ) {
    return undefined;
  }
  return text(value);
}

export function bytes(value: unknown): Buffer {
  if (value == null) {
    return Buffer.alloc(0);
  }
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  if (typeof value === "object") {
    return Buffer.from(JSON.stringify(value) ?? "");
  }
  if (typeof value === "string") {
    return Buffer.from(value);
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return Buffer.from(String(value));
  }
  if (typeof value === "symbol") {
    return Buffer.from(value.description ?? "");
  }
  if (typeof value === "function") {
    return Buffer.from(value.name);
  }
  return Buffer.alloc(0);
}

export function integer(value: unknown, defaultValue?: number): number {
  if (value == null || value === "") {
    if (defaultValue != null) return defaultValue;
    throw new TypeError("integer response is not an integer");
  }
  if (typeof value === "bigint") {
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new RangeError("integer exceeds the JavaScript safe range");
    }
    return Number(value);
  }
  const source = Buffer.isBuffer(value) || value instanceof Uint8Array
    ? Buffer.from(value).toString("utf8")
    : value;
  if (typeof source !== "number" && typeof source !== "string") {
    throw new TypeError("integer response is not an integer");
  }
  if (typeof source === "string" && !/^[+-]?\d+$/u.test(source)) {
    throw new TypeError("integer response is not an integer");
  }
  const number = Number(source);
  if (!Number.isFinite(number) || !Number.isInteger(number)) {
    throw new TypeError("integer response is not an integer");
  }
  if (!Number.isSafeInteger(number)) {
    throw new RangeError("integer exceeds the JavaScript safe range");
  }
  return number;
}

/** Preserve an integer reply exactly, using bigint only outside Number's safe range. */
export function integerReply(value: unknown): number | bigint {
  let result: bigint;
  if (typeof value === "bigint") {
    result = value;
  } else if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new TypeError("integer response is not an integer");
    }
    if (!Number.isSafeInteger(value)) {
      throw new RangeError("integer response exceeds the JavaScript safe range");
    }
    return value;
  } else {
    const source = Buffer.isBuffer(value) || value instanceof Uint8Array
      ? Buffer.from(value).toString("utf8")
      : String(value);
    if (!/^[+-]?\d+$/u.test(source)) {
      throw new TypeError("integer response is not an integer");
    }
    result = BigInt(source);
  }
  return result <= BigInt(Number.MAX_SAFE_INTEGER) && result >= BigInt(Number.MIN_SAFE_INTEGER)
    ? Number(result)
    : result;
}

export function field(source: unknown, key: string): unknown {
  if (source instanceof Map) {
    if (source.has(key)) {
      return source.get(key);
    }
    const raw = Buffer.from(key);
    for (const [itemKey, value] of source.entries()) {
      const binaryKey = Buffer.isBuffer(itemKey) ? itemKey
        : itemKey instanceof Uint8Array ? Buffer.from(itemKey) : undefined;
      if (binaryKey?.equals(raw) === true) {
        return value;
      }
    }
    return undefined;
  }

  if (typeof source === "object" && source != null) {
    const record = source as Record<PropertyKey, unknown>;
    if (Object.hasOwn(record, key)) {
      return record[key];
    }
    const rawKey = Buffer.from(key).toString();
    if (Object.hasOwn(record, rawKey)) {
      return record[rawKey];
    }
  }

  return undefined;
}

export function toStringKeyMap(value: unknown): Record<string, unknown> | undefined {
  if (value == null) {
    return undefined;
  }

  const result: Record<string, unknown> = {};
  if (value instanceof Map) {
    for (const [key, item] of value.entries()) {
      setOwnValue(result, text(key), normalizeRefMeta(item));
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
      setOwnValue(result, text(key), normalizeRefMeta(item));
    }
    return result;
  }

  return undefined;
}

export function normalizeRefMeta(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      setOwnValue(result, text(key), normalizeRefMeta(item));
    }
    return result;
  }
  if (Array.isArray(value)) {
    return mapDenseResponseArray(value, normalizeRefMeta);
  }
  if (typeof value === "object" && value != null) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      setOwnValue(result, text(key), normalizeRefMeta(item));
    }
    return result;
  }
  return value;
}

export function parseKvResponse(value: unknown): Record<string, unknown> {
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      setOwnValue(result, responseKey(key), item);
    }
    return result;
  }
  if (isPlainResponseObject(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [text(key), item]));
  }
  if (Array.isArray(value)) {
    if (value.length % 2 !== 0) {
      throw new TypeError("server returned an invalid key-value response");
    }
    const result: Record<string, unknown> = {};
    for (let index = 0; index < value.length; index += 2) {
      if (!Object.hasOwn(value, index) || !Object.hasOwn(value, index + 1)) {
        const missing = Object.hasOwn(value, index) ? index + 1 : index;
        throw new TypeError(`key-value response item ${missing} is missing`);
      }
      setOwnValue(result, responseKey(value[index]), value[index + 1]);
    }
    return result;
  }
  if (typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array) {
    const source = text(value);
    const result = parseTextSections(source);
    if (source.trim() !== "" && Object.keys(result).length === 0) {
      throw new TypeError("server returned an invalid key-value response");
    }
    return result;
  }
  throw new TypeError("server returned an invalid key-value response");
}

function responseKey(value: unknown): string {
  if (typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return text(value);
  }
  throw new TypeError("server returned an invalid key-value response");
}

function isPlainResponseObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value == null) return false;
  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
}

export function normalizeFerricUrlHost(host: string): string {
  const normalized = host || "127.0.0.1";
  return normalized.startsWith("[") && normalized.endsWith("]")
    ? normalized.slice(1, -1)
    : normalized;
}

export function setOwnValue<T>(target: Record<string, T>, key: string, value: T): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true
  });
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortError(signal.reason));
      return;
    }

    const onAbort = (): void => {
      timer.cancel();
      reject(abortError(signal?.reason));
    };
    const timer = setLongTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function abortError(reason: unknown): Error {
  if (reason instanceof Error) {
    return reason;
  }
  return new Error(typeof reason === "string" ? reason : "operation aborted", { cause: reason });
}

function parseTextSections(value: string): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  let section: Record<string, unknown> | undefined;

  for (const rawLine of value.split(/\r?\n/u)) {
    const line = rawLine.trimEnd();
    if (line.length === 0) {
      continue;
    }
    if (line.endsWith(":") && !line.startsWith(" ")) {
      section = {};
      setOwnValue(result, line.slice(0, -1), section);
      continue;
    }

    const target = rawLine.startsWith(" ") && section != null ? section : result;
    const colon = line.indexOf(":");
    if (colon >= 0) {
      setOwnValue(target, line.slice(0, colon).trim(), coerceDiagnosticValue(line.slice(colon + 1).trim()));
    }
  }

  return result;
}

function coerceDiagnosticValue(value: string): unknown {
  if (value === "") {
    return value;
  }
  if (value === "true" || value === "false") {
    return value === "true";
  }
  if (/^-?[0-9]+$/u.test(value)) {
    return integerReply(value);
  }
  return value;
}
