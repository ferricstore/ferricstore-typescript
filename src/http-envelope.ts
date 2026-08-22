import { Buffer } from "node:buffer";
import { HTTPTransportError } from "./errors.js";

const encoding = "ferricstore-json-v1";
const bytesMarker = "$ferricstore_bytes";
const mapMarker = "$ferricstore_map";
const maxDepth = 64;
const bytesMarkerBaseBytes = Buffer.byteLength(bytesMarker) + 7;
// {"$ferricstore_map":[]} is the smallest JSON representation of a map.
// Keep this a lower bound: over-estimating here could reject a request whose
// final encoded body is still within maxRequestBytes.
const mapMarkerBaseBytes = Buffer.byteLength(mapMarker) + 7;

interface EncodeBudget {
  remaining: number;
}

export function encodeHTTPCommands(
  commands: readonly unknown[],
  maxBytes = Number.MAX_SAFE_INTEGER
): Buffer {
  const budget: EncodeBudget = { remaining: maxBytes };
  return Buffer.from(JSON.stringify({
    encoding,
    commands: commands.map((command) => encodeValue(command, 0, budget))
  }));
}

export function decodeHTTPEnvelope(source: Buffer): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(source.toString("utf8"));
  } catch (error) {
    throw new TypeError("invalid HTTP command response JSON", { cause: error });
  }
  if (!isRecord(parsed)) throw new TypeError("HTTP command response must be an object");
  return decodePlainRecord(parsed, 0);
}

function encodeValue(value: unknown, depth: number, budget: EncodeBudget): unknown {
  if (depth > maxDepth) throw new TypeError("HTTP command value exceeds maximum depth");
  if (value == null) {
    consumeBudget(budget, 1);
    return value;
  }
  if (typeof value === "string") {
    consumeBudget(budget, Buffer.byteLength(value) + 2);
    return value;
  }
  if (typeof value === "boolean") {
    consumeBudget(budget, 1);
    return value;
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    consumeBudget(budget, bytesMarkerBaseBytes + 4 * Math.ceil(value.byteLength / 3));
    return { [bytesMarker]: Buffer.from(value).toString("base64") };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("HTTP command numbers must be finite");
    consumeBudget(budget, 1);
    return value;
  }
  if (typeof value === "bigint") {
    const encoded = value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
    consumeBudget(budget, typeof encoded === "number" ? 1 : Buffer.byteLength(encoded) + 2);
    return encoded;
  }
  if (Array.isArray(value)) {
    consumeBudget(budget, 2 + Math.max(0, value.length - 1));
    return denseArray(value, depth + 1, (item, itemDepth) => encodeValue(item, itemDepth, budget));
  }
  if (value instanceof Map) {
    consumeBudget(budget, mapMarkerBaseBytes + value.size);
    const pairs: unknown[] = [];
    for (const [key, item] of value.entries()) {
      pairs.push([
        encodeValue(key, depth + 1, budget),
        encodeValue(item, depth + 1, budget)
      ]);
    }
    return { [mapMarker]: pairs };
  }
  if (isRecord(value)) {
    const keys = Object.keys(value);
    consumeBudget(budget, mapMarkerBaseBytes + keys.length);
    return { [mapMarker]: keys.map((key) => [
      encodeValue(key, depth + 1, budget),
      encodeValue(value[key], depth + 1, budget)
    ]) };
  }
  throw new TypeError(`unsupported HTTP command value: ${typeof value}`);
}

function decodeValue(value: unknown, depth: number): unknown {
  if (depth > maxDepth) throw new TypeError("HTTP response value exceeds maximum depth");
  if (Array.isArray(value)) return denseArray(value, depth + 1, decodeValue);
  if (!isRecord(value)) return value;
  if (Object.keys(value).length === 1 && typeof value[bytesMarker] === "string") {
    return decodeBase64(value[bytesMarker]);
  }
  if (Object.keys(value).length === 1 && Array.isArray(value[mapMarker])) {
    const result = new Map<unknown, unknown>();
    for (const pair of value[mapMarker]) {
      if (!Array.isArray(pair) || pair.length !== 2) throw new TypeError("invalid HTTP map marker");
      result.set(decodeValue(pair[0], depth + 1), decodeValue(pair[1], depth + 1));
    }
    return result;
  }
  return decodePlainRecord(value, depth + 1);
}

function decodeBase64(value: string): Buffer {
  if (value.length % 4 !== 0 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new TypeError("invalid HTTP bytes marker");
  }
  const decoded = Buffer.from(value, "base64");
  if (decoded.toString("base64") !== value) throw new TypeError("invalid HTTP bytes marker");
  return decoded;
}

function decodePlainRecord(value: Record<string, unknown>, depth: number): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    Object.defineProperty(result, key, {
      configurable: true,
      enumerable: true,
      value: decodeValue(item, depth),
      writable: true
    });
  }
  return result;
}

function consumeBudget(budget: EncodeBudget, amount: number): void {
  if (amount > budget.remaining) {
    throw new HTTPTransportError("HTTP command request exceeds maxRequestBytes");
  }
  budget.remaining -= amount;
}

function denseArray(
  values: readonly unknown[],
  depth: number,
  transform: (value: unknown, depth: number) => unknown
): unknown[] {
  const result = new Array<unknown>(values.length);
  for (let index = 0; index < values.length; index += 1) {
    if (!Object.hasOwn(values, index)) throw new TypeError("HTTP command arrays must be dense");
    result[index] = transform(values[index], depth);
  }
  return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value);
}
