import { Buffer } from "node:buffer";

const encoding = "ferricstore-json-v1";
const bytesMarker = "$ferricstore_bytes";
const mapMarker = "$ferricstore_map";
const maxDepth = 64;

export function encodeHTTPCommands(commands: readonly unknown[]): Buffer {
  return Buffer.from(JSON.stringify({
    encoding,
    commands: commands.map((command) => encodeValue(command, 0))
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

function encodeValue(value: unknown, depth: number): unknown {
  if (depth > maxDepth) throw new TypeError("HTTP command value exceeds maximum depth");
  if (value == null || typeof value === "string" || typeof value === "boolean") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return { [bytesMarker]: Buffer.from(value).toString("base64") };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("HTTP command numbers must be finite");
    return value;
  }
  if (typeof value === "bigint") {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
  }
  if (Array.isArray(value)) return denseArray(value, depth + 1, encodeValue);
  if (value instanceof Map) {
    return { [mapMarker]: [...value.entries()].map(([key, item]) => [
      encodeValue(key, depth + 1), encodeValue(item, depth + 1)
    ]) };
  }
  if (isRecord(value)) {
    return { [mapMarker]: Object.entries(value).map(([key, item]) => [
      encodeValue(key, depth + 1), encodeValue(item, depth + 1)
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
  for (const [key, item] of Object.entries(value)) result[key] = decodeValue(item, depth);
  return result;
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
