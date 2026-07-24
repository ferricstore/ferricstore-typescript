import { Buffer } from "node:buffer";

const MAX_ERROR_MESSAGE_CHARS = 4_096;
const MAX_ERROR_TEXT_BYTES = MAX_ERROR_MESSAGE_CHARS * 4;

/** Build a bounded message without stringifying attacker-sized response containers. */
export function protocolErrorMessage(status: number | string, value: unknown): string {
  const direct = errorText(value);
  if (direct != null) return truncate(direct);
  return truncate(`ERR native request failed status=${status}: ${errorDescription(value)}`);
}

function errorText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return boundedBinaryText(value);
  }
  if (typeof value === "object" && value != null && Object.hasOwn(value, "message")) {
    const message = (value as Record<string, unknown>).message;
    if (typeof message === "string") return message;
    if (Buffer.isBuffer(message) || message instanceof Uint8Array) {
      return boundedBinaryText(message);
    }
    if (message == null || typeof message !== "object") return String(message);
  }
  return undefined;
}

function boundedBinaryText(value: Buffer | Uint8Array): string {
  const bytes = Buffer.isBuffer(value)
    ? value
    : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  return bytes.subarray(0, MAX_ERROR_TEXT_BYTES).toString("utf8");
}

function errorDescription(value: unknown): string {
  if (Array.isArray(value)) return `[array length=${value.length}]`;
  if (value instanceof Map) return `[map size=${value.size}]`;
  if (typeof value === "object" && value != null) return "[object]";
  return String(value);
}

function truncate(value: string): string {
  return value.length <= MAX_ERROR_MESSAGE_CHARS
    ? value
    : `${value.slice(0, MAX_ERROR_MESSAGE_CHARS - 1)}…`;
}
