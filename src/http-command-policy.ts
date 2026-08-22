import { Buffer } from "node:buffer";
import { InvalidCommandError } from "./errors.js";

const nativeOnlyCommands = new Set([
  "AUTH",
  "BACKPRESSURE",
  "CLIENT",
  "CLIENT.INFO",
  "CLIENT.SETNAME",
  "COMMAND_EXEC",
  "EVENT",
  "GOAWAY",
  "HELLO",
  "OPTIONS",
  "PIPELINE",
  "QUIT",
  "ROUTE",
  "ROUTE_BATCH",
  "SHARDS",
  "STARTUP",
  "SUBSCRIBE_EVENTS",
  "UNSUBSCRIBE_EVENTS",
  "WINDOW_UPDATE"
]);

const sessionOnlyCommands = new Set([
  "ASKING",
  "AUTH",
  "CLIENT",
  "DISCARD",
  "EXEC",
  "HELLO",
  "MONITOR",
  "MULTI",
  "PSUBSCRIBE",
  "PSYNC",
  "PUNSUBSCRIBE",
  "QUIT",
  "READONLY",
  "READWRITE",
  "REPLCONF",
  "RESET",
  "SANDBOX",
  "SELECT",
  "SSUBSCRIBE",
  "SUBSCRIBE",
  "SUNSUBSCRIBE",
  "SYNC",
  "UNSUBSCRIBE",
  "UNWATCH",
  "WATCH"
]);

export type HTTPCommandDisposition = "supported" | "native_only";

export function httpCommandDisposition(name: string): HTTPCommandDisposition {
  const normalized = name.toUpperCase();
  return nativeOnlyCommands.has(normalized) || sessionOnlyCommands.has(normalized)
    ? "native_only"
    : "supported";
}

export function assertHTTPCommandSupported(name: unknown): void {
  const normalized = normalizedCommandName(name);
  if (normalized == null || normalized === "") throw new TypeError("HTTP command must have a name");
  if (sessionOnlyCommands.has(normalized)) {
    throw new InvalidCommandError(`${normalized} requires a persistent native TCP session`);
  }
  if (nativeOnlyCommands.has(normalized)) {
    throw new InvalidCommandError(`${normalized} is a native TCP transport control command`);
  }
}

function normalizedCommandName(value: unknown): string | undefined {
  if (typeof value === "string") return value.toUpperCase();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase();
  }
  return undefined;
}
