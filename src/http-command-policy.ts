const nativeOnlyCommands = new Set([
  "AUTH",
  "BACKPRESSURE",
  "CLIENT",
  "CLIENT.INFO",
  "CLIENT.SETNAME",
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
  "AUTH",
  "BLMOVE",
  "BLMPOP",
  "BLPOP",
  "BRPOP",
  "CLIENT",
  "DISCARD",
  "EXEC",
  "HELLO",
  "MULTI",
  "PSUBSCRIBE",
  "PUNSUBSCRIBE",
  "QUIT",
  "SELECT",
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "UNWATCH",
  "WATCH",
  "XREAD",
  "XREADGROUP"
]);

export type HTTPCommandDisposition = "supported" | "native_only";

export function httpCommandDisposition(name: string): HTTPCommandDisposition {
  const normalized = name.toUpperCase();
  return nativeOnlyCommands.has(normalized) || sessionOnlyCommands.has(normalized)
    ? "native_only"
    : "supported";
}

export function assertHTTPCommandSupported(name: unknown): void {
  if (typeof name !== "string" || name === "") throw new TypeError("HTTP command must have a name");
  if (sessionOnlyCommands.has(name.toUpperCase())) {
    throw new Error(`${name.toUpperCase()} requires a persistent native TCP session`);
  }
}
