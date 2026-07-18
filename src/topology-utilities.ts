import { Buffer } from "node:buffer";
import { crc32, crc32Utf8 } from "./crc32.js";
import { FerricStoreError } from "./errors.js";
import { normalizeFerricUrlHost } from "./internal.js";
import type { NativeProtocolEvent } from "./adapters.js";
export { crc32, crc32Utf8 } from "./crc32.js";

export interface RoutingEndpoint {
  readonly node: string;
  readonly host: string;
  readonly nativePort: number;
  readonly nativeTlsPort?: number;
}

export interface ParsedFerricUrl {
  readonly host: string;
  readonly password?: string;
  readonly port: number;
  readonly tls: boolean;
  readonly username?: string;
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value);
}

export function getField(source: unknown, ...keys: string[]): unknown {
  if (source instanceof Map) {
    for (const key of keys) {
      if (source.has(key)) return source.get(key);
      const bufferKey = Buffer.from(key);
      for (const [itemKey, value] of source.entries()) {
        if (Buffer.isBuffer(itemKey) && itemKey.equals(bufferKey)) return value;
      }
    }
    return undefined;
  }
  if (isPlainObject(source)) {
    for (const key of keys) {
      if (Object.hasOwn(source, key)) return source[key];
    }
  }
  return undefined;
}

export function textOrUndefined(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  return undefined;
}

export function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : undefined;
}

export function nativeEventName(event: NativeProtocolEvent): string | undefined {
  return textOrUndefined(getField(event.value, "event"))?.toUpperCase();
}

export function endpointFromRange(item: unknown): RoutingEndpoint {
  const endpointValue = getField(item, "endpoint");
  if (endpointValue != null && !isPlainObject(endpointValue) && !(endpointValue instanceof Map)) {
    throw new FerricStoreError("invalid SHARDS endpoint", { raw: item });
  }
  const raw = endpointValue ?? item;
  const host = textOrUndefined(getField(raw, "host", "native_host"));
  const nativePort = numberOrUndefined(getField(raw, "native_port"));
  if (host == null || !validEndpointHost(host) || nativePort == null || nativePort <= 0 || nativePort > 65_535) {
    throw new FerricStoreError("invalid SHARDS endpoint", { raw: item });
  }
  const nativeTlsPortValue = getField(raw, "native_tls_port");
  const nativeTlsPort = numberOrUndefined(nativeTlsPortValue);
  if (nativeTlsPortValue != null && (nativeTlsPort == null || nativeTlsPort <= 0 || nativeTlsPort > 65_535)) {
    throw new FerricStoreError("invalid SHARDS endpoint", { raw: item });
  }
  return {
    host,
    nativePort,
    node: textOrUndefined(getField(raw, "node", "leader_node", "owner_node")) ?? host,
    ...(nativeTlsPort == null ? {} : { nativeTlsPort })
  };
}

function validEndpointHost(host: string): boolean {
  if (host.length === 0 || host.trim() !== host || /[\s\\/?#@]/u.test(host)) return false;
  const authorityHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
  try {
    const url = new URL(`ferric://${authorityHost}:6388`);
    return url.hostname !== "" && url.port === "6388" && url.username === "" && url.password === "";
  } catch {
    return false;
  }
}

export function sameEndpoint(left: RoutingEndpoint, right: RoutingEndpoint): boolean {
  return normalizedEndpointHost(left.host) === normalizedEndpointHost(right.host) &&
    left.nativePort === right.nativePort &&
    left.nativeTlsPort === right.nativeTlsPort &&
    left.node === right.node;
}

export function parseFerricUrl(value: string): ParsedFerricUrl {
  const url = new URL(value);
  if (url.protocol !== "ferric:" && url.protocol !== "ferrics:") {
    throw new FerricStoreError(`unsupported FerricStore URL scheme: ${url.protocol}`);
  }
  return {
    host: normalizeFerricUrlHost(url.hostname),
    ...(url.password === "" ? {} : { password: decodeURIComponent(url.password) }),
    port: Number(url.port || (url.protocol === "ferrics:" ? 6389 : 6388)),
    tls: url.protocol === "ferrics:",
    ...(url.username === "" ? {} : { username: decodeURIComponent(url.username) })
  };
}

export function endpointKeyFor(endpoint: RoutingEndpoint): string {
  return `${normalizedEndpointHost(endpoint.host)}:${endpoint.nativePort}`;
}

export function connectionKeyForEndpoint(endpoint: RoutingEndpoint, useTls: boolean): string {
  const port = useTls && endpoint.nativeTlsPort != null ? endpoint.nativeTlsPort : endpoint.nativePort;
  return `${useTls ? "ferrics" : "ferric"}://${normalizedEndpointHost(endpoint.host)}:${port}`;
}

export function connectionKeyFromUrl(url: string): string {
  const parsed = parseFerricUrl(url);
  return `${parsed.tls ? "ferrics" : "ferric"}://${normalizedEndpointHost(parsed.host)}:${parsed.port}`;
}

export function normalizedEndpointHost(host: string): string {
  const lower = host.toLowerCase();
  return lower.startsWith("[") && lower.endsWith("]") ? lower.slice(1, -1) : lower;
}

export function urlFromEndpoint(endpoint: RoutingEndpoint, useTls: boolean): string {
  const port = useTls && endpoint.nativeTlsPort != null ? endpoint.nativeTlsPort : endpoint.nativePort;
  const host = endpoint.host.includes(":") && !endpoint.host.startsWith("[") ? `[${endpoint.host}]` : endpoint.host;
  return `${useTls ? "ferrics" : "ferric"}://${host}:${port}`;
}

export async function mapSettledWithConcurrency<T, R>(
  items: readonly T[],
  concurrency: number,
  operation: (item: T, index: number) => Promise<R>
): Promise<PromiseSettledResult<R>[]> {
  const results = new Array<PromiseSettledResult<R>>(items.length);
  let nextIndex = 0;
  const worker = async (): Promise<void> => {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      const item = items[index] as T;
      try {
        results[index] = { status: "fulfilled", value: await operation(item, index) };
      } catch (reason) {
        results[index] = { status: "rejected", reason };
      }
    }
  };
  await Promise.all(Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => await worker()
  ));
  return results;
}

export function throwFirstRejected(results: readonly PromiseSettledResult<unknown>[]): void {
  for (const result of results) {
    if (result.status === "rejected") throw result.reason;
  }
}

export class TaskConcurrencyLimiter {
  private active = 0;
  private head = 0;
  private readonly pending: (() => void)[] = [];

  constructor(private readonly concurrency: number) {}

  async run<T>(operation: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await operation();
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (this.active < this.concurrency) {
      this.active += 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.pending.push(resolve));
  }

  private release(): void {
    const next = this.pending[this.head];
    if (next == null) {
      this.active -= 1;
      return;
    }
    this.head += 1;
    next();
    if (this.head === this.pending.length) {
      this.pending.length = 0;
      this.head = 0;
    } else if (this.head >= 1_024 && this.head * 2 >= this.pending.length) {
      this.pending.splice(0, this.head);
      this.head = 0;
    }
  }
}

export function normalizedHostSet(hosts: readonly (string | undefined)[]): ReadonlySet<string> {
  return new Set(
    hosts
      .filter((host): host is string => host != null && host !== "")
      .map((host) => normalizedEndpointHost(host))
  );
}

export function bufferStartsWith(value: Buffer, prefix: Buffer): boolean {
  return value.byteLength >= prefix.byteLength && value.subarray(0, prefix.byteLength).equals(prefix);
}

const OPEN_BRACE = "{".charCodeAt(0);
const CLOSE_BRACE = "}".charCodeAt(0);

export function hashTagOrKey(key: Buffer): Buffer {
  const start = key.indexOf(OPEN_BRACE);
  if (start < 0) return key;
  const end = key.indexOf(CLOSE_BRACE, start + 1);
  return end > start + 1 ? key.subarray(start + 1, end) : key;
}

const ROUTE_SLOT_MASK = 1_023;

export function routingSlotForKey(key: string | Buffer): number {
  if (typeof key === "string") {
    const [start, end] = stringHashRange(key);
    return crc32Utf8(key, start, end) & ROUTE_SLOT_MASK;
  }
  const keyBytes = key;
  return crc32(hashTagOrKey(keyBytes)) & ROUTE_SLOT_MASK;
}

function stringHashRange(key: string): readonly [number, number] {
  const startBrace = key.indexOf("{");
  const endBrace = startBrace < 0 ? -1 : key.indexOf("}", startBrace + 1);
  return endBrace > startBrace + 1 ? [startBrace + 1, endBrace] : [0, key.length];
}
