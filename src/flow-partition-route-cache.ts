import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

const AUTO_PREFIX = Buffer.from("__flow_auto__:", "ascii");
const MAX_CACHEABLE_PARTITION_BYTES = 4 * 1_024;
const MAX_CACHE_BYTES = 1 * 1_024 * 1_024;
const MAX_CACHE_ENTRIES = 1_024;
const CACHE_ENTRY_OVERHEAD_BYTES = 96;

interface CacheEntry {
  readonly bytes: number;
  readonly routeKey: string;
}

const routeCache = new Map<string, CacheEntry>();
let cacheBytes = 0;
let cacheHits = 0;
let cacheMisses = 0;

/** Produce the server-compatible route key for one explicit Flow partition. */
export function flowLogicalPartitionRoutingKey(value: unknown): string | undefined {
  if (typeof value !== "string" && !Buffer.isBuffer(value)) return undefined;
  const autoBucket = flowAutoBucket(value);
  if (autoBucket != null) return `{fa:${autoBucket}}`;

  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
  if (bytes.byteLength > MAX_CACHEABLE_PARTITION_BYTES) return hashRoute(bytes);
  const cacheKey = bytes.toString("base64");
  const cached = routeCache.get(cacheKey);
  if (cached != null) {
    cacheHits += 1;
    routeCache.delete(cacheKey);
    routeCache.set(cacheKey, cached);
    return cached.routeKey;
  }

  cacheMisses += 1;
  const routeKey = hashRoute(bytes);
  const entryBytes = estimatedEntryBytes(cacheKey, routeKey);
  while (
    routeCache.size >= MAX_CACHE_ENTRIES ||
    (routeCache.size > 0 && cacheBytes + entryBytes > MAX_CACHE_BYTES)
  ) {
    evictOldest();
  }
  if (entryBytes <= MAX_CACHE_BYTES) {
    routeCache.set(cacheKey, { bytes: entryBytes, routeKey });
    cacheBytes += entryBytes;
  }
  return routeKey;
}

/** @internal Deterministic cache telemetry for regression and benchmark guards. */
export function flowPartitionRouteCacheStats(): {
  readonly bytes: number;
  readonly entries: number;
  readonly hits: number;
  readonly maxBytes: number;
  readonly maxEntries: number;
  readonly misses: number;
} {
  return {
    bytes: cacheBytes,
    entries: routeCache.size,
    hits: cacheHits,
    maxBytes: MAX_CACHE_BYTES,
    maxEntries: MAX_CACHE_ENTRIES,
    misses: cacheMisses
  };
}

/** @internal Clear process-local routing memoization between deterministic tests. */
export function resetFlowPartitionRouteCache(): void {
  routeCache.clear();
  cacheBytes = 0;
  cacheHits = 0;
  cacheMisses = 0;
}

function flowAutoBucket(value: string | Buffer): number | undefined {
  if (typeof value === "string") {
    const match = /^__flow_auto__:(0|[1-9]\d{0,2})$/u.exec(value);
    if (match?.[1] == null) return undefined;
    const bucket = Number(match[1]);
    return bucket < 256 ? bucket : undefined;
  }
  const digitCount = value.byteLength - AUTO_PREFIX.byteLength;
  if (digitCount < 1 || digitCount > 3) return undefined;
  for (let index = 0; index < AUTO_PREFIX.byteLength; index += 1) {
    if (value[index] !== AUTO_PREFIX[index]) return undefined;
  }
  if (digitCount > 1 && value[AUTO_PREFIX.byteLength] === 0x30) return undefined;
  let bucket = 0;
  for (let index = AUTO_PREFIX.byteLength; index < value.byteLength; index += 1) {
    const digit = value[index];
    if (digit == null || digit < 0x30 || digit > 0x39) return undefined;
    bucket = bucket * 10 + digit - 0x30;
  }
  return bucket < 256 ? bucket : undefined;
}

function hashRoute(value: Buffer): string {
  const digest = createHash("sha256").update(value).digest("base64url");
  return `{f:${digest}}`;
}

function estimatedEntryBytes(cacheKey: string, routeKey: string): number {
  return CACHE_ENTRY_OVERHEAD_BYTES + 2 * (cacheKey.length + routeKey.length);
}

function evictOldest(): void {
  const oldest = routeCache.entries().next().value;
  if (oldest == null) return;
  routeCache.delete(oldest[0]);
  cacheBytes -= oldest[1].bytes;
}
