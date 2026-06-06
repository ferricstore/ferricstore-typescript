import type { Codec } from "./codecs.js";
import { bytes, field, integer, normalizeRefMeta, optionalString, text, toStringKeyMap } from "./internal.js";

export type ExceptionPolicy = "retry" | "fail" | "raise";
export type BackoffKind = "fixed" | "linear" | "exponential";

export interface RetryPolicy {
  maxRetries?: number;
  backoff?: BackoffKind | string;
  baseMs?: number;
  maxMs?: number;
  jitterPct?: number;
  exhaustedTo?: string;
}

export interface BackpressurePolicy {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterPct?: number;
}

export interface ValueConfig {
  valueMaxBytes?: number;
  localCache?: boolean;
}

export interface WorkerConfig {
  workers?: number;
  concurrency?: number;
  batchSize?: number;
  leaseMs?: number;
  priority?: number;
  reclaimExpired?: boolean;
  reclaimRatio?: number;
  claimValues?: string[];
  valueMaxBytes?: number;
  blockMs?: number;
  idleSleepMs?: number;
  maxIdleSleepMs?: number;
  exceptionPolicy?: ExceptionPolicy;
  completeIndependent?: boolean;
  signal?: AbortSignal;
  worker?: string;
}

export interface ChildSpec {
  id: string;
  type: string;
  payload?: unknown;
  partitionKey?: string;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
}

export interface CreateItem {
  id: string;
  payload?: unknown;
  partitionKey?: string;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
}

export interface ClaimedItem<TPayload = unknown> {
  id: string;
  leaseToken: Buffer;
  fencingToken: number;
  partitionKey?: string;
  type: string;
  state: string;
  runState?: string;
  payload?: TPayload | null;
}

export interface FencedItem {
  id: string;
  fencingToken: number;
  leaseToken?: Buffer;
  partitionKey?: string;
}

export interface RateLimitResult {
  status: string;
  count: number;
  remaining: number;
  resetMs: number;
  allowed: boolean;
}

export interface KeyInfo {
  type: string;
  valueSize: number;
  ttlMs: number;
  hotCacheStatus: string;
  lastWriteShard: number;
  raw: Record<string, unknown>;
}

export interface FetchOrComputeResult<T = unknown> {
  status: "hit" | "compute" | string;
  value?: T | null;
  computeToken?: Buffer;
  hit: boolean;
  shouldCompute: boolean;
}

export interface FlowRecord<TPayload = unknown> {
  id: string;
  type: string;
  state: string;
  partitionKey: string;
  runState?: string;
  payload?: TPayload | null;
  leaseToken: Buffer;
  fencingToken: number;
  version: number;
  parentFlowId?: string;
  rootFlowId?: string;
  correlationId?: string;
  valueRefs?: Record<string, unknown>;
  values?: Record<string, unknown>;
  valueSizes?: Record<string, unknown>;
  valueOmitted?: Record<string, unknown>;
  valueMissing?: Record<string, unknown>;
  raw?: unknown;
}

export function normalizeExceptionPolicy(value: ExceptionPolicy | undefined): ExceptionPolicy {
  if (value == null) {
    return "retry";
  }
  if (value === "retry" || value === "fail" || value === "raise") {
    return value;
  }
  throw new Error("exceptionPolicy must be 'retry', 'fail', or 'raise'");
}

export function claimedItemFromResp<TPayload = unknown>(
  value: unknown,
  codec?: Codec<unknown>
): ClaimedItem<TPayload> {
  if (Array.isArray(value)) {
    return {
      id: text(value[0]),
      partitionKey: optionalString(value[1]),
      leaseToken: bytes(value[2]),
      fencingToken: integer(value[3]),
      runState: optionalString(value[4]),
      type: "",
      state: "running"
    };
  }

  const payload = field(value, "payload");
  return {
    id: text(field(value, "id") ?? ""),
    leaseToken: bytes(field(value, "lease_token")),
    fencingToken: integer(field(value, "fencing_token")),
    partitionKey: optionalString(field(value, "partition_key")),
    type: text(field(value, "type") ?? ""),
    state: optionalString(field(value, "state")) ?? "running",
    runState: optionalString(field(value, "run_state")),
    payload: payload == null ? undefined : (decodePayload(codec, payload) as TPayload | null)
  };
}

export function flowRecordFromResp<TPayload = unknown>(
  value: unknown,
  codec?: Codec<unknown>
): FlowRecord<TPayload> {
  const payload = field(value, "payload");
  const values = decodeValues(field(value, "values"), codec);

  return {
    id: text(field(value, "id") ?? ""),
    type: text(field(value, "type") ?? ""),
    state: text(field(value, "state") ?? ""),
    partitionKey: text(field(value, "partition_key") ?? ""),
    runState: optionalString(field(value, "run_state")),
    payload: payload == null ? undefined : (decodePayload(codec, payload) as TPayload | null),
    leaseToken: bytes(field(value, "lease_token")),
    fencingToken: integer(field(value, "fencing_token")),
    version: integer(field(value, "version")),
    parentFlowId: optionalString(field(value, "parent_flow_id")),
    rootFlowId: optionalString(field(value, "root_flow_id")),
    correlationId: optionalString(field(value, "correlation_id")),
    valueRefs: toStringKeyMap(field(value, "value_refs")),
    values,
    valueSizes: toStringKeyMap(field(value, "value_sizes")),
    valueOmitted: toStringKeyMap(field(value, "value_omitted")),
    valueMissing: toStringKeyMap(field(value, "value_missing")),
    raw: value
  };
}

export function rateLimitResultFromResp(value: unknown): RateLimitResult {
  if (!Array.isArray(value)) {
    throw new TypeError("RATELIMIT.ADD returned an unexpected response");
  }
  const status = text(value[0]);
  return {
    status,
    count: integer(value[1]),
    remaining: integer(value[2]),
    resetMs: integer(value[3]),
    allowed: status === "allowed"
  };
}

export function keyInfoFromResp(value: unknown): KeyInfo {
  const raw = toStringKeyMap(value) ?? {};
  return {
    type: text(raw.type ?? ""),
    valueSize: integer(raw.value_size),
    ttlMs: integer(raw.ttl_ms),
    hotCacheStatus: text(raw.hot_cache_status ?? ""),
    lastWriteShard: integer(raw.last_write_shard),
    raw
  };
}

export function fetchOrComputeResultFromResp<T = unknown>(
  value: unknown,
  codec: Codec<unknown>
): FetchOrComputeResult<T> {
  if (!Array.isArray(value)) {
    throw new TypeError("FETCH_OR_COMPUTE returned an unexpected response");
  }
  const status = text(value[0]);
  if (status === "hit") {
    return {
      status,
      value: decodePayload(codec, value[1]) as T | null,
      hit: true,
      shouldCompute: false
    };
  }
  return {
    status,
    computeToken: bytes(value[1]),
    hit: false,
    shouldCompute: true
  };
}

function decodePayload(codec: Codec<unknown> | undefined, value: unknown): unknown {
  if (value == null) {
    return null;
  }
  if (Buffer.isBuffer(value)) {
    return codec?.decode(value) ?? value;
  }
  if (value instanceof Uint8Array) {
    const buffer = Buffer.from(value);
    return codec?.decode(buffer) ?? buffer;
  }
  return normalizeRefMeta(value);
}

function decodeValues(value: unknown, codec: Codec<unknown> | undefined): Record<string, unknown> | undefined {
  const map = toStringKeyMap(value);
  if (map == null) {
    return undefined;
  }

  const decoded: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(map)) {
    decoded[key] = decodePayload(codec, item);
  }
  return decoded;
}
