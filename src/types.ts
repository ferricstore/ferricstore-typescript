import type { Codec } from "./codecs.js";
import {
  bytes,
  field,
  integer,
  integerReply,
  normalizeRefMeta,
  optionalString,
  setOwnValue,
  text,
  toStringKeyMap,
  type CommandArgument
} from "./internal.js";
import { toStringKeyMapPreservingValues } from "./response-map-preservation.js";
import type { ExceptionPolicy } from "./worker-types.js";
export type {
  BackoffKind,
  BackpressurePolicy,
  ExceptionPolicy,
  RetryPolicy,
  ValueConfig,
  WorkerConfig,
  WorkerProfile,
  WorkerRefillStrategy
} from "./worker-types.js";

export type MaxActiveMs = number | "infinity";
/** An exact Flow fencing token; bigint is used beyond JavaScript's safe integer range. */
export type FencingToken = number | bigint;

export interface ChildSpec {
  id: string;
  type: string;
  payload?: unknown;
  partitionKey?: string;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
}

export type StateMetaValue = string | number | boolean | Buffer;
export type StateMeta = Record<string, StateMetaValue>;

export interface CreateItem {
  id: string;
  payload?: unknown;
  partitionKey?: string;
  attributes?: Record<string, CommandArgument>;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  stateMeta?: StateMeta;
}

/** @internal */
export const CLAIMED_ITEM_WIRE: unique symbol = Symbol("ferricstore.claimedItemWire");

/** @internal */
export interface ClaimedItemWire {
  id: Buffer;
  partitionKey?: Buffer | null;
  leaseToken: Buffer;
  fencingToken: FencingToken;
}

export interface ClaimedItem<TPayload = unknown> {
  id: string;
  leaseToken: Buffer;
  fencingToken: FencingToken;
  partitionKey?: string;
  /** Present when supplied by a full response or known compact-claim context. */
  type?: string;
  state: string;
  runState?: string;
  payload?: TPayload | null;
  attributes?: Record<string, unknown>;
  /** @internal */
  [CLAIMED_ITEM_WIRE]?: ClaimedItemWire;
}

export interface FencedItem {
  id: string;
  fencingToken: FencingToken;
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

export interface FetchOrComputeHitResult<T = unknown> {
  readonly computeMode: "hit";
  readonly hit: true;
  readonly shouldCompute: false;
  readonly status: "hit";
  readonly value: T | null;
}

export interface FetchOrComputeComputeResult {
  /** Opaque application hint echoed for the process elected to compute. */
  readonly computeHint: Buffer;
  readonly computeMode: "fenced";
  /** Fencing token required when publishing the computed result or error. */
  readonly computeToken: Buffer;
  readonly hit: false;
  readonly shouldCompute: true;
  readonly status: "compute";
}

export type FetchOrComputeFencedResult = FetchOrComputeComputeResult;
export type FetchOrComputeResult<T = unknown> = FetchOrComputeHitResult<T> | FetchOrComputeComputeResult;

export interface FlowMaxActiveFailure {
  readonly maxActiveMs: number;
  readonly reason: "max_active_ms";
}

export interface FlowRecord<TPayload = unknown> {
  id: string;
  type: string;
  state: string;
  partitionKey: string;
  runState?: string;
  payload?: TPayload | null;
  leaseToken: Buffer;
  fencingToken: FencingToken;
  version: number;
  parentFlowId?: string;
  rootFlowId?: string;
  correlationId?: string;
  maxActiveMs?: number;
  error?: unknown;
  failureReason?: string;
  valueRefs?: Record<string, unknown>;
  values?: Record<string, unknown>;
  valueSizes?: Record<string, unknown>;
  valueOmitted?: Record<string, unknown>;
  valueMissing?: Record<string, unknown>;
  attributes?: Record<string, unknown>;
  stateMeta?: Record<string, unknown>;
  indexedStateMeta?: string;
  raw?: unknown;
}

export { fetchOrComputeResultFromResp, keyInfoFromResp, rateLimitResultFromResp } from "./native-kv-responses.js";

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
  codec?: Codec,
  context: { readonly type?: string } = {}
): ClaimedItem<TPayload> {
  if (Array.isArray(value)) {
    const tuple = value as unknown[];
    if (tuple.length < 4 || tuple.length > 6) {
      throw new TypeError("compact claim returned an unexpected response");
    }
    const id = requiredBytes(tuple[0], "compact claim id");
    const partition = tuple[1] == null ? null : requiredBytes(tuple[1], "compact claim partition");
    const leaseToken = requiredBytes(tuple[2], "compact claim lease token");
    const fencingToken = requiredFencingToken(tuple[3], "compact claim fencing token");
    const legacyAttributes = toStringKeyMapPreservingValues(tuple[4]);
    const rawAttributes = tuple[5] ?? legacyAttributes;
    const attributes = rawAttributes == null ? undefined : toStringKeyMapPreservingValues(rawAttributes);
    if (rawAttributes != null && attributes == null) {
      throw new TypeError("compact claim attributes returned an unexpected response");
    }
    const item: ClaimedItem<TPayload> = {
      id: text(id),
      partitionKey: optionalString(partition),
      leaseToken,
      fencingToken,
      runState: legacyAttributes == null ? optionalResponseString(tuple[4], "compact claim run state") : undefined,
      ...(attributes == null ? {} : { attributes }),
      ...(context.type == null ? {} : { type: context.type }),
      state: "running"
    };
    Object.defineProperty(item, CLAIMED_ITEM_WIRE, {
      enumerable: false,
      value: { id, partitionKey: partition, leaseToken, fencingToken }
    });
    return item;
  }

  assertResponseRecord(value, "FLOW claim");
  const payload = field(value, "payload");
  return {
    id: requiredTextField(value, "id", "FLOW claim"),
    leaseToken: requiredBytes(field(value, "lease_token"), "FLOW claim lease_token"),
    fencingToken: requiredFencingToken(field(value, "fencing_token"), "FLOW claim fencing_token"),
    partitionKey: optionalResponseString(field(value, "partition_key"), "FLOW claim partition_key"),
    type: requiredTextField(value, "type", "FLOW claim"),
    state: requiredTextField(value, "state", "FLOW claim"),
    runState: optionalResponseString(field(value, "run_state"), "FLOW claim run_state"),
    attributes: optionalMapField(value, "attributes", "FLOW claim", true),
    payload: payload == null ? undefined : (decodePayload(codec, payload) as TPayload | null)
  };
}

export function flowRecordFromResp<TPayload = unknown>(
  value: unknown,
  codec?: Codec
): FlowRecord<TPayload> {
  assertResponseRecord(value, "FLOW record");
  const payload = field(value, "payload");
  const values = decodeValues(field(value, "values"), codec, "FLOW record values");
  const maxActiveMs = optionalPositiveResponseInteger(
    field(value, "max_active_ms"),
    "FLOW record max_active_ms"
  );
  const rawError = field(value, "error");
  const error = flowError(rawError, codec);
  const failureReason = optionalResponseString(field(rawError, "reason"), "FLOW record error reason");

  return {
    id: requiredTextField(value, "id", "FLOW record"),
    type: requiredTextField(value, "type", "FLOW record"),
    state: requiredTextField(value, "state", "FLOW record"),
    partitionKey: optionalResponseString(field(value, "partition_key"), "FLOW record partition_key") ?? "",
    runState: optionalResponseString(field(value, "run_state"), "FLOW record run_state"),
    payload: payload == null ? undefined : (decodePayload(codec, payload) as TPayload | null),
    leaseToken: optionalResponseBytes(field(value, "lease_token"), "FLOW record lease_token"),
    fencingToken: optionalFencingToken(field(value, "fencing_token"), "FLOW record fencing_token"),
    version: optionalResponseInteger(field(value, "version"), "FLOW record version"),
    parentFlowId: optionalResponseString(field(value, "parent_flow_id"), "FLOW record parent_flow_id"),
    rootFlowId: optionalResponseString(field(value, "root_flow_id"), "FLOW record root_flow_id"),
    correlationId: optionalResponseString(field(value, "correlation_id"), "FLOW record correlation_id"),
    ...(maxActiveMs == null ? {} : { maxActiveMs }),
    ...(error == null ? {} : { error }),
    ...(failureReason == null ? {} : { failureReason }),
    valueRefs: optionalMapField(value, "value_refs", "FLOW record"),
    values,
    valueSizes: optionalMapField(value, "value_sizes", "FLOW record"),
    valueOmitted: optionalMapField(value, "value_omitted", "FLOW record"),
    valueMissing: optionalMapField(value, "value_missing", "FLOW record"),
    attributes: optionalMapField(value, "attributes", "FLOW record", true),
    stateMeta: optionalMapField(value, "state_meta", "FLOW record", true),
    indexedStateMeta: optionalResponseString(field(value, "indexed_state_meta"), "FLOW record indexed_state_meta"),
    raw: value
  };
}

function flowError(value: unknown, codec: Codec | undefined): unknown {
  if (value == null) return undefined;
  const reason = optionalResponseString(field(value, "reason"), "FLOW record error reason");
  if (reason === "max_active_ms") {
    const maxActiveMs = optionalPositiveResponseInteger(
      field(value, "max_active_ms"),
      "FLOW record error max_active_ms"
    );
    if (maxActiveMs == null) {
      throw new TypeError("FLOW record error max_active_ms returned an unexpected response");
    }
    return { maxActiveMs, reason } satisfies FlowMaxActiveFailure;
  }
  return decodePayload(codec, value);
}

function decodePayload(codec: Codec | undefined, value: unknown): unknown {
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
  if (typeof value === "string" && codec != null) {
    return codec.decode(Buffer.from(value));
  }
  return normalizeRefMeta(value);
}

function decodeValues(
  value: unknown,
  codec: Codec | undefined,
  context: string
): Record<string, unknown> | undefined {
  if (value == null) {
    return undefined;
  }
  const map = toStringKeyMapPreservingValues(value);
  if (map == null) {
    throw new TypeError(`${context} returned an unexpected response`);
  }

  const decoded: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(map)) {
    setOwnValue(decoded, key, decodePayload(codec, item));
  }
  return decoded;
}

function optionalMapField(
  value: unknown,
  key: string,
  context: string,
  preserveValues = false
): Record<string, unknown> | undefined {
  const item = field(value, key);
  if (item == null) {
    return undefined;
  }
  const map = preserveValues ? toStringKeyMapPreservingValues(item) : toStringKeyMap(item);
  if (map == null) {
    throw new TypeError(`${context} ${key} returned an unexpected response`);
  }
  return map;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value == null || Buffer.isBuffer(value) || value instanceof Uint8Array || Array.isArray(value)) {
    return false;
  }
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertResponseRecord(value: unknown, context: string): asserts value is Map<unknown, unknown> | Record<string, unknown> {
  if (!(value instanceof Map) && !isPlainObject(value)) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
}

function requiredTextField(value: unknown, key: string, context: string): string {
  const item = field(value, key);
  if (typeof item !== "string" && !Buffer.isBuffer(item) && !(item instanceof Uint8Array)) {
    throw new TypeError(`${context} is missing required ${key}`);
  }
  const result = text(item);
  if (result.length === 0) {
    throw new TypeError(`${context} is missing required ${key}`);
  }
  return result;
}

function requiredBytes(value: unknown, context: string): Buffer {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  const result = bytes(value);
  if (result.byteLength === 0) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return result;
}

function requiredInteger(value: unknown, context: string): number {
  if (value == null || value === "") {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return integer(value);
}

function requiredFencingToken(value: unknown, context: string): FencingToken {
  if (value == null || value === "") {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return integerReply(value);
}

function optionalResponseString(value: unknown, context: string): string | undefined {
  if (
    value == null ||
    value === "" ||
    ((Buffer.isBuffer(value) || value instanceof Uint8Array) && value.byteLength === 0)
  ) {
    return undefined;
  }
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return text(value);
}

function responseBytes(value: unknown, context: string): Buffer {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return bytes(value);
}

function optionalResponseBytes(value: unknown, context: string): Buffer {
  return value == null ? Buffer.alloc(0) : responseBytes(value, context);
}

function optionalResponseInteger(value: unknown, context: string): number {
  return value == null ? 0 : requiredInteger(value, context);
}

function optionalFencingToken(value: unknown, context: string): FencingToken {
  return value == null ? 0 : requiredFencingToken(value, context);
}

function optionalPositiveResponseInteger(value: unknown, context: string): number | undefined {
  if (value == null) return undefined;
  const result = requiredInteger(value, context);
  if (result <= 0) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return result;
}
