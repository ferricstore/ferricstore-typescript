import { Buffer } from "node:buffer";
import type { Codec } from "./codecs.js";
import { snapshotFlowValue } from "./flow-value-snapshot.js";
import type { CommandArgument } from "./internal.js";
import {
  CLAIMED_ITEM_WIRE,
  type ClaimedItem,
  type ClaimedItemWire,
  type CreateItem,
  type FencedItem,
  type StateMeta
} from "./types.js";

interface FlowManySnapshotOptions {
  attributes?: Record<string, CommandArgument>;
  attributesDelete?: string[];
  attributesMerge?: Record<string, CommandArgument>;
  dropValues?: string[];
  error?: unknown;
  overrideValues?: string[];
  payload?: unknown;
  reason?: unknown;
  result?: unknown;
  stateMeta?: StateMeta;
  valueRefs?: Record<string, string>;
  values?: Record<string, unknown>;
}

const encodedOptionFields = ["error", "payload", "reason", "result"] as const;

/** Capture fields consumed by a later independent chunk at admission. */
export function snapshotCreateItem(item: CreateItem, codec: Codec): CreateItem {
  const snapshot: CreateItem = {
    ...item,
    payload: snapshotFlowValue(codec, item.payload)
  };
  if (item.attributes != null) snapshot.attributes = snapshotCommandRecord(item.attributes);
  if (item.stateMeta != null) snapshot.stateMeta = snapshotStateMeta(item.stateMeta);
  if (item.valueRefs != null) snapshot.valueRefs = snapshotRecord(item.valueRefs, (value) => value);
  if (item.values != null) {
    snapshot.values = snapshotRecord(item.values, (value) => snapshotFlowValue(codec, value));
  }
  return Object.freeze(snapshot);
}

/** Capture shared mutation/create options once so every later chunk sees one wire plan. */
export function snapshotFlowManyOptions<T extends FlowManySnapshotOptions>(
  options: T,
  codec: Codec
): T {
  const snapshot = { ...options };
  const mutable = snapshot as FlowManySnapshotOptions;
  for (const field of encodedOptionFields) {
    const value = options[field];
    if (value != null) mutable[field] = snapshotFlowValue(codec, value);
  }
  if (options.attributes != null) mutable.attributes = snapshotCommandRecord(options.attributes);
  if (options.attributesMerge != null) {
    mutable.attributesMerge = snapshotCommandRecord(options.attributesMerge);
  }
  if (options.attributesDelete != null) mutable.attributesDelete = snapshotArray(options.attributesDelete);
  if (options.dropValues != null) mutable.dropValues = snapshotArray(options.dropValues);
  if (options.overrideValues != null) mutable.overrideValues = snapshotArray(options.overrideValues);
  if (options.stateMeta != null) mutable.stateMeta = snapshotStateMeta(options.stateMeta);
  if (options.valueRefs != null) {
    mutable.valueRefs = snapshotRecord(options.valueRefs, (value) => value);
  }
  if (options.values != null) {
    mutable.values = snapshotRecord(options.values, (value) => snapshotFlowValue(codec, value));
  }
  return Object.freeze(snapshot);
}

/** Capture lease authority before an earlier independent chunk can yield. */
export function snapshotClaimedItem(item: ClaimedItem): ClaimedItem {
  const wire = item[CLAIMED_ITEM_WIRE];
  const leaseToken = Buffer.from(wire?.leaseToken ?? item.leaseToken);
  const snapshot: ClaimedItem = {
    ...item,
    fencingToken: wire?.fencingToken ?? item.fencingToken,
    leaseToken
  };
  if (wire != null) {
    Object.defineProperty(snapshot, CLAIMED_ITEM_WIRE, {
      enumerable: false,
      value: snapshotClaimedItemWire(wire, leaseToken)
    });
  }
  return Object.freeze(snapshot);
}

/** Capture transition/cancellation authority before a later chunk is built. */
export function snapshotFencedItem(item: FencedItem): FencedItem {
  return Object.freeze({
    ...item,
    ...(item.leaseToken == null ? {} : { leaseToken: Buffer.from(item.leaseToken) })
  });
}

function snapshotClaimedItemWire(wire: ClaimedItemWire, leaseToken: Buffer): ClaimedItemWire {
  return Object.freeze({
    fencingToken: wire.fencingToken,
    id: Buffer.from(wire.id),
    leaseToken,
    partitionKey: wire.partitionKey == null ? wire.partitionKey : Buffer.from(wire.partitionKey)
  });
}

function snapshotArray<T>(values: readonly T[]): T[] {
  const snapshot = new Array<T>(values.length);
  for (let index = 0; index < values.length; index += 1) {
    if (Object.hasOwn(values, index)) snapshot[index] = values[index] as T;
  }
  return Object.freeze(snapshot) as T[];
}

function snapshotStateMeta(stateMeta: StateMeta): StateMeta {
  return snapshotRecord(stateMeta, (value) => Buffer.isBuffer(value) ? Buffer.from(value) : value);
}

function snapshotCommandRecord(
  values: Record<string, CommandArgument>
): Record<string, CommandArgument> {
  const seen = new WeakMap<object, CommandArgument>();
  return snapshotRecord(values, (value) => snapshotCommandArgument(value, seen));
}

function snapshotRecord<T, U>(values: Record<string, T>, capture: (value: T) => U): Record<string, U> {
  const snapshot: Record<string, U> = {};
  for (const [name, value] of Object.entries(values)) {
    Object.defineProperty(snapshot, name, {
      enumerable: true,
      value: capture(value)
    });
  }
  return Object.freeze(snapshot);
}

function snapshotCommandArgument(
  value: CommandArgument,
  seen: WeakMap<object, CommandArgument>
): CommandArgument {
  if (typeof value !== "object" || value == null) return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value);
  const objectValue: object = value;
  const existing = seen.get(objectValue);
  if (existing != null) return existing;
  if (Array.isArray(objectValue)) {
    const values: readonly unknown[] = objectValue;
    const snapshot = new Array<CommandArgument>(values.length);
    seen.set(objectValue, snapshot);
    for (let index = 0; index < values.length; index += 1) {
      if (Object.hasOwn(values, index)) {
        snapshot[index] = snapshotCommandArgument(values[index] as CommandArgument, seen);
      }
    }
    return Object.freeze(snapshot);
  }
  const snapshot: Record<string, unknown> = {};
  seen.set(objectValue, snapshot);
  for (const [name, nested] of Object.entries(objectValue)) {
    Object.defineProperty(snapshot, name, {
      enumerable: true,
      value: snapshotCommandArgument(nested as CommandArgument, seen)
    });
  }
  return Object.freeze(snapshot);
}
