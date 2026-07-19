import { Buffer } from "node:buffer";
import { field, integer, setOwnValue } from "./internal.js";
import type { FlowStateMode } from "./client-options.js";

export const MAX_FLOW_POLICY_GENERATION = Number.MAX_SAFE_INTEGER;

export type FlowPolicyBackoffKind = "none" | "fixed" | "linear" | "exponential";

export interface FlowPolicyBackoffSnapshot {
  readonly kind: FlowPolicyBackoffKind;
  readonly baseMs: number;
  readonly maxMs: number;
  readonly jitterPct: number;
}

export interface FlowPolicyRetrySnapshot {
  readonly maxRetries: number;
  readonly backoff: FlowPolicyBackoffSnapshot;
  readonly exhaustedTo: string;
}

export interface FlowPolicyRetentionSnapshot {
  readonly ttlMs: number;
  readonly historyMaxEvents: number;
}

export interface FlowPolicyStateSnapshot {
  readonly mode: FlowStateMode;
  readonly maxActiveMs?: number;
  readonly retry: FlowPolicyRetrySnapshot;
  readonly retention: FlowPolicyRetentionSnapshot;
  readonly governance?: Readonly<Record<string, unknown>>;
}

/** Resolved policy state returned by FLOW.POLICY.SET and FLOW.POLICY.GET. */
export interface FlowPolicySnapshot {
  readonly type: string;
  /** Server-allocated monotonic generation in the JavaScript safe-integer range. */
  readonly generation: number;
  readonly state?: string;
  readonly version?: string | number;
  readonly mode?: FlowStateMode;
  readonly maxActiveMs?: number;
  readonly retry: FlowPolicyRetrySnapshot;
  readonly retention: FlowPolicyRetentionSnapshot;
  readonly indexedAttributes: readonly string[];
  readonly indexedStateMeta?: string;
  readonly governance?: Readonly<Record<string, unknown>>;
  readonly states?: Readonly<Record<string, FlowPolicyStateSnapshot>>;
  /** Original protocol value for forward-compatible fields not yet modeled by the SDK. */
  readonly raw: unknown;
}

export function assertFlowPolicyGeneration(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError("expectedGeneration must be a nonnegative safe integer");
  }
}

export function flowPolicySnapshotFromResp(value: unknown): FlowPolicySnapshot {
  const policy = requiredRecord(value, "FLOW policy snapshot");
  const maxActiveMs = optionalPositiveInteger(field(policy, "max_active_ms"), "max_active_ms");
  const state = optionalText(field(policy, "state"), "state");
  const mode = optionalMode(field(policy, "mode"), "mode");
  const version = optionalVersion(field(policy, "version"));
  const indexedStateMeta = optionalText(field(policy, "indexed_state_meta"), "indexed_state_meta");
  const governance = optionalRecord(field(policy, "governance"), "governance");
  const states = optionalStates(field(policy, "states"));

  return {
    type: requiredText(field(policy, "type"), "type"),
    generation: nonNegativeInteger(field(policy, "generation"), "generation"),
    ...(state == null ? {} : { state }),
    ...(version == null ? {} : { version }),
    ...(mode == null ? {} : { mode }),
    ...(maxActiveMs == null ? {} : { maxActiveMs }),
    retry: retrySnapshot(field(policy, "retry"), "retry"),
    retention: retentionSnapshot(field(policy, "retention"), "retention"),
    indexedAttributes: stringArray(field(policy, "indexed_attributes"), "indexed_attributes"),
    ...(indexedStateMeta == null ? {} : { indexedStateMeta }),
    ...(governance == null ? {} : { governance }),
    ...(states == null ? {} : { states }),
    raw: value
  };
}

function retrySnapshot(value: unknown, context: string): FlowPolicyRetrySnapshot {
  const retry = requiredRecord(value, `FLOW policy ${context}`);
  const backoff = requiredRecord(field(retry, "backoff"), `FLOW policy ${context}.backoff`);
  return {
    maxRetries: nonNegativeInteger(field(retry, "max_retries"), `${context}.max_retries`),
    backoff: {
      kind: backoffKind(field(backoff, "kind"), `${context}.backoff.kind`),
      baseMs: nonNegativeInteger(field(backoff, "base_ms"), `${context}.backoff.base_ms`),
      maxMs: nonNegativeInteger(field(backoff, "max_ms"), `${context}.backoff.max_ms`),
      jitterPct: nonNegativeInteger(field(backoff, "jitter_pct"), `${context}.backoff.jitter_pct`)
    },
    exhaustedTo: requiredText(field(retry, "exhausted_to"), `${context}.exhausted_to`)
  };
}

function retentionSnapshot(value: unknown, context: string): FlowPolicyRetentionSnapshot {
  const retention = requiredRecord(value, `FLOW policy ${context}`);
  return {
    ttlMs: nonNegativeInteger(field(retention, "ttl_ms"), `${context}.ttl_ms`),
    historyMaxEvents: nonNegativeInteger(
      field(retention, "history_max_events"),
      `${context}.history_max_events`
    )
  };
}

function optionalStates(value: unknown): Readonly<Record<string, FlowPolicyStateSnapshot>> | undefined {
  if (value == null) return undefined;
  const source = requiredRecord(value, "FLOW policy states");
  const result: Record<string, FlowPolicyStateSnapshot> = {};
  for (const [state, raw] of Object.entries(source)) {
    const policy = requiredRecord(raw, `FLOW policy state ${state}`);
    const maxActiveMs = optionalPositiveInteger(
      field(policy, "max_active_ms"),
      `states.${state}.max_active_ms`
    );
    const governance = optionalRecord(field(policy, "governance"), `states.${state}.governance`);
    Object.defineProperty(result, state, {
      configurable: true,
      enumerable: true,
      value: {
        mode: requiredMode(field(policy, "mode"), `states.${state}.mode`),
        ...(maxActiveMs == null ? {} : { maxActiveMs }),
        retry: retrySnapshot(field(policy, "retry"), `states.${state}.retry`),
        retention: retentionSnapshot(field(policy, "retention"), `states.${state}.retention`),
        ...(governance == null ? {} : { governance })
      },
      writable: true
    });
  }
  return result;
}

function requiredRecord(value: unknown, context: string): Record<string, unknown> {
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      setOwnValue(result, requiredText(key, `${context} key`), item);
    }
    return result;
  }
  if (
    typeof value === "object"
    && value != null
    && !Array.isArray(value)
    && !Buffer.isBuffer(value)
    && !(value instanceof Uint8Array)
  ) return value as Record<string, unknown>;
  throw new TypeError(`${context} returned an unexpected response`);
}

function optionalRecord(
  value: unknown,
  context: string
): Readonly<Record<string, unknown>> | undefined {
  return value == null ? undefined : requiredRecord(value, `FLOW policy ${context}`);
}

function stringArray(value: unknown, context: string): readonly string[] {
  if (!Array.isArray(value)) {
    throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
  }
  const result = new Array<string>(value.length);
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
    }
    result[index] = requiredText(value[index], `${context}[${index}]`);
  }
  return result;
}

function requiredText(value: unknown, context: string): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
}

function optionalText(value: unknown, context: string): string | undefined {
  return value == null ? undefined : requiredText(value, context);
}

function nonNegativeInteger(value: unknown, context: string): number {
  let parsed: number;
  try {
    parsed = integer(value);
  } catch {
    throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
  }
  if (parsed < 0) throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
  return parsed;
}

function optionalPositiveInteger(value: unknown, context: string): number | undefined {
  if (value == null) return undefined;
  const parsed = nonNegativeInteger(value, context);
  if (parsed === 0) throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
  return parsed;
}

function optionalVersion(value: unknown): string | number | undefined {
  if (value == null) return undefined;
  if (typeof value === "number" || typeof value === "bigint") {
    return nonNegativeInteger(value, "version");
  }
  return requiredText(value, "version");
}

function backoffKind(value: unknown, context: string): FlowPolicyBackoffKind {
  const kind = requiredText(value, context).toLowerCase();
  if (kind === "none" || kind === "fixed" || kind === "linear" || kind === "exponential") {
    return kind;
  }
  throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
}

function requiredMode(value: unknown, context: string): FlowStateMode {
  const mode = requiredText(value, context).toLowerCase();
  if (mode === "fifo" || mode === "parallel") return mode;
  throw new TypeError(`FLOW policy ${context} returned an unexpected response`);
}

function optionalMode(value: unknown, context: string): FlowStateMode | undefined {
  return value == null ? undefined : requiredMode(value, context);
}
