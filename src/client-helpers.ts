import { isDeepStrictEqual } from "node:util";
import type { Codec } from "./codecs.js";
import { encodeFlowValue } from "./flow-value-snapshot.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";
import type {
  AdminListOptions,
  ApprovalListOptions,
  AttributeQueryOptions,
  ClaimDueOptions,
  FlowAdminRecord,
  FlowStateMode,
  FlowStatePolicy,
  FlowStatePolicyLike,
  ManagementPairs,
  RequestContext
} from "./client-options.js";
import { append, appendBool, arrayResponse, normalizeRefMeta, text, type CommandArgument } from "./internal.js";
import { normalizeRequestContext } from "./request-context.js";
export { appendAttributeMutations } from "./flow-argument-helpers.js";
import {
  CLAIMED_ITEM_WIRE,
  type ClaimedItem,
  type CreateItem,
  type FencedItem,
  type RetryPolicy,
  type StateMeta
} from "./types.js";
export * from "./client-response-helpers.js";

export function finiteNonNegativeInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value)
    ? fallback
    : Math.max(0, Math.trunc(value));
}

export function finiteNonNegativeNumber(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(0, value);
}

export function appendAttributeQueryOptions(args: CommandArgument[], options: AttributeQueryOptions): void {
  append(args, "STATE", options.state);
  append(args, "PARTITION", options.partitionKey);
  append(args, "COUNT", options.count);
  appendBool(args, "CONSISTENT_PROJECTION", options.consistentProjection);
}

export function approvalListArgs(command: string, options: ApprovalListOptions): CommandArgument[] {
  const args: CommandArgument[] = [command];
  append(args, "STATUS", options.status);
  append(args, "SCOPE", options.scope);
  append(args, "PARTITION", options.partitionKey);
  append(args, "FLOW_ID", options.flowId);
  append(args, "LIMIT", options.limit);
  return args;
}

export function adminListArgs(
  command: string,
  options: AdminListOptions & { nowMs?: number }
): CommandArgument[] {
  const args: CommandArgument[] = [command];
  append(args, "SCOPE", options.scope);
  append(args, "PARTITION", options.partitionKey);
  append(args, "LIMIT", options.limit);
  append(args, "NOW", options.nowMs);
  return args;
}

export function adminRecordResponse(value: unknown, context: string): FlowAdminRecord {
  const normalized = normalizeRefMeta(value);
  if (!isPlainObject(normalized)) {
    throw new TypeError(`${context} returned an invalid record response`);
  }
  return normalized;
}

export function optionalAdminRecord(value: unknown, context: string): FlowAdminRecord | null {
  return value == null ? null : adminRecordResponse(value, context);
}

export function adminRecordList(value: unknown, context: string): FlowAdminRecord[] {
  return arrayResponse(value).map((item) => adminRecordResponse(item, context));
}

export function okLike(value: unknown): boolean {
  if (typeof value === "string") return value.toUpperCase() === "OK";
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase() === "OK";
  }
  return false;
}

export function appendStateMeta(args: CommandArgument[], stateMeta: StateMeta | undefined): void {
  for (const [name, value] of Object.entries(stateMeta ?? {})) {
    args.push("STATE_META", name, value);
  }
}

export function appendAttributes(args: CommandArgument[], attributes: Record<string, CommandArgument> | undefined): void {
  for (const [name, value] of Object.entries(attributes ?? {})) {
    args.push("ATTRIBUTE", name, value);
  }
}

export function normalizeAdminResponse(value: unknown): unknown {
  return normalizeRefMeta(value);
}

export function jsonArg(value: Record<string, unknown> | string): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}

export function commandWithRequestContext(
  command: string,
  args: readonly CommandArgument[],
  requestContext: RequestContext | undefined
): CommandArgument[] {
  const commandArgs: CommandArgument[] = [command, ...args];
  if (requestContext != null) {
    commandArgs.push("REQUEST_CONTEXT", normalizeRequestContext(requestContext));
  }
  return commandArgs;
}

export function infoText(value: unknown): string {
  if (typeof value === "string" || Buffer.isBuffer(value)) {
    return text(value);
  }
  const normalized = normalizeRefMeta(value);
  if (isPlainObject(normalized)) {
    return Object.entries(normalized)
      .map(([key, item]) => `${key}=${diagnosticValueText(item)}`)
      .join(" ");
  }
  return diagnosticValueText(normalized);
}

export function diagnosticValueText(value: unknown): string {
  if (value == null) {
    return "";
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
}

export function managementPairArgs(pairs: ManagementPairs): CommandArgument[] {
  const args: CommandArgument[] = [];
  for (const [key, value] of Object.entries(pairs)) {
    if (value != null) {
      args.push(key.toUpperCase(), value);
    }
  }
  return args;
}

export function appendFlowStatePolicy(args: CommandArgument[], policy: FlowStatePolicyLike): void {
  if (isFlowStatePolicy(policy)) {
    appendPolicyMode(args, "state", policy.mode);
    if (policy.retry != null) {
      appendRetryPolicy(args, policy.retry);
    }
    return;
  }
  appendRetryPolicy(args, policy);
}

export function isFlowStatePolicy(policy: FlowStatePolicyLike): policy is FlowStatePolicy {
  return typeof policy === "object" && policy != null && (
    Object.hasOwn(policy, "mode") || Object.hasOwn(policy, "retry")
  );
}

export function ruleArgs(rules: string | readonly string[]): readonly string[] {
  return typeof rules === "string" ? [rules] : snapshotOwnStringArray(rules, "ACL rules");
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value);
}

export function sharedCreateManyStateMeta(items: readonly CreateItem[], stateMeta: StateMeta | undefined): StateMeta | undefined {
  if (stateMeta != null) {
    for (const item of items) {
      if (item.stateMeta != null && !stateMetaEquals(stateMeta, item.stateMeta)) {
        throw new Error("createMany item stateMeta must match shared stateMeta when both are provided");
      }
    }
    return stateMeta;
  }

  let shared: StateMeta | undefined;
  let sawMissing = false;
  for (const item of items) {
    const itemMeta = item.stateMeta;
    if (itemMeta == null) {
      sawMissing = true;
      continue;
    }
    if (sawMissing || (shared != null && !stateMetaEquals(itemMeta, shared))) {
      throw new Error("createMany supports shared stateMeta only; use stateMeta or separate create calls for per-item stateMeta");
    }
    shared = itemMeta;
  }
  if (shared != null && sawMissing) {
    throw new Error("createMany supports shared stateMeta only; use stateMeta or separate create calls for per-item stateMeta");
  }
  return shared;
}

export function sharedCreateManyAttributes(
  items: readonly CreateItem[],
  attributes: Record<string, CommandArgument> | undefined
): Record<string, CommandArgument> | undefined {
  if (attributes != null) {
    for (const item of items) {
      if (hasOwnEnumerableProperty(item.attributes) && !isDeepStrictEqual(attributes, item.attributes)) {
        throw new Error("createMany item attributes must match shared attributes when both are provided");
      }
    }
    return attributes;
  }

  let shared: Record<string, CommandArgument> | undefined;
  let sawMissing = false;
  for (const item of items) {
    const itemAttributes = item.attributes;
    if (!hasOwnEnumerableProperty(itemAttributes)) {
      sawMissing = true;
      continue;
    }
    if (sawMissing || (shared != null && !isDeepStrictEqual(itemAttributes, shared))) {
      throw new Error(
        "createMany supports shared attributes only; use attributes or separate create calls for per-item attributes"
      );
    }
    shared = itemAttributes;
  }
  if (shared != null && sawMissing) {
    throw new Error(
      "createMany supports shared attributes only; use attributes or separate create calls for per-item attributes"
    );
  }
  return shared;
}

export function hasOwnEnumerableProperty(value: Record<string, CommandArgument> | undefined): value is Record<string, CommandArgument> {
  if (value == null) return false;
  for (const key in value) {
    if (Object.prototype.hasOwnProperty.call(value, key)) return true;
  }
  return false;
}

export function stateMetaEquals(left: StateMeta, right: StateMeta): boolean {
  const leftEntries = Object.entries(left);
  if (leftEntries.length !== Object.keys(right).length) {
    return false;
  }
  return leftEntries.every(([key, value]) => stateMetaValueEquals(value, right[key]));
}

export function stateMetaValueEquals(left: StateMeta[keyof StateMeta], right: StateMeta[keyof StateMeta] | undefined): boolean {
  if (Buffer.isBuffer(left) || Buffer.isBuffer(right)) {
    return Buffer.isBuffer(left) && Buffer.isBuffer(right) && left.equals(right);
  }
  return Object.is(left, right);
}

export function appendNamedCounts(
  args: CommandArgument[],
  codec: Codec,
  values: Record<string, unknown>,
  valueRefs: Record<string, string>
): void {
  args.push(Object.keys(values).length);
  for (const [name, value] of Object.entries(values)) {
    args.push(name, encodeFlowValue(codec, value));
  }
  args.push(Object.keys(valueRefs).length);
  for (const [name, ref] of Object.entries(valueRefs)) {
    args.push(name, ref);
  }
}

export function appendClaimedItems(
  args: CommandArgument[],
  partitionKey: string | undefined,
  items: ClaimedItem[],
  command: string
): void {
  assertManyPartitionMatches(partitionKey, items, command);
  args.push("ITEMS");
  for (const item of items) {
    const wire = item[CLAIMED_ITEM_WIRE];
    if (partitionKey == null) {
      args.push(wire?.id ?? item.id, wire?.partitionKey ?? item.partitionKey ?? "-", Buffer.from(wire?.leaseToken ?? item.leaseToken), wire?.fencingToken ?? item.fencingToken);
    } else {
      args.push(wire?.id ?? item.id, Buffer.from(wire?.leaseToken ?? item.leaseToken), wire?.fencingToken ?? item.fencingToken);
    }
  }
}

export function appendFencedItems(
  args: CommandArgument[],
  partitionKey: string | undefined,
  items: FencedItem[],
  command: string,
  includeLease: boolean
): void {
  assertManyPartitionMatches(partitionKey, items, command);
  args.push("ITEMS");
  for (const item of items) {
    if (partitionKey == null) {
      args.push(item.id, item.partitionKey ?? "-", item.fencingToken);
    } else {
      args.push(item.id, item.fencingToken);
    }
    if (includeLease) {
      args.push(item.leaseToken == null ? Buffer.alloc(0) : Buffer.from(item.leaseToken));
    }
  }
}

export function assertManyPartitionMatches(
  partitionKey: string | undefined,
  items: readonly { readonly partitionKey?: string }[],
  command: string
): void {
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!Object.hasOwn(items, index) || item == null) {
      throw new TypeError(`${command} items must be dense`);
    }
    if (partitionKey != null && item.partitionKey != null && item.partitionKey !== partitionKey) {
      throw new Error(`${command} item partitionKey does not match batch partitionKey`);
    }
  }
}

export function appendRetryPolicy(args: CommandArgument[], policy: RetryPolicy): void {
  append(args, "MAX_RETRIES", policy.maxRetries);
  append(args, "BACKOFF", policy.backoff);
  append(args, "BASE_MS", policy.baseMs);
  append(args, "MAX_MS", policy.maxMs);
  append(args, "JITTER_PCT", policy.jitterPct);
  append(args, "EXHAUSTED_TO", policy.exhaustedTo);
}

export function appendPolicyMode(args: CommandArgument[], state: string | undefined, mode: FlowStateMode | undefined): void {
  if (mode == null) {
    return;
  }
  if (state == null) {
    throw new Error("policy mode requires state");
  }
  switch (mode) {
    case "fifo":
      args.push("MODE", "FIFO");
      return;
    case "parallel":
      args.push("MODE", "PARALLEL");
      return;
    default:
      throw new Error("policy mode must be 'fifo' or 'parallel'");
  }
}

export function appendPayloadRead(args: CommandArgument[], payload: boolean | undefined, maxBytes: number | undefined): void {
  if (payload === false) {
    args.push("NOPAYLOAD");
    return;
  }
  if (payload === true || maxBytes != null) {
    args.push("PAYLOAD");
  }
  if (maxBytes != null) {
    args.push("MAXBYTES", maxBytes);
  }
}

export function claimDataRequested(options: Pick<ClaimDueOptions, "payload" | "payloadMaxBytes" | "values">): boolean {
  return options.payload === true || options.payloadMaxBytes != null || (options.values?.length ?? 0) > 0;
}

export function compactClaimReturnMode(includeState: boolean, includeAttributes: boolean): string {
  if (includeState) {
    return includeAttributes ? "JOBS_COMPACT_STATE_ATTRS" : "JOBS_COMPACT_STATE";
  }
  return includeAttributes ? "JOBS_COMPACT_ATTRS" : "JOBS_COMPACT";
}
