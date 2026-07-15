import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { CommandArgument } from "./internal.js";
import { denseCommandArgumentSlice, hasOwnCommandArgument } from "./protocol-array-validation.js";
import * as core from "./protocol-core.js";

export const OPTION_FIELDS: Record<string, string> = {
  CONSISTENT_PROJECTION: "consistent_projection",
  COUNT: "count",
  FROM_MS: "from_ms",
  INCLUDE_COLD: "include_cold",
  PARTITION: "partition_key",
  REV: "rev",
  STATE: "state",
  TERMINAL_ONLY: "terminal_only",
  TO_MS: "to_ms"
};

export const BOOL_OPTION_FIELDS = new Set(["consistent_projection", "include_cold", "rev", "terminal_only"]);

export function optionMap(args: readonly CommandArgument[]): Record<string, unknown> | undefined {
  const payload: Record<string, unknown> = {};
  for (let index = 0; index < args.length; ) {
    hasOwnCommandArgument(args, index, args.length, "Flow options");
    const token = core.asText(args[index]).toUpperCase();
    if (token === "ATTRIBUTE") {
      if (
        !hasOwnCommandArgument(args, index + 1, args.length, "Flow options") ||
        !hasOwnCommandArgument(args, index + 2, args.length, "Flow options")
      ) {
        throw new FerricStoreError("ATTRIBUTE requires name and value");
      }
      const name = core.asText(args[index + 1]);
      const attributes = ensureObjectField(payload, "attributes");
      core.setOwnValue(attributes, name, args[index + 2]);
      index += 3;
      continue;
    }
    if (token === "STATE_META") {
      if (
        !hasOwnCommandArgument(args, index + 1, args.length, "Flow options") ||
        !hasOwnCommandArgument(args, index + 2, args.length, "Flow options")
      ) {
        throw new FerricStoreError("STATE_META requires name and value");
      }
      const name = core.asText(args[index + 1]);
      const stateMeta = ensureObjectField(payload, "state_meta");
      core.setOwnValue(stateMeta, name, args[index + 2]);
      index += 3;
      continue;
    }

    const field = OPTION_FIELDS[token] ?? token.toLowerCase();
    if (!hasOwnCommandArgument(args, index + 1, args.length, "Flow options")) {
      throw new FerricStoreError(`${token} requires a value`);
    }
    const value = args[index + 1];
    if (BOOL_OPTION_FIELDS.has(field) && !core.isBoolToken(value)) {
      return undefined;
    }
    core.setOwnValue(payload, field, BOOL_OPTION_FIELDS.has(field) ? core.boolArg(value) : value);
    index += 2;
  }
  return payload;
}

export function ensureObjectField(payload: Record<string, unknown>, field: string): Record<string, unknown> {
  const value = payload[field];
  if (isPlainObject(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  core.setOwnValue(payload, field, next);
  return next;
}

export function normalizeFlowSearchStateMeta(payload: Record<string, unknown>): void {
  const stateMeta = payload.state_meta;
  if (!isPlainObject(stateMeta) || Object.keys(stateMeta).length === 0) {
    return;
  }
  if (Object.values(stateMeta).every(isPlainObject)) {
    return;
  }

  const state = payload.state;
  if (state == null) {
    throw new FerricStoreError("FLOW.SEARCH STATE_META filters require STATE or nested state metadata");
  }
  payload.state_meta = { [core.asText(state)]: stateMeta };
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value) && !(value instanceof Uint8Array);
}

export function parseFlowOptions(
  args: readonly CommandArgument[],
  start: number,
  end: number,
  config: {
    readonly allowed: ReadonlySet<string>;
    readonly payloadValue?: boolean;
    readonly readValues?: boolean;
    readonly required?: ReadonlySet<string>;
  }
): Record<string, unknown> | undefined {
  const payload: Record<string, unknown> = {};
  const seen = new Set<string>();
  for (let index = start; index < end; ) {
    hasOwnCommandArgument(args, index, end, "Flow options");
    const token = core.asText(args[index]).toUpperCase();
    if (!config.allowed.has(token)) return undefined;
    seen.add(token);
    switch (token) {
      case "TYPE":
        if (!putScalar(payload, "type", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "STATE":
        if (!hasOwnCommandArgument(args, index + 1, end, "Flow options")) return undefined;
        if (Array.isArray(payload.states)) {
          const states = payload.states as unknown[];
          core.setOwnValue(payload, "states", [...states, args[index + 1]]);
        } else if (Object.hasOwn(payload, "state")) {
          core.setOwnValue(payload, "states", [payload.state, args[index + 1]]);
          delete payload.state;
        } else {
          core.setOwnValue(payload, "state", args[index + 1]);
        }
        index += 2;
        break;
      case "STATES": {
        if (!hasOwnCommandArgument(args, index + 1, end, "Flow options")) return undefined;
        const count = core.safeIntegerNumberArg(args[index + 1]);
        if (
          !Number.isInteger(count) ||
          count < 0 ||
          index + 2 + count > end
        ) return undefined;
        const states = denseCommandArgumentSlice(args, index + 2, count, "STATES");
        if (Array.isArray(payload.states)) {
          const existingStates = payload.states as unknown[];
          core.setOwnValue(payload, "states", [...existingStates, ...states]);
        } else if (Object.hasOwn(payload, "state")) {
          core.setOwnValue(payload, "states", [payload.state, ...states]);
          delete payload.state;
        } else {
          core.setOwnValue(payload, "states", states);
        }
        index += 2 + count;
        break;
      }
      case "PARTITION":
        if (!putScalar(payload, "partition_key", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "PARTITIONS": {
        if (!hasOwnCommandArgument(args, index + 1, end, "Flow options")) return undefined;
        const count = core.safeIntegerNumberArg(args[index + 1]);
        if (!Number.isInteger(count) || count < 0 || index + 2 + count > end) return undefined;
        payload.partition_keys = denseCommandArgumentSlice(args, index + 2, count, "PARTITIONS");
        index += 2 + count;
        break;
      }
      case "PARENT_FLOW_ID":
        if (!putScalar(payload, "parent_flow_id", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "ROOT_FLOW_ID":
        if (!putScalar(payload, "root_flow_id", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "CORRELATION_ID":
        if (!putScalar(payload, "correlation_id", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "OWNER_FLOW_ID":
        if (!putScalar(payload, "owner_flow_id", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "NAME":
        if (!putScalar(payload, "name", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "GROUP":
        if (!putScalar(payload, "group_id", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "WAIT":
      case "WAIT_STATE":
      case "SUCCESS":
      case "FAILURE":
      case "FROM_STATE":
      case "ON_CHILD_FAILED":
      case "ON_PARENT_CLOSED":
      case "SIGNAL":
      case "IDEMPOTENCY":
      case "IF_STATE":
      case "TRANSITION_TO":
      case "TO_EVENT":
      case "EXPECT_STATE": {
        const field = token.toLowerCase();
        if (!putScalar(payload, field, args, index + 1, end)) return undefined;
        index += 2;
        break;
      }
      case "LEASE_TOKEN":
      case "RESULT":
      case "ERROR":
      case "REASON": {
        const field = token === "LEASE_TOKEN"
          ? "lease_token"
          : token.toLowerCase();
        if (!putScalar(payload, field, args, index + 1, end)) return undefined;
        index += 2;
        break;
      }
      case "VALUE": {
        if (config.readValues === true) {
          if (!hasOwnCommandArgument(args, index + 1, end, "Flow options")) return undefined;
          ensureArrayField(payload, "values").push(args[index + 1]);
          index += 2;
          break;
        }
        if (
          !hasOwnCommandArgument(args, index + 1, end, "Flow options") ||
          !hasOwnCommandArgument(args, index + 2, end, "Flow options")
        ) return undefined;
        const values = ensureObjectField(payload, "values");
        core.setOwnValue(values, core.asText(args[index + 1]), args[index + 2]);
        index += 3;
        break;
      }
      case "ATTRIBUTE":
      case "ATTRIBUTE_MERGE":
      case "VALUE_REF":
      case "STATE_META": {
        if (
          !hasOwnCommandArgument(args, index + 1, end, "Flow options") ||
          !hasOwnCommandArgument(args, index + 2, end, "Flow options")
        ) return undefined;
        const field = token === "ATTRIBUTE"
          ? "attributes"
          : token === "ATTRIBUTE_MERGE"
            ? "attributes_merge"
            : token === "VALUE_REF"
              ? "value_refs"
              : "state_meta";
        const values = ensureObjectField(payload, field);
        core.setOwnValue(values, core.asText(args[index + 1]), args[index + 2]);
        index += 3;
        break;
      }
      case "DROP_VALUE":
      case "OVERRIDE_VALUE":
      case "ATTRIBUTE_DELETE": {
        if (!hasOwnCommandArgument(args, index + 1, end, "Flow options")) return undefined;
        const field = token === "DROP_VALUE"
          ? "drop_values"
          : token === "OVERRIDE_VALUE"
            ? "override_values"
            : "attributes_delete";
        const values = ensureArrayField(payload, field);
        values.push(args[index + 1]);
        index += 2;
        break;
      }
      case "WORKER":
        if (!putScalar(payload, "worker", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "LEASE_MS":
        if (!putNumber(payload, "lease_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "LIMIT":
        if (!putNumber(payload, "limit", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "PRIORITY":
        if (!putNumber(payload, "priority", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "NOW":
        if (!putNumber(payload, "now_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "RUN_AT":
        if (!putNumber(payload, "run_at_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "RETENTION_TTL_MS":
        if (!putNumber(payload, "retention_ttl_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "MAX_ACTIVE_MS":
        if (!putPositiveOrInfinity(payload, "max_active_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "TTL":
        if (!putNumber(payload, "ttl_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "FENCING":
        if (!putInteger(payload, "fencing_token", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "BLOCK":
      case "BLOCK_MS":
        if (!putNumber(payload, "block_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "RECLAIM_RATIO":
        if (!putNumber(payload, "reclaim_ratio", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "MAXBYTES":
        if (!putNumber(payload, "payload_max_bytes", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "VALUE_MAX_BYTES":
        if (!putNumber(payload, "value_max_bytes", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "RETURN":
        if (!putScalar(payload, "return", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "IDEMPOTENT":
        index = putOptionalBool(payload, "idempotent", args, index, end);
        if (index < 0) return undefined;
        break;
      case "INDEPENDENT":
        index = putOptionalBool(payload, "independent", args, index, end);
        if (index < 0) return undefined;
        break;
      case "RECLAIM_EXPIRED":
        index = putOptionalBool(payload, "reclaim_expired", args, index, end);
        if (index < 0) return undefined;
        break;
      case "INCLUDE_STATE":
        index = putOptionalBool(payload, "include_state", args, index, end);
        if (index < 0) return undefined;
        break;
      case "FULL":
        index = putOptionalBool(payload, "full", args, index, end);
        if (index < 0) return undefined;
        break;
      case "OVERRIDE":
        index = putOptionalBool(payload, "override", args, index, end);
        if (index < 0) return undefined;
        break;
      case "PAYLOAD":
        if (config.payloadValue === true) {
          if (!putScalar(payload, "payload", args, index + 1, end)) return undefined;
          index += 2;
        } else {
          payload.payload = true;
          index += 1;
        }
        break;
      case "NOPAYLOAD":
        payload.payload = false;
        index += 1;
        break;
      default:
        return undefined;
    }
  }
  for (const required of config.required ?? []) {
    if (!seen.has(required)) return undefined;
  }
  return payload;
}

export function putScalar(payload: Record<string, unknown>, key: string, args: readonly CommandArgument[], index: number, end: number): boolean {
  if (!hasOwnCommandArgument(args, index, end, "Flow options")) return false;
  core.setOwnValue(payload, key, args[index]);
  return true;
}

export function ensureArrayField(payload: Record<string, unknown>, field: string): unknown[] {
  const value = payload[field];
  if (Array.isArray(value)) {
    return value;
  }
  const next: unknown[] = [];
  core.setOwnValue(payload, field, next);
  return next;
}

export function putNumber(payload: Record<string, unknown>, key: string, args: readonly CommandArgument[], index: number, end: number): boolean {
  if (!hasOwnCommandArgument(args, index, end, "Flow options")) return false;
  const value = core.integerArg(args[index]);
  if (typeof value === "bigint") {
    throw new FerricStoreError("numeric command argument exceeds the JavaScript safe range");
  }
  core.setOwnValue(payload, key, value);
  return true;
}

export function putInteger(
  payload: Record<string, unknown>,
  key: string,
  args: readonly CommandArgument[],
  index: number,
  end: number
): boolean {
  if (!hasOwnCommandArgument(args, index, end, "Flow options")) return false;
  core.setOwnValue(payload, key, core.integerArg(args[index]));
  return true;
}

export function putPositiveOrInfinity(
  payload: Record<string, unknown>,
  key: string,
  args: readonly CommandArgument[],
  index: number,
  end: number
): boolean {
  if (!hasOwnCommandArgument(args, index, end, "Flow options")) return false;
  if (core.asText(args[index]).toUpperCase() === "INFINITY") {
    core.setOwnValue(payload, key, "infinity");
    return true;
  }
  core.setOwnValue(payload, key, core.integerArg(args[index]));
  return true;
}

export function putOptionalBool(
  payload: Record<string, unknown>,
  key: string,
  args: readonly CommandArgument[],
  index: number,
  end: number
): number {
  if (index + 1 < end) hasOwnCommandArgument(args, index + 1, end, "Flow options");
  if (index + 1 < end && core.isBoolToken(args[index + 1])) {
    core.setOwnValue(payload, key, core.boolArg(args[index + 1]));
    return index + 2;
  }
  core.setOwnValue(payload, key, true);
  return index + 1;
}
