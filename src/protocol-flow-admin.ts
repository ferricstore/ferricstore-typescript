import type { CommandArgument } from "./internal.js";
import { hasOwnCommandArgument } from "./protocol-array-validation.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";
import { ensureArrayField, ensureObjectField } from "./protocol-flow-options.js";

export interface FlowAdminCommandShape {
  readonly opcode: number;
  readonly leadingFields: readonly string[];
}

export const FLOW_ADMIN_COMMANDS: Readonly<Record<string, FlowAdminCommandShape>> = {
  "FLOW.STATS": { opcode: wire.COMMAND_OPCODES["FLOW.STATS"], leadingFields: ["type"] },
  "FLOW.ATTRIBUTES": { opcode: wire.COMMAND_OPCODES["FLOW.ATTRIBUTES"], leadingFields: ["type"] },
  "FLOW.ATTRIBUTE_VALUES": {
    opcode: wire.COMMAND_OPCODES["FLOW.ATTRIBUTE_VALUES"],
    leadingFields: ["type", "attribute"]
  },
  "FLOW.SCHEDULE.CREATE": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.CREATE"], leadingFields: ["id"] },
  "FLOW.SCHEDULE.GET": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.GET"], leadingFields: ["id"] },
  "FLOW.SCHEDULE.DELETE": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.DELETE"], leadingFields: ["id"] },
  "FLOW.SCHEDULE.FIRE_DUE": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.FIRE_DUE"], leadingFields: [] },
  "FLOW.SCHEDULE.LIST": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.LIST"], leadingFields: [] },
  "FLOW.SCHEDULE.FIRE": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.FIRE"], leadingFields: ["id"] },
  "FLOW.SCHEDULE.PAUSE": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.PAUSE"], leadingFields: ["id"] },
  "FLOW.SCHEDULE.RESUME": { opcode: wire.COMMAND_OPCODES["FLOW.SCHEDULE.RESUME"], leadingFields: ["id"] },
  "FLOW.EFFECT.RESERVE": { opcode: wire.COMMAND_OPCODES["FLOW.EFFECT.RESERVE"], leadingFields: ["id"] },
  "FLOW.EFFECT.CONFIRM": { opcode: wire.COMMAND_OPCODES["FLOW.EFFECT.CONFIRM"], leadingFields: ["id"] },
  "FLOW.EFFECT.FAIL": { opcode: wire.COMMAND_OPCODES["FLOW.EFFECT.FAIL"], leadingFields: ["id"] },
  "FLOW.EFFECT.COMPENSATE": { opcode: wire.COMMAND_OPCODES["FLOW.EFFECT.COMPENSATE"], leadingFields: ["id"] },
  "FLOW.EFFECT.GET": { opcode: wire.COMMAND_OPCODES["FLOW.EFFECT.GET"], leadingFields: ["id"] },
  "FLOW.GOVERNANCE.LEDGER": { opcode: wire.COMMAND_OPCODES["FLOW.GOVERNANCE.LEDGER"], leadingFields: ["id"] },
  "FLOW.APPROVAL.REQUEST": { opcode: wire.COMMAND_OPCODES["FLOW.APPROVAL.REQUEST"], leadingFields: ["id"] },
  "FLOW.APPROVAL.APPROVE": { opcode: wire.COMMAND_OPCODES["FLOW.APPROVAL.APPROVE"], leadingFields: ["id"] },
  "FLOW.APPROVAL.REJECT": { opcode: wire.COMMAND_OPCODES["FLOW.APPROVAL.REJECT"], leadingFields: ["id"] },
  "FLOW.APPROVAL.GET": { opcode: wire.COMMAND_OPCODES["FLOW.APPROVAL.GET"], leadingFields: ["id"] },
  "FLOW.APPROVAL.LIST": { opcode: wire.COMMAND_OPCODES["FLOW.APPROVAL.LIST"], leadingFields: [] },
  "FLOW.GOVERNANCE.OVERVIEW": { opcode: wire.COMMAND_OPCODES["FLOW.GOVERNANCE.OVERVIEW"], leadingFields: [] },
  "FLOW.CIRCUIT.OPEN": { opcode: wire.COMMAND_OPCODES["FLOW.CIRCUIT.OPEN"], leadingFields: ["scope"] },
  "FLOW.CIRCUIT.CLOSE": { opcode: wire.COMMAND_OPCODES["FLOW.CIRCUIT.CLOSE"], leadingFields: ["scope"] },
  "FLOW.CIRCUIT.GET": { opcode: wire.COMMAND_OPCODES["FLOW.CIRCUIT.GET"], leadingFields: ["scope"] },
  "FLOW.BUDGET.RESERVE": { opcode: wire.COMMAND_OPCODES["FLOW.BUDGET.RESERVE"], leadingFields: ["scope"] },
  "FLOW.BUDGET.GET": { opcode: wire.COMMAND_OPCODES["FLOW.BUDGET.GET"], leadingFields: ["scope"] },
  "FLOW.BUDGET.LIST": { opcode: wire.COMMAND_OPCODES["FLOW.BUDGET.LIST"], leadingFields: [] },
  "FLOW.BUDGET.COMMIT": { opcode: wire.COMMAND_OPCODES["FLOW.BUDGET.COMMIT"], leadingFields: ["scope"] },
  "FLOW.BUDGET.RELEASE": { opcode: wire.COMMAND_OPCODES["FLOW.BUDGET.RELEASE"], leadingFields: ["scope"] },
  "FLOW.LIMIT.LEASE": { opcode: wire.COMMAND_OPCODES["FLOW.LIMIT.LEASE"], leadingFields: ["scope"] },
  "FLOW.LIMIT.SPEND": { opcode: wire.COMMAND_OPCODES["FLOW.LIMIT.SPEND"], leadingFields: ["scope"] },
  "FLOW.LIMIT.RELEASE": { opcode: wire.COMMAND_OPCODES["FLOW.LIMIT.RELEASE"], leadingFields: ["scope"] },
  "FLOW.LIMIT.GET": { opcode: wire.COMMAND_OPCODES["FLOW.LIMIT.GET"], leadingFields: ["scope"] },
  "FLOW.LIMIT.LIST": { opcode: wire.COMMAND_OPCODES["FLOW.LIMIT.LIST"], leadingFields: [] }
};

const FLOW_ADMIN_BOOLEAN_FIELDS = new Set([
  "consistent_projection",
  "include_cold",
  "overwrite",
  "rev",
  "terminal_only",
  "values"
]);

const FLOW_ADMIN_FIELDS: Readonly<Record<string, string>> = {
  ACTUAL_AMOUNT: "actual_amount", AMOUNT: "amount", APPROVER: "approver", ASSIGNEES: "assignees",
  AT_MS: "at_ms", BLOCK: "block_ms", BLOCK_MS: "block_ms", CONSISTENT_PROJECTION: "consistent_projection",
  CORRELATION_ID: "correlation_id", COUNT: "count", CRON: "cron", DEADLINE_MS: "deadline_ms",
  DELAY_MS: "delay_ms", EFFECT_KEY: "effect_key", EFFECT_TYPE: "effect_type", END_AT_MS: "end_at_ms",
  ERROR: "error", EVENT: "event", EVERY_MS: "every_ms", EXPIRES_AT_MS: "expires_at_ms",
  EXTERNAL_ID: "external_id", FAILURE_THRESHOLD: "failure_threshold", FENCING: "fencing_token",
  FENCING_TOKEN: "fencing_token", FLOW_ID: "flow_id", FROM_EVENT: "from_event", FROM_MS: "from_ms",
  FROM_VERSION: "from_version", GOVERNANCE_SCOPE: "governance_scope", IDEMPOTENCY_KEY: "idempotency_key",
  INCLUDE_COLD: "include_cold", INITIAL_STATE: "initial_state", ITEMS: "items", KIND: "kind",
  LATENCY_MS: "latency_ms", LEASE_MS: "lease_ms", LEASE_TOKEN: "lease_token", LIMIT: "limit",
  MAX_FIRES: "max_fires", MAXBYTES: "payload_max_bytes", NOW: "now_ms", OPEN_MS: "open_ms",
  OPERATION_DIGEST: "operation_digest", OVERLAP_POLICY: "overlap_policy", OVERLAP_RETRY_MS: "overlap_retry_ms",
  OVERWRITE: "overwrite", PARENT_FLOW_ID: "parent_flow_id", PARENT_ID: "parent_id", PARTITION: "partition_key",
  PAYLOAD: "payload", PAYLOAD_MAX_BYTES: "payload_max_bytes", POLICY_HASH: "policy_hash",
  POLICY_VERSION: "policy_version", PRIORITY: "priority", REASON: "reason", REQUESTED_BY: "requested_by",
  RESERVATION_ID: "reservation_id", RESULT: "result", RETENTION_TTL_MS: "retention_ttl_ms", REV: "rev",
  RETURN: "return", ROOT_FLOW_ID: "root_flow_id", ROOT_ID: "root_id", SCOPE: "scope", SHARD_ID: "shard_id",
  START_AT_MS: "start_at_ms", STATE: "state", STATUS: "status", STATES: "states", STEPS: "steps",
  TARGET: "target", TARGET_TYPE: "target_type", TERMINAL_ONLY: "terminal_only", TIMEOUT_MS: "timeout_ms",
  TIMEZONE: "timezone", TO_EVENT: "to_event", TO_MS: "to_ms", TO_VERSION: "to_version", TTL_MS: "ttl_ms",
  TYPE: "type", USAGE: "usage", VALUES: "values", WINDOW_MS: "window_ms", WORKER: "worker"
};

export function flowHistoryPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length === 0 || args.length % 2 === 0) return undefined;
  hasOwnCommandArgument(args, 0, args.length, "Flow history arguments");
  const payload: Record<string, unknown> = { id: args[0] };
  for (let index = 1; index < args.length; index += 2) {
    hasOwnCommandArgument(args, index, args.length, "Flow history arguments");
    hasOwnCommandArgument(args, index + 1, args.length, "Flow history arguments");
    const value = args[index + 1];
    const token = core.optionalText(args[index])?.toUpperCase();
    if (token == null || value == null) return undefined;
    switch (token) {
      case "COUNT": payload.count = value; break;
      case "PARTITION": payload.partition_key = value; break;
      case "FROM_EVENT": payload.from_event = value; break;
      case "TO_EVENT": payload.to_event = value; break;
      case "FROM_MS": payload.from_ms = value; break;
      case "TO_MS": payload.to_ms = value; break;
      case "FROM_VERSION": payload.from_version = value; break;
      case "TO_VERSION": payload.to_version = value; break;
      case "EVENT": payload.event = value; break;
      case "WORKER": payload.worker = value; break;
      case "PAYLOAD_MAX_BYTES":
      case "MAXBYTES": payload.payload_max_bytes = value; break;
      case "REV":
      case "INCLUDE_COLD":
      case "CONSISTENT_PROJECTION":
      case "VALUES": {
        if (!core.isBoolToken(value)) return undefined;
        const field = token === "REV" ? "rev" : token === "INCLUDE_COLD" ? "include_cold"
          : token === "CONSISTENT_PROJECTION" ? "consistent_projection" : "values";
        payload[field] = core.boolArg(value);
        break;
      }
      default: return undefined;
    }
  }
  return { opcode: wire.COMMAND_OPCODES["FLOW.HISTORY"], payload };
}

export function flowAdminPayload(
  opcode: number,
  args: readonly CommandArgument[],
  leadingFields: readonly string[] = []
): wire.ProtocolCommand | undefined {
  if (args.length < leadingFields.length) return undefined;
  const payload: Record<string, unknown> = {};
  for (const [index, field] of leadingFields.entries()) {
    hasOwnCommandArgument(args, index, args.length, "Flow admin arguments");
    core.setOwnValue(payload, field, args[index]);
  }

  for (let index = leadingFields.length; index < args.length; ) {
    hasOwnCommandArgument(args, index, args.length, "Flow admin arguments");
    const token = core.optionalText(args[index])?.toUpperCase();
    if (token == null) return undefined;
    if (["ATTRIBUTE", "ATTRIBUTE_MERGE", "STATE_META", "VALUE", "VALUE_REF"].includes(token)) {
      if (
        !hasOwnCommandArgument(args, index + 1, args.length, "Flow admin arguments") ||
        !hasOwnCommandArgument(args, index + 2, args.length, "Flow admin arguments")
      ) return undefined;
      const field = token === "ATTRIBUTE" ? "attributes" : token === "ATTRIBUTE_MERGE" ? "attributes_merge"
        : token === "STATE_META" ? "state_meta" : token === "VALUE" ? "values" : "value_refs";
      core.setOwnValue(ensureObjectField(payload, field), core.asText(args[index + 1]), args[index + 2]);
      index += 3;
      continue;
    }
    if (token === "ATTRIBUTE_DELETE" || token === "DROP_VALUE" || token === "OVERRIDE_VALUE") {
      if (!hasOwnCommandArgument(args, index + 1, args.length, "Flow admin arguments")) return undefined;
      const field = token === "ATTRIBUTE_DELETE" ? "attributes_delete"
        : token === "DROP_VALUE" ? "drop_values" : "override_values";
      ensureArrayField(payload, field).push(args[index + 1]);
      index += 2;
      continue;
    }
    if (token === "RESERVATION_IDS") {
      if (!hasOwnCommandArgument(args, index + 1, args.length, "Flow admin arguments")) return undefined;
      const count = core.safeIntegerNumberArg(args[index + 1]);
      if (count <= 0 || index + 2 + count > args.length) return undefined;
      const reservationIds = new Array<CommandArgument>(count);
      for (let itemIndex = 0; itemIndex < count; itemIndex += 1) {
        hasOwnCommandArgument(args, index + 2 + itemIndex, args.length, "Flow admin arguments");
        const reservationId = args[index + 2 + itemIndex];
        if (reservationId == null) return undefined;
        reservationIds[itemIndex] = reservationId;
      }
      core.setOwnValue(payload, "reservation_ids", reservationIds);
      index += 2 + count;
      continue;
    }
    const field = FLOW_ADMIN_FIELDS[token];
    if (field == null || !hasOwnCommandArgument(args, index + 1, args.length, "Flow admin arguments")) {
      return undefined;
    }
    const rawValue = args[index + 1];
    if (FLOW_ADMIN_BOOLEAN_FIELDS.has(field) && !core.isBoolToken(rawValue)) return undefined;
    core.setOwnValue(payload, field, FLOW_ADMIN_BOOLEAN_FIELDS.has(field) ? core.boolArg(rawValue) : rawValue);
    index += 2;
  }
  const serverBlockMs = opcode === wire.COMMAND_OPCODES["FLOW.SCHEDULE.FIRE_DUE"] && payload.block_ms != null
    ? core.blockDurationMs(payload.block_ms as CommandArgument, 1)
    : undefined;
  return serverBlockMs == null ? { opcode, payload } : { opcode, payload, serverBlockMs };
}
