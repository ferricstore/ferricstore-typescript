import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import { hasOwnCommandArgument } from "./protocol-array-validation.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";

const BACKOFF_FIELDS: Readonly<Record<string, string>> = {
  BASE_MS: "base_ms",
  MAX_MS: "max_ms",
  JITTER_PCT: "jitter_pct"
};

const RETENTION_FIELDS: Readonly<Record<string, string>> = {
  RETENTION_TTL: "ttl_ms",
  RETENTION_TTL_MS: "ttl_ms",
  HISTORY_MAX_EVENTS: "history_max_events"
};

/** Build the structured v1 native Flow policy mutation body. */
export function flowPolicySetPayload(
  args: readonly CommandArgument[]
): wire.ProtocolCommand | undefined {
  if (!hasOwnCommandArgument(args, 0, args.length, "Flow policy arguments")) return undefined;
  if (!isBinaryArgument(args[0])) return undefined;
  const payload: Record<string, unknown> = { type: args[0] };
  const states: Record<string, unknown> = {};
  let target = payload;

  for (let index = 1; index < args.length; index += 2) {
    if (
      !hasOwnCommandArgument(args, index, args.length, "Flow policy arguments")
      || !hasOwnCommandArgument(args, index + 1, args.length, "Flow policy arguments")
    ) return undefined;
    const token = core.optionalText(args[index])?.toUpperCase();
    const value = args[index + 1];
    if (token == null || value == null) return undefined;

    if (token === "STATE") {
      if (!isBinaryArgument(value)) return undefined;
      const state = core.asText(value);
      target = {};
      core.setOwnValue(states, state, target);
      core.setOwnValue(payload, "states", states);
      continue;
    }
    if (token === "MODE") {
      if (!isBinaryArgument(value)) return undefined;
      const mode = core.optionalText(value)?.toLowerCase();
      if (target === payload || (mode !== "fifo" && mode !== "parallel")) return undefined;
      core.setOwnValue(target, "mode", mode);
      continue;
    }
    if (token === "EXPECTED_GENERATION") {
      if (target !== payload) return undefined;
      const generation = integerOption(value);
      if (generation == null || generation < 0) return undefined;
      core.setOwnValue(payload, "expected_generation", generation);
      continue;
    }
    if (token === "REPLACE") {
      if (target !== payload || !core.isBoolToken(value)) return undefined;
      core.setOwnValue(payload, "replace", core.boolArg(value));
      continue;
    }
    if (token === "MAX_ACTIVE_MS") {
      if (target !== payload) return undefined;
      const maxActiveMs = maxActiveOption(value);
      if (maxActiveMs == null) return undefined;
      core.setOwnValue(payload, "max_active_ms", maxActiveMs);
      continue;
    }
    if (token === "INDEXED_STATE_META") {
      if (target !== payload || !isBinaryArgument(value)) return undefined;
      core.setOwnValue(payload, "indexed_state_meta", value);
      continue;
    }
    if (token === "INDEXED_ATTRIBUTES") {
      if (target !== payload) return undefined;
      const names = indexedAttributes(value);
      if (names == null) return undefined;
      core.setOwnValue(payload, "indexed_attributes", names);
      continue;
    }
    if (token === "MAX_RETRIES") {
      const parsed = integerOption(value);
      if (parsed == null) return undefined;
      core.setOwnValue(objectField(target, "retry"), "max_retries", parsed);
      continue;
    }
    if (token === "EXHAUSTED_TO") {
      if (!isBinaryArgument(value)) return undefined;
      core.setOwnValue(objectField(target, "retry"), "exhausted_to", value);
      continue;
    }
    const backoffField = BACKOFF_FIELDS[token];
    if (backoffField != null) {
      const parsed = integerOption(value);
      if (parsed == null) return undefined;
      core.setOwnValue(objectField(objectField(target, "retry"), "backoff"), backoffField, parsed);
      continue;
    }
    if (token === "BACKOFF") {
      if (!isBinaryArgument(value)) return undefined;
      const kind = core.optionalText(value)?.toLowerCase();
      if (kind == null) return undefined;
      core.setOwnValue(objectField(objectField(target, "retry"), "backoff"), "kind", kind);
      continue;
    }
    const retentionField = RETENTION_FIELDS[token];
    if (retentionField != null) {
      // FerricStore 0.9.1 rejects top-level nested retention in the native
      // policy payload. The generic command path preserves type/state scope.
      if (target === payload) return undefined;
      const parsed = integerOption(value);
      if (parsed == null) return undefined;
      core.setOwnValue(objectField(target, "retention"), retentionField, parsed);
      continue;
    }
    return undefined;
  }

  return { opcode: wire.OPCODES.flowPolicySet, payload };
}

export function flowPolicyGetPayload(
  args: readonly CommandArgument[]
): wire.ProtocolCommand | undefined {
  if (!hasOwnCommandArgument(args, 0, args.length, "Flow policy arguments")) return undefined;
  if (!isBinaryArgument(args[0])) return undefined;
  if (args.length !== 1 && args.length !== 3) return undefined;
  const payload: Record<string, unknown> = { type: args[0] };
  if (args.length === 3) {
    if (
      !hasOwnCommandArgument(args, 1, args.length, "Flow policy arguments")
      || !hasOwnCommandArgument(args, 2, args.length, "Flow policy arguments")
      || core.optionalText(args[1])?.toUpperCase() !== "STATE"
      || !isBinaryArgument(args[2])
    ) return undefined;
    core.setOwnValue(payload, "state", args[2]);
  }
  return { opcode: wire.OPCODES.flowPolicyGet, payload };
}

function integerOption(value: CommandArgument): number | undefined {
  try {
    return core.safeIntegerNumberArg(value);
  } catch {
    return undefined;
  }
}

function maxActiveOption(value: CommandArgument): number | "infinity" | undefined {
  if (isBinaryArgument(value) && core.asText(value).toLowerCase() === "infinity") {
    return "infinity";
  }
  return integerOption(value);
}

function isBinaryArgument(value: unknown): value is string | Buffer | Uint8Array {
  return typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array;
}

function objectField(target: Record<string, unknown>, name: string): Record<string, unknown> {
  const existing = Object.hasOwn(target, name) ? target[name] : undefined;
  if (typeof existing === "object" && existing != null && !Array.isArray(existing)) {
    return existing as Record<string, unknown>;
  }
  const value: Record<string, unknown> = {};
  core.setOwnValue(target, name, value);
  return value;
}

function indexedAttributes(value: CommandArgument): readonly string[] | undefined {
  if (Array.isArray(value)) {
    return value.every((item) => typeof item === "string") ? value : undefined;
  }
  const source = core.optionalText(value);
  if (source == null) return undefined;
  try {
    const parsed: unknown = JSON.parse(source);
    return Array.isArray(parsed) && parsed.every((item) => typeof item === "string")
      ? parsed
      : undefined;
  } catch {
    return undefined;
  }
}
