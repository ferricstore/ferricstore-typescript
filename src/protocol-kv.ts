import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { CommandArgument } from "./internal.js";
import { integerArg, numberArg, optionalText, setOwnValue } from "./protocol-core.js";
import { COMMAND_OPCODES, type ProtocolCommand } from "./protocol-constants.js";

export function collectionProtocolCommand(
  command: string,
  args: readonly CommandArgument[]
): ProtocolCommand | undefined {
  switch (command) {
    case "CAS":
      return compareAndSwapCommand(args);
    case "LOCK":
      return lockCommand(COMMAND_OPCODES.LOCK, args);
    case "UNLOCK":
      return binaryFieldsCommand(COMMAND_OPCODES.UNLOCK, args, "owner", 2);
    case "EXTEND":
      return lockCommand(COMMAND_OPCODES.EXTEND, args);
    case "RATELIMIT.ADD":
      return rateLimitCommand(args);
    case "FETCH_OR_COMPUTE":
      return fetchOrComputeCommand(args);
    case "FETCH_OR_COMPUTE_RESULT":
      return fetchOrComputeResultCommand(args);
    case "FETCH_OR_COMPUTE_ERROR":
      return fetchOrComputeErrorCommand(args);
    case "HSET":
      return hashSetCommand(args);
    case "HGET":
      return binaryFieldsCommand(COMMAND_OPCODES.HGET, args, "field", 2);
    case "HMGET":
      return binaryListCommand(COMMAND_OPCODES.HMGET, args, "fields");
    case "HGETALL":
      return keyOnlyCommand(COMMAND_OPCODES.HGETALL, args);
    case "LPUSH":
      return binaryListCommand(COMMAND_OPCODES.LPUSH, args, "values");
    case "RPUSH":
      return binaryListCommand(COMMAND_OPCODES.RPUSH, args, "values");
    case "LPOP":
      return listPopCommand(COMMAND_OPCODES.LPOP, args);
    case "RPOP":
      return listPopCommand(COMMAND_OPCODES.RPOP, args);
    case "LRANGE":
      return rangeCommand(COMMAND_OPCODES.LRANGE, args, false);
    case "SADD":
      return binaryListCommand(COMMAND_OPCODES.SADD, args, "members");
    case "SREM":
      return binaryListCommand(COMMAND_OPCODES.SREM, args, "members");
    case "SMEMBERS":
      return keyOnlyCommand(COMMAND_OPCODES.SMEMBERS, args);
    case "SISMEMBER":
      return binaryFieldsCommand(COMMAND_OPCODES.SISMEMBER, args, "member", 2);
    case "ZADD":
      return sortedSetAddCommand(args);
    case "ZREM":
      return binaryListCommand(COMMAND_OPCODES.ZREM, args, "members");
    case "ZRANGE":
      return rangeCommand(COMMAND_OPCODES.ZRANGE, args, true);
    case "ZSCORE":
      return binaryFieldsCommand(COMMAND_OPCODES.ZSCORE, args, "member", 2);
    default:
      return undefined;
  }
}

export function stringSetPayload(args: readonly CommandArgument[]): Record<string, unknown> | undefined {
  if (!isBinaryCommandArgument(args[0]) || !isDirectKvValueArgument(args[1])) return undefined;
  const payload: Record<string, unknown> = { key: args[0], value: args[1] };
  let expirySeen = false;
  let nxSeen = false;
  let xxSeen = false;
  for (let index = 2; index < args.length; ) {
    const token = optionalText(args[index])?.toUpperCase();
    if (token === "EX" || token === "PX" || token === "EXAT" || token === "PXAT") {
      if (expirySeen || index + 1 >= args.length) return undefined;
      const value = positiveSafeInteger(args[index + 1]);
      if (value == null || (token === "EX" && value > Math.floor(Number.MAX_SAFE_INTEGER / 1_000))) {
        return undefined;
      }
      const field = token === "EX" || token === "PX" ? "ttl" : token.toLowerCase();
      setOwnValue(payload, field, token === "EX" ? value * 1_000 : value);
      expirySeen = true;
      index += 2;
    } else if (token === "NX") {
      if (xxSeen) return undefined;
      payload.nx = true;
      nxSeen = true;
      index += 1;
    } else if (token === "XX") {
      if (nxSeen) return undefined;
      payload.xx = true;
      xxSeen = true;
      index += 1;
    } else if (token === "GET") {
      payload.get = true;
      index += 1;
    } else if (token === "KEEPTTL") {
      if (expirySeen) return undefined;
      payload.keepttl = true;
      expirySeen = true;
      index += 1;
    } else {
      return undefined;
    }
  }
  return payload;
}

export function typedKeyValuePairs(args: readonly CommandArgument[]): unknown[][] | undefined {
  if (args.length % 2 !== 0) throw new FerricStoreError("MSET requires key/value pairs");
  const result = new Array<unknown[]>(args.length / 2);
  for (let index = 0; index < args.length; index += 2) {
    if (!isBinaryCommandArgument(args[index]) || !isDirectKvValueArgument(args[index + 1])) {
      return undefined;
    }
    result[index / 2] = [args[index], args[index + 1]];
  }
  return result;
}

function compareAndSwapCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || (args.length !== 3 && args.length !== 5)) return undefined;
  const payload: Record<string, unknown> = { expected: args[1], key: args[0], value: args[2] };
  if (args.length === 5) {
    if (binaryToken(args[3]) !== "EX") return undefined;
    const seconds = positiveSafeInteger(args[4]);
    if (seconds == null || seconds > Math.floor(Number.MAX_SAFE_INTEGER / 1_000)) return undefined;
    payload.ttl = seconds * 1_000;
  }
  return { opcode: COMMAND_OPCODES.CAS, payload };
}

function lockCommand(opcode: number, args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length !== 3 || !isBinaryCommandArgument(args[0]) || !isBinaryCommandArgument(args[1])) return undefined;
  const ttlMs = positiveSafeInteger(args[2]);
  return ttlMs == null ? undefined : { opcode, payload: { key: args[0], owner: args[1], ttl_ms: ttlMs } };
}

function rateLimitCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || (args.length !== 3 && args.length !== 4)) return undefined;
  const windowMs = positiveSafeInteger(args[1]);
  const max = positiveSafeInteger(args[2]);
  const count = args.length === 4 ? positiveSafeInteger(args[3]) : undefined;
  if (windowMs == null || max == null || (args.length === 4 && count == null)) return undefined;
  return {
    opcode: COMMAND_OPCODES["RATELIMIT.ADD"],
    payload: { ...(count == null ? {} : { count }), key: args[0], max, window_ms: windowMs }
  };
}

function fetchOrComputeCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || (args.length !== 2 && args.length !== 3)) return undefined;
  const ttlMs = positiveSafeInteger(args[1]);
  if (ttlMs == null || (args.length === 3 && !isBinaryCommandArgument(args[2]))) return undefined;
  return {
    opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE,
    payload: { ...(args.length === 3 ? { hint: args[2] } : {}), key: args[0], ttl_ms: ttlMs },
    serverBlockMs: ttlMs
  };
}

function fetchOrComputeResultCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length !== 4 || !isBinaryCommandArgument(args[0]) || !isBinaryCommandArgument(args[1])) return undefined;
  const ttlMs = positiveSafeInteger(args[3]);
  return ttlMs == null ? undefined : {
    opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE_RESULT,
    payload: { key: args[0], token: args[1], ttl_ms: ttlMs, value: args[2] }
  };
}

function fetchOrComputeErrorCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (
    args.length !== 3 ||
    !isBinaryCommandArgument(args[0]) ||
    !isBinaryCommandArgument(args[1]) ||
    !isBinaryCommandArgument(args[2])
  ) return undefined;
  return {
    opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE_ERROR,
    payload: { key: args[0], message: args[2], token: args[1] }
  };
}

function hashSetCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || args.length < 3 || args.length % 2 !== 1) return undefined;
  const fields = Object.create(null) as Record<string, CommandArgument>;
  for (let index = 1; index < args.length; index += 2) {
    const field = args[index];
    const value = args[index + 1];
    // Native map keys are textual. Binary fields stay on COMMAND_EXEC so
    // arbitrary bytes cannot be changed by UTF-8 coercion.
    if (typeof field !== "string" || !isBinaryCommandArgument(value)) return undefined;
    setOwnValue(fields, field, value);
  }
  return { opcode: COMMAND_OPCODES.HSET, payload: { fields, key: args[0] } };
}

function keyOnlyCommand(opcode: number, args: readonly CommandArgument[]): ProtocolCommand | undefined {
  return args.length === 1 && isBinaryCommandArgument(args[0])
    ? { opcode, payload: { key: args[0] } }
    : undefined;
}

function binaryFieldsCommand(
  opcode: number,
  args: readonly CommandArgument[],
  field: "field" | "member" | "owner",
  length: number
): ProtocolCommand | undefined {
  if (
    args.length !== length ||
    !isBinaryCommandArgument(args[0]) ||
    !isBinaryCommandArgument(args[1])
  ) return undefined;
  return { opcode, payload: { [field]: args[1], key: args[0] } };
}

function binaryListCommand(
  opcode: number,
  args: readonly CommandArgument[],
  field: "fields" | "members" | "values"
): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || args.length < 2) return undefined;
  const values = args.slice(1);
  if (!values.every(isBinaryCommandArgument)) return undefined;
  return { opcode, payload: { [field]: values, key: args[0] } };
}

function listPopCommand(opcode: number, args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || (args.length !== 1 && args.length !== 2)) return undefined;
  if (args.length === 1) return { opcode, payload: { key: args[0] } };
  const count = parsedInteger(args[1]);
  if (count == null || count <= 0 || count === 1) return undefined;
  return { opcode, payload: { count, key: args[0] } };
}

function rangeCommand(
  opcode: number,
  args: readonly CommandArgument[],
  allowWithScores: boolean
): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || (args.length !== 3 && (!allowWithScores || args.length !== 4))) {
    return undefined;
  }
  const start = parsedInteger(args[1]);
  const stop = parsedInteger(args[2]);
  if (start == null || stop == null) return undefined;
  if (args.length === 4 && binaryToken(args[3]) !== "WITHSCORES") return undefined;
  return {
    opcode,
    payload: {
      key: args[0],
      start,
      stop,
      ...(args.length === 4 ? { withscores: true } : {})
    }
  };
}

function sortedSetAddCommand(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (!isBinaryCommandArgument(args[0]) || args.length < 3 || args.length % 2 !== 1) return undefined;
  const items: [number, string | Buffer][] = [];
  for (let index = 1; index < args.length; index += 2) {
    const score = parsedNumber(args[index]);
    const member = args[index + 1];
    if (score == null || !isBinaryCommandArgument(member)) return undefined;
    items.push([score, member]);
  }
  return { opcode: COMMAND_OPCODES.ZADD, payload: { items, key: args[0] } };
}

function parsedInteger(value: CommandArgument): number | undefined {
  try {
    const parsed = integerArg(value);
    return typeof parsed === "number" ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function positiveSafeInteger(value: CommandArgument): number | undefined {
  const parsed = parsedInteger(value);
  return parsed != null && parsed > 0 ? parsed : undefined;
}

function parsedNumber(value: CommandArgument): number | undefined {
  try {
    return numberArg(value);
  } catch {
    return undefined;
  }
}

export function isBinaryCommandArgument(value: unknown): value is string | Buffer {
  return typeof value === "string" || Buffer.isBuffer(value);
}

function isDirectKvValueArgument(value: unknown): boolean {
  return isBinaryCommandArgument(value) || (typeof value === "object" && value != null);
}

function binaryToken(value: unknown): string | undefined {
  return typeof value === "string"
    ? value.toUpperCase()
    : Buffer.isBuffer(value)
      ? value.toString("utf8").toUpperCase()
      : undefined;
}
