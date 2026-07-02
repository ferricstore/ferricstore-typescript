import { Buffer } from "node:buffer";
import { FerricStoreError, classifyServerError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";

export const MAGIC = "FSNP";
export const REQUEST_VERSION = 0x01;
export const RESPONSE_VERSION = 0x81;
export const HEADER_SIZE = 24;

export const FLAG_CUSTOM_PAYLOAD = 0x02;
export const FLAG_COMPRESSED = 0x08;
export const FLAG_MORE_CHUNKS = 0x20;

export const STATUS_OK = 0;

export const OPCODES = {
  startup: 0x000c,
  auth: 0x0002,
  ping: 0x0003,
  clientSetName: 0x0004,
  clientInfo: 0x0005,
  commandExec: 0x0100,
  get: 0x0101,
  set: 0x0102,
  del: 0x0103,
  mget: 0x0104,
  mset: 0x0105,
  pipeline: 0x000e,
  flowCreate: 0x0201,
  flowClaimDue: 0x0203,
  flowComplete: 0x0204,
  flowCreateMany: 0x020f,
  flowCompleteMany: 0x0210
} as const;

const COMPACT_FLOW_CLAIM_JOBS = 0x80;
const COMPACT_OK_LIST = 0x81;
const COMPACT_KV_GET = 0x82;
const COMPACT_KV_MGET = 0x83;
const COMPACT_FLOW_RECORD = 0x84;
const COMPACT_FLOW_RECORD_LIST = 0x85;
const COMPACT_KV_MGET_FIXED = 0x89;
const COMPACT_FLOW_CREATE_MANY_REQUEST = 0x90;
const COMPACT_FLOW_CLAIM_DUE_REQUEST = 0x91;
const COMPACT_FLOW_COMPLETE_MANY_REQUEST = 0x92;
const COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST = 0x93;
const COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST = 0x96;
const COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST = 0x9e;
const COMPACT_PIPELINE_REQUEST = 0x94;
const COMPACT_PIPELINE_RESPONSE = 0x95;
const NULL_U32 = 0xffff_ffff;
const MIN_I64 = -9_223_372_036_854_775_808n;
const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_INTEGER_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

const FLOW_RECORD_FIELD_KEYS = [
  "",
  "id",
  "type",
  "state",
  "version",
  "priority",
  "partition_key",
  "payload_ref",
  "result_ref",
  "error_ref",
  "payload",
  "result",
  "error",
  "created_at_ms",
  "updated_at_ms",
  "next_run_at_ms",
  "lease_deadline_ms",
  "lease_owner",
  "lease_token",
  "fencing_token",
  "attempts",
  "history_max_events",
  "history_hot_max_events",
  "child_groups",
  "parent_flow_id",
  "parent_partition_key",
  "root_flow_id",
  "correlation_id",
  "terminal_retention_until_ms",
  "ttl_ms",
  "retention_ttl_ms",
  "run_state",
  "value_refs",
  "values",
  "payload_omitted",
  "payload_size",
  "result_omitted",
  "result_size",
  "error_omitted",
  "error_size",
  "max_attempts",
  "attributes"
] as const;

export interface ProtocolCommand {
  readonly opcode: number;
  readonly payload: unknown;
  readonly flags?: number;
  readonly laneId?: number;
}

export interface ResponseFrame {
  readonly flags: number;
  readonly laneId: number;
  readonly opcode: number;
  readonly requestId: bigint;
  readonly bodyLength: number;
  readonly body: Buffer;
}

export function encodeRequest(command: ProtocolCommand, requestId: bigint): Buffer {
  const body = ((command.flags ?? 0) & FLAG_CUSTOM_PAYLOAD) !== 0 ? toBuffer(command.payload) : encodeValue(command.payload);
  const flags = command.flags ?? 0;
  const frame = Buffer.allocUnsafe(HEADER_SIZE + body.byteLength);
  frame.write(MAGIC, 0, "ascii");
  frame.writeUInt8(REQUEST_VERSION, 4);
  frame.writeUInt8(flags, 5);
  frame.writeUInt32BE(command.laneId ?? laneForOpcode(command.opcode), 6);
  frame.writeUInt16BE(command.opcode, 10);
  frame.writeBigUInt64BE(requestId, 12);
  frame.writeUInt32BE(body.byteLength, 20);
  body.copy(frame, HEADER_SIZE);
  return frame;
}

export function tryDecodeFrame(buffer: Buffer): { readonly frame: ResponseFrame; readonly rest: Buffer } | null {
  if (buffer.byteLength < HEADER_SIZE) {
    return null;
  }
  if (buffer.toString("ascii", 0, 4) !== MAGIC) {
    throw new FerricStoreError("invalid FerricStore protocol magic", { raw: buffer.subarray(0, 4) });
  }
  const version = buffer.readUInt8(4);
  if (version !== RESPONSE_VERSION) {
    throw new FerricStoreError(`invalid FerricStore protocol response version ${version}`);
  }
  const bodyLength = buffer.readUInt32BE(20);
  const frameLength = HEADER_SIZE + bodyLength;
  if (buffer.byteLength < frameLength) {
    return null;
  }
  const frame: ResponseFrame = {
    body: buffer.subarray(HEADER_SIZE, frameLength),
    bodyLength,
    flags: buffer.readUInt8(5),
    laneId: buffer.readUInt32BE(6),
    opcode: buffer.readUInt16BE(10),
    requestId: buffer.readBigUInt64BE(12)
  };
  return { frame, rest: buffer.subarray(frameLength) };
}

export function decodeResponse(frame: ResponseFrame, expectedOpcode: number): unknown {
  if (frame.opcode !== expectedOpcode) {
    throw new FerricStoreError(
      `protocol response mismatch: expected opcode ${expectedOpcode}, got ${frame.opcode}`,
      { raw: frame }
    );
  }
  if ((frame.flags & FLAG_COMPRESSED) !== 0) {
    throw new FerricStoreError("compressed native protocol responses are not supported by this SDK yet");
  }
  if (frame.body.byteLength < 2) {
    throw new FerricStoreError("short native protocol response body", { raw: frame.body });
  }
  const status = frame.body.readUInt16BE(0);
  const body = frame.body.subarray(2);
  const value = decodeResponseValue(frame.opcode, body);
  if (status === STATUS_OK) {
    return value;
  }
  const message = errorMessage(status, value);
  throw classifyServerError(message, value);
}

export function buildProtocolCommand(args: readonly CommandArgument[]): ProtocolCommand {
  if (args.length === 0) {
    throw new FerricStoreError("command requires at least one argument");
  }
  const command = asText(args[0]).toUpperCase();
  const commandArgs = args.slice(1);

  if (command === "PING") {
    return { laneId: 0, opcode: OPCODES.ping, payload: commandArgs.length === 0 ? {} : { message: commandArgs[0] } };
  }
  if (command === "CLIENT" && asText(commandArgs[0]).toUpperCase() === "SETNAME" && commandArgs.length === 2) {
    return { laneId: 0, opcode: OPCODES.clientSetName, payload: { name: commandArgs[1] } };
  }
  if (command === "GET" && commandArgs.length >= 1) {
    return { opcode: OPCODES.get, payload: { key: commandArgs[0] } };
  }
  if (command === "SET" && commandArgs.length >= 2) {
    return { opcode: OPCODES.set, payload: kvSetPayload(commandArgs) };
  }
  if (command === "MGET") {
    return compactKeyPipelinePayload(OPCODES.mget, commandArgs, 2) ?? {
      opcode: OPCODES.mget,
      payload: { keys: commandArgs }
    };
  }
  if (command === "MSET") {
    return compactMsetPayload(commandArgs) ?? { opcode: OPCODES.mset, payload: { pairs: pairs(commandArgs) } };
  }
  if (command === "DEL") {
    return { opcode: OPCODES.del, payload: { keys: commandArgs } };
  }
  if (command.startsWith("FLOW.") && hasFlowCommandOnlyOption(commandArgs)) {
    return commandExec(args);
  }
  if (command === "FLOW.CREATE") {
    return flowCreatePayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.CREATE_MANY") {
    return flowCreateManyPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.CLAIM_DUE") {
    return flowClaimDuePayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.COMPLETE") {
    return flowCompletePayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.COMPLETE_MANY") {
    return flowCompleteManyPayload(commandArgs) ?? commandExec(args);
  }

  return commandExec(args);
}

export function commandExec(args: readonly CommandArgument[]): ProtocolCommand {
  if (args.length === 0) {
    throw new FerricStoreError("command requires at least one argument");
  }
  return {
    opcode: OPCODES.commandExec,
    payload: {
      args: args.slice(1),
      command: asText(args[0]).toUpperCase()
    }
  };
}

export function pipelineCommand(commands: readonly Command[]): ProtocolCommand {
  const compact = compactPipelinePayload(commands);
  if (compact != null) {
    return { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.pipeline, payload: compact };
  }

  return {
    opcode: OPCODES.pipeline,
    payload: {
      atomicity: "none",
      commands: commands.map((command, index) => {
        const protocol = buildProtocolCommand(command);
        return {
          body: protocol.payload,
          lane_id: protocol.laneId ?? laneForOpcode(protocol.opcode),
          opcode: protocol.opcode,
          request_id: index + 1
        };
      }),
      return: "compact"
    }
  };
}

export function unwrapPipelineResponse(
  value: unknown,
  options: { readonly throwOnItemError?: boolean } = {}
): unknown[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const items = value as unknown[];

  let hasStatusTuple = false;
  for (const item of items) {
    if (Array.isArray(item) && item.length >= 2 && pipelineStatus(item[0]) != null) {
      hasStatusTuple = true;
      break;
    }
  }
  if (!hasStatusTuple) {
    return items;
  }

  const out = new Array<unknown>(items.length);
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (Array.isArray(item) && item.length >= 2) {
      const status = pipelineStatus(item[0]);
      if (status != null) {
        const payload: unknown = item[1];
        if (status === "ok") {
          out[index] = payload;
          continue;
        }
        const error = classifyServerError(errorMessage(status === "busy" ? 4 : 1, payload), payload);
        if (options.throwOnItemError !== false) {
          throw error;
        }
        out[index] = error;
        continue;
      }
    }
    out[index] = item;
  }
  return out;
}

function pipelineStatus(value: unknown): "busy" | "error" | "ok" | null {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    return null;
  }
  const status = asText(value).toLowerCase();
  return status === "ok" || status === "busy" || status === "error" ? status : null;
}

export function encodeValue(value: unknown): Buffer {
  if (value == null) {
    return Buffer.from([0]);
  }
  if (value === true) {
    return Buffer.from([1]);
  }
  if (value === false) {
    return Buffer.from([2]);
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    const buffer = Buffer.allocUnsafe(9);
    buffer.writeUInt8(3, 0);
    buffer.writeBigInt64BE(BigInt(value), 1);
    return buffer;
  }
  if (typeof value === "bigint") {
    const buffer = Buffer.allocUnsafe(9);
    buffer.writeUInt8(3, 0);
    buffer.writeBigInt64BE(value, 1);
    return buffer;
  }
  if (typeof value === "number") {
    const buffer = Buffer.allocUnsafe(9);
    buffer.writeUInt8(7, 0);
    buffer.writeDoubleBE(value, 1);
    return buffer;
  }
  if (typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return encodeBinary(toBuffer(value), 4);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) => encodeValue(item));
    return Buffer.concat([u8(5), u32(items.length), ...items]);
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    const encoded = entries.flatMap(([key, item]) => [u32(Buffer.byteLength(key)), Buffer.from(key), encodeValue(item)]);
    return Buffer.concat([u8(6), u32(entries.length), ...encoded]);
  }
  throw new FerricStoreError(`unsupported native protocol value type: ${typeof value}`);
}

export function decodeValue(data: Buffer, offset = 0): { readonly value: unknown; readonly offset: number } {
  requireAvailable(data, offset, 1);
  const tag = data.readUInt8(offset);
  offset += 1;
  if (tag === 0) return { value: null, offset };
  if (tag === 1) return { value: true, offset };
  if (tag === 2) return { value: false, offset };
  if (tag === 3) {
    requireAvailable(data, offset, 8);
    return { value: Number(data.readBigInt64BE(offset)), offset: offset + 8 };
  }
  if (tag === 4) {
    const read = readBinary(data, offset);
    return { value: read.value, offset: read.offset };
  }
  if (tag === 5) {
    requireAvailable(data, offset, 4);
    const count = data.readUInt32BE(offset);
    offset += 4;
    const values: unknown[] = [];
    for (let index = 0; index < count; index += 1) {
      const read = decodeValue(data, offset);
      values.push(read.value);
      offset = read.offset;
    }
    return { value: values, offset };
  }
  if (tag === 6) {
    requireAvailable(data, offset, 4);
    const count = data.readUInt32BE(offset);
    offset += 4;
    const value: Record<string, unknown> = {};
    for (let index = 0; index < count; index += 1) {
      const key = readBinary(data, offset);
      offset = key.offset;
      const item = decodeValue(data, offset);
      value[key.value.toString("utf8")] = item.value;
      offset = item.offset;
    }
    return { value, offset };
  }
  if (tag === 7) {
    requireAvailable(data, offset, 8);
    return { value: data.readDoubleBE(offset), offset: offset + 8 };
  }
  throw new FerricStoreError(`unknown protocol value tag ${tag}`);
}

function decodeResponseValue(opcode: number, body: Buffer): unknown {
  const compact = tryDecodeCompactResponse(opcode, body);
  if (compact.found) {
    return compact.value;
  }
  const decoded = decodeValue(body);
  if (decoded.offset !== body.byteLength) {
    throw new FerricStoreError("native protocol response has trailing bytes", { raw: body });
  }
  return decoded.value;
}

function tryDecodeCompactResponse(opcode: number, body: Buffer): { readonly found: boolean; readonly value: unknown } {
  if (body.byteLength === 0) {
    return { found: false, value: undefined };
  }
  const tag = body.readUInt8(0);
  if (tag === COMPACT_OK_LIST && isOkListOpcode(opcode)) {
    const values = decodeCompactOkList(body);
    return { found: true, value: opcode === OPCODES.pipeline ? values : values.length === 1 ? "OK" : values };
  }
  if (tag === COMPACT_KV_GET && opcode === OPCODES.get) {
    return { found: true, value: decodeCompactKvGet(body) };
  }
  if ((tag === COMPACT_KV_MGET || tag === COMPACT_KV_MGET_FIXED) && (opcode === OPCODES.mget || opcode === OPCODES.pipeline)) {
    return { found: true, value: tag === COMPACT_KV_MGET ? decodeCompactMget(body) : decodeCompactMgetFixed(body) };
  }
  if (tag === COMPACT_PIPELINE_RESPONSE && opcode === OPCODES.pipeline) {
    return { found: true, value: decodeCompactPipeline(body) };
  }
  if (tag === COMPACT_FLOW_CLAIM_JOBS) {
    return { found: true, value: decodeCompactClaimJobs(body) };
  }
  if (tag === COMPACT_FLOW_RECORD) {
    const read = readCompactFlowRecord(body, 0);
    if (read.offset !== body.byteLength) throw new FerricStoreError("trailing compact Flow record bytes");
    return { found: true, value: read.value };
  }
  if (tag === COMPACT_FLOW_RECORD_LIST) {
    const read = readCompactFlowRecordList(body, 0);
    if (read.offset !== body.byteLength) throw new FerricStoreError("trailing compact Flow record list bytes");
    return { found: true, value: read.value };
  }
  return { found: false, value: undefined };
}

function kvSetPayload(args: readonly CommandArgument[]): Record<string, unknown> {
  const payload: Record<string, unknown> = { key: args[0], value: args[1] };
  for (let index = 2; index < args.length; ) {
    const token = asText(args[index]).toUpperCase();
    if (token === "EX") {
      payload.ttl = Number(args[index + 1]) * 1_000;
      index += 2;
    } else if (token === "PX") {
      payload.ttl = Number(args[index + 1]);
      index += 2;
    } else if (token === "NX") {
      payload.nx = true;
      index += 1;
    } else if (token === "XX") {
      payload.xx = true;
      index += 1;
    } else if (token === "GET") {
      payload.get = true;
      index += 1;
    } else if (token === "KEEPTTL") {
      payload.keep_ttl = true;
      index += 1;
    } else {
      payload.args = args.slice(2);
      break;
    }
  }
  return payload;
}

function pairs(args: readonly CommandArgument[]): unknown[][] {
  if (args.length % 2 !== 0) {
    throw new FerricStoreError("MSET requires key/value pairs");
  }
  const result: unknown[][] = [];
  for (let index = 0; index < args.length; index += 2) {
    result.push([args[index], args[index + 1]]);
  }
  return result;
}

function compactMsetPayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length % 2 !== 0) {
    throw new FerricStoreError("MSET requires key/value pairs");
  }
  const items: Buffer[] = [u8(COMPACT_PIPELINE_REQUEST), u8(1), u32(args.length / 2)];
  for (let index = 0; index < args.length; index += 2) {
    items.push(compactBinary(args[index]), compactBinary(args[index + 1]));
  }
  return { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.mset, payload: Buffer.concat(items) };
}

function compactKeyPipelinePayload(opcode: number, args: readonly CommandArgument[], mode: number): ProtocolCommand | undefined {
  const items: Buffer[] = [u8(COMPACT_PIPELINE_REQUEST), u8(mode), u32(args.length)];
  for (const arg of args) {
    items.push(compactBinary(arg));
  }
  return { flags: FLAG_CUSTOM_PAYLOAD, opcode, payload: Buffer.concat(items) };
}

function compactPipelinePayload(commands: readonly Command[]): Buffer | undefined {
  if (commands.length === 0) {
    return Buffer.concat([u8(COMPACT_PIPELINE_REQUEST), u8(0x80 | 2), u32(0)]);
  }
  if (commandNameIs(commands[0]?.[0], "SET")) {
    return compactSetPipelinePayload(commands);
  }
  if (commandNameIs(commands[0]?.[0], "GET")) {
    return compactGetPipelinePayload(commands);
  }
  return undefined;
}

function compactSetPipelinePayload(commands: readonly Command[]): Buffer | undefined {
  const keys = new Array<Buffer>(commands.length);
  const values = new Array<Buffer>(commands.length);
  let total = 6;

  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (command == null) return undefined;
    if (command.length !== 3 || !commandNameIs(command[0], "SET")) return undefined;
    const key = toBuffer(command[1]);
    const value = toBuffer(command[2]);
    keys[index] = key;
    values[index] = value;
    total += 8 + key.byteLength + value.byteLength;
  }

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 0x80 | 1, commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    const key = keys[index];
    const value = values[index];
    if (key == null || value == null) return undefined;
    offset = writeBinary(out, offset, key);
    offset = writeBinary(out, offset, value);
  }
  return out;
}

function compactGetPipelinePayload(commands: readonly Command[]): Buffer | undefined {
  const keys = new Array<Buffer>(commands.length);
  let total = 6;

  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (command == null) return undefined;
    if (command.length !== 2 || !commandNameIs(command[0], "GET")) return undefined;
    const key = toBuffer(command[1]);
    keys[index] = key;
    total += 4 + key.byteLength;
  }

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 0x80 | 2, commands.length);
  for (const key of keys) {
    offset = writeBinary(out, offset, key);
  }
  return out;
}

function writeCompactPipelineHeader(out: Buffer, mode: number, count: number): number {
  out.writeUInt8(COMPACT_PIPELINE_REQUEST, 0);
  out.writeUInt8(mode, 1);
  out.writeUInt32BE(count, 2);
  return 6;
}

function writeBinary(out: Buffer, offset: number, value: Buffer): number {
  out.writeUInt32BE(value.byteLength, offset);
  offset += 4;
  value.copy(out, offset);
  return offset + value.byteLength;
}

function writeOptionalBinary(out: Buffer, offset: number, value: Buffer | null): number {
  if (value == null) {
    out.writeUInt32BE(NULL_U32, offset);
    return offset + 4;
  }
  return writeBinary(out, offset, value);
}

function hasFlowCommandOnlyOption(args: readonly CommandArgument[]): boolean {
  return args.some((arg) => {
    if (typeof arg !== "string" && !Buffer.isBuffer(arg)) {
      return false;
    }
    const token = asText(arg).toUpperCase();
    return token === "STATE_META" || token === "INDEXED_STATE_META";
  });
}

function flowCreatePayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length < 7) return undefined;
  const id = args[0];
  if (hasToken(args, "PAYLOAD", 1) || hasToken(args, "VALUES", 1) || hasToken(args, "VALUE_REFS", 1)) {
    return undefined;
  }
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set(["TYPE", "STATE", "NOW", "PARTITION", "RUN_AT", "PRIORITY", "IDEMPOTENT", "RETENTION_TTL_MS"]),
    required: new Set(["TYPE", "STATE", "NOW"])
  });
  if (options == null) return undefined;
  return { opcode: OPCODES.flowCreate, payload: { id, ...options } };
}

function flowCreateManyPayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const partition = asText(args[0]);
  const itemsIndex = findToken(args, "ITEMS", 1);
  if (itemsIndex < 0 || hasToken(args, "ITEMS_EXT", 1)) return undefined;

  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed: new Set(["TYPE", "STATE", "NOW", "RUN_AT", "PRIORITY", "IDEMPOTENT", "INDEPENDENT", "RETENTION_TTL_MS"]),
    required: new Set(["TYPE"])
  });
  if (options == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const auto = partition.toUpperCase() === "AUTO";
  const rawItems = args.slice(itemsIndex + 1);
  const width = mixed ? 3 : 2;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;

  const items: unknown[][] = [];
  for (let index = 0; index < rawItems.length; index += width) {
    if (mixed) {
      items.push([rawItems[index], rawItems[index + 1], rawItems[index + 2]]);
    } else {
      items.push([rawItems[index], rawItems[index + 1]]);
    }
  }

  const payload: Record<string, unknown> = { ...options, items };
  if (!auto && !mixed) payload.partition_key = partition;
  const compact = compactFlowCreateManyPayload(partition, rawItems, mixed, auto, options);
  if (compact != null) return compact;
  return { opcode: OPCODES.flowCreateMany, payload };
}

function flowClaimDuePayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length < 1) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set([
      "STATE",
      "STATES",
      "PARTITION",
      "PARTITIONS",
      "WORKER",
      "LEASE_MS",
      "LIMIT",
      "PRIORITY",
      "NOW",
      "BLOCK",
      "BLOCK_MS",
      "RECLAIM_EXPIRED",
      "RECLAIM_RATIO",
      "RETURN",
      "PAYLOAD",
      "NOPAYLOAD",
      "MAXBYTES",
      "INCLUDE_STATE"
    ])
  });
  if (options == null) return undefined;
  const returnMode = options.return == null ? "" : asText(options.return).toUpperCase();
  if (!returnMode.startsWith("JOBS_COMPACT")) return undefined;
  const compact = compactFlowClaimDuePayload(args[0], options);
  if (compact != null) return compact;
  return { opcode: OPCODES.flowClaimDue, payload: { ...options, type: args[0] } };
}

function flowCompletePayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length < 6) return undefined;
  const id = args[0];
  const leaseToken = args[1];
  if (
    hasToken(args, "RESULT", 2) ||
    hasToken(args, "PAYLOAD", 2) ||
    hasToken(args, "VALUES", 2) ||
    hasToken(args, "VALUE_REFS", 2)
  ) {
    return undefined;
  }
  const options = parseFlowOptions(args, 2, args.length, {
    allowed: new Set(["FENCING", "NOW", "PARTITION", "TTL"]),
    required: new Set(["FENCING", "NOW"])
  });
  if (options == null) return undefined;
  return { opcode: OPCODES.flowComplete, payload: { id, lease_token: leaseToken, ...options } };
}

function flowCompleteManyPayload(args: readonly CommandArgument[]): ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const partition = asText(args[0]);
  const itemsIndex = findToken(args, "ITEMS", 1);
  if (itemsIndex < 0) return undefined;

  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed: new Set(["TTL", "NOW", "INDEPENDENT", "RETURN"])
  });
  if (options == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const auto = partition.toUpperCase() === "AUTO";
  const rawItems = args.slice(itemsIndex + 1);
  const width = mixed ? 4 : 3;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;

  const compact = compactFlowCompleteManyPayload(partition, rawItems, mixed, auto, options);
  if (compact != null) return compact;

  const items: unknown[][] = [];
  for (let index = 0; index < rawItems.length; index += width) {
    if (mixed) {
      items.push([rawItems[index], rawItems[index + 1], rawItems[index + 2], numberArg(rawItems[index + 3])]);
    } else {
      items.push([rawItems[index], rawItems[index + 1], numberArg(rawItems[index + 2])]);
    }
  }

  const payload: Record<string, unknown> = { ...options, items };
  if (!auto && !mixed) payload.partition_key = partition;
  return { opcode: OPCODES.flowCompleteMany, payload };
}

function compactFlowCreateManyPayload(
  partition: string,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  auto: boolean,
  options: Record<string, unknown>
): ProtocolCommand | undefined {
  if (
    "priority" in options ||
    "idempotent" in options ||
    "retention_ttl_ms" in options ||
    typeof options.type === "undefined" ||
    typeof options.state === "undefined" ||
    typeof options.now_ms !== "number"
  ) {
    return undefined;
  }
  const runAtMs = typeof options.run_at_ms === "number" ? options.run_at_ms : options.now_ms;

  const tag = mixed ? COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST : auto ? COMPACT_FLOW_CREATE_MANY_REQUEST : COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST;
  const items: Buffer[] = [
    u8(tag),
    compactBinary(options.type),
    compactBinary(options.state)
  ];
  if (!auto && !mixed) {
    items.push(compactOptionalBinary(partition));
  }
  items.push(
    i64(BigInt(options.now_ms)),
    i64(BigInt(runAtMs)),
    u8(independentMode(options.independent)),
    u8(1),
    u32(mixed ? rawItems.length / 3 : rawItems.length / 2)
  );
  const width = mixed ? 3 : 2;
  for (let index = 0; index < rawItems.length; index += width) {
    items.push(compactBinary(rawItems[index]));
    if (mixed) items.push(compactBinary(rawItems[index + 1]));
    items.push(compactBinary(rawItems[index + width - 1]));
  }
  return { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.flowCreateMany, payload: Buffer.concat(items) };
}

function compactFlowClaimDuePayload(type: CommandArgument, options: Record<string, unknown>): ProtocolCommand | undefined {
  if (
    "states" in options ||
    "now_ms" in options ||
    "payload_max_bytes" in options ||
    "include_state" in options ||
    options.payload === true ||
    typeof options.worker === "undefined" ||
    typeof options.lease_ms !== "number" ||
    typeof options.limit !== "number"
  ) {
    return undefined;
  }
  const returnMode = compactClaimReturnMode(options.return);
  if (returnMode == null) return undefined;
  const items: Buffer[] = [
    u8(COMPACT_FLOW_CLAIM_DUE_REQUEST),
    compactBinary(type),
    compactOptionalBinary(options.state),
    compactBinary(options.worker),
    i64(BigInt(options.lease_ms)),
    i64(BigInt(options.limit)),
    i64(optionalI64(options.block_ms)),
    u8(options.reclaim_expired === false ? 0 : options.reclaim_expired === true ? 1 : 0),
    i64(BigInt(typeof options.reclaim_ratio === "number" ? options.reclaim_ratio : 25)),
    i64(optionalI64(options.priority)),
    u8(returnMode)
  ];
  const partitionKeys = Array.isArray(options.partition_keys) ? options.partition_keys : undefined;
  if (partitionKeys != null) {
    items.push(u8(2), u32(partitionKeys.length), ...partitionKeys.map((key) => compactBinary(key)));
  } else if (options.partition_key != null) {
    items.push(u8(1), compactBinary(options.partition_key));
  } else {
    items.push(u8(0));
  }
  return { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.flowClaimDue, payload: Buffer.concat(items) };
}

function compactFlowCompleteManyPayload(
  partition: string,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  auto: boolean,
  options: Record<string, unknown>
): ProtocolCommand | undefined {
  if ("ttl_ms" in options || typeof options.now_ms !== "number") {
    return undefined;
  }
  const returnMode = options.return == null ? "" : asText(options.return).toUpperCase();
  if (returnMode !== "" && returnMode !== "OK_ON_SUCCESS") {
    return undefined;
  }
  const width = mixed ? 4 : 3;
  const count = rawItems.length / width;
  const items = new Array<{
    readonly id: Buffer;
    readonly partition: Buffer | null;
    readonly lease: Buffer;
    readonly fencing: bigint;
  }>(count);

  let total = 1 + 4 + 8 + 1 + 4;
  const headerPartition = !auto && !mixed ? toBuffer(partition) : null;
  if (headerPartition != null) {
    total += headerPartition.byteLength;
  }

  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    const id = toBuffer(rawItems[rawIndex]);
    const itemPartition = mixed ? toBuffer(rawItems[rawIndex + 1]) : null;
    const lease = toBuffer(rawItems[rawIndex + width - 2]);
    const fencing = BigInt(numberArg(rawItems[rawIndex + width - 1]));
    items[itemIndex] = { id, partition: itemPartition, lease, fencing };
    total += 4 + id.byteLength + 4 + (itemPartition?.byteLength ?? 0) + 4 + lease.byteLength + 8;
  }

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(returnMode === "OK_ON_SUCCESS" ? COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST : COMPACT_FLOW_COMPLETE_MANY_REQUEST, offset);
  offset += 1;
  offset = writeOptionalBinary(out, offset, headerPartition);
  out.writeBigInt64BE(BigInt(options.now_ms), offset);
  offset += 8;
  out.writeUInt8(independentMode(options.independent), offset);
  offset += 1;
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (const item of items) {
    offset = writeBinary(out, offset, item.id);
    offset = writeOptionalBinary(out, offset, item.partition);
    offset = writeBinary(out, offset, item.lease);
    out.writeBigInt64BE(item.fencing, offset);
    offset += 8;
  }
  return { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.flowCompleteMany, payload: out };
}

function parseFlowOptions(
  args: readonly CommandArgument[],
  start: number,
  end: number,
  config: { readonly allowed: ReadonlySet<string>; readonly required?: ReadonlySet<string> }
): Record<string, unknown> | undefined {
  const payload: Record<string, unknown> = {};
  const seen = new Set<string>();
  for (let index = start; index < end; ) {
    const token = asText(args[index]).toUpperCase();
    if (!config.allowed.has(token)) return undefined;
    seen.add(token);
    switch (token) {
      case "TYPE":
        if (!putScalar(payload, "type", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "STATE":
        if (!putScalar(payload, "state", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "STATES": {
        const count = numberArg(args[index + 1]);
        if (!Number.isInteger(count) || count < 0 || index + 2 + count > end) return undefined;
        payload.states = args.slice(index + 2, index + 2 + count);
        index += 2 + count;
        break;
      }
      case "PARTITION":
        if (!putScalar(payload, "partition_key", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "PARTITIONS": {
        const count = numberArg(args[index + 1]);
        if (!Number.isInteger(count) || count < 0 || index + 2 + count > end) return undefined;
        payload.partition_keys = args.slice(index + 2, index + 2 + count);
        index += 2 + count;
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
      case "TTL":
        if (!putNumber(payload, "ttl_ms", args, index + 1, end)) return undefined;
        index += 2;
        break;
      case "FENCING":
        if (!putNumber(payload, "fencing_token", args, index + 1, end)) return undefined;
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
      case "PAYLOAD":
        payload.payload = true;
        index += 1;
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

function putScalar(payload: Record<string, unknown>, key: string, args: readonly CommandArgument[], index: number, end: number): boolean {
  if (index >= end) return false;
  payload[key] = args[index];
  return true;
}

function putNumber(payload: Record<string, unknown>, key: string, args: readonly CommandArgument[], index: number, end: number): boolean {
  if (index >= end) return false;
  payload[key] = numberArg(args[index]);
  return true;
}

function putOptionalBool(
  payload: Record<string, unknown>,
  key: string,
  args: readonly CommandArgument[],
  index: number,
  end: number
): number {
  if (index + 1 < end && isBoolToken(args[index + 1])) {
    payload[key] = boolArg(args[index + 1]);
    return index + 2;
  }
  payload[key] = true;
  return index + 1;
}

function findToken(args: readonly CommandArgument[], token: string, start: number): number {
  for (let index = start; index < args.length; index += 1) {
    if (asText(args[index]).toUpperCase() === token) return index;
  }
  return -1;
}

function hasToken(args: readonly CommandArgument[], token: string, start: number): boolean {
  return findToken(args, token, start) >= 0;
}

function numberArg(value: CommandArgument): number {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  return Number(asText(value));
}

function boolArg(value: CommandArgument): boolean {
  if (typeof value === "boolean") return value;
  const text = asText(value).toUpperCase();
  return text === "1" || text === "TRUE" || text === "YES" || text === "ON";
}

function isBoolToken(value: CommandArgument): boolean {
  if (typeof value === "boolean") return true;
  const text = asText(value).toUpperCase();
  return text === "1" || text === "0" || text === "TRUE" || text === "FALSE" || text === "YES" || text === "NO" || text === "ON" || text === "OFF";
}

function isOkListOpcode(opcode: number): boolean {
  return (
    opcode === OPCODES.set ||
    opcode === OPCODES.mset ||
    opcode === OPCODES.pipeline ||
    opcode === OPCODES.flowCreateMany ||
    opcode === OPCODES.flowCompleteMany
  );
}

function decodeCompactOkList(data: Buffer): Buffer[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  if (data.byteLength !== 5) {
    throw new FerricStoreError("invalid compact OK list response");
  }
  return Array.from({ length: count }, () => Buffer.from("OK"));
}

function decodeCompactKvGet(data: Buffer): Buffer | null {
  requireAvailable(data, 0, 2);
  const present = data.readUInt8(1);
  if (present === 0) return null;
  if (present !== 1) throw new FerricStoreError("invalid compact GET response");
  const read = readBinary(data, 2);
  if (read.offset !== data.byteLength) throw new FerricStoreError("trailing compact GET bytes");
  return read.value;
}

function decodeCompactMget(data: Buffer): (Buffer | null)[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  let offset = 5;
  const values: (Buffer | null)[] = [];
  for (let index = 0; index < count; index += 1) {
    requireAvailable(data, offset, 1);
    const present = data.readUInt8(offset);
    offset += 1;
    if (present === 0) {
      values.push(null);
    } else if (present === 1) {
      const read = readBinary(data, offset);
      values.push(read.value);
      offset = read.offset;
    } else {
      throw new FerricStoreError("invalid compact MGET value marker");
    }
  }
  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact MGET bytes");
  return values;
}

function decodeCompactMgetFixed(data: Buffer): Buffer[] {
  requireAvailable(data, 0, 9);
  const count = data.readUInt32BE(1);
  const size = data.readUInt32BE(5);
  const expected = 9 + count * size;
  if (size === NULL_U32 || data.byteLength !== expected) {
    throw new FerricStoreError("invalid compact fixed MGET response");
  }
  const values: Buffer[] = [];
  for (let offset = 9; offset < expected; offset += size) {
    values.push(data.subarray(offset, offset + size));
  }
  return values;
}

function decodeCompactPipeline(data: Buffer): unknown[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  let offset = 5;
  const values = new Array<unknown>(count);
  for (let index = 0; index < count; index += 1) {
    requireAvailable(data, offset, 1);
    const status = data.readUInt8(offset);
    offset += 1;
    if (status === 0) {
      requireAvailable(data, offset, 1);
      const kind = data.readUInt8(offset);
      offset += 1;
      if (kind === 0) {
        values[index] = null;
      } else if (kind === 1) {
        const read = readBinary(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 2) {
        const read = readCompactFlowRecord(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 3) {
        const read = readCompactFlowRecordList(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 4) {
        const read = readCompactClaimJob(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 5) {
        const read = readCompactFlowValueRef(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 6) {
        const read = readCompactBinaryList(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 7) {
        const read = readCompactBinaryMap(data, offset);
        values[index] = read.value;
        offset = read.offset;
      } else {
        throw new FerricStoreError("unknown compact pipeline success kind");
      }
    } else if (status === 1 || status === 2) {
      const read = readBinary(data, offset);
      values[index] = [status === 1 ? "busy" : "error", read.value];
      offset = read.offset;
    } else {
      throw new FerricStoreError("unknown compact pipeline status");
    }
  }
  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact pipeline bytes");
  return values;
}

function decodeCompactClaimJobs(data: Buffer): unknown[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  for (const mode of ["stateAttrs", "attrs", "base"] as const) {
    const decoded = tryReadCompactClaimJobs(data, 5, count, mode);
    if (decoded?.offset === data.byteLength) {
      return decoded.value;
    }
  }
  try {
    readCompactClaimJob(data, 5, "base");
  } catch (error) {
    if (error instanceof FerricStoreError && error.message === "unsafe compact Flow fencing token") throw error;
  }
  throw new FerricStoreError("trailing compact claim jobs bytes");
}

function tryReadCompactClaimJobs(
  data: Buffer,
  offset: number,
  count: number,
  mode: "base" | "attrs" | "stateAttrs"
): { readonly value: unknown[]; readonly offset: number } | null {
  try {
    const values: unknown[] = [];
    for (let index = 0; index < count; index += 1) {
      const read = readCompactClaimJob(data, offset, mode);
      values.push(read.value);
      offset = read.offset;
    }
    return { value: values, offset };
  } catch (error) {
    if (error instanceof FerricStoreError || error instanceof RangeError) return null;
    throw error;
  }
}

function readCompactClaimJob(
  data: Buffer,
  offset: number,
  mode: "base" | "attrs" | "stateAttrs" = "base"
): { readonly value: unknown[]; readonly offset: number } {
  const id = readBinary(data, offset);
  const partition = readOptionalBinary(data, id.offset);
  const lease = readBinary(data, partition.offset);
  requireAvailable(data, lease.offset, 8);
  const fencingBig = data.readBigInt64BE(lease.offset);
  if (fencingBig > MAX_SAFE_INTEGER_BIGINT || fencingBig < MIN_SAFE_INTEGER_BIGINT) {
    throw new FerricStoreError("unsafe compact Flow fencing token");
  }
  const fencing = Number(fencingBig);
  offset = lease.offset + 8;
  if (mode === "base") {
    return { value: [id.value, partition.value, lease.value, fencing], offset };
  }
  if (mode === "attrs") {
    const attrs = decodeValue(data, offset);
    return { value: [id.value, partition.value, lease.value, fencing, null, attrs.value], offset: attrs.offset };
  }
  const runState = readOptionalBinary(data, offset);
  const attrs = decodeValue(data, runState.offset);
  return {
    value: [id.value, partition.value, lease.value, fencing, runState.value, attrs.value],
    offset: attrs.offset
  };
}

function readCompactFlowValueRef(data: Buffer, offset: number): { readonly value: Record<string, unknown>; readonly offset: number } {
  const ref = readBinary(data, offset);
  const partition = readOptionalBinary(data, ref.offset);
  const owner = readOptionalBinary(data, partition.offset);
  const value: Record<string, unknown> = { ref: ref.value };
  if (partition.value != null) value.partition_key = partition.value;
  if (owner.value != null) value.owner_flow_id = owner.value;
  return { value, offset: owner.offset };
}

function readCompactFlowRecord(data: Buffer, offset: number): { readonly value: Record<string, unknown>; readonly offset: number } {
  requireAvailable(data, offset, 5);
  if (data.readUInt8(offset) !== COMPACT_FLOW_RECORD) {
    throw new FerricStoreError("expected compact Flow record");
  }
  offset += 1;
  const count = data.readUInt32BE(offset);
  offset += 4;
  const record: Record<string, unknown> = {};
  for (let index = 0; index < count; index += 1) {
    requireAvailable(data, offset, 1);
    const keyId = data.readUInt8(offset);
    offset += 1;
    let key: string;
    if (keyId === 0) {
      const read = readBinary(data, offset);
      key = read.value.toString("utf8");
      offset = read.offset;
    } else {
      key = FLOW_RECORD_FIELD_KEYS[keyId] ?? `field_${keyId}`;
    }
    const read = decodeValue(data, offset);
    record[key] = read.value;
    offset = read.offset;
  }
  return { value: record, offset };
}

function readCompactFlowRecordList(data: Buffer, offset: number): { readonly value: Record<string, unknown>[]; readonly offset: number } {
  requireAvailable(data, offset, 5);
  if (data.readUInt8(offset) !== COMPACT_FLOW_RECORD_LIST) {
    throw new FerricStoreError("expected compact Flow record list");
  }
  offset += 1;
  const count = data.readUInt32BE(offset);
  offset += 4;
  const records: Record<string, unknown>[] = [];
  for (let index = 0; index < count; index += 1) {
    const read = readCompactFlowRecord(data, offset);
    records.push(read.value);
    offset = read.offset;
  }
  return { value: records, offset };
}

function readCompactBinaryList(data: Buffer, offset: number): { readonly value: Buffer[]; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const count = data.readUInt32BE(offset);
  offset += 4;
  const values: Buffer[] = [];
  for (let index = 0; index < count; index += 1) {
    const read = readBinary(data, offset);
    values.push(read.value);
    offset = read.offset;
  }
  return { value: values, offset };
}

function readCompactBinaryMap(data: Buffer, offset: number): { readonly value: Record<string, Buffer>; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const count = data.readUInt32BE(offset);
  offset += 4;
  const values: Record<string, Buffer> = {};
  for (let index = 0; index < count; index += 1) {
    const key = readBinary(data, offset);
    const value = readBinary(data, key.offset);
    values[key.value.toString("utf8")] = value.value;
    offset = value.offset;
  }
  return { value: values, offset };
}

function readOptionalBinary(data: Buffer, offset: number): { readonly value: Buffer | null; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const size = data.readUInt32BE(offset);
  offset += 4;
  if (size === NULL_U32) {
    return { value: null, offset };
  }
  requireAvailable(data, offset, size);
  return { value: data.subarray(offset, offset + size), offset: offset + size };
}

function readBinary(data: Buffer, offset: number): { readonly value: Buffer; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const size = data.readUInt32BE(offset);
  offset += 4;
  if (size === NULL_U32) {
    throw new FerricStoreError("invalid null binary length");
  }
  requireAvailable(data, offset, size);
  return { value: data.subarray(offset, offset + size), offset: offset + size };
}

function compactBinary(value: unknown): Buffer {
  const buffer = toBuffer(value);
  return Buffer.concat([u32(buffer.byteLength), buffer]);
}

function compactOptionalBinary(value: unknown): Buffer {
  if (value == null) return u32(NULL_U32);
  return compactBinary(value);
}

function encodeBinary(value: Buffer, tag: number): Buffer {
  return Buffer.concat([u8(tag), u32(value.byteLength), value]);
}

function toBuffer(value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (typeof value === "string") return Buffer.from(value);
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return Buffer.from(String(value));
  }
  if (typeof value === "symbol") {
    return Buffer.from(value.description ?? value.toString());
  }
  if (value == null) return Buffer.alloc(0);
  throw new FerricStoreError(`unsupported binary command argument type: ${typeof value}`);
}

function asText(value: unknown): string {
  return toBuffer(value).toString("utf8");
}

function commandNameIs(value: unknown, expected: string): boolean {
  if (typeof value === "string") {
    return value.toUpperCase() === expected;
  }
  return asText(value).toUpperCase() === expected;
}

function u8(value: number): Buffer {
  return Buffer.from([value]);
}

function u32(value: number): Buffer {
  const buffer = Buffer.allocUnsafe(4);
  buffer.writeUInt32BE(value, 0);
  return buffer;
}

function i64(value: bigint): Buffer {
  const buffer = Buffer.allocUnsafe(8);
  buffer.writeBigInt64BE(value, 0);
  return buffer;
}

function independentMode(value: unknown): number {
  if (value === true) return 2;
  if (value === false) return 1;
  return 0;
}

function optionalI64(value: unknown): bigint {
  return typeof value === "number" ? BigInt(value) : MIN_I64;
}

function compactClaimReturnMode(value: unknown): number | undefined {
  const mode = value == null ? "" : asText(value).toUpperCase();
  if (mode === "JOBS_COMPACT") return 1;
  if (mode === "JOBS_COMPACT_STATE" || mode === "JOBS_COMPACT_WITH_STATE") return 2;
  if (mode === "JOBS_COMPACT_ATTRS" || mode === "JOBS_COMPACT_ATTRIBUTES") return 3;
  if (
    mode === "JOBS_COMPACT_STATE_ATTRS" ||
    mode === "JOBS_COMPACT_WITH_STATE_ATTRS" ||
    mode === "JOBS_COMPACT_STATE_ATTRIBUTES" ||
    mode === "JOBS_COMPACT_WITH_STATE_ATTRIBUTES"
  ) {
    return 4;
  }
  return undefined;
}

function requireAvailable(data: Buffer, offset: number, size: number): void {
  if (offset < 0 || size < 0 || data.byteLength - offset < size) {
    throw new FerricStoreError("native protocol value is truncated", { raw: data });
  }
}

function laneForOpcode(opcode: number): number {
  return opcode < 0x0100 ? 0 : 1;
}

function errorMessage(status: number | string, value: unknown): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  if (typeof value === "object" && value != null && "message" in value) {
    const message = (value as Record<string, unknown>).message;
    return typeof message === "string" ? message : String(message);
  }
  return `ERR native request failed status=${status}: ${String(value)}`;
}
