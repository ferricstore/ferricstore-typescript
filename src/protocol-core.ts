import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { CommandArgument } from "./internal.js";
import { connectionBlockingCommands } from "./command-metadata.js";
import * as wire from "./protocol-constants.js";
import { asText, commandName, commandTokenIs, toBuffer } from "./protocol-text.js";
import { isRequestContextCommand, normalizeRequestContext } from "./request-context.js";
export { asText, commandName, commandNameIs, commandTokenIs, optionalText, toBuffer } from "./protocol-text.js";

/** Encode an arbitrary command through the server's generic command path. */
export function commandExec(args: readonly CommandArgument[]): wire.ProtocolCommand {
  if (args.length === 0) {
    throw new FerricStoreError("command requires at least one argument");
  }
  const command = asText(args[0]).toUpperCase();
  const commandArgs = args.slice(1);
  const payload: Record<string, unknown> = {
    args: commandArgs,
    command
  };
  if (
    isRequestContextCommand(command)
    && commandArgs.length >= 2
    && commandTokenIs(commandArgs[commandArgs.length - 2], "REQUEST_CONTEXT")
  ) {
    payload.args = commandArgs.slice(0, -2);
    payload.request_context = normalizeRequestContext(commandArgs[commandArgs.length - 1]);
  }
  return {
    opcode: wire.OPCODES.commandExec,
    payload,
    ...serverBlockMetadata(args)
  };
}

function serverBlockMetadata(args: readonly CommandArgument[]): { readonly serverBlockMs?: number } {
  const command = asText(args[0]).toUpperCase();
  let serverBlockMs: number | undefined;

  if ((command === "BLPOP" || command === "BRPOP") && args.length >= 3) {
    serverBlockMs = blockDurationMs(args[args.length - 1], 1_000);
  } else if (command === "BRPOPLPUSH" && args.length === 4) {
    serverBlockMs = blockDurationMs(args[3], 1_000);
  } else if (command === "BLMOVE" && args.length >= 6) {
    serverBlockMs = blockDurationMs(args[args.length - 1], 1_000);
  } else if ((command === "BLMPOP" || command === "BZMPOP") && args.length >= 2) {
    serverBlockMs = blockDurationMs(args[1], 1_000);
  } else if ((command === "BZPOPMAX" || command === "BZPOPMIN") && args.length >= 3) {
    serverBlockMs = blockDurationMs(args[args.length - 1], 1_000);
  } else if (command === "XREAD" || command === "XREADGROUP") {
    serverBlockMs = optionBlockDurationMs(args, ["BLOCK"], "STREAMS");
  } else if (command === "WAIT" && args.length === 3) {
    serverBlockMs = blockDurationMs(args[2], 1);
  } else if (command === "WAITAOF" && args.length === 4) {
    serverBlockMs = blockDurationMs(args[3], 1);
  } else if (command === "FLOW.CLAIM_DUE" || command === "FLOW.SCHEDULE.FIRE_DUE") {
    serverBlockMs = optionBlockDurationMs(args, ["BLOCK", "BLOCK_MS"]);
  } else if (command === "FETCH_OR_COMPUTE" && args.length >= 3) {
    // A remote-owner wait cannot outlive the compute lease.
    serverBlockMs = blockDurationMs(args[2], 1);
  }

  return serverBlockMs == null ? {} : { serverBlockMs };
}

/** @internal Return whether a command may keep its response pending on the server. */
export function commandHasServerBlock(args: readonly CommandArgument[]): boolean {
  const normalized = commandName(args[0]) === "COMMAND_EXEC" ? args.slice(1) : args;
  return normalized.length > 0 && serverBlockMetadata(normalized).serverBlockMs != null;
}

/** @internal Commands requiring the connection-level blocking dispatcher. */
export function isConnectionBlockingCommand(args: readonly CommandArgument[]): boolean {
  const normalized = commandName(args[0]) === "COMMAND_EXEC" ? args.slice(1) : args;
  const command = commandName(normalized[0]);
  return command != null
    && connectionBlockingCommands.has(command)
    && commandHasServerBlock(normalized);
}

function optionBlockDurationMs(
  args: readonly CommandArgument[],
  optionNames: readonly string[],
  stopOption?: string
): number | undefined {
  let serverBlockMs: number | undefined;
  for (let index = 1; index + 1 < args.length; index += 1) {
    if (stopOption != null && commandTokenIs(args[index], stopOption)) break;
    if (optionNames.some((name) => commandTokenIs(args[index], name))) {
      const candidate = blockDurationMs(args[index + 1], 1);
      if (candidate != null) serverBlockMs = candidate;
      index += 1;
    }
  }
  return serverBlockMs;
}

export function blockDurationMs(value: CommandArgument, multiplier: number): number | undefined {
  let duration: number;
  try {
    duration = numberArg(value);
  } catch {
    return undefined;
  }
  if (duration < 0) return undefined;
  return Math.min(Number.MAX_SAFE_INTEGER, duration * multiplier);
}

/** @internal Normalize a request-body limit once for every encoder. */
export function normalizeEncodeLimit(value: number): number {
  return Number.isFinite(value)
    ? Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.trunc(value)))
    : Number.MAX_SAFE_INTEGER;
}

/** @internal Build the shared oversized-request error. */
export function requestFrameTooLarge(maxBytes: number): wire.RequestFrameTooLargeError {
  return new wire.RequestFrameTooLargeError(
    `native protocol request exceeds server-advertised ${maxBytes}-byte frame limit`
  );
}

/** @internal Return the wire byte length of a scalar custom payload. */
export function binaryValueByteLength(value: unknown): number {
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value.byteLength;
  if (typeof value === "string") return Buffer.byteLength(value);
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return Buffer.byteLength(String(value));
  }
  if (typeof value === "symbol") return Buffer.byteLength(value.description ?? value.toString());
  if (value == null) return 0;
  return toBuffer(value).byteLength;
}

/** @internal Write a scalar directly into its final frame allocation. */
export function writeCustomPayload(
  output: Buffer,
  offset: number,
  value: unknown,
  knownByteLength?: number
): void {
  if (Buffer.isBuffer(value)) {
    value.copy(output, offset);
    return;
  }
  if (value instanceof Uint8Array) {
    output.set(value, offset);
    return;
  }
  if (value == null) return;
  let text: string;
  if (typeof value === "string") {
    text = value;
  } else if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    text = String(value);
  } else if (typeof value === "symbol") {
    text = value.description ?? value.toString();
  } else {
    throw new FerricStoreError(`unsupported binary command argument type: ${typeof value}`);
  }
  output.write(text, offset, knownByteLength ?? Buffer.byteLength(text), "utf8");
}

export function compactBinaryEncodedLength(value: unknown): number {
  return 4 + compactBinaryByteLength(value);
}

export function compactBinaryByteLength(value: unknown): number {
  const byteLength = binaryValueByteLength(value);
  if (byteLength >= wire.NULL_U32) {
    throw new FerricStoreError("compact binary command argument exceeds the 32-bit length limit");
  }
  return byteLength;
}

export function isCompactBinaryArgument(value: unknown): boolean {
  return typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array;
}

export function isCompactBinaryScalar(value: unknown): boolean {
  return value == null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    typeof value === "symbol" ||
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array;
}

export function compactOptionalBinaryEncodedLength(value: unknown): number {
  return value == null ? 4 : compactBinaryEncodedLength(value);
}

export function assertCompactPayloadFits(byteLength: number, maxBodyBytes: number): void {
  const bodyLimit = normalizeEncodeLimit(maxBodyBytes);
  if (byteLength > bodyLimit) throw requestFrameTooLarge(bodyLimit);
}

/** @internal Check a compact lower bound without allocating its planning tables. */
export function compactPayloadFits(byteLength: number, maxBodyBytes: number): boolean {
  return byteLength <= normalizeEncodeLimit(maxBodyBytes);
}

export function writeBinary(out: Buffer, offset: number, value: Buffer): number {
  out.writeUInt32BE(value.byteLength, offset);
  offset += 4;
  value.copy(out, offset);
  return offset + value.byteLength;
}

export function writeBinaryValue(
  out: Buffer,
  offset: number,
  value: unknown,
  byteLength: number
): number {
  out.writeUInt32BE(byteLength, offset);
  offset += 4;
  writeCustomPayload(out, offset, value, byteLength);
  return offset + byteLength;
}

export function writeOptionalBinary(out: Buffer, offset: number, value: Buffer | null): number {
  if (value == null) {
    out.writeUInt32BE(wire.NULL_U32, offset);
    return offset + 4;
  }
  return writeBinary(out, offset, value);
}

export function writeOptionalBinaryValue(
  out: Buffer,
  offset: number,
  value: unknown,
  byteLength: number
): number {
  if (value == null) {
    out.writeUInt32BE(wire.NULL_U32, offset);
    return offset + 4;
  }
  return writeBinaryValue(out, offset, value, byteLength);
}

export function numberArg(value: CommandArgument): number {
  if (
    typeof value === "bigint" &&
    (value > wire.MAX_SAFE_INTEGER_BIGINT || value < wire.MIN_SAFE_INTEGER_BIGINT)
  ) {
    throw new FerricStoreError("numeric command argument exceeds the JavaScript safe range");
  }
  const number = typeof value === "number"
    ? value
    : typeof value === "bigint"
      ? Number(value)
      : Number(asText(value));
  if (!Number.isFinite(number)) {
    throw new FerricStoreError("numeric command argument must be finite");
  }
  if (Number.isInteger(number) && !Number.isSafeInteger(number)) {
    throw new FerricStoreError("numeric command argument exceeds the JavaScript safe range");
  }
  return number;
}

/** Parse an exact signed-64-bit command integer without narrowing unsafe values. */
export function integerArg(value: CommandArgument): number | bigint {
  if (typeof value === "number") {
    if (!Number.isInteger(value)) {
      throw new FerricStoreError("integer command argument must be an integer");
    }
    if (!Number.isSafeInteger(value)) {
      throw new FerricStoreError("unsafe integer command argument; use bigint for exact encoding");
    }
    return value;
  }
  const source = typeof value === "bigint" ? value.toString() : asText(value);
  if (!/^[+-]?\d+$/u.test(source)) {
    throw new FerricStoreError("integer command argument must be an integer");
  }
  const integer = BigInt(source);
  if (integer < wire.MIN_I64 || integer > wire.MAX_I64) {
    throw new FerricStoreError("integer command argument exceeds the signed 64-bit range");
  }
  return integer >= wire.MIN_SAFE_INTEGER_BIGINT && integer <= wire.MAX_SAFE_INTEGER_BIGINT
    ? Number(integer)
    : integer;
}

/** Parse an exact command integer that is safe for JavaScript indexing and counts. */
export function safeIntegerNumberArg(value: CommandArgument): number {
  const integer = integerArg(value);
  if (typeof integer === "bigint") {
    throw new FerricStoreError("integer command argument exceeds the JavaScript safe range");
  }
  return integer;
}

export function i64Arg(value: CommandArgument): bigint {
  return BigInt(integerArg(value));
}

export function boolArg(value: CommandArgument): boolean {
  if (typeof value === "boolean") return value;
  const text = asText(value).toUpperCase();
  return text === "1" || text === "TRUE" || text === "YES" || text === "ON";
}

export function isBoolToken(value: CommandArgument): boolean {
  if (typeof value === "boolean") return true;
  const text = asText(value).toUpperCase();
  return text === "1" || text === "0" || text === "TRUE" || text === "FALSE" ||
    text === "YES" || text === "NO" || text === "ON" || text === "OFF";
}

export function independentMode(value: unknown): number {
  if (value === true) return 2;
  if (value === false) return 1;
  return 0;
}

export function optionalI64(value: unknown): bigint {
  return typeof value === "number" ? BigInt(value) : wire.MIN_I64;
}

export function compactClaimReturnMode(value: unknown): 1 | 2 | 3 | 4 | undefined {
  const mode = value == null ? "" : asText(value).toUpperCase();
  if (mode === "JOBS_COMPACT") return 1;
  if (mode === "JOBS_COMPACT_STATE" || mode === "JOBS_COMPACT_WITH_STATE") return 2;
  if (mode === "JOBS_COMPACT_ATTRS" || mode === "JOBS_COMPACT_ATTRIBUTES") return 3;
  if (
    mode === "JOBS_COMPACT_STATE_ATTRS" ||
    mode === "JOBS_COMPACT_WITH_STATE_ATTRS" ||
    mode === "JOBS_COMPACT_STATE_ATTRIBUTES" ||
    mode === "JOBS_COMPACT_WITH_STATE_ATTRIBUTES"
  ) return 4;
  return undefined;
}

export function compactClaimResponseMode(value: unknown): wire.CompactClaimMode | undefined {
  switch (compactClaimReturnMode(value)) {
    case 1: return "base";
    case 2: return "state";
    case 3: return "attrs";
    case 4: return "stateAttrs";
    case undefined: return undefined;
  }
}

export function setOwnValue<T>(target: Record<string, T>, key: string, value: T): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true
  });
}
