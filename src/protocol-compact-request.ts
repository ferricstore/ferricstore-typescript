import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import {
  assertCompactPayloadFits,
  commandNameIs,
  commandTokenIs,
  compactBinaryByteLength,
  compactPayloadFits,
  isCompactBinaryScalar,
  writeBinaryValue
} from "./protocol-core.js";
import * as wire from "./protocol-constants.js";

export function compactMsetPayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  if (args.length % 2 !== 0) throw new FerricStoreError("MSET requires key/value pairs");
  const minimum = 6 + args.length * 4;
  if (!compactPayloadFits(minimum, maxBodyBytes)) {
    if (!compactArgumentsEligible(args, "MSET arguments must be dense")) return undefined;
    assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const byteLengths = new Uint32Array(args.length);
  let total = 6;
  for (let index = 0; index < args.length; index += 1) {
    if (!Object.hasOwn(args, index)) throw new TypeError("MSET arguments must be dense");
    const value = args[index];
    if (!isCompactBinaryScalar(value)) return undefined;
    const byteLength = compactBinaryByteLength(value);
    byteLengths[index] = byteLength;
    total += 4 + byteLength;
  }
  assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 1, args.length / 2);
  for (let index = 0; index < args.length; index += 1) {
    offset = writeBinaryValue(out, offset, args[index], byteLengths[index] ?? 0);
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode: wire.OPCODES.mset, payload: out };
}

export function compactKeyPipelinePayload(
  opcode: number,
  args: readonly CommandArgument[],
  mode: number,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  const minimum = 6 + args.length * 4;
  if (!compactPayloadFits(minimum, maxBodyBytes)) {
    if (!compactArgumentsEligible(args, "command arguments must be dense")) return undefined;
    assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const byteLengths = new Uint32Array(args.length);
  let total = 6;
  for (let index = 0; index < args.length; index += 1) {
    if (!Object.hasOwn(args, index)) throw new TypeError("command arguments must be dense");
    const value = args[index];
    if (!isCompactBinaryScalar(value)) return undefined;
    const byteLength = compactBinaryByteLength(value);
    byteLengths[index] = byteLength;
    total += 4 + byteLength;
  }
  assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, mode, args.length);
  for (let index = 0; index < args.length; index += 1) {
    offset = writeBinaryValue(out, offset, args[index], byteLengths[index] ?? 0);
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode, payload: out };
}

export function compactPipelinePayload(
  commands: readonly Command[],
  maxBodyBytes: number,
  allowStreamXAdd = true
): Buffer | undefined {
  if (commands.length === 0) {
    assertCompactPayloadFits(6, maxBodyBytes);
    const out = Buffer.allocUnsafe(6);
    writeCompactPipelineHeader(out, 0x80 | 2, 0);
    return out;
  }
  if (!Object.hasOwn(commands, 0)) {
    throw new TypeError("pipeline commands must be a dense array of command arrays");
  }
  if (commandNameIs(commands[0]?.[0], "SET")) return compactSetPipelinePayload(commands, maxBodyBytes);
  if (commandNameIs(commands[0]?.[0], "GET")) return compactGetPipelinePayload(commands, maxBodyBytes);
  if (commandNameIs(commands[0]?.[0], "XADD")) {
    return allowStreamXAdd ? compactStreamXAddPipelinePayload(commands, maxBodyBytes) : undefined;
  }
  return undefined;
}

function compactStreamXAddPipelinePayload(
  commands: readonly Command[],
  maxBodyBytes: number
): Buffer | undefined {
  let argumentCount = 0;
  let minimum = 6;
  for (const command of commands) {
    if (!Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    if (command.length < 5 || (command.length - 3) % 2 !== 0) return undefined;
    for (let argumentIndex = 0; argumentIndex < command.length; argumentIndex += 1) {
      if (!Object.hasOwn(command, argumentIndex)) {
        throw new TypeError("pipeline command arguments must be dense");
      }
    }
    if (!commandTokenIs(command[0], "XADD") || !commandTokenIs(command[2], "*")) return undefined;
    if ((command.length - 3) / 2 > 0xffff) return undefined;
    for (let argumentIndex = 1; argumentIndex < command.length; argumentIndex += 1) {
      if (argumentIndex !== 2 && !isCompactBinaryScalar(command[argumentIndex])) return undefined;
    }
    argumentCount += command.length - 2;
    minimum += 2 + (command.length - 2) * 4;
  }
  assertCompactPayloadFits(minimum, maxBodyBytes);

  const byteLengths = new Uint32Array(argumentCount);
  let lengthIndex = 0;
  let total = 6;
  for (const command of commands) {
    total += 2;
    for (let argumentIndex = 1; argumentIndex < command.length; argumentIndex += 1) {
      if (argumentIndex === 2) continue;
      const value = command[argumentIndex];
      const byteLength = compactBinaryByteLength(value);
      byteLengths[lengthIndex] = byteLength;
      lengthIndex += 1;
      total += 4 + byteLength;
    }
  }
  assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 0x80 | 34, commands.length);
  lengthIndex = 0;
  for (const command of commands) {
    offset = writeBinaryValue(out, offset, command[1], byteLengths[lengthIndex] ?? 0);
    lengthIndex += 1;
    out.writeUInt16BE((command.length - 3) / 2, offset);
    offset += 2;
    for (let argumentIndex = 3; argumentIndex < command.length; argumentIndex += 1) {
      offset = writeBinaryValue(out, offset, command[argumentIndex], byteLengths[lengthIndex] ?? 0);
      lengthIndex += 1;
    }
  }
  return out;
}

function compactSetPipelinePayload(commands: readonly Command[], maxBodyBytes: number): Buffer | undefined {
  let total = 6;
  const minimum = total + commands.length * 8;
  if (!compactPayloadFits(minimum, maxBodyBytes)) {
    if (!compactPipelineEligible(commands, "SET", 3)) return undefined;
    assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const argumentByteLengths = new Uint32Array(commands.length * 2);
  const argumentValues = new Array<unknown>(commands.length * 2);
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    if (
      command.length !== 3
      || !Object.hasOwn(command, 0)
      || !Object.hasOwn(command, 1)
      || !Object.hasOwn(command, 2)
    ) {
      if (command.length === 3) throw new TypeError("pipeline command arguments must be dense");
      return undefined;
    }
    if (!commandNameIs(command[0], "SET")) return undefined;
    const key: unknown = command[1];
    const value: unknown = command[2];
    if (!isCompactBinaryScalar(key) || !isCompactBinaryScalar(value)) return undefined;
    const keyByteLength = compactBinaryByteLength(key);
    const valueByteLength = compactBinaryByteLength(value);
    argumentValues[index * 2] = key;
    argumentValues[index * 2 + 1] = value;
    argumentByteLengths[index * 2] = keyByteLength;
    argumentByteLengths[index * 2 + 1] = valueByteLength;
    total += 8 + keyByteLength + valueByteLength;
  }
  assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 0x80 | 1, commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    offset = writeBinaryValue(out, offset, argumentValues[index * 2], argumentByteLengths[index * 2] ?? 0);
    offset = writeBinaryValue(out, offset, argumentValues[index * 2 + 1], argumentByteLengths[index * 2 + 1] ?? 0);
  }
  return out;
}

function compactGetPipelinePayload(commands: readonly Command[], maxBodyBytes: number): Buffer | undefined {
  let total = 6;
  const minimum = total + commands.length * 4;
  if (!compactPayloadFits(minimum, maxBodyBytes)) {
    if (!compactPipelineEligible(commands, "GET", 2)) return undefined;
    assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const keyByteLengths = new Uint32Array(commands.length);
  const keys = new Array<unknown>(commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    if (
      command.length !== 2
      || !Object.hasOwn(command, 0)
      || !Object.hasOwn(command, 1)
    ) {
      if (command.length === 2) throw new TypeError("pipeline command arguments must be dense");
      return undefined;
    }
    if (!commandNameIs(command[0], "GET")) return undefined;
    const key: unknown = command[1];
    if (!isCompactBinaryScalar(key)) return undefined;
    const keyByteLength = compactBinaryByteLength(key);
    keys[index] = key;
    keyByteLengths[index] = keyByteLength;
    total += 4 + keyByteLength;
  }
  assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = writeCompactPipelineHeader(out, 0x80 | 2, commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    offset = writeBinaryValue(out, offset, keys[index], keyByteLengths[index] ?? 0);
  }
  return out;
}

function compactArgumentsEligible(args: readonly CommandArgument[], sparseMessage: string): boolean {
  for (let index = 0; index < args.length; index += 1) {
    if (!Object.hasOwn(args, index)) throw new TypeError(sparseMessage);
    if (!isCompactBinaryScalar(args[index])) return false;
  }
  return true;
}

function compactPipelineEligible(
  commands: readonly Command[],
  commandName: "GET" | "SET",
  argumentCount: number
): boolean {
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    if (command.length !== argumentCount) return false;
    for (let argumentIndex = 0; argumentIndex < argumentCount; argumentIndex += 1) {
      if (!Object.hasOwn(command, argumentIndex)) {
        throw new TypeError("pipeline command arguments must be dense");
      }
    }
    if (!commandNameIs(command[0], commandName)) return false;
    for (let argumentIndex = 1; argumentIndex < argumentCount; argumentIndex += 1) {
      if (!isCompactBinaryScalar(command[argumentIndex])) return false;
    }
  }
  return true;
}

function writeCompactPipelineHeader(out: Buffer, mode: number, count: number): number {
  out.writeUInt8(wire.COMPACT_PIPELINE_REQUEST, 0);
  out.writeUInt8(mode, 1);
  out.writeUInt32BE(count, 2);
  return 6;
}
