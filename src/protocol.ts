import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import { flowQueryPayload } from "./flow-query-request.js";

import * as flow from "./protocol-flow.js";
import { flowSignalPayload } from "./protocol-flow-signal.js";
import { flowPolicyGetPayload, flowPolicySetPayload } from "./protocol-flow-policy.js";
import {
  assertNormalizedCommandDoesNotRequirePinnedConnection,
  commandMutatesConnectionState
} from "./protocol-connection-policy.js";
export * from "./protocol-connection-policy.js";
export * from "./protocol-core.js";
export * from "./protocol-constants.js";
import {
  asText,
  binaryValueByteLength,
  commandExec,
  isConnectionBlockingCommand,
  normalizeEncodeLimit,
  requestFrameTooLarge,
  writeCustomPayload
} from "./protocol-core.js";
import {
  compactKeyPipelinePayload,
  compactMsetPayload,
  compactPipelinePayload
} from "./protocol-compact-request.js";
import * as wire from "./protocol-constants.js";
import {
  collectionProtocolCommand,
  isBinaryCommandArgument,
  stringSetPayload,
  typedKeyValuePairs
} from "./protocol-kv.js";
import {
  normalizeDecodeLimit,
  planValueWithLimit,
  writeValuePlan
} from "./protocol-value.js";
import { assertKeyValueCommandSharesSlot } from "./key-slot-validation.js";
export { decodeValue, encodeValue } from "./protocol-value.js";
export { decodeResponse, unwrapPipelineResponse } from "./protocol-response.js";

export function encodeRequest(
  command: wire.ProtocolCommand,
  requestId: bigint,
  maxBodyBytes = Number.MAX_SAFE_INTEGER
): Buffer {
  const bodyLimit = normalizeEncodeLimit(maxBodyBytes);
  const customPayload = ((command.flags ?? 0) & wire.FLAG_CUSTOM_PAYLOAD) !== 0;
  const plan = customPayload ? undefined : planValueWithLimit(command.payload, bodyLimit);
  const bodyLength = customPayload
    ? binaryValueByteLength(command.payload)
    : plan?.byteLength ?? 0;
  if (bodyLength > bodyLimit) throw requestFrameTooLarge(bodyLimit);
  const flags = command.flags ?? 0;
  const frame = Buffer.allocUnsafe(wire.HEADER_SIZE + bodyLength);
  frame.write(wire.MAGIC, 0, "ascii");
  frame.writeUInt8(wire.REQUEST_VERSION, 4);
  frame.writeUInt8(flags, 5);
  frame.writeUInt32BE(command.laneId ?? laneForOpcode(command.opcode), 6);
  frame.writeUInt16BE(command.opcode, 10);
  frame.writeBigUInt64BE(requestId, 12);
  frame.writeUInt32BE(bodyLength, 20);
  if (customPayload) {
    writeCustomPayload(frame, wire.HEADER_SIZE, command.payload, bodyLength);
  } else if (plan != null) {
    writeValuePlan(frame, wire.HEADER_SIZE, plan);
  }
  return frame;
}

export function tryDecodeFrame(
  buffer: Buffer,
  maxBodyBytes = wire.DEFAULT_MAX_FRAME_BYTES
): { readonly frame: wire.ResponseFrame; readonly rest: Buffer } | null {
  if (buffer.byteLength < wire.HEADER_SIZE) {
    return null;
  }
  if (buffer.toString("ascii", 0, 4) !== wire.MAGIC) {
    throw new FerricStoreError("invalid FerricStore protocol magic", { raw: buffer.subarray(0, 4) });
  }
  const version = buffer.readUInt8(4);
  if (version !== wire.RESPONSE_VERSION) {
    throw new FerricStoreError(`invalid FerricStore protocol response version ${version}`);
  }
  const bodyLength = buffer.readUInt32BE(20);
  const bodyLimit = normalizeDecodeLimit(maxBodyBytes, wire.DEFAULT_MAX_FRAME_BYTES);
  if (bodyLength > bodyLimit) {
    throw new FerricStoreError(`native protocol frame exceeded ${bodyLimit} bytes`);
  }
  const frameLength = wire.HEADER_SIZE + bodyLength;
  if (buffer.byteLength < frameLength) {
    return null;
  }
  const frame: wire.ResponseFrame = {
    body: buffer.subarray(wire.HEADER_SIZE, frameLength),
    bodyLength,
    flags: buffer.readUInt8(5),
    laneId: buffer.readUInt32BE(6),
    opcode: buffer.readUInt16BE(10),
    requestId: buffer.readBigUInt64BE(12)
  };
  return { frame, rest: buffer.subarray(frameLength) };
}

export function buildProtocolCommand(
  args: readonly CommandArgument[],
  maxBodyBytes = Number.MAX_SAFE_INTEGER,
  allowCustomPayload = true
): wire.ProtocolCommand {
  if (args.length === 0) {
    throw new FerricStoreError("command requires at least one argument");
  }
  const command = asText(args[0]).toUpperCase();
  assertNormalizedCommandDoesNotRequirePinnedConnection(args, command);
  if (command === "MSET" || command === "MSETNX") {
    assertKeyValueCommandSharesSlot(args, command);
  }
  const commandArgs = args.slice(1);

  if (command === "COMMAND_EXEC" && commandArgs.length >= 1) {
    return commandExec(commandArgs);
  }
  if (command === "HELLO" || command === "STARTUP" || command === "WINDOW_UPDATE") {
    const payload = flow.optionMap(commandArgs);
    return payload == null
      ? commandExec(args)
      : { laneId: 0, opcode: wire.COMMAND_OPCODES[command], payload };
  }
  if (command === "AUTH" && (commandArgs.length === 1 || commandArgs.length === 2)) {
    const payload =
      commandArgs.length === 1
        ? { password: commandArgs[0], username: "default" }
        : { password: commandArgs[1], username: commandArgs[0] };
    return { laneId: 0, opcode: wire.OPCODES.auth, payload };
  }
  if (command === "PING" && commandArgs.length <= 1) {
    return { laneId: 0, opcode: wire.OPCODES.ping, payload: commandArgs.length === 0 ? {} : { message: commandArgs[0] } };
  }
  if (command === "OPTIONS" && commandArgs.length === 0) {
    return { laneId: 0, opcode: wire.OPCODES.options, payload: {} };
  }
  if (command === "BACKPRESSURE" && commandArgs.length === 0) {
    return { laneId: 0, opcode: wire.OPCODES.backpressure, payload: {} };
  }
  if (command === "QUIT" && commandArgs.length === 0) {
    return { laneId: 0, opcode: wire.OPCODES.quit, payload: {} };
  }
  if (command === "ROUTE" && commandArgs.length === 1) {
    return { laneId: 0, opcode: wire.OPCODES.route, payload: { key: commandArgs[0] } };
  }
  if (command === "ROUTE_BATCH") {
    return { laneId: 0, opcode: wire.OPCODES.routeBatch, payload: { keys: commandArgs } };
  }
  if (command === "SHARDS" && commandArgs.length === 0) {
    return { laneId: 0, opcode: wire.OPCODES.shards, payload: {} };
  }
  if (command === "CLIENT" && asText(commandArgs[0]).toUpperCase() === "SETNAME" && commandArgs.length === 2) {
    return { laneId: 0, opcode: wire.OPCODES.clientSetName, payload: { name: commandArgs[1] } };
  }
  if (command === "CLIENT.SETNAME" && commandArgs.length === 1) {
    return { laneId: 0, opcode: wire.OPCODES.clientSetName, payload: { name: commandArgs[0] } };
  }
  if (
    (command === "CLIENT" && asText(commandArgs[0]).toUpperCase() === "INFO" && commandArgs.length === 1) ||
    (command === "CLIENT.INFO" && commandArgs.length === 0)
  ) {
    return { laneId: 0, opcode: wire.OPCODES.clientInfo, payload: {} };
  }
  if (command === "GET" && commandArgs.length === 1) {
    return isBinaryCommandArgument(commandArgs[0])
      ? { opcode: wire.OPCODES.get, payload: { key: commandArgs[0] } }
      : commandExec(args);
  }
  if (command === "SET" && commandArgs.length >= 2) {
    const payload = stringSetPayload(commandArgs);
    return payload == null
      ? commandExec(args)
      : { compactResponseItems: 1, opcode: wire.OPCODES.set, payload };
  }
  if (command === "MGET" && commandArgs.length > 0) {
    const compact = allowCustomPayload
      ? compactKeyPipelinePayload(wire.OPCODES.mget, commandArgs, 2, maxBodyBytes)
      : undefined;
    if (compact != null) return { ...compact, compactResponseItems: commandArgs.length };
    return commandArgs.every(isBinaryCommandArgument)
      ? { compactResponseItems: commandArgs.length, opcode: wire.OPCODES.mget, payload: { keys: commandArgs } }
      : commandExec(args);
  }
  if (command === "MSET" && commandArgs.length >= 2 && commandArgs.length % 2 === 0) {
    const compact = allowCustomPayload ? compactMsetPayload(commandArgs, maxBodyBytes) : undefined;
    if (compact != null) return { ...compact, compactResponseItems: 1 };
    const pairs = typedKeyValuePairs(commandArgs);
    return pairs == null
      ? commandExec(args)
      : { compactResponseItems: 1, opcode: wire.OPCODES.mset, payload: { pairs } };
  }
  if (command === "DEL" && commandArgs.length > 0) {
    return commandArgs.every(isBinaryCommandArgument)
      ? { opcode: wire.OPCODES.del, payload: { keys: commandArgs } }
      : commandExec(args);
  }
  const collectionCommand = collectionProtocolCommand(command, commandArgs);
  if (collectionCommand != null) return collectionCommand;
  if (command === "FLOW.HISTORY") {
    return flow.flowHistoryPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.QUERY") {
    return { opcode: wire.OPCODES.flowQuery, payload: flowQueryPayload(commandArgs) };
  }
  if (command === "FLOW.POLICY.SET") {
    return flowPolicySetPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.POLICY.GET") {
    return flowPolicyGetPayload(commandArgs) ?? commandExec(args);
  }
  const flowAdmin = flow.FLOW_ADMIN_COMMANDS[command];
  if (flowAdmin != null) {
    return flow.flowAdminPayload(flowAdmin.opcode, commandArgs, flowAdmin.leadingFields) ?? commandExec(args);
  }
  if (command === "FLOW.CREATE") {
    return flow.flowCreatePayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.GET") {
    return flow.flowGetPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.VALUE.PUT") {
    return flow.flowValuePutPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.VALUE.MGET") {
    return flow.flowValueMGetPayload(commandArgs, maxBodyBytes, allowCustomPayload) ?? commandExec(args);
  }
  if (command === "FLOW.CREATE_MANY") {
    return flow.flowCreateManyPayload(commandArgs, maxBodyBytes, allowCustomPayload) ?? commandExec(args);
  }
  if (command === "FLOW.CLAIM_DUE") {
    return flow.flowClaimDuePayload(commandArgs, maxBodyBytes, allowCustomPayload) ?? commandExec(args);
  }
  if (command === "FLOW.RECLAIM") {
    return flow.flowReclaimPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.COMPLETE") {
    return flow.flowCompletePayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.TRANSITION") {
    return flow.flowTransitionPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.RETRY") {
    return flow.flowLeaseMutationPayload(wire.OPCODES.flowRetry, commandArgs, new Set([
      "FENCING", "NOW", "PARTITION", "ERROR", "PAYLOAD", "RUN_AT", "STATE_META",
      "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
    ])) ?? commandExec(args);
  }
  if (command === "FLOW.FAIL") {
    return flow.flowLeaseMutationPayload(wire.OPCODES.flowFail, commandArgs, new Set([
      "FENCING", "NOW", "PARTITION", "ERROR", "PAYLOAD", "TTL", "STATE_META",
      "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
    ])) ?? commandExec(args);
  }
  if (command === "FLOW.SIGNAL") {
    return flowSignalPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.CANCEL") {
    return flow.flowCommandExecWithRouting(command, commandArgs, 1, new Set([
      "FENCING", "NOW", "LEASE_TOKEN", "PARTITION", "REASON", "TTL", "STATE_META",
      "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
    ]));
  }
  if (command === "FLOW.EXTEND_LEASE") {
    return flow.flowCommandExecWithRouting(command, commandArgs, 2, new Set([
      "FENCING", "LEASE_MS", "NOW", "PARTITION", "RETURN"
    ]));
  }
  if (command === "FLOW.REWIND") {
    return flow.flowCommandExecWithRouting(command, commandArgs, 1, new Set([
      "NOW", "PARTITION", "TO_EVENT", "EXPECT_STATE"
    ]));
  }
  if (command === "FLOW.COMPLETE_MANY") {
    return flow.flowClaimedManyPayload(
      command,
      wire.OPCODES.flowCompleteMany,
      commandArgs,
      maxBodyBytes,
      allowCustomPayload
    ) ?? commandExec(args);
  }
  if (command === "FLOW.RETRY_MANY") {
    return flow.flowClaimedManyPayload(
      command,
      wire.OPCODES.flowRetryMany,
      commandArgs,
      maxBodyBytes,
      allowCustomPayload
    ) ?? commandExec(args);
  }
  if (command === "FLOW.FAIL_MANY") {
    return flow.flowClaimedManyPayload(
      command,
      wire.OPCODES.flowFailMany,
      commandArgs,
      maxBodyBytes,
      allowCustomPayload
    ) ?? commandExec(args);
  }
  if (command === "FLOW.CANCEL_MANY") {
    return (allowCustomPayload
      ? flow.compactFlowCancelManyPayload(commandArgs, maxBodyBytes)
      : undefined) ?? commandExec(args);
  }
  if (command === "FLOW.TRANSITION_MANY") {
    return flow.flowTransitionManyPayload(commandArgs, maxBodyBytes, allowCustomPayload) ?? commandExec(args);
  }
  if (command === "FLOW.SPAWN_CHILDREN") {
    return flow.flowSpawnChildrenPayload(commandArgs) ?? commandExec(args);
  }
  if (command === "FLOW.START_AND_CLAIM") {
    return flow.flowAdminPayload(wire.OPCODES.flowStartAndClaim, commandArgs, ["id"]) ?? commandExec(args);
  }
  if (command === "FLOW.STEP_CONTINUE") {
    return flow.flowAdminPayload(
      wire.OPCODES.flowStepContinue,
      commandArgs,
      ["id", "lease_token", "from_state", "to_state"]
    ) ?? commandExec(args);
  }
  if (command === "FLOW.RUN_STEPS_MANY") {
    return flow.flowAdminPayload(wire.OPCODES.flowRunStepsMany, commandArgs) ?? commandExec(args);
  }
  return commandExec(args);
}

 export function pipelineCommand(
  commands: readonly Command[],
  maxBodyBytes = Number.MAX_SAFE_INTEGER
): wire.ProtocolCommand {
  const command = tryPipelineCommand(commands, maxBodyBytes);
  if (command == null) {
    throw new FerricStoreError("native pipeline could not be encoded");
  }
  return command;
}

export function tryPipelineCommand(
  commands: readonly Command[],
  maxBodyBytes = Number.MAX_SAFE_INTEGER
): wire.ProtocolCommand | undefined {
  const compact = compactPipelinePayload(commands, maxBodyBytes);
  if (compact != null) {
    return {
      compactResponseItems: commands.length,
      flags: wire.FLAG_CUSTOM_PAYLOAD,
      opcode: wire.OPCODES.pipeline,
      payload: compact
    };
  }

  const protocols = new Array<wire.ProtocolCommand>(commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    protocols[index] = buildProtocolCommand(command, maxBodyBytes, false);
  }
  let hasControlCommand = false;
  let hasConnectionSensitiveCommand = false;
  let pipelineBlockMs: number | undefined;
  const pipelineProtocols = protocols.map((protocol, index) => {
    if (protocol.opcode < wire.OPCODES.commandExec) {
      hasControlCommand = true;
    }
    const original = commands[index];
    const pipelineProtocol = (protocol.flags ?? 0) === 0 || original == null
      ? protocol
      : commandExec(original);
    if (original != null && (
      commandMutatesConnectionState(original) || isConnectionBlockingCommand(original)
    )) {
      hasConnectionSensitiveCommand = true;
    }
    const commandBlockMs = pipelineProtocol.serverBlockMs;
    if (commandBlockMs === 0) {
      pipelineBlockMs = 0;
    } else if (commandBlockMs != null && pipelineBlockMs !== 0) {
      pipelineBlockMs = Math.min(
        Number.MAX_SAFE_INTEGER,
        (pipelineBlockMs ?? 0) + commandBlockMs
      );
    }
    return pipelineProtocol;
  });
  if (hasControlCommand || hasConnectionSensitiveCommand) {
    return undefined;
  }

  const pipelineClaimModes = pipelineProtocols.map((protocol) => protocol.compactClaimMode);
  const hasPipelineClaimMode = pipelineClaimModes.some((mode) => mode != null);

  return {
    compactResponseItems: commands.length,
    opcode: wire.OPCODES.pipeline,
    payload: {
      atomicity: "none",
      commands: pipelineProtocols.map((protocol, index) => {
        return {
          body: protocol.payload,
          lane_id: protocol.laneId ?? laneForOpcode(protocol.opcode),
          opcode: protocol.opcode,
          request_id: index + 1
        };
      }),
      return: "compact"
    },
    ...(hasPipelineClaimMode ? { pipelineClaimModes } : {}),
    ...(pipelineBlockMs == null ? {} : { serverBlockMs: pipelineBlockMs })
  };
}

function laneForOpcode(opcode: number): number {
  return opcode < 0x0100 ? 0 : 1;
}
