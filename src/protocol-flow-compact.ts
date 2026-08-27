import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import { denseCommandArgumentTail } from "./protocol-array-validation.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";
import { compactManyRequestTag } from "./protocol-flow-compact-common.js";
import { findItemToken } from "./protocol-flow-items.js";
import { parseFlowOptions } from "./protocol-flow-options.js";
export { compactManyRequestTag } from "./protocol-flow-compact-common.js";
export { compactFlowTransitionManyPayload } from "./protocol-flow-compact-transition.js";

export function compactFlowCreateManyPayload(
  partition: string,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  auto: boolean,
  options: Record<string, unknown>,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  const nowMs = options.now_ms;
  const runAtMs = options.run_at_ms ?? nowMs;
  if (
    "priority" in options ||
    "idempotent" in options ||
    "max_active_ms" in options ||
    "retention_ttl_ms" in options ||
    "attributes" in options ||
    "state_meta" in options ||
    "values" in options ||
    "value_refs" in options ||
    !core.isCompactBinaryScalar(options.type) ||
    !core.isCompactBinaryScalar(options.state) ||
    typeof options.type === "undefined" ||
    typeof options.state === "undefined" ||
    !isSafeIntegerNumber(nowMs) ||
    !isSafeIntegerNumber(runAtMs)
  ) return undefined;

  const width = mixed ? 3 : 2;
  const count = rawItems.length / width;
  if (count > 0xffff_ffff) return undefined;
  let total = 1
    + core.compactBinaryEncodedLength(options.type)
    + core.compactBinaryEncodedLength(options.state)
    + 8 + 8 + 1 + 1 + 4;
  if (!auto && !mixed) total += core.compactOptionalBinaryEncodedLength(partition);
  for (let index = 0; index < rawItems.length; index += width) {
    if (
      !core.isCompactBinaryScalar(rawItems[index]) ||
      (mixed && !core.isCompactBinaryScalar(rawItems[index + 1])) ||
      !core.isCompactBinaryScalar(rawItems[index + width - 1])
    ) return undefined;
    total += core.compactBinaryEncodedLength(rawItems[index]);
    if (mixed) total += core.compactBinaryEncodedLength(rawItems[index + 1]);
    total += core.compactBinaryEncodedLength(rawItems[index + width - 1]);
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const tag = mixed
    ? wire.COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST
    : auto
      ? wire.COMPACT_FLOW_CREATE_MANY_REQUEST
      : wire.COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST;
  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(tag, offset++);
  offset = core.writeBinary(out, offset, core.toBuffer(options.type));
  offset = core.writeBinary(out, offset, core.toBuffer(options.state));
  if (!auto && !mixed) offset = core.writeOptionalBinary(out, offset, core.toBuffer(partition));
  out.writeBigInt64BE(BigInt(nowMs), offset);
  offset += 8;
  out.writeBigInt64BE(BigInt(runAtMs), offset);
  offset += 8;
  out.writeUInt8(core.independentMode(options.independent), offset++);
  out.writeUInt8(1, offset++);
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (let index = 0; index < rawItems.length; index += width) {
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[index]));
    if (mixed) offset = core.writeBinary(out, offset, core.toBuffer(rawItems[index + 1]));
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[index + width - 1]));
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode: wire.OPCODES.flowCreateMany, payload: out };
}

export function compactFlowClaimDuePayload(
  type: CommandArgument,
  options: Record<string, unknown>,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  const leaseMs = options.lease_ms;
  const limit = options.limit;
  const blockMs = options.block_ms;
  const reclaimRatio = options.reclaim_ratio ?? 25;
  const priority = options.priority;
  if (
    "states" in options || "now_ms" in options || "payload_max_bytes" in options ||
    "value_max_bytes" in options || "values" in options || "include_state" in options ||
    options.payload === true || typeof options.worker === "undefined" ||
    !isSafeIntegerNumber(leaseMs) || !isSafeIntegerNumber(limit) ||
    !isOptionalSafeIntegerNumber(blockMs) || !isSafeIntegerNumber(reclaimRatio) ||
    !isOptionalSafeIntegerNumber(priority)
  ) return undefined;

  const returnMode = core.compactClaimReturnMode(options.return);
  if (returnMode == null) return undefined;
  const partitionKeys = Array.isArray(options.partition_keys) ? options.partition_keys : undefined;
  if (partitionKeys != null && partitionKeys.length > 0xffff_ffff) return undefined;
  let total = 1
    + core.compactBinaryEncodedLength(type)
    + core.compactOptionalBinaryEncodedLength(options.state)
    + core.compactBinaryEncodedLength(options.worker)
    + 8 + 8 + 8 + 1 + 8 + 8 + 1;
  if (partitionKeys != null) {
    total += 5;
    for (let index = 0; index < partitionKeys.length; index += 1) {
      if (!Object.hasOwn(partitionKeys, index)) throw new TypeError("PARTITIONS values must be dense");
      total += core.compactBinaryEncodedLength(partitionKeys[index]);
    }
  } else if (options.partition_key != null) {
    total += 1 + core.compactBinaryEncodedLength(options.partition_key);
  } else {
    total += 1;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(wire.COMPACT_FLOW_CLAIM_DUE_REQUEST, offset++);
  offset = core.writeBinary(out, offset, core.toBuffer(type));
  offset = core.writeOptionalBinary(out, offset, options.state == null ? null : core.toBuffer(options.state));
  offset = core.writeBinary(out, offset, core.toBuffer(options.worker));
  out.writeBigInt64BE(BigInt(leaseMs), offset);
  offset += 8;
  out.writeBigInt64BE(BigInt(limit), offset);
  offset += 8;
  out.writeBigInt64BE(core.optionalI64(blockMs), offset);
  offset += 8;
  out.writeUInt8(options.reclaim_expired === false ? 0 : 1, offset++);
  out.writeBigInt64BE(BigInt(reclaimRatio), offset);
  offset += 8;
  out.writeBigInt64BE(core.optionalI64(priority), offset);
  offset += 8;
  out.writeUInt8(returnMode, offset++);
  if (partitionKeys != null) {
    out.writeUInt8(2, offset++);
    out.writeUInt32BE(partitionKeys.length, offset);
    offset += 4;
    for (const key of partitionKeys) offset = core.writeBinary(out, offset, core.toBuffer(key));
  } else if (options.partition_key != null) {
    out.writeUInt8(1, offset++);
    core.writeBinary(out, offset, core.toBuffer(options.partition_key));
  } else {
    out.writeUInt8(0, offset);
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode: wire.OPCODES.flowClaimDue, payload: out };
}

export function compactFlowCompleteManyPayload(
  opcode: number,
  partition: string,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  auto: boolean,
  options: Record<string, unknown>,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  const nowMs = options.now_ms;
  if (
    "error" in options || "result" in options || "payload" in options || "ttl_ms" in options ||
    "state_meta" in options || "values" in options || "value_refs" in options ||
    "drop_values" in options || "override_values" in options || "attributes_merge" in options ||
    "attributes_delete" in options || !isSafeIntegerNumber(nowMs)
  ) return undefined;

  const returnMode = options.return == null ? "" : core.asText(options.return).toUpperCase();
  if (returnMode !== "" && returnMode !== "OK_ON_SUCCESS") return undefined;
  const width = mixed ? 4 : 3;
  const count = rawItems.length / width;
  if (count > 0xffff_ffff) return undefined;
  const minimum = 18 + count * 20;
  if (!core.compactPayloadFits(minimum, maxBodyBytes)) {
    for (let rawIndex = 0; rawIndex < rawItems.length; rawIndex += width) {
      if (
        !core.isCompactBinaryScalar(rawItems[rawIndex]) ||
        (mixed && !core.isCompactBinaryScalar(rawItems[rawIndex + 1])) ||
        !core.isCompactBinaryScalar(rawItems[rawIndex + width - 2])
      ) return undefined;
      core.i64Arg(rawItems[rawIndex + width - 1]);
    }
    core.assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const fencingValues = new Array<bigint>(count);
  let total = 1
    + core.compactOptionalBinaryEncodedLength(!auto && !mixed ? partition : null)
    + 8 + 1 + 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    fencingValues[itemIndex] = core.i64Arg(rawItems[rawIndex + width - 1]);
    total += core.compactBinaryEncodedLength(rawItems[rawIndex])
      + core.compactOptionalBinaryEncodedLength(mixed ? rawItems[rawIndex + 1] : null)
      + core.compactBinaryEncodedLength(rawItems[rawIndex + width - 2]) + 8;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(
    returnMode === "OK_ON_SUCCESS"
      ? wire.COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST
      : wire.COMPACT_FLOW_COMPLETE_MANY_REQUEST,
    offset++
  );
  offset = core.writeOptionalBinary(out, offset, !auto && !mixed ? core.toBuffer(partition) : null);
  out.writeBigInt64BE(BigInt(nowMs), offset);
  offset += 8;
  out.writeUInt8(core.independentMode(options.independent), offset++);
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[rawIndex]));
    offset = core.writeOptionalBinary(out, offset, mixed ? core.toBuffer(rawItems[rawIndex + 1]) : null);
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[rawIndex + width - 2]));
    const fencing = fencingValues[itemIndex];
    if (fencing == null) return undefined;
    out.writeBigInt64BE(fencing, offset);
    offset += 8;
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode, payload: out };
}

export function compactFlowRetryManyPayload(
  partition: string,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  auto: boolean,
  options: Record<string, unknown>,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  const nowMs = options.now_ms;
  const runAtMs = options.run_at_ms;
  if (
    "error" in options || "payload" in options || "state_meta" in options || "values" in options ||
    "value_refs" in options || "drop_values" in options || "override_values" in options ||
    "attributes_merge" in options || "attributes_delete" in options ||
    !isSafeIntegerNumber(nowMs) || !isSafeIntegerNumber(runAtMs)
  ) return undefined;

  const returnMode = options.return == null ? "" : core.asText(options.return).toUpperCase();
  if (returnMode !== "" && returnMode !== "OK_ON_SUCCESS") return undefined;
  const width = mixed ? 4 : 3;
  const count = rawItems.length / width;
  if (count > 0xffff_ffff) return undefined;
  const minimum = 26 + count * 20;
  if (!core.compactPayloadFits(minimum, maxBodyBytes)) {
    for (let rawIndex = 0; rawIndex < rawItems.length; rawIndex += width) {
      if (
        !core.isCompactBinaryScalar(rawItems[rawIndex]) ||
        (mixed && !core.isCompactBinaryScalar(rawItems[rawIndex + 1])) ||
        !core.isCompactBinaryScalar(rawItems[rawIndex + width - 2])
      ) return undefined;
      core.i64Arg(rawItems[rawIndex + width - 1]);
    }
    core.assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const fencingValues = new Array<bigint>(count);
  let total = 1
    + core.compactOptionalBinaryEncodedLength(!auto && !mixed ? partition : null)
    + 8 + 8 + 1 + 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    fencingValues[itemIndex] = core.i64Arg(rawItems[rawIndex + width - 1]);
    total += core.compactBinaryEncodedLength(rawItems[rawIndex])
      + core.compactOptionalBinaryEncodedLength(mixed ? rawItems[rawIndex + 1] : null)
      + core.compactBinaryEncodedLength(rawItems[rawIndex + width - 2]) + 8;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(
    returnMode === "OK_ON_SUCCESS"
      ? wire.COMPACT_FLOW_RETRY_MANY_OK_REQUEST
      : wire.COMPACT_FLOW_RETRY_MANY_REQUEST,
    offset++
  );
  offset = core.writeOptionalBinary(out, offset, !auto && !mixed ? core.toBuffer(partition) : null);
  out.writeBigInt64BE(BigInt(nowMs), offset);
  offset += 8;
  out.writeBigInt64BE(BigInt(runAtMs), offset);
  offset += 8;
  out.writeUInt8(core.independentMode(options.independent), offset++);
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[rawIndex]));
    offset = core.writeOptionalBinary(out, offset, mixed ? core.toBuffer(rawItems[rawIndex + 1]) : null);
    offset = core.writeBinary(out, offset, core.toBuffer(rawItems[rawIndex + width - 2]));
    const fencing = fencingValues[itemIndex];
    if (fencing == null) return undefined;
    out.writeBigInt64BE(fencing, offset);
    offset += 8;
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode: wire.OPCODES.flowRetryMany, payload: out };
}

 export function compactFlowCancelManyPayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  if (args.length < 3 || !core.isCompactBinaryArgument(args[0])) return undefined;
  const partition = core.asText(args[0]);
  const itemsIndex = findItemToken(args, 1);
  if (itemsIndex < 0 || !core.commandTokenIs(args[itemsIndex], "ITEMS")) return undefined;
  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed: new Set([
      "REASON", "TTL", "NOW", "INDEPENDENT", "STATE_META", "VALUE", "VALUE_REF",
      "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE", "RETURN"
    ])
  });
  if (
    options == null ||
    "reason" in options ||
    "ttl_ms" in options ||
    "state_meta" in options ||
    "values" in options ||
    "value_refs" in options ||
    "drop_values" in options ||
    "override_values" in options ||
    "attributes_merge" in options ||
    "attributes_delete" in options ||
    typeof options.now_ms !== "number" ||
    !Number.isSafeInteger(options.now_ms) ||
    options.now_ms < 0
  ) {
    return undefined;
  }
  const tag = compactManyRequestTag(
    options.return,
    wire.COMPACT_FLOW_CANCEL_MANY_REQUEST,
    wire.COMPACT_FLOW_CANCEL_MANY_OK_REQUEST
  );
  if (tag == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const rawItems = denseCommandArgumentTail(args, itemsIndex + 1, "ITEMS");
  const width = mixed ? 3 : 2;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;
  const count = rawItems.length / width;
  if (count > 0xffff_ffff) return undefined;

  const minimum = 18 + count * 16;
  if (!core.compactPayloadFits(minimum, maxBodyBytes)) {
    for (let rawIndex = 0; rawIndex < rawItems.length; rawIndex += width) {
      if (
        !core.isCompactBinaryArgument(rawItems[rawIndex]) ||
        (mixed && !core.isCompactBinaryArgument(rawItems[rawIndex + 1])) ||
        core.i64Arg(rawItems[rawIndex + width - 1]) < 0n
      ) return undefined;
    }
    core.assertCompactPayloadFits(minimum, maxBodyBytes);
  }

  const lengths = new Uint32Array(count * 2);
  const fencingValues = new BigInt64Array(count);
  const globalPartition = mixed ? undefined : args[0];
  const globalPartitionLength = globalPartition == null ? 0 : core.compactBinaryByteLength(globalPartition);
  let total = 1 + 4 + globalPartitionLength + 8 + 1 + 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    if (
      !core.isCompactBinaryArgument(rawItems[rawIndex]) ||
      (mixed && !core.isCompactBinaryArgument(rawItems[rawIndex + 1]))
    ) {
      return undefined;
    }
    const idLength = core.compactBinaryByteLength(rawItems[rawIndex]);
    const itemPartition = mixed ? rawItems[rawIndex + 1] : undefined;
    const itemPartitionLength = itemPartition == null ? 0 : core.compactBinaryByteLength(itemPartition);
    const fencing = core.i64Arg(rawItems[rawIndex + width - 1]);
    if (fencing < 0n) return undefined;
    lengths[itemIndex * 2] = idLength;
    lengths[itemIndex * 2 + 1] = itemPartitionLength;
    fencingValues[itemIndex] = fencing;
    total += 4 + idLength + 4 + itemPartitionLength + 8;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(tag, offset);
  offset += 1;
  offset = core.writeOptionalBinaryValue(out, offset, globalPartition, globalPartitionLength);
  out.writeBigInt64BE(BigInt(options.now_ms), offset);
  offset += 8;
  out.writeUInt8(core.independentMode(options.independent), offset);
  offset += 1;
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    offset = core.writeBinaryValue(out, offset, rawItems[rawIndex], lengths[itemIndex * 2] ?? 0);
    offset = core.writeOptionalBinaryValue(
      out,
      offset,
      mixed ? rawItems[rawIndex + 1] : undefined,
      lengths[itemIndex * 2 + 1] ?? 0
    );
    out.writeBigInt64BE(fencingValues[itemIndex] ?? 0n, offset);
    offset += 8;
  }
  return {
    compactResponseItems: count,
    flags: wire.FLAG_CUSTOM_PAYLOAD,
    opcode: wire.COMMAND_OPCODES["FLOW.CANCEL_MANY"],
    payload: out
  };
}

function isSafeIntegerNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

function isOptionalSafeIntegerNumber(value: unknown): value is number | null | undefined {
  return value == null || isSafeIntegerNumber(value);
}
