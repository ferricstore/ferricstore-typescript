import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";
import { compactManyRequestTag } from "./protocol-flow-compact-common.js";

export function compactFlowTransitionManyPayload(
  partition: CommandArgument,
  fromState: CommandArgument,
  toState: CommandArgument,
  rawItems: readonly CommandArgument[],
  mixed: boolean,
  options: Record<string, unknown>,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  if (
    "payload" in options ||
    "priority" in options ||
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
  const runAtMs = options.run_at_ms ?? options.now_ms;
  if (typeof runAtMs !== "number" || !Number.isSafeInteger(runAtMs) || runAtMs < 0) {
    return undefined;
  }
  const tag = compactManyRequestTag(
    options.return,
    wire.COMPACT_FLOW_TRANSITION_MANY_REQUEST,
    wire.COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST
  );
  if (tag == null) return undefined;

  const width = mixed ? 4 : 3;
  const count = rawItems.length / width;
  if (count > 0xffff_ffff) return undefined;
  const minimum = 34 + count * 20;
  if (!core.compactPayloadFits(minimum, maxBodyBytes)) {
    for (let rawIndex = 0; rawIndex < rawItems.length; rawIndex += width) {
      const lease = rawItems[rawIndex + (mixed ? 3 : 2)];
      if (
        !core.isCompactBinaryArgument(rawItems[rawIndex]) ||
        (mixed && !core.isCompactBinaryArgument(rawItems[rawIndex + 1])) ||
        !core.isCompactBinaryArgument(lease) ||
        core.i64Arg(rawItems[rawIndex + (mixed ? 2 : 1)]) < 0n
      ) return undefined;
    }
    core.assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const lengths = new Uint32Array(count * 3);
  const fencingValues = new BigInt64Array(count);
  const fromStateLength = core.compactBinaryByteLength(fromState);
  const toStateLength = core.compactBinaryByteLength(toState);
  const globalPartition: CommandArgument | undefined = mixed ? undefined : partition;
  const globalPartitionLength = globalPartition == null ? 0 : core.compactBinaryByteLength(globalPartition);
  let total = 1
    + 4 + fromStateLength
    + 4 + toStateLength
    + 4 + globalPartitionLength
    + 8
    + 8
    + 1
    + 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    const lease = rawItems[rawIndex + (mixed ? 3 : 2)];
    if (
      !core.isCompactBinaryArgument(rawItems[rawIndex]) ||
      (mixed && !core.isCompactBinaryArgument(rawItems[rawIndex + 1])) ||
      !core.isCompactBinaryArgument(lease)
    ) {
      return undefined;
    }
    const idLength = core.compactBinaryByteLength(rawItems[rawIndex]);
    const itemPartition = mixed ? rawItems[rawIndex + 1] : undefined;
    const itemPartitionLength = itemPartition == null ? 0 : core.compactBinaryByteLength(itemPartition);
    const fencing = core.i64Arg(rawItems[rawIndex + (mixed ? 2 : 1)]);
    if (fencing < 0n) return undefined;
    const leaseValue = core.commandTokenIs(lease, "-") ? undefined : lease;
    const leaseLength = leaseValue == null ? 0 : core.compactBinaryByteLength(leaseValue);
    lengths[itemIndex * 3] = idLength;
    lengths[itemIndex * 3 + 1] = itemPartitionLength;
    lengths[itemIndex * 3 + 2] = leaseLength;
    fencingValues[itemIndex] = fencing;
    total += 4 + idLength + 4 + itemPartitionLength + 8 + 4 + leaseLength;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(tag, offset);
  offset += 1;
  offset = core.writeBinaryValue(out, offset, fromState, fromStateLength);
  offset = core.writeBinaryValue(out, offset, toState, toStateLength);
  offset = core.writeOptionalBinaryValue(out, offset, globalPartition, globalPartitionLength);
  out.writeBigInt64BE(BigInt(options.now_ms), offset);
  offset += 8;
  out.writeBigInt64BE(BigInt(runAtMs), offset);
  offset += 8;
  out.writeUInt8(core.independentMode(options.independent), offset);
  offset += 1;
  out.writeUInt32BE(count, offset);
  offset += 4;
  for (let rawIndex = 0, itemIndex = 0; rawIndex < rawItems.length; rawIndex += width, itemIndex += 1) {
    offset = core.writeBinaryValue(out, offset, rawItems[rawIndex], lengths[itemIndex * 3] ?? 0);
    offset = core.writeOptionalBinaryValue(
      out,
      offset,
      mixed ? rawItems[rawIndex + 1] : undefined,
      lengths[itemIndex * 3 + 1] ?? 0
    );
    out.writeBigInt64BE(fencingValues[itemIndex] ?? 0n, offset);
    offset += 8;
    const lease = rawItems[rawIndex + (mixed ? 3 : 2)];
    offset = core.writeOptionalBinaryValue(
      out,
      offset,
      core.commandTokenIs(lease, "-") ? undefined : lease,
      lengths[itemIndex * 3 + 2] ?? 0
    );
  }
  return {
    flags: wire.FLAG_CUSTOM_PAYLOAD,
    opcode: wire.OPCODES.flowTransitionMany,
    payload: out
  };
}
