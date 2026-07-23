import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";

export function compactFlowValueMGetPayload(
  refs: readonly CommandArgument[],
  maxBytes: unknown,
  maxBodyBytes: number
): wire.ProtocolCommand | undefined {
  if (
    refs.length > 0xffff_ffff ||
    (maxBytes != null && (
      typeof maxBytes !== "number" ||
      !Number.isSafeInteger(maxBytes) ||
      maxBytes < 0
    ))
  ) return undefined;
  const minimum = 13 + refs.length * 4;
  if (!core.compactPayloadFits(minimum, maxBodyBytes)) {
    for (const ref of refs) if (!core.isCompactBinaryArgument(ref)) return undefined;
    core.assertCompactPayloadFits(minimum, maxBodyBytes);
  }
  const lengths = new Uint32Array(refs.length);
  let total = 1 + 8 + 4;
  for (let index = 0; index < refs.length; index += 1) {
    if (!core.isCompactBinaryArgument(refs[index])) return undefined;
    const byteLength = core.compactBinaryByteLength(refs[index]);
    lengths[index] = byteLength;
    total += 4 + byteLength;
  }
  core.assertCompactPayloadFits(total, maxBodyBytes);

  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  out.writeUInt8(wire.COMPACT_FLOW_VALUE_MGET_REQUEST, offset);
  offset += 1;
  out.writeBigInt64BE(core.optionalI64(maxBytes), offset);
  offset += 8;
  out.writeUInt32BE(refs.length, offset);
  offset += 4;
  for (let index = 0; index < refs.length; index += 1) {
    offset = core.writeBinaryValue(out, offset, refs[index], lengths[index] ?? 0);
  }
  return { flags: wire.FLAG_CUSTOM_PAYLOAD, opcode: wire.OPCODES.flowValueMGet, payload: out };
}
