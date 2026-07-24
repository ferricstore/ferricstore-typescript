import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { detachDecodedBinary } from "./protocol-binary-detacher.js";
import * as wire from "./protocol-constants.js";
import { setOwnValue } from "./protocol-core.js";
import {
  consumeDecodeItems,
  decodeValueWithBudget,
  requireAvailable
} from "./protocol-value.js";

const CONTRACT = "ferric.flow.query.result/v1";
const MAX_RECORDS = 100;
const MAX_CURSOR_BYTES = 4_096;
export const RECORD_FIELDS = [
  "id",
  "type",
  "state",
  "version",
  "priority",
  "partition_key",
  "created_at_ms",
  "updated_at_ms",
  "next_run_at_ms",
  "lease_deadline_ms",
  "attempts",
  "run_state",
  "max_active_ms",
  "parent_flow_id",
  "root_flow_id",
  "correlation_id",
  "attributes",
  "state_meta",
  "event_id",
  "fields"
] as const;
const RECORD_FIELD_MASK = (1 << RECORD_FIELDS.length) - 1;
const QUALITY_VALUES = [
  ["authoritative", "projected_exact", "exact", "not_applicable"],
  ["current", "projection_watermark", "not_applicable"],
  ["complete", "unavailable"],
  ["none", "complete", "authenticated_seek", "live_seek"]
] as const;
export const QUALITY_FIELDS = ["exactness", "freshness", "coverage", "pagination"] as const;
export const USAGE_FIELDS = [
  "range_seeks",
  "range_pages",
  "scanned_entries",
  "scanned_bytes",
  "hydrated_records",
  "residual_checks",
  "duplicate_entries",
  "result_records",
  "response_bytes",
  "memory_high_water_bytes",
  "wall_time_us"
] as const;

export function decodeCompactFlowQueryResult(
  data: Buffer,
  budget: wire.DecodeValueBudget
): Record<string, unknown> {
  requireAvailable(data, 0, 2 + QUALITY_FIELDS.length + USAGE_FIELDS.length * 8);
  if (data.readUInt8(0) !== wire.COMPACT_FLOW_QUERY_RESULT) {
    throw new FerricStoreError("expected compact FLOW.QUERY result");
  }

  const kind = data.readUInt8(1);
  let offset = 2;
  const quality: Record<string, unknown> = {};
  for (let index = 0; index < QUALITY_FIELDS.length; index += 1) {
    const field = QUALITY_FIELDS[index];
    const values = QUALITY_VALUES[index];
    if (field == null || values == null) throw new FerricStoreError("invalid compact FLOW.QUERY quality schema");
    const value = values[data.readUInt8(offset)];
    if (value == null) throw new FerricStoreError("invalid compact FLOW.QUERY quality code");
    quality[field] = Buffer.from(value);
    offset += 1;
  }

  const usage: Record<string, unknown> = {};
  for (const field of USAGE_FIELDS) {
    const read = readU64(data, offset);
    usage[field] = read.value;
    offset = read.offset;
  }

  let result: Record<string, unknown>;
  if (kind === 0) {
    const page = readPage(data, offset);
    offset = page.offset;
    requireAvailable(data, offset, 4);
    const count = data.readUInt32BE(offset);
    offset += 4;
    if (count > MAX_RECORDS) throw new FerricStoreError("compact FLOW.QUERY page exceeds 100 records");
    consumeDecodeItems(count, budget);
    const records = new Array<Record<string, unknown>>(count);
    for (let index = 0; index < count; index += 1) {
      const read = readRecord(data, offset, budget);
      records[index] = read.value;
      offset = read.offset;
    }
    result = { version: Buffer.from(CONTRACT), records, page: page.value, quality, usage };
  } else if (kind === 1) {
    const count = readU64(data, offset);
    offset = count.offset;
    result = {
      version: Buffer.from(CONTRACT),
      result: { kind: Buffer.from("count"), value: count.value },
      quality,
      usage
    };
  } else {
    throw new FerricStoreError(`unsupported compact FLOW.QUERY result kind ${kind}`);
  }

  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact FLOW.QUERY result bytes");
  if (BigInt(usage.response_bytes as number | bigint) !== BigInt(data.byteLength)) {
    throw new FerricStoreError("compact FLOW.QUERY response_bytes does not match its payload");
  }
  return result;
}

function readPage(
  data: Buffer,
  offset: number
): { readonly value: Record<string, unknown>; readonly offset: number } {
  requireAvailable(data, offset, 5);
  const hasMoreCode = data.readUInt8(offset);
  const size = data.readUInt32BE(offset + 1);
  offset += 5;
  if (hasMoreCode === 0 && size === wire.NULL_U32) {
    return { value: { has_more: false, cursor: null }, offset };
  }
  if (hasMoreCode !== 1 || size === 0 || size === wire.NULL_U32 || size > MAX_CURSOR_BYTES) {
    throw new FerricStoreError("invalid compact FLOW.QUERY page cursor");
  }
  requireAvailable(data, offset, size);
  return {
    value: {
      has_more: true,
      cursor: detachDecodedBinary(data.subarray(offset, offset + size), data.byteLength)
    },
    offset: offset + size
  };
}

function readRecord(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget
): { readonly value: Record<string, unknown>; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const bitmap = data.readUInt32BE(offset);
  offset += 4;
  if ((bitmap & ~RECORD_FIELD_MASK) !== 0) {
    throw new FerricStoreError("compact FLOW.QUERY record contains reserved fields");
  }
  consumeDecodeItems(populationCount(bitmap), budget);
  const record: Record<string, unknown> = {};
  for (let index = 0; index < RECORD_FIELDS.length; index += 1) {
    if ((bitmap & (1 << index)) === 0) continue;
    const field = RECORD_FIELDS[index];
    if (field == null) throw new FerricStoreError("invalid compact FLOW.QUERY record schema");
    const read = decodeValueWithBudget(data, offset, budget);
    setOwnValue(record, field, detachDecodedBinary(read.value, data.byteLength));
    offset = read.offset;
  }
  return { value: record, offset };
}

function readU64(
  data: Buffer,
  offset: number
): { readonly value: number | bigint; readonly offset: number } {
  requireAvailable(data, offset, 8);
  const value = data.readBigUInt64BE(offset);
  return {
    value: value <= wire.MAX_SAFE_INTEGER_BIGINT ? Number(value) : value,
    offset: offset + 8
  };
}

function populationCount(value: number): number {
  let remaining = value >>> 0;
  let count = 0;
  while (remaining !== 0) {
    remaining &= remaining - 1;
    count += 1;
  }
  return count;
}
