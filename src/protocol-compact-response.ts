import { Buffer } from "node:buffer";
import { FerricStoreError, classifyServerError } from "./errors.js";
import { compactResponseOpcodeSupports } from "./native-negotiation.js";
import {
  decodeCompactBinaryListList,
  decodeCompactBinaryMapList,
  decodeCompactIntegerList,
  markCompactPipelineDecoded,
  readCompactBinaryList,
  readCompactBinaryMap
} from "./protocol-compact-collections.js";
import * as wire from "./protocol-constants.js";
import { protocolErrorMessage } from "./protocol-error-message.js";
import { decodeCompactFlowQueryResult } from "./protocol-flow-query-result.js";
import { setProtocolMapEntry } from "./protocol-map-key.js";
import {
  consumeDecodeItems,
  decodeValueWithBudget,
  readBinary,
  requireAvailable
} from "./protocol-value.js";

export function tryDecodeCompactResponse(
  opcode: number,
  body: Buffer,
  hints: wire.ResponseDecodeHints
): { readonly found: boolean; readonly value: unknown } {
  if (body.byteLength === 0) return { found: false, value: undefined };
  const budget: wire.DecodeValueBudget = { remainingItems: wire.DEFAULT_MAX_VALUE_ITEMS };
  const tag = body.readUInt8(0);
  validateCompactResponseItems(tag, opcode, body, hints);
  const pipelineValues = opcode === wire.OPCODES.pipeline && supports(hints, "pipeline_v1", opcode);
  if (tag === wire.COMPACT_OK_LIST && (supports(hints, "ok_list_v1", opcode) || pipelineValues)) {
    const values = decodeCompactOkList(body, budget);
    return {
      found: true,
      value: opcode === wire.OPCODES.pipeline
        ? markCompactPipelineDecoded(values)
        : values.length === 1 ? "OK" : values
    };
  }
  if (tag === wire.COMPACT_KV_GET && supports(hints, "kv_get_v1", opcode)) {
    return { found: true, value: decodeCompactKvGet(body) };
  }
  if (
    (tag === wire.COMPACT_KV_MGET || tag === wire.COMPACT_KV_MGET_FIXED) &&
    (supports(hints, "kv_mget_v1", opcode) || pipelineValues)
  ) {
    const values = tag === wire.COMPACT_KV_MGET
      ? decodeCompactMget(body, budget)
      : decodeCompactMgetFixed(body, budget);
    return {
      found: true,
      value: opcode === wire.OPCODES.pipeline ? markCompactPipelineDecoded(values) : values
    };
  }
  if (tag === wire.COMPACT_INTEGER_LIST && pipelineValues) {
    return { found: true, value: markCompactPipelineDecoded(decodeCompactIntegerList(body, budget)) };
  }
  if (tag === wire.COMPACT_BINARY_LIST_LIST && pipelineValues) {
    return { found: true, value: markCompactPipelineDecoded(decodeCompactBinaryListList(body, budget)) };
  }
  if (tag === wire.COMPACT_BINARY_MAP_LIST && pipelineValues) {
    return { found: true, value: markCompactPipelineDecoded(decodeCompactBinaryMapList(body, budget)) };
  }
  if (tag === wire.COMPACT_PIPELINE_RESPONSE && pipelineValues) {
    return { found: true, value: decodeCompactPipeline(body, budget, hints.pipelineClaimModes) };
  }
  if (
    tag === wire.COMPACT_FLOW_CLAIM_JOBS &&
    (supports(hints, "flow_claim_jobs_v1", opcode) || pipelineValues)
  ) {
    const values = decodeCompactClaimJobs(body, budget, hints.compactClaimMode);
    return {
      found: true,
      value: opcode === wire.OPCODES.pipeline ? markCompactPipelineDecoded(values) : values
    };
  }
  if (tag === wire.COMPACT_FLOW_RECORD && supports(hints, "flow_record_v1", opcode)) {
    const read = readCompactFlowRecord(body, 0, budget);
    if (read.offset !== body.byteLength) throw new FerricStoreError("trailing compact Flow record bytes");
    return { found: true, value: read.value };
  }
  if (
    tag === wire.COMPACT_FLOW_RECORD_LIST &&
    (supports(hints, "flow_record_list_v1", opcode) || pipelineValues)
  ) {
    const read = readCompactFlowRecordList(body, 0, budget);
    if (read.offset !== body.byteLength) throw new FerricStoreError("trailing compact Flow record list bytes");
    return {
      found: true,
      value: opcode === wire.OPCODES.pipeline ? markCompactPipelineDecoded(read.value) : read.value
    };
  }
  if (tag === wire.COMPACT_FLOW_QUERY_RESULT && supports(hints, "flow_query_result_v1", opcode)) {
    return { found: true, value: decodeCompactFlowQueryResult(body, budget) };
  }
  return { found: false, value: undefined };
}

function supports(hints: wire.ResponseDecodeHints, codec: string, opcode: number): boolean {
  return compactResponseOpcodeSupports(hints.compactResponseOpcodes, codec, opcode);
}

function decodeCompactOkList(data: Buffer, budget: wire.DecodeValueBudget): Buffer[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  if (data.byteLength !== 5) throw new FerricStoreError("invalid compact OK list response");
  return Array.from({ length: count }, () => Buffer.from("OK"));
}

function decodeCompactKvGet(data: Buffer): Buffer | null {
  requireAvailable(data, 0, 2);
  const present = data.readUInt8(1);
  if (present === 0) {
    if (data.byteLength !== 2) throw new FerricStoreError("trailing compact GET bytes");
    return null;
  }
  if (present !== 1) throw new FerricStoreError("invalid compact GET response");
  const read = readBinary(data, 2);
  if (read.offset !== data.byteLength) throw new FerricStoreError("trailing compact GET bytes");
  return read.value;
}

function decodeCompactMget(data: Buffer, budget: wire.DecodeValueBudget): (Buffer | null)[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
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

function decodeCompactMgetFixed(data: Buffer, budget: wire.DecodeValueBudget): Buffer[] {
  requireAvailable(data, 0, 9);
  const count = data.readUInt32BE(1);
  const size = data.readUInt32BE(5);
  requireCompactContainer(count, budget);
  const expected = 9 + count * size;
  if (size === wire.NULL_U32 || data.byteLength !== expected) {
    throw new FerricStoreError("invalid compact fixed MGET response");
  }
  if (size === 0) return new Array<Buffer>(count).fill(Buffer.alloc(0));
  const values = new Array<Buffer>(count);
  for (let index = 0, offset = 9; index < count; index += 1, offset += size) {
    values[index] = data.subarray(offset, offset + size);
  }
  return values;
}

function decodeCompactPipeline(
  data: Buffer,
  budget: wire.DecodeValueBudget,
  claimModes: readonly (wire.CompactClaimMode | undefined)[] | undefined
): unknown[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  if (count > Math.floor((data.byteLength - 5) / 2)) {
    throw new FerricStoreError("compact pipeline item count exceeds response bytes");
  }
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
        const read = readCompactFlowRecord(data, offset, budget);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 3) {
        const read = readCompactFlowRecordList(data, offset, budget);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 4) {
        const read = readCompactClaimJob(data, offset, claimModes?.[index] ?? "base", budget);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 5) {
        const read = readCompactFlowValueRef(data, offset, budget);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 6) {
        const read = readCompactBinaryList(data, offset, budget);
        values[index] = read.value;
        offset = read.offset;
      } else if (kind === 7) {
        const read = readCompactBinaryMap(data, offset, budget);
        values[index] = read.value;
        offset = read.offset;
      } else {
        throw new FerricStoreError("unknown compact pipeline success kind");
      }
    } else if (status === 1 || status === 2) {
      const read = readBinary(data, offset);
      const errorStatus = status === 1 ? "busy" : "error";
      values[index] = classifyServerError(
        protocolErrorMessage(status === 1 ? 4 : 1, read.value),
        read.value,
        undefined,
        errorStatus
      );
      offset = read.offset;
    } else {
      throw new FerricStoreError(`unknown compact pipeline status ${status}`, {
        retryable: false,
        safeToRetry: false
      });
    }
  }
  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact pipeline bytes");
  return markCompactPipelineDecoded(values);
}

function decodeCompactClaimJobs(
  data: Buffer,
  budget: wire.DecodeValueBudget,
  mode?: wire.CompactClaimMode
): unknown[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  if (mode != null) {
    const values: unknown[] = [];
    let offset = 5;
    for (let index = 0; index < count; index += 1) {
      const read = readCompactClaimJob(data, offset, mode, budget);
      values.push(read.value);
      offset = read.offset;
    }
    if (offset !== data.byteLength) {
      throw new FerricStoreError(`compact claim response did not match expected ${mode} mode`);
    }
    return values;
  }
  for (const candidate of ["stateAttrs", "attrs", "state", "base"] as const) {
    const attemptBudget = { remainingItems: budget.remainingItems };
    const decoded = tryReadCompactClaimJobs(data, 5, count, candidate, attemptBudget);
    if (decoded?.offset === data.byteLength) {
      budget.remainingItems = attemptBudget.remainingItems;
      return decoded.value;
    }
  }
  throw new FerricStoreError("trailing compact claim jobs bytes");
}

function tryReadCompactClaimJobs(
  data: Buffer,
  offset: number,
  count: number,
  mode: wire.CompactClaimMode,
  budget: wire.DecodeValueBudget
): { readonly value: unknown[]; readonly offset: number } | null {
  try {
    const values: unknown[] = [];
    for (let index = 0; index < count; index += 1) {
      const read = readCompactClaimJob(data, offset, mode, budget);
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
  mode: wire.CompactClaimMode = "base",
  budget: wire.DecodeValueBudget = { remainingItems: wire.DEFAULT_MAX_VALUE_ITEMS }
): { readonly value: unknown[]; readonly offset: number } {
  const id = readBinary(data, offset);
  const partition = readOptionalBinary(data, id.offset);
  const lease = readBinary(data, partition.offset);
  requireAvailable(data, lease.offset, 8);
  const fencingBig = data.readBigInt64BE(lease.offset);
  const fencing = fencingBig <= wire.MAX_SAFE_INTEGER_BIGINT && fencingBig >= wire.MIN_SAFE_INTEGER_BIGINT
    ? Number(fencingBig)
    : fencingBig;
  offset = lease.offset + 8;
  if (mode === "base") {
    consumeDecodeItems(4, budget);
    return { value: [id.value, partition.value, lease.value, fencing], offset };
  }
  if (mode === "attrs") {
    consumeDecodeItems(6, budget);
    const attrs = decodeValueWithBudget(data, offset, budget);
    return { value: [id.value, partition.value, lease.value, fencing, null, attrs.value], offset: attrs.offset };
  }
  const runState = readOptionalBinary(data, offset);
  if (mode === "state") {
    consumeDecodeItems(5, budget);
    return { value: [id.value, partition.value, lease.value, fencing, runState.value], offset: runState.offset };
  }
  consumeDecodeItems(6, budget);
  const attrs = decodeValueWithBudget(data, runState.offset, budget);
  return {
    value: [id.value, partition.value, lease.value, fencing, runState.value, attrs.value],
    offset: attrs.offset
  };
}

function readCompactFlowValueRef(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget
): { readonly value: Record<string, unknown>; readonly offset: number } {
  consumeDecodeItems(1, budget);
  const ref = readBinary(data, offset);
  const partition = readOptionalBinary(data, ref.offset);
  if (partition.value != null) consumeDecodeItems(1, budget);
  const owner = readOptionalBinary(data, partition.offset);
  if (owner.value != null) consumeDecodeItems(1, budget);
  const value: Record<string, unknown> = { ref: ref.value };
  if (partition.value != null) value.partition_key = partition.value;
  if (owner.value != null) value.owner_flow_id = owner.value;
  return { value, offset: owner.offset };
}

function readCompactFlowRecord(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget = { remainingItems: wire.DEFAULT_MAX_VALUE_ITEMS }
): { readonly value: Record<string, unknown>; readonly offset: number } {
  requireAvailable(data, offset, 5);
  if (data.readUInt8(offset) !== wire.COMPACT_FLOW_RECORD) throw new FerricStoreError("expected compact Flow record");
  offset += 1;
  const count = data.readUInt32BE(offset);
  requireCompactContainer(count, budget);
  offset += 4;
  const record: Record<string, unknown> = {};
  for (let index = 0; index < count; index += 1) {
    requireAvailable(data, offset, 1);
    const keyId = data.readUInt8(offset);
    offset += 1;
    let key: string | Buffer;
    if (keyId === 0) {
      const read = readBinary(data, offset);
      key = read.value;
      offset = read.offset;
    } else {
      key = wire.FLOW_RECORD_FIELD_KEYS[keyId] ?? `field_${keyId}`;
    }
    const read = decodeValueWithBudget(data, offset, budget);
    setProtocolMapEntry(record, key, read.value);
    offset = read.offset;
  }
  return { value: record, offset };
}

function readCompactFlowRecordList(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget = { remainingItems: wire.DEFAULT_MAX_VALUE_ITEMS }
): { readonly value: Record<string, unknown>[]; readonly offset: number } {
  requireAvailable(data, offset, 5);
  if (data.readUInt8(offset) !== wire.COMPACT_FLOW_RECORD_LIST) {
    throw new FerricStoreError("expected compact Flow record list");
  }
  offset += 1;
  const count = data.readUInt32BE(offset);
  requireCompactContainer(count, budget);
  offset += 4;
  const records: Record<string, unknown>[] = [];
  for (let index = 0; index < count; index += 1) {
    const read = readCompactFlowRecord(data, offset, budget);
    records.push(read.value);
    offset = read.offset;
  }
  return { value: records, offset };
}

function readOptionalBinary(data: Buffer, offset: number): { readonly value: Buffer | null; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const size = data.readUInt32BE(offset);
  offset += 4;
  if (size === wire.NULL_U32) return { value: null, offset };
  requireAvailable(data, offset, size);
  return { value: data.subarray(offset, offset + size), offset: offset + size };
}

function requireCompactContainer(count: number, budget: wire.DecodeValueBudget): void {
  if (count > wire.DEFAULT_MAX_VALUE_ITEMS) {
    throw new FerricStoreError("native compact response container exceeds max items");
  }
  consumeDecodeItems(count, budget);
}

function validateCompactResponseItems(
  tag: number,
  opcode: number,
  data: Buffer,
  hints: wire.ResponseDecodeHints
): void {
  const scalarItems = opcode === wire.OPCODES.set || opcode === wire.OPCODES.mset ? 1 : undefined;
  const expected = scalarItems ?? hints.compactResponseItems;
  if (expected == null || !COUNTED_COMPACT_TAGS.has(tag)) return;
  requireAvailable(data, 0, 5);
  const actual = data.readUInt32BE(1);
  if (actual !== expected) {
    throw new FerricStoreError(
      `compact response returned ${actual} items; expected ${expected} items`
    );
  }
}

const COUNTED_COMPACT_TAGS = new Set<number>([
  wire.COMPACT_FLOW_CLAIM_JOBS,
  wire.COMPACT_OK_LIST,
  wire.COMPACT_KV_MGET,
  wire.COMPACT_FLOW_RECORD_LIST,
  wire.COMPACT_BINARY_LIST_LIST,
  wire.COMPACT_BINARY_MAP_LIST,
  wire.COMPACT_INTEGER_LIST,
  wire.COMPACT_KV_MGET_FIXED,
  wire.COMPACT_PIPELINE_RESPONSE
]);
