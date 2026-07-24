import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import * as wire from "./protocol-constants.js";
import { setProtocolMapEntry } from "./protocol-map-key.js";
import { consumeDecodeItems, readBinary, requireAvailable } from "./protocol-value.js";

export function decodeCompactIntegerList(
  data: Buffer,
  budget: wire.DecodeValueBudget
): (number | bigint)[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  if (data.byteLength !== 5 + count * 8) {
    throw new FerricStoreError("invalid compact integer list response");
  }
  const values = new Array<number | bigint>(count);
  for (let index = 0, offset = 5; index < count; index += 1, offset += 8) {
    const value = data.readBigInt64BE(offset);
    values[index] = value <= wire.MAX_SAFE_INTEGER_BIGINT && value >= wire.MIN_SAFE_INTEGER_BIGINT
      ? Number(value)
      : value;
  }
  return values;
}

export function decodeCompactBinaryListList(
  data: Buffer,
  budget: wire.DecodeValueBudget
): Buffer[][] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  let offset = 5;
  const values = new Array<Buffer[]>(count);
  for (let index = 0; index < count; index += 1) {
    const read = readCompactBinaryList(data, offset, budget);
    values[index] = read.value;
    offset = read.offset;
  }
  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact binary list-list bytes");
  return values;
}

export function decodeCompactBinaryMapList(
  data: Buffer,
  budget: wire.DecodeValueBudget
): Record<string, Buffer>[] {
  requireAvailable(data, 0, 5);
  const count = data.readUInt32BE(1);
  requireCompactContainer(count, budget);
  let offset = 5;
  const values = new Array<Record<string, Buffer>>(count);
  for (let index = 0; index < count; index += 1) {
    const read = readCompactBinaryMap(data, offset, budget);
    values[index] = read.value;
    offset = read.offset;
  }
  if (offset !== data.byteLength) throw new FerricStoreError("trailing compact binary map-list bytes");
  return values;
}

export function readCompactBinaryList(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget
): { readonly value: Buffer[]; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const count = data.readUInt32BE(offset);
  requireCompactContainer(count, budget);
  offset += 4;
  const values = new Array<Buffer>(count);
  for (let index = 0; index < count; index += 1) {
    const read = readBinary(data, offset);
    values[index] = read.value;
    offset = read.offset;
  }
  return { value: values, offset };
}

export function readCompactBinaryMap(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget
): { readonly value: Record<string, Buffer>; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const count = data.readUInt32BE(offset);
  requireCompactContainer(count, budget);
  offset += 4;
  const values: Record<string, Buffer> = {};
  for (let index = 0; index < count; index += 1) {
    const key = readBinary(data, offset);
    const value = readBinary(data, key.offset);
    setProtocolMapEntry(values, key.value, value.value);
    offset = value.offset;
  }
  return { value: values, offset };
}

export function markCompactPipelineDecoded<T extends unknown[]>(values: T): T {
  Object.defineProperty(values, wire.COMPACT_PIPELINE_DECODED, { value: true });
  return values;
}

function requireCompactContainer(count: number, budget: wire.DecodeValueBudget): void {
  if (count > wire.DEFAULT_MAX_VALUE_ITEMS) {
    throw new FerricStoreError("native compact response container exceeds max items");
  }
  consumeDecodeItems(count, budget);
}
