import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import {
  binaryValueByteLength,
  requestFrameTooLarge,
  setOwnValue
} from "./protocol-core.js";
import * as wire from "./protocol-constants.js";

export function encodeValue(value: unknown): Buffer {
  return encodeValueWithLimit(value, Number.MAX_SAFE_INTEGER);
}

function encodeValueWithLimit(value: unknown, maxBytes: number): Buffer {
  const plan = planValueWithLimit(value, maxBytes);
  const output = Buffer.allocUnsafe(plan.byteLength);
  writeValuePlan(output, 0, plan);
  return output;
}

export function planValueWithLimit(value: unknown, maxBytes: number): wire.EncodeValuePlan {
  const budget: wire.EncodeValueBudget = {
    maxBytes,
    remainingBytes: maxBytes,
    remainingItems: wire.DEFAULT_MAX_VALUE_ITEMS
  };
  return planValueAt(value, budget, [], 0);
}

function planValueAt(
  value: unknown,
  budget: wire.EncodeValueBudget,
  ancestors: object[],
  depth: number
): wire.EncodeValuePlan {
  return isProtocolContainer(value)
    ? planContainerValue(value, budget, ancestors, depth)
    : planScalarValue(value, budget);
}

function planContainerValue(
  value: readonly unknown[] | Record<string, unknown>,
  budget: wire.EncodeValueBudget,
  ancestors: object[],
  depth: number
): wire.EncodeValuePlan {
  if (ancestors.includes(value)) {
    throw new FerricStoreError("native protocol value contains a circular reference");
  }
  if (depth >= wire.DEFAULT_MAX_VALUE_DEPTH) {
    throw new FerricStoreError("native protocol value nesting exceeds max depth");
  }

  const arrayValue: readonly unknown[] | undefined = Array.isArray(value) ? value : undefined;
  const entries = arrayValue == null ? Object.entries(value) : undefined;
  const count = arrayValue?.length ?? entries?.length ?? 0;
  if (count > wire.DEFAULT_MAX_VALUE_ITEMS) {
    throw new FerricStoreError("native protocol value container exceeds max items");
  }
  if (count > budget.remainingItems) {
    throw new FerricStoreError("native protocol value total items exceed max items");
  }
  budget.remainingItems -= count;
  consumeEncodeBytes(budget, 5);
  if (arrayValue != null && count > budget.remainingBytes) {
    throw requestFrameTooLarge(budget.maxBytes);
  }
  ancestors.push(value);
  try {
    if (arrayValue != null) {
      const items = new Array<wire.EncodeValuePlan>(arrayValue.length);
      let byteLength = 5;
      for (let index = 0; index < arrayValue.length; index += 1) {
        if (!Object.hasOwn(arrayValue, index)) {
          throw new FerricStoreError("native protocol value arrays must be dense");
        }
        const item = planValueAt(arrayValue[index], budget, ancestors, depth + 1);
        items[index] = item;
        byteLength += item.byteLength;
      }
      return { byteLength, items, tag: 5 };
    }
    if (entries == null) {
      throw new FerricStoreError("native protocol value container could not be encoded");
    }

    const entryPlans = new Array<wire.EncodeMapEntryPlan>(entries.length);
    let byteLength = 5;
    let index = 0;
    for (const [key, item] of entries) {
      const keyByteLength = Buffer.byteLength(key);
      consumeEncodeBytes(budget, 4 + keyByteLength);
      const valuePlan = planValueAt(item, budget, ancestors, depth + 1);
      entryPlans[index] = { key, keyByteLength, value: valuePlan };
      byteLength += 4 + keyByteLength + valuePlan.byteLength;
      index += 1;
    }
    return { byteLength, entries: entryPlans, tag: 6 };
  } finally {
    ancestors.pop();
  }
}

function isProtocolContainer(value: unknown): value is readonly unknown[] | Record<string, unknown> {
  if (Array.isArray(value)) return true;
  if (typeof value !== "object" || value == null) return false;
  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
}

function planScalarValue(value: unknown, budget: wire.EncodeValueBudget): wire.EncodeValuePlan {
  if (value == null) {
    consumeEncodeBytes(budget, 1);
    return { byteLength: 1, tag: 0 };
  }
  if (value === true) {
    consumeEncodeBytes(budget, 1);
    return { byteLength: 1, tag: 1 };
  }
  if (value === false) {
    consumeEncodeBytes(budget, 1);
    return { byteLength: 1, tag: 2 };
  }
  if (typeof value === "number" && Number.isInteger(value) && !Object.is(value, -0)) {
    if (!Number.isSafeInteger(value)) {
      throw new FerricStoreError("unsafe integer number; use bigint for exact native encoding");
    }
    consumeEncodeBytes(budget, 9);
    return { byteLength: 9, tag: 3, value: BigInt(value) };
  }
  if (typeof value === "bigint") {
    if (value >= wire.MIN_I64 && value <= wire.MAX_I64) {
      consumeEncodeBytes(budget, 9);
      return { byteLength: 9, tag: 3, value };
    }
    if (value > wire.MAX_I64 && value <= wire.MAX_U64) {
      consumeEncodeBytes(budget, 9);
      return { byteLength: 9, tag: 8, value };
    }
    throw new FerricStoreError("integer exceeds the signed or unsigned 64-bit native range");
  }
  if (typeof value === "number") {
    consumeEncodeBytes(budget, 9);
    return { byteLength: 9, tag: 7, value };
  }
  if (typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array) {
    const valueByteLength = binaryValueByteLength(value);
    const byteLength = 5 + valueByteLength;
    consumeEncodeBytes(budget, byteLength);
    return { byteLength, tag: 4, value, valueByteLength };
  }
  throw new FerricStoreError(`unsupported native protocol value type: ${typeof value}`);
}

export function writeValuePlan(output: Buffer, start: number, plan: wire.EncodeValuePlan): number {
  output.writeUInt8(plan.tag, start);
  let offset = start + 1;
  switch (plan.tag) {
    case 0:
    case 1:
    case 2:
      return offset;
    case 3:
      output.writeBigInt64BE(plan.value, offset);
      return offset + 8;
    case 4:
      output.writeUInt32BE(plan.valueByteLength, offset);
      offset += 4;
      if (typeof plan.value === "string") {
        output.write(plan.value, offset, plan.valueByteLength, "utf8");
      } else if (Buffer.isBuffer(plan.value)) {
        plan.value.copy(output, offset);
      } else {
        output.set(plan.value, offset);
      }
      return offset + plan.valueByteLength;
    case 5:
      output.writeUInt32BE(plan.items.length, offset);
      offset += 4;
      for (const item of plan.items) offset = writeValuePlan(output, offset, item);
      return offset;
    case 6:
      output.writeUInt32BE(plan.entries.length, offset);
      offset += 4;
      for (const entry of plan.entries) {
        output.writeUInt32BE(entry.keyByteLength, offset);
        offset += 4;
        output.write(entry.key, offset, entry.keyByteLength, "utf8");
        offset += entry.keyByteLength;
        offset = writeValuePlan(output, offset, entry.value);
      }
      return offset;
    case 7:
      output.writeDoubleBE(plan.value, offset);
      return offset + 8;
    case 8:
      output.writeBigUInt64BE(plan.value, offset);
      return offset + 8;
  }
}

function consumeEncodeBytes(budget: wire.EncodeValueBudget, bytes: number): void {
  if (bytes > budget.remainingBytes) {
    throw requestFrameTooLarge(budget.maxBytes);
  }
  budget.remainingBytes -= bytes;
}

export function decodeValue(
  data: Buffer,
  offset = 0,
  options: wire.DecodeValueOptions = {}
): { readonly value: unknown; readonly offset: number } {
  const limits = {
    maxDepth: normalizeDecodeLimit(options.maxDepth, wire.DEFAULT_MAX_VALUE_DEPTH),
    maxItems: normalizeDecodeLimit(options.maxItems, wire.DEFAULT_MAX_VALUE_ITEMS)
  };
  return decodeValueAt(data, offset, limits, { remainingItems: limits.maxItems }, 0);
}

function decodeValueAt(
  data: Buffer,
  offset: number,
  limits: wire.DecodeValueLimits,
  budget: wire.DecodeValueBudget,
  depth: number
): { readonly value: unknown; readonly offset: number } {
  requireAvailable(data, offset, 1);
  const tag = data.readUInt8(offset);
  offset += 1;
  if (tag === 0) return { value: null, offset };
  if (tag === 1) return { value: true, offset };
  if (tag === 2) return { value: false, offset };
  if (tag === 3) {
    requireAvailable(data, offset, 8);
    const integer = data.readBigInt64BE(offset);
    return {
      value:
        integer <= wire.MAX_SAFE_INTEGER_BIGINT && integer >= wire.MIN_SAFE_INTEGER_BIGINT
          ? Number(integer)
          : integer,
      offset: offset + 8
    };
  }
  if (tag === 4) {
    const read = readBinary(data, offset);
    return { value: read.value, offset: read.offset };
  }
  if (tag === 5) {
    requireAvailable(data, offset, 4);
    const count = data.readUInt32BE(offset);
    requireValueContainer(count, depth, limits, budget);
    offset += 4;
    const values: unknown[] = [];
    for (let index = 0; index < count; index += 1) {
      const read = decodeValueAt(data, offset, limits, budget, depth + 1);
      values.push(read.value);
      offset = read.offset;
    }
    return { value: values, offset };
  }
  if (tag === 6) {
    requireAvailable(data, offset, 4);
    const count = data.readUInt32BE(offset);
    requireValueContainer(count, depth, limits, budget);
    offset += 4;
    const value: Record<string, unknown> = {};
    for (let index = 0; index < count; index += 1) {
      const key = readBinary(data, offset);
      offset = key.offset;
      const item = decodeValueAt(data, offset, limits, budget, depth + 1);
      setOwnValue(value, key.value.toString("utf8"), item.value);
      offset = item.offset;
    }
    return { value, offset };
  }
  if (tag === 7) {
    requireAvailable(data, offset, 8);
    return { value: data.readDoubleBE(offset), offset: offset + 8 };
  }
  if (tag === 8) {
    requireAvailable(data, offset, 8);
    const integer = data.readBigUInt64BE(offset);
    return {
      value: integer <= wire.MAX_SAFE_INTEGER_BIGINT ? Number(integer) : integer,
      offset: offset + 8
    };
  }
  throw new FerricStoreError(`unknown protocol value tag ${tag}`);
}

export function decodeValueWithBudget(
  data: Buffer,
  offset: number,
  budget: wire.DecodeValueBudget
): { readonly value: unknown; readonly offset: number } {
  return decodeValueAt(data, offset, {
    maxDepth: wire.DEFAULT_MAX_VALUE_DEPTH,
    maxItems: wire.DEFAULT_MAX_VALUE_ITEMS
  }, budget, 0);
}

export function readBinary(data: Buffer, offset: number): { readonly value: Buffer; readonly offset: number } {
  requireAvailable(data, offset, 4);
  const size = data.readUInt32BE(offset);
  offset += 4;
  if (size === wire.NULL_U32) {
    throw new FerricStoreError("invalid null binary length");
  }
  requireAvailable(data, offset, size);
  return { value: data.subarray(offset, offset + size), offset: offset + size };
}

export function normalizeDecodeLimit(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(0, Math.trunc(value));
}

function requireValueContainer(
  count: number,
  depth: number,
  limits: wire.DecodeValueLimits,
  budget: wire.DecodeValueBudget
): void {
  if (depth >= limits.maxDepth) {
    throw new FerricStoreError("native protocol value nesting exceeds max depth");
  }
  if (count > limits.maxItems) {
    throw new FerricStoreError("native protocol value container exceeds max items");
  }
  consumeDecodeItems(count, budget);
}

export function consumeDecodeItems(count: number, budget: wire.DecodeValueBudget): void {
  if (count > budget.remainingItems) {
    throw new FerricStoreError("native protocol value total items exceed max items");
  }
  budget.remainingItems -= count;
}

export function requireAvailable(data: Buffer, offset: number, size: number): void {
  if (offset < 0 || size < 0 || data.byteLength - offset < size) {
    throw new FerricStoreError("native protocol value is truncated", { raw: data });
  }
}
