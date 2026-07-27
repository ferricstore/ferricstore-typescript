import { field } from "./internal.js";
import {
  boundedInteger,
  boundedText,
  decodeError,
  positiveBoundedInteger,
  requiredBoundedText,
} from "./flow-query-response-validation.js";
import type { FlowQueryInteger } from "./flow-query-types.js";

const MAX_UNSIGNED_64 = (1n << 64n) - 1n;

export type FlowQueryResponseMap =
  Map<unknown, unknown> | Record<string, unknown>;

export function decodeUniqueTextArray(
  value: unknown,
  context: string,
  maximum: number,
  maximumBytes: number,
): readonly string[] {
  if (!Array.isArray(value) || value.length > maximum) {
    throw decodeError(`${context} must be a bounded array`, value);
  }
  const result = new Array<string>(value.length);
  const seen = new Set<string>();
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw decodeError(`${context} must be dense`, value);
    }
    const text = boundedText(
      value[index],
      `${context} entry ${index}`,
      maximumBytes,
    );
    if (seen.has(text)) {
      throw decodeError(`${context} contains duplicates`, value);
    }
    seen.add(text);
    result[index] = text;
  }
  return Object.freeze(result);
}

export function requiredChoice<const T extends readonly string[]>(
  mapping: FlowQueryResponseMap,
  name: string,
  context: string,
  choices: T,
): T[number] {
  const value = requiredBoundedText(mapping, name, context, 64);
  if (!choices.includes(value)) {
    throw decodeError(`${context} ${name} is unsupported`, mapping);
  }
  return value;
}

export function optionalBoundedText(
  mapping: FlowQueryResponseMap,
  name: string,
  context: string,
  maximumBytes: number,
): string | undefined {
  const value = field(mapping, name);
  if (value == null) return undefined;
  return boundedText(value, `${context} ${name}`, maximumBytes);
}

export function optionalIndexInteger(
  mapping: FlowQueryResponseMap,
  name: string,
  context: string,
): FlowQueryInteger | undefined {
  const value = field(mapping, name);
  return value == null
    ? undefined
    : unsignedIndexInteger(value, `${context} ${name}`);
}

export function indexCounter(
  mapping: FlowQueryResponseMap,
  name: string,
  section: string,
): FlowQueryInteger {
  return unsignedIndexInteger(
    field(mapping, name),
    `FLOW.QUERY.INDEXES index ${section} ${name}`,
  );
}

export function unsignedIndexInteger(
  value: unknown,
  context: string,
): FlowQueryInteger {
  return boundedInteger(value, MAX_UNSIGNED_64, context);
}

export function positiveIndexInteger(
  value: unknown,
  context: string,
): FlowQueryInteger {
  return positiveBoundedInteger(value, MAX_UNSIGNED_64, context);
}

export function indexInteger(value: FlowQueryInteger): bigint {
  return BigInt(value);
}
