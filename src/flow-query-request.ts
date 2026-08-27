import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import type { FlowQueryParameter, FlowQueryParameters } from "./flow-query-types.js";

export const FLOW_QUERY_LANGUAGE_VERSION = "FQL1";
export const FLOW_QUERY_REQUEST_CONTRACT = "ferric.flow.query.request/v1";
export const FLOW_QUERY_MAX_BYTES = 16 * 1_024;
export const FLOW_QUERY_MAX_PARAMETERS = 64;
export const FLOW_QUERY_MAX_PARAMETER_NAME_BYTES = 128;
export const FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES = 65_535;

const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;
const INDEX_ID = /^[A-Za-z0-9_.:-]{1,64}$/u;
const PARAMETER_NAME = /^[A-Za-z0-9_.-]+$/u;

export function flowQueryArgs(
  query: string,
  params: FlowQueryParameters = {}
): CommandArgument[] {
  validateFlowQueryText(query);
  const entries = flowQueryParameterEntries(params);
  const args: CommandArgument[] = ["FLOW.QUERY", FLOW_QUERY_LANGUAGE_VERSION, query];
  for (const [name, value] of entries) args.push(name, value);
  return args;
}

export function flowQueryPayload(args: readonly CommandArgument[]): Record<string, unknown> {
  if (args.length < 2) throw new TypeError("FLOW.QUERY requires version and query");
  if ((args.length - 2) % 2 !== 0) {
    throw new TypeError("FLOW.QUERY parameters must be name/value pairs");
  }
  const version = commandText(args[0], "FLOW.QUERY version");
  if (version !== FLOW_QUERY_LANGUAGE_VERSION) {
    throw new TypeError(`FLOW.QUERY requires version ${FLOW_QUERY_LANGUAGE_VERSION}`);
  }
  const query = commandText(args[1], "FLOW.QUERY query");
  validateFlowQueryText(query);
  const parameterCount = (args.length - 2) / 2;
  if (parameterCount > FLOW_QUERY_MAX_PARAMETERS) {
    throw new TypeError(`FLOW.QUERY accepts at most ${FLOW_QUERY_MAX_PARAMETERS} named parameters`);
  }
  const params = Object.create(null) as Record<string, FlowQueryParameter>;
  for (let index = 2; index < args.length; index += 2) {
    const name = commandText(args[index], "FLOW.QUERY parameter name");
    validateFlowQueryParameterName(name);
    if (Object.hasOwn(params, name)) {
      throw new TypeError(`FLOW.QUERY parameter ${JSON.stringify(name)} is duplicated`);
    }
    params[name] = normalizeFlowQueryParameter(args[index + 1], name);
  }
  return {
    version,
    query,
    ...(parameterCount === 0 ? {} : { params })
  };
}

export function validateFlowQueryText(query: string): void {
  if (typeof query !== "string") throw new TypeError("FLOW.QUERY query must be text");
  if (query.trim().length === 0) throw new TypeError("FLOW.QUERY query must not be empty");
  validateUnicodeScalarText(query, "FLOW.QUERY query");
  if (Buffer.byteLength(query, "utf8") > FLOW_QUERY_MAX_BYTES) {
    throw new TypeError(`FLOW.QUERY query exceeds ${FLOW_QUERY_MAX_BYTES} bytes`);
  }
}

export function validateFlowQueryIndexId(indexId: string): void {
  if (typeof indexId !== "string" || !INDEX_ID.test(indexId)) {
    throw new TypeError(
      "query index id must be 1..64 ASCII letters, digits, '_', '-', ':', or '.'"
    );
  }
}

export function hasFlowExplainPrefix(query: string): boolean {
  const source = trimFlowQueryAsciiWhitespaceStart(query);
  return matchesFlowQueryAsciiKeyword(source, 0, "EXPLAIN") &&
    (source.length === 7 || isFlowQueryAsciiWhitespaceCode(source.charCodeAt(7)));
}

export function matchesFlowQueryAsciiKeyword(
  value: string,
  offset: number,
  keyword: string
): boolean {
  if (offset < 0 || offset + keyword.length > value.length) return false;
  for (let index = 0; index < keyword.length; index += 1) {
    let actual = value.charCodeAt(offset + index);
    if (actual >= 0x61 && actual <= 0x7a) actual -= 0x20;
    if (actual !== keyword.charCodeAt(index)) return false;
  }
  return true;
}

export function trimFlowQueryAsciiWhitespaceStart(value: string): string {
  let start = 0;
  while (start < value.length && isFlowQueryAsciiWhitespaceCode(value.charCodeAt(start))) {
    start += 1;
  }
  return start === 0 ? value : value.slice(start);
}

export function trimFlowQueryAsciiWhitespace(value: string): string {
  let start = 0;
  let end = value.length;
  while (start < end && isFlowQueryAsciiWhitespaceCode(value.charCodeAt(start))) start += 1;
  while (end > start && isFlowQueryAsciiWhitespaceCode(value.charCodeAt(end - 1))) end -= 1;
  return start === 0 && end === value.length ? value : value.slice(start, end);
}

export function isFlowQueryAsciiWhitespaceCode(code: number): boolean {
  return code === 0x20 || code === 0x09 || code === 0x0a || code === 0x0d;
}

function flowQueryParameterEntries(
  params: FlowQueryParameters
): readonly (readonly [string, FlowQueryParameter])[] {
  if (typeof params !== "object" || params == null || Array.isArray(params)) {
    throw new TypeError("FLOW.QUERY params must be a plain object");
  }
  const prototype = Object.getPrototypeOf(params) as unknown;
  if (prototype !== null && prototype !== Object.prototype) {
    throw new TypeError("FLOW.QUERY params must be a plain object");
  }
  const keys = Reflect.ownKeys(params);
  if (keys.some((key) => typeof key !== "string")) {
    throw new TypeError("FLOW.QUERY parameter names must be strings");
  }
  if (keys.length > FLOW_QUERY_MAX_PARAMETERS) {
    throw new TypeError(`FLOW.QUERY accepts at most ${FLOW_QUERY_MAX_PARAMETERS} named parameters`);
  }
  const names = (keys as string[]).sort();
  return names.map((name) => {
    validateFlowQueryParameterName(name);
    return [name, normalizeFlowQueryParameter(params[name], name)] as const;
  });
}

function validateFlowQueryParameterName(name: string): void {
  validateUnicodeScalarText(name, "FLOW.QUERY parameter name");
  const size = Buffer.byteLength(name, "utf8");
  if (
    size === 0 ||
    size > FLOW_QUERY_MAX_PARAMETER_NAME_BYTES ||
    !PARAMETER_NAME.test(name)
  ) {
    throw new TypeError(
      `FLOW.QUERY parameter names must be 1..${FLOW_QUERY_MAX_PARAMETER_NAME_BYTES} ` +
      "ASCII letters, digits, '_', '.', or '-'"
    );
  }
}

function normalizeFlowQueryParameter(value: unknown, name: string): FlowQueryParameter {
  if (typeof value === "string") {
    validateUnicodeScalarText(value, `FLOW.QUERY parameter ${JSON.stringify(name)}`);
    validateParameterValueSize(Buffer.byteLength(value, "utf8"), name);
    return value;
  }
  if (Buffer.isBuffer(value)) {
    validateParameterValueSize(value.byteLength, name);
    return value;
  }
  if (value instanceof Uint8Array) {
    validateParameterValueSize(value.byteLength, name);
    return Buffer.from(value);
  }
  if (typeof value === "boolean") return value;
  if (typeof value === "bigint" && value >= MIN_I64 && value <= MAX_I64) return value;
  if (typeof value === "number" && Number.isFinite(value)) {
    if (!Number.isInteger(value) || Number.isSafeInteger(value)) return value;
  }
  throw new TypeError(
    `FLOW.QUERY parameter ${JSON.stringify(name)} must be text, bytes, boolean, ` +
    "a finite float, or a signed 64-bit integer"
  );
}

function validateParameterValueSize(size: number, name: string): void {
  if (size > FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES) {
    throw new TypeError(
      `FLOW.QUERY parameter ${JSON.stringify(name)} exceeds ` +
      `${FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES} bytes`
    );
  }
}

function commandText(value: unknown, context: string): string {
  if (typeof value === "string") {
    validateUnicodeScalarText(value, context);
    return value;
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(value);
    } catch (error) {
      throw new TypeError(`${context} must be valid UTF-8`, { cause: error });
    }
  }
  throw new TypeError(`${context} must be text`);
}

function validateUnicodeScalarText(value: string, context: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (!(low >= 0xdc00 && low <= 0xdfff)) {
        throw new TypeError(`${context} must be valid UTF-8`);
      }
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new TypeError(`${context} must be valid UTF-8`);
    }
  }
}
