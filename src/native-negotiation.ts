import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { field } from "./internal.js";

export type CompactResponseOpcodes = ReadonlyMap<string, ReadonlySet<number>>;

export interface NativeNegotiation {
  readonly compactResponseOpcodes: CompactResponseOpcodes;
  readonly flowQuery: FlowQueryNegotiation;
  readonly maxResponseBytes?: number;
}

export interface FlowQueryNegotiation {
  readonly requestContract: "ferric.flow.query.request/v1";
  readonly resultContract: "ferric.flow.query.result/v1";
  readonly explainContract: "ferric.flow.explain/v1";
  readonly indexStatusContract: "ferric.flow.query.indexes/v1";
  readonly capabilities: ReadonlySet<string>;
  readonly languageVersions: ReadonlySet<string>;
  readonly shapes: ReadonlySet<string>;
}

export const EMPTY_COMPACT_RESPONSE_OPCODES: CompactResponseOpcodes = new Map();

/** Parse the HELLO-shaped capability payload returned by HELLO or STARTUP. */
export function nativeNegotiation(value: unknown): NativeNegotiation {
  const capabilities = field(value, "capabilities") ?? value;
  const limits = field(capabilities, "limits");
  const rawResponseCodecs = field(capabilities, "response_codecs");
  const responseCodecs = rawResponseCodecs == null
    ? undefined
    : requiredMap(rawResponseCodecs, "HELLO response_codecs");
  const schemas = requiredMap(field(capabilities, "schemas"), "HELLO schemas");
  const maxResponseBytes = positiveSafeInteger(field(limits, "max_response_bytes"));
  return {
    compactResponseOpcodes: parseCompactResponseOpcodes(
      field(responseCodecs, "compact_response_opcodes")
    ),
    flowQuery: parseFlowQueryNegotiation(capabilities, schemas),
    ...(maxResponseBytes == null ? {} : { maxResponseBytes })
  };
}

const REQUIRED_FLOW_QUERY_CAPABILITIES = new Set([
  "flow_query_v1",
  "flow_query_result_projection_v1",
  "flow_explain_v1",
  "flow_explain_analyze_v1",
  "flow_composite_index_v1",
  "flow_query_index_status_v1"
]);

const REQUIRED_FLOW_QUERY_SHAPES = new Set([
  "runs_by_run_id_record",
  "runs_by_partition_and_run_id_record",
  "runs_by_partition_predicates_ordered_records",
  "runs_by_partition_type_state_ordered_records",
  "runs_by_partition_type_terminals_ordered_records",
  "runs_by_partition_metadata_ordered_records",
  "runs_by_partition_type_running_lease_deadline_ordered_records",
  "runs_by_partition_parent_ordered_records",
  "runs_by_partition_root_ordered_records",
  "runs_by_partition_correlation_ordered_records",
  "runs_by_partition_predicates_count",
  "events_by_run_id_ordered_records"
]);

function parseFlowQueryNegotiation(
  capabilities: unknown,
  schemas: Map<unknown, unknown> | Record<string, unknown>
): FlowQueryNegotiation {
  const manifest = requiredMap(field(capabilities, "flow_query"), "HELLO flow_query");
  const requestContract = requiredBoundedText(manifest, "request_contract");
  const resultContract = requiredBoundedText(manifest, "result_contract");
  const explainContract = requiredBoundedText(manifest, "explain_contract");
  const indexStatusContract = requiredBoundedText(manifest, "index_status_contract");
  requireEqual(requestContract, "ferric.flow.query.request/v1", "flow_query request_contract");
  requireEqual(resultContract, "ferric.flow.query.result/v1", "flow_query result_contract");
  requireEqual(explainContract, "ferric.flow.explain/v1", "flow_query explain_contract");
  requireEqual(
    indexStatusContract,
    "ferric.flow.query.indexes/v1",
    "flow_query index_status_contract"
  );

  const queryCapabilities = boundedTextSet(manifest, "capabilities", 64);
  const languageVersions = boundedTextSet(manifest, "language_versions", 16);
  const shapes = boundedTextSet(manifest, "shapes", 128);
  requireMembers(queryCapabilities, REQUIRED_FLOW_QUERY_CAPABILITIES, "flow_query capability");
  requireMembers(languageVersions, new Set(["FQL1"]), "flow_query language");
  requireMembers(shapes, REQUIRED_FLOW_QUERY_SHAPES, "flow_query shape");

  const schema = requiredMap(field(schemas, "FLOW.QUERY"), "HELLO FLOW.QUERY schema");
  const fields = boundedTextSet(schema, "fields", 64);
  requireMembers(
    fields,
    new Set(["version", "query", "params", "deadline_ms"]),
    "FLOW.QUERY schema field"
  );
  const required = boundedTextSet(schema, "required", 16);
  if (required.size !== 2 || !required.has("version") || !required.has("query")) {
    throw incompatibleServer("FLOW.QUERY schema must require exactly version and query");
  }
  return {
    requestContract: "ferric.flow.query.request/v1",
    resultContract: "ferric.flow.query.result/v1",
    explainContract: "ferric.flow.explain/v1",
    indexStatusContract: "ferric.flow.query.indexes/v1",
    capabilities: queryCapabilities,
    languageVersions,
    shapes
  };
}

function requiredMap(
  value: unknown,
  context: string
): Map<unknown, unknown> | Record<string, unknown> {
  if (value instanceof Map) return value;
  if (
    typeof value === "object" &&
    value != null &&
    !Array.isArray(value) &&
    !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array)
  ) {
    return value as Record<string, unknown>;
  }
  throw incompatibleServer(`${context} is missing or invalid`);
}

function requiredBoundedText(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string
): string {
  const value = strictText(field(mapping, name));
  if (value == null || value.length === 0 || Buffer.byteLength(value) > 256) {
    throw incompatibleServer(`HELLO flow_query ${name} must be bounded non-empty text`);
  }
  return value;
}

function boundedTextSet(
  mapping: Map<unknown, unknown> | Record<string, unknown>,
  name: string,
  maximum: number
): ReadonlySet<string> {
  const raw = field(mapping, name);
  if (!Array.isArray(raw) || raw.length < 1 || raw.length > maximum) {
    throw incompatibleServer(`HELLO ${name} must contain 1..${maximum} entries`);
  }
  const result = new Set<string>();
  for (let index = 0; index < raw.length; index += 1) {
    if (!Object.hasOwn(raw, index)) throw incompatibleServer(`HELLO ${name} must be dense`);
    const item = strictText(raw[index]);
    if (item == null || item.length === 0 || Buffer.byteLength(item) > 256) {
      throw incompatibleServer(`HELLO ${name} contains invalid text`);
    }
    if (result.has(item)) throw incompatibleServer(`HELLO ${name} contains duplicate ${item}`);
    result.add(item);
  }
  return result;
}

function requireMembers(
  actual: ReadonlySet<string>,
  required: ReadonlySet<string>,
  context: string
): void {
  for (const item of required) {
    if (!actual.has(item)) throw incompatibleServer(`missing ${context} ${JSON.stringify(item)}`);
  }
}

function requireEqual(actual: string, expected: string, context: string): void {
  if (actual !== expected) throw incompatibleServer(`${context} must be ${JSON.stringify(expected)}`);
}

function strictText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(value);
    } catch {
      return undefined;
    }
  }
  return undefined;
}

function incompatibleServer(message: string): FerricStoreError {
  return new FerricStoreError(`incompatible FerricStore server: ${message}`);
}

export function compactResponseOpcodeSupports(
  capabilities: CompactResponseOpcodes | undefined,
  codec: string,
  opcode: number
): boolean {
  return capabilities?.get(codec)?.has(opcode) === true;
}

function parseCompactResponseOpcodes(value: unknown): CompactResponseOpcodes {
  if (value == null) return EMPTY_COMPACT_RESPONSE_OPCODES;
  const entries = mapEntries(value);
  if (entries == null) {
    throw incompatibleServer("HELLO compact_response_opcodes must be a map");
  }
  if (entries.length > 32) {
    throw incompatibleServer("HELLO compact_response_opcodes supports at most 32 codecs");
  }
  const result = new Map<string, ReadonlySet<number>>();
  let totalOpcodes = 0;
  for (const [rawName, rawOpcodes] of entries) {
    const name = strictText(rawName);
    if (name == null || name.length === 0 || Buffer.byteLength(name) > 128) {
      throw incompatibleServer("HELLO compact_response_opcodes contains an invalid codec name");
    }
    if (result.has(name)) {
      throw incompatibleServer(`HELLO compact_response_opcodes contains duplicate codec ${name}`);
    }
    if (!Array.isArray(rawOpcodes)) {
      throw incompatibleServer(`HELLO compact_response_opcodes codec ${name} must be an array`);
    }
    totalOpcodes += rawOpcodes.length;
    if (totalOpcodes > 1_024) {
      throw incompatibleServer("HELLO compact_response_opcodes supports at most 1024 opcodes");
    }
    const opcodes = new Set<number>();
    for (let index = 0; index < rawOpcodes.length; index += 1) {
      if (!Object.hasOwn(rawOpcodes, index)) {
        throw incompatibleServer(`HELLO compact_response_opcodes codec ${name} must be dense`);
      }
      const opcode = strictUnsigned16(rawOpcodes[index]);
      if (opcode == null) {
        throw incompatibleServer(`HELLO compact_response_opcodes codec ${name} has invalid opcode`);
      }
      if (opcodes.has(opcode)) {
        throw incompatibleServer(`HELLO compact_response_opcodes codec ${name} has duplicate opcode`);
      }
      opcodes.add(opcode);
    }
    result.set(name, opcodes);
  }
  return result;
}

function mapEntries(value: unknown): readonly (readonly [unknown, unknown])[] | undefined {
  if (value instanceof Map) return [...value.entries()];
  if (
    typeof value !== "object" ||
    value == null ||
    Array.isArray(value) ||
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array
  ) return undefined;
  return Object.entries(value as Record<string, unknown>);
}

function strictUnsigned16(value: unknown): number | undefined {
  if (typeof value === "bigint") {
    return value >= 0n && value <= 0xffffn ? Number(value) : undefined;
  }
  return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffff
    ? value
    : undefined;
}

function positiveSafeInteger(value: unknown): number | undefined {
  const parsed = nonNegativeSafeInteger(value);
  return parsed != null && parsed > 0 ? parsed : undefined;
}

function nonNegativeSafeInteger(value: unknown): number | undefined {
  if (typeof value === "bigint") {
    return value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : undefined;
  }
  const source = text(value) ?? value;
  if (typeof source !== "number" && (typeof source !== "string" || !/^\d+$/u.test(source))) {
    return undefined;
  }
  const parsed = Number(source);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function text(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  return undefined;
}
