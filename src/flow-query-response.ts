import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { field } from "./internal.js";
import { tryDecodeFlowQueryError } from "./flow-query-diagnostic-response.js";
import {
  boundedInteger,
  boundedText,
  decodeError,
  freezeMap,
  freezeMetadataMap,
  hasKey,
  nonNegativeInteger,
  optionalText,
  requiredBoolean,
  requiredBoundedText,
  requiredMap,
  requiredText,
  requireContract
} from "./flow-query-response-validation.js";
import {
  FlowQueryError,
  type FlowExplainCapabilities,
  type FlowExplainResult,
  type FlowQueryInteger,
  type FlowQueryPage,
  type FlowQueryQuality,
  type FlowQueryResult,
  type FlowQueryUsage
} from "./flow-query-types.js";

export {
  decodeFlowQueryIndexStatus,
  FLOW_QUERY_INDEXES_CONTRACT
} from "./flow-query-index-response.js";
export { tryDecodeFlowQueryError } from "./flow-query-diagnostic-response.js";

export const FLOW_QUERY_RESULT_CONTRACT = "ferric.flow.query.result/v1";
export const FLOW_EXPLAIN_CONTRACT = "ferric.flow.explain/v1";

const USAGE_FIELDS = [
  ["range_seeks", "rangeSeeks"],
  ["range_pages", "rangePages"],
  ["scanned_entries", "scannedEntries"],
  ["scanned_bytes", "scannedBytes"],
  ["hydrated_records", "hydratedRecords"],
  ["residual_checks", "residualChecks"],
  ["duplicate_entries", "duplicateEntries"],
  ["result_records", "resultRecords"],
  ["response_bytes", "responseBytes"],
  ["memory_high_water_bytes", "memoryHighWaterBytes"],
  ["wall_time_us", "wallTimeUs"]
] as const;

const MAX_SIGNED_64 = (1n << 63n) - 1n;
const QUALITY_VALUES = {
  exactness: ["authoritative", "projected_exact", "exact", "not_applicable"],
  freshness: ["current", "projection_watermark", "not_applicable"],
  coverage: ["complete", "unavailable"],
  pagination: ["none", "complete", "authenticated_seek", "live_seek"],
} as const;

type FlowQueryResponseMap = Map<unknown, unknown> | Record<string, unknown>;

interface DecodedFlowQueryBase {
  readonly quality: FlowQueryQuality;
  readonly raw: Readonly<Record<string, unknown>>;
  readonly usage: FlowQueryUsage;
}

interface DecodedFlowQueryRecords<TRecord> extends DecodedFlowQueryBase {
  readonly kind: "records";
  readonly page: FlowQueryPage;
  readonly records: TRecord[];
}

interface DecodedFlowQueryCount extends DecodedFlowQueryBase {
  readonly count: FlowQueryInteger;
  readonly kind: "count";
}

type DecodedFlowQuery<TRecord> =
  | DecodedFlowQueryRecords<TRecord>
  | DecodedFlowQueryCount;

export function decodeFlowQueryResult(value: unknown): FlowQueryResult {
  const result = decodeFlowQueryResponse(value, (mapping) => freezeMap(mapping));
  if (result.kind === "records") {
    return Object.freeze({
      kind: "records",
      version: FLOW_QUERY_RESULT_CONTRACT,
      records: Object.freeze(result.records),
      page: result.page,
      quality: result.quality,
      usage: result.usage,
      raw: result.raw
    });
  }
  return Object.freeze({
    kind: "count",
    version: FLOW_QUERY_RESULT_CONTRACT,
    count: result.count,
    quality: result.quality,
    usage: result.usage,
    raw: result.raw
  });
}

export function decodeFlowQueryRecords<TRecord>(
  value: unknown,
  decodeRecord: (value: FlowQueryResponseMap, index: number) => TRecord
): TRecord[] {
  const result = decodeFlowQueryResponse(value, (mapping, index) =>
    decodeRecord(mapping instanceof Map ? freezeMap(mapping) : mapping, index)
  );
  if (result.kind === "count") {
    throw new FerricStoreError("Flow record convenience query returned a count result", {
      raw: result.raw
    });
  }
  return result.records;
}

function decodeFlowQueryResponse<TRecord>(
  value: unknown,
  decodeRecord: (value: FlowQueryResponseMap, index: number) => TRecord
): DecodedFlowQuery<TRecord> {
  const mapping = requiredMap(value, "FLOW.QUERY result");
  requireContract(mapping, "version", FLOW_QUERY_RESULT_CONTRACT, "FLOW.QUERY result");
  const quality = decodeQuality(field(mapping, "quality"));
  const usage = decodeUsage(field(mapping, "usage"));
  const hasRecords = hasKey(mapping, "records");
  const hasCount = hasKey(mapping, "result");
  if (hasRecords === hasCount) {
    throw decodeError("FLOW.QUERY result must contain exactly one records or count shape", value);
  }

  const raw = freezeMap(mapping);
  if (hasRecords) {
    const rawRecords = field(mapping, "records");
    if (!Array.isArray(rawRecords) || rawRecords.length > 100) {
      throw decodeError("FLOW.QUERY records must be an array of at most 100 maps", value);
    }
    const records = new Array<TRecord>(rawRecords.length);
    for (let index = 0; index < rawRecords.length; index += 1) {
      if (!Object.hasOwn(rawRecords, index)) {
        throw decodeError("FLOW.QUERY records must be a dense array", value);
      }
      records[index] = decodeRecord(
        requiredMap(rawRecords[index], `FLOW.QUERY record ${index}`),
        index
      );
    }
    if (
      usage.resultRecords !== records.length ||
      usage.resultRecords > usage.scannedEntries
    ) {
      throw decodeError("FLOW.QUERY usage result_records is inconsistent with records", value);
    }
    return {
      kind: "records",
      records,
      page: decodePage(field(mapping, "page")),
      quality,
      usage,
      raw
    };
  }

  if (hasKey(mapping, "page")) {
    throw decodeError("FLOW.QUERY count result contains an unexpected page", value);
  }
  const countResult = requiredMap(field(mapping, "result"), "FLOW.QUERY count result");
  if (requiredText(countResult, "kind", "FLOW.QUERY count result") !== "count") {
    throw decodeError("FLOW.QUERY count result kind must be count", value);
  }
  const count = boundedInteger(
    field(countResult, "value"),
    MAX_SIGNED_64,
    "FLOW.QUERY count value"
  );
  if (usage.resultRecords !== 1) {
    throw decodeError("FLOW.QUERY count usage result_records must be 1", value);
  }
  return {
    kind: "count",
    count,
    quality,
    usage,
    raw
  };
}

export function decodeFlowExplainResult(value: unknown): FlowExplainResult {
  const mapping = requiredMap(value, "FLOW.QUERY explain");
  requireContract(mapping, "version", FLOW_EXPLAIN_CONTRACT, "FLOW.QUERY explain");
  const queryFingerprint = requiredText(mapping, "query_fingerprint", "FLOW.QUERY explain");
  if (!/^[0-9a-f]{64}$/iu.test(queryFingerprint)) {
    throw decodeError("FLOW.QUERY explain query_fingerprint is invalid", value);
  }
  const status = requiredText(mapping, "status", "FLOW.QUERY explain");
  if (status !== "planned" && status !== "rejected" && status !== "executed") {
    throw decodeError(`FLOW.QUERY explain status ${JSON.stringify(status)} is unsupported`, value);
  }
  const plan = freezeMetadataMap(
    requiredMap(field(mapping, "plan"), "FLOW.QUERY explain plan"),
    "FLOW.QUERY explain plan"
  );
  const estimate = freezeMetadataMap(
    requiredMap(field(mapping, "estimate"), "FLOW.QUERY explain estimate"),
    "FLOW.QUERY explain estimate"
  );
  const bounds = freezeMetadataMap(
    requiredMap(field(mapping, "bounds"), "FLOW.QUERY explain bounds"),
    "FLOW.QUERY explain bounds"
  );
  const capabilities = decodeExplainCapabilities(mapping);
  const extendedFields = ["stats", "quality", "pressure", "decision", "alternatives"];
  const extendedPresence = extendedFields.map((name) => hasKey(mapping, name));
  const specialized = capabilities != null && !extendedPresence.some(Boolean);
  let stats: Readonly<Record<string, unknown>> | undefined;
  let quality: FlowQueryQuality | undefined;
  let pressure: Readonly<Record<string, unknown>> | undefined;
  let decision: Readonly<Record<string, unknown>> | undefined;
  let alternatives: readonly Readonly<Record<string, unknown>>[];
  if (specialized) {
    if (status !== "planned") {
      throw decodeError("FLOW.QUERY specialized explain must be planned", value);
    }
    if (hasKey(mapping, "actual") || hasKey(mapping, "diagnostic")) {
      throw decodeError("FLOW.QUERY specialized explain has extended status fields", value);
    }
    alternatives = Object.freeze([]);
  } else {
    if (
      !extendedPresence.every(Boolean) ||
      !hasKey(mapping, "actual") ||
      !hasKey(mapping, "diagnostic")
    ) {
      throw decodeError("FLOW.QUERY explain is missing required v1 fields", value);
    }
    stats = freezeMetadataMap(
      requiredMap(field(mapping, "stats"), "FLOW.QUERY explain stats"),
      "FLOW.QUERY explain stats"
    );
    quality = decodeQuality(field(mapping, "quality"));
    pressure = freezeMetadataMap(
      requiredMap(field(mapping, "pressure"), "FLOW.QUERY explain pressure"),
      "FLOW.QUERY explain pressure"
    );
    decision = freezeMetadataMap(
      requiredMap(field(mapping, "decision"), "FLOW.QUERY explain decision"),
      "FLOW.QUERY explain decision"
    );
    alternatives = decodeExplainAlternatives(field(mapping, "alternatives"));
  }
  const actualValue = field(mapping, "actual");
  let actual: FlowQueryUsage | undefined;
  if (status === "executed") {
    if (actualValue == null) {
      throw decodeError("FLOW.QUERY executed explain is missing actual usage", value);
    }
    actual = decodeUsage(actualValue);
  } else if (actualValue != null) {
    throw decodeError("FLOW.QUERY non-executed explain contains actual usage", value);
  }

  const diagnosticValue = field(mapping, "diagnostic");
  let diagnostic: FlowQueryError | undefined;
  if (status === "rejected") {
    diagnostic = tryDecodeFlowQueryError(diagnosticValue);
    if (diagnostic == null) {
      throw decodeError("FLOW.QUERY rejected explain has an invalid diagnostic", value);
    }
  } else if (diagnosticValue != null) {
    throw decodeError("FLOW.QUERY non-rejected explain contains a diagnostic", value);
  }

  return Object.freeze({
    version: FLOW_EXPLAIN_CONTRACT,
    queryFingerprint,
    status,
    plan,
    estimate,
    stats,
    quality,
    bounds,
    pressure,
    decision,
    alternatives,
    capabilities,
    actual,
    diagnostic,
    raw: freezeMap(mapping)
  });
}

function decodeExplainCapabilities(
  mapping: FlowQueryResponseMap
): FlowExplainCapabilities | undefined {
  if (!hasKey(mapping, "capabilities")) return undefined;
  const value = requiredMap(
    field(mapping, "capabilities"),
    "FLOW.QUERY explain capabilities"
  );
  return Object.freeze({
    requested: decodeExplainCapabilityList(field(value, "requested"), "requested"),
    available: decodeExplainCapabilityList(field(value, "available"), "available"),
    missing: decodeExplainCapabilityList(field(value, "missing"), "missing"),
    raw: freezeMetadataMap(value, "FLOW.QUERY explain capabilities")
  });
}

function decodeExplainCapabilityList(value: unknown, name: string): readonly string[] {
  if (!Array.isArray(value) || value.length > 64) {
    throw decodeError(
      `FLOW.QUERY explain capabilities ${name} must contain at most 64 entries`,
      value
    );
  }
  const result = new Array<string>(value.length);
  const seen = new Set<string>();
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw decodeError(`FLOW.QUERY explain capabilities ${name} must be dense`, value);
    }
    const capability = boundedText(
      value[index],
      `FLOW.QUERY explain capabilities ${name} entry ${index}`,
      128
    );
    if (seen.has(capability)) {
      throw decodeError(
        `FLOW.QUERY explain capabilities ${name} contains duplicates`,
        value
      );
    }
    seen.add(capability);
    result[index] = capability;
  }
  return Object.freeze(result);
}

function decodeExplainAlternatives(value: unknown): readonly Readonly<Record<string, unknown>>[] {
  if (!Array.isArray(value) || value.length > 31) {
    throw decodeError("FLOW.QUERY explain alternatives must be an array of at most 31 maps", value);
  }
  const alternatives = new Array<Readonly<Record<string, unknown>>>(value.length);
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw decodeError("FLOW.QUERY explain alternatives must be dense", value);
    }
    alternatives[index] = freezeMetadataMap(
      requiredMap(value[index], `FLOW.QUERY explain alternative ${index}`),
      `FLOW.QUERY explain alternative ${index}`
    );
  }
  return Object.freeze(alternatives);
}

function decodeQuality(value: unknown): FlowQueryQuality {
  const mapping = requiredMap(value, "FLOW.QUERY quality");
  return Object.freeze({
    exactness: decodeQualityValue(mapping, "exactness", QUALITY_VALUES.exactness),
    freshness: decodeQualityValue(mapping, "freshness", QUALITY_VALUES.freshness),
    coverage: decodeQualityValue(mapping, "coverage", QUALITY_VALUES.coverage),
    pagination: decodeQualityValue(mapping, "pagination", QUALITY_VALUES.pagination),
  });
}

function decodeQualityValue<const T extends readonly string[]>(
  mapping: FlowQueryResponseMap,
  name: string,
  allowed: T,
): T[number] {
  const value = requiredBoundedText(mapping, name, "FLOW.QUERY quality", 64);
  if (!allowed.includes(value)) {
    throw decodeError(`FLOW.QUERY quality ${name} is unsupported`, mapping);
  }
  return value;
}

function decodeUsage(value: unknown): FlowQueryUsage {
  const mapping = requiredMap(value, "FLOW.QUERY usage");
  const usage = Object.create(null) as Record<string, number>;
  for (const [wireName, property] of USAGE_FIELDS) {
    usage[property] = nonNegativeInteger(
      field(mapping, wireName),
      `FLOW.QUERY usage ${wireName}`
    );
  }
  const decoded = usage as unknown as FlowQueryUsage;
  if (
    decoded.hydratedRecords > decoded.scannedEntries ||
    decoded.duplicateEntries > decoded.scannedEntries ||
    decoded.rangePages > decoded.scannedEntries + decoded.rangeSeeks ||
    decoded.residualChecks > decoded.scannedEntries * 12
  ) {
    throw decodeError("FLOW.QUERY usage counters are inconsistent", value);
  }
  return Object.freeze(decoded);
}

function decodePage(value: unknown): FlowQueryPage {
  const mapping = requiredMap(value, "FLOW.QUERY page");
  const hasMore = requiredBoolean(mapping, "has_more", "FLOW.QUERY page");
  const cursor = optionalText(mapping, "cursor", "FLOW.QUERY page");
  if (
    cursor != null &&
    (!cursor.startsWith("fqc1_") ||
      Buffer.byteLength(cursor) < 16 ||
      Buffer.byteLength(cursor) > 4_096)
  ) {
    throw decodeError("FLOW.QUERY page cursor is invalid", value);
  }
  if (hasMore !== (cursor != null)) {
    throw decodeError("FLOW.QUERY page has_more and cursor are inconsistent", value);
  }
  return Object.freeze({ hasMore, ...(cursor == null ? {} : { cursor }) });
}
