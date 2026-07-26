import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { field } from "./internal.js";
import {
  boundedText,
  boundedInteger,
  decodeError,
  freezeMap,
  freezeMetadataMap,
  hasKey,
  nonNegativeInteger,
  optionalText,
  positiveBoundedInteger,
  positiveInteger,
  requiredBoolean,
  requiredBoundedText,
  requiredMap,
  requiredText,
  requireContract
} from "./flow-query-response-validation.js";
import {
  FlowQueryError,
  type FlowExplainResult,
  type FlowQueryErrorPosition,
  type FlowQueryInteger,
  type FlowQueryIndex,
  type FlowQueryIndexFormat,
  type FlowQueryIndexStatus,
  type FlowQueryPage,
  type FlowQueryQuality,
  type FlowQueryResult,
  type FlowQueryUsage
} from "./flow-query-types.js";

export const FLOW_QUERY_RESULT_CONTRACT = "ferric.flow.query.result/v1";
export const FLOW_EXPLAIN_CONTRACT = "ferric.flow.explain/v1";
export const FLOW_QUERY_INDEXES_CONTRACT = "ferric.flow.query.indexes/v1";

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
const MAX_UNSIGNED_64 = (1n << 64n) - 1n;

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
    if (usage.resultRecords !== records.length) {
      throw decodeError("FLOW.QUERY usage result_records does not match records", value);
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
    bounds,
    actual,
    diagnostic,
    raw: freezeMap(mapping)
  });
}

export function tryDecodeFlowQueryError(
  value: unknown,
  cause?: unknown
): FlowQueryError | undefined {
  try {
    const mapping = requiredMap(value, "FLOW.QUERY diagnostic");
    const contextValue = field(mapping, "context");
    const context = contextValue == null
      ? undefined
      : freezeMetadataMap(
        requiredMap(contextValue, "FLOW.QUERY diagnostic context"),
        "FLOW.QUERY diagnostic context"
      );
    const position = decodePosition(field(mapping, "position"));
    return new FlowQueryError({
      code: requiredText(mapping, "code", "FLOW.QUERY diagnostic"),
      message: requiredText(mapping, "message", "FLOW.QUERY diagnostic"),
      detail: optionalText(mapping, "detail", "FLOW.QUERY diagnostic"),
      hint: optionalText(mapping, "hint", "FLOW.QUERY diagnostic"),
      retryable: requiredBoolean(mapping, "retryable", "FLOW.QUERY diagnostic"),
      safeToRetry: requiredBoolean(mapping, "safe_to_retry", "FLOW.QUERY diagnostic"),
      retryAfterMs: nonNegativeInteger(
        field(mapping, "retry_after_ms"),
        "FLOW.QUERY diagnostic retry_after_ms"
      ),
      position,
      context,
      raw: value,
      cause
    });
  } catch (error) {
    if (error instanceof FerricStoreError) return undefined;
    throw error;
  }
}

export function decodeFlowQueryIndexStatus(value: unknown): FlowQueryIndexStatus {
  const mapping = requiredMap(value, "FLOW.QUERY.INDEXES");
  requireContract(
    mapping,
    "contract_version",
    FLOW_QUERY_INDEXES_CONTRACT,
    "FLOW.QUERY.INDEXES"
  );
  const registry = requiredMap(field(mapping, "registry"), "FLOW.QUERY.INDEXES registry");
  const services = freezeMetadataMap(
    requiredMap(field(mapping, "services"), "FLOW.QUERY.INDEXES services"),
    "FLOW.QUERY.INDEXES services"
  );
  const rawIndexes = field(mapping, "indexes");
  if (!Array.isArray(rawIndexes) || rawIndexes.length > 32) {
    throw decodeError("FLOW.QUERY.INDEXES indexes must contain at most 32 entries", value);
  }
  const indexes = new Array<FlowQueryIndex>(rawIndexes.length);
  for (let index = 0; index < rawIndexes.length; index += 1) {
    if (!Object.hasOwn(rawIndexes, index)) {
      throw decodeError("FLOW.QUERY.INDEXES indexes must be a dense array", value);
    }
    indexes[index] = decodeIndex(rawIndexes[index], index);
  }
  return Object.freeze({
    contractVersion: FLOW_QUERY_INDEXES_CONTRACT,
    observedAtMs: nonNegativeInteger(
      field(mapping, "observed_at_ms"),
      "FLOW.QUERY.INDEXES observed_at_ms"
    ),
    statisticsMaxAgeMs: nonNegativeInteger(
      field(mapping, "statistics_max_age_ms"),
      "FLOW.QUERY.INDEXES statistics_max_age_ms"
    ),
    registry: Object.freeze({
      epoch: boundedInteger(
        field(registry, "epoch"),
        MAX_UNSIGNED_64,
        "FLOW.QUERY.INDEXES epoch"
      ),
      catalogVersion: positiveBoundedInteger(
        field(registry, "catalog_version"),
        MAX_UNSIGNED_64,
        "FLOW.QUERY.INDEXES catalog_version"
      )
    }),
    services,
    indexes: Object.freeze(indexes),
    raw: freezeMap(mapping)
  });
}

function decodeIndex(value: unknown, index: number): FlowQueryIndex {
  const mapping = requiredMap(value, `FLOW.QUERY.INDEXES index ${index}`);
  return Object.freeze({
    id: requiredText(mapping, "id", "FLOW.QUERY.INDEXES index"),
    version: positiveBoundedInteger(
      field(mapping, "version"),
      MAX_UNSIGNED_64,
      "FLOW.QUERY.INDEXES index version"
    ),
    buildId: requiredText(mapping, "build_id", "FLOW.QUERY.INDEXES index"),
    state: requiredText(mapping, "state", "FLOW.QUERY.INDEXES index"),
    queryable: requiredBoolean(mapping, "queryable", "FLOW.QUERY.INDEXES index"),
    coveringFields: decodeCoveringFields(mapping),
    format: decodeIndexFormat(mapping),
    raw: freezeMap(mapping)
  });
}

function decodeCoveringFields(mapping: FlowQueryResponseMap): readonly string[] {
  const raw = field(mapping, "covering_fields");
  if (!Array.isArray(raw) || raw.length > 32) {
    throw decodeError(
      "FLOW.QUERY.INDEXES index covering_fields must contain at most 32 entries",
      raw
    );
  }
  const fields = new Array<string>(raw.length);
  const seen = new Set<string>();
  for (let index = 0; index < raw.length; index += 1) {
    if (!Object.hasOwn(raw, index)) {
      throw decodeError("FLOW.QUERY.INDEXES index covering_fields must be dense", raw);
    }
    const value = boundedText(
      raw[index],
      `FLOW.QUERY.INDEXES index covering_fields entry ${index}`,
      512
    );
    if (seen.has(value)) {
      throw decodeError("FLOW.QUERY.INDEXES index covering_fields contains duplicates", raw);
    }
    seen.add(value);
    fields[index] = value;
  }
  return Object.freeze(fields);
}

function decodeIndexFormat(mapping: FlowQueryResponseMap): FlowQueryIndexFormat {
  const raw = requiredMap(field(mapping, "format"), "FLOW.QUERY.INDEXES index format");
  if (!hasKey(raw, "counter")) {
    throw decodeError("FLOW.QUERY.INDEXES index format counter is missing", raw);
  }
  const counterValue = field(raw, "counter");
  const counter =
    counterValue === null
      ? undefined
      : boundedText(counterValue, "FLOW.QUERY.INDEXES index format counter", 128);
  return Object.freeze({
    queryRow: requiredBoundedText(raw, "query_row", "FLOW.QUERY.INDEXES index format", 128),
    key: requiredBoundedText(raw, "key", "FLOW.QUERY.INDEXES index format", 128),
    entry: requiredBoundedText(raw, "entry", "FLOW.QUERY.INDEXES index format", 128),
    reverse: requiredBoundedText(raw, "reverse", "FLOW.QUERY.INDEXES index format", 128),
    counter,
    raw: freezeMap(raw)
  });
}

function decodeQuality(value: unknown): FlowQueryQuality {
  const mapping = requiredMap(value, "FLOW.QUERY quality");
  return Object.freeze({
    exactness: requiredBoundedText(mapping, "exactness", "FLOW.QUERY quality", 64),
    freshness: requiredBoundedText(mapping, "freshness", "FLOW.QUERY quality", 64),
    coverage: requiredBoundedText(mapping, "coverage", "FLOW.QUERY quality", 64),
    pagination: requiredBoundedText(mapping, "pagination", "FLOW.QUERY quality", 64)
  });
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
  return Object.freeze(usage) as unknown as FlowQueryUsage;
}

function decodePage(value: unknown): FlowQueryPage {
  const mapping = requiredMap(value, "FLOW.QUERY page");
  const hasMore = requiredBoolean(mapping, "has_more", "FLOW.QUERY page");
  const cursor = optionalText(mapping, "cursor", "FLOW.QUERY page");
  if (cursor != null && (!cursor.startsWith("fqc1_") || Buffer.byteLength(cursor) > 4_096)) {
    throw decodeError("FLOW.QUERY page cursor is invalid", value);
  }
  if (hasMore !== (cursor != null)) {
    throw decodeError("FLOW.QUERY page has_more and cursor are inconsistent", value);
  }
  return Object.freeze({ hasMore, ...(cursor == null ? {} : { cursor }) });
}

function decodePosition(value: unknown): FlowQueryErrorPosition | undefined {
  if (value == null) return undefined;
  const mapping = requiredMap(value, "FLOW.QUERY diagnostic position");
  return Object.freeze({
    byte: positiveInteger(field(mapping, "byte"), "FLOW.QUERY diagnostic position byte"),
    line: positiveInteger(field(mapping, "line"), "FLOW.QUERY diagnostic position line"),
    column: positiveInteger(
      field(mapping, "column"),
      "FLOW.QUERY diagnostic position column"
    )
  });
}
