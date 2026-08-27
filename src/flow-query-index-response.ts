import { field } from "./internal.js";
import {
  boundedText,
  decodeError,
  freezeMap,
  hasKey,
  positiveInteger,
  requiredBoolean,
  requiredBoundedText,
  requiredMap,
  requireContract,
} from "./flow-query-response-validation.js";
import { validateFlowQueryIndexContract } from "./flow-query-index-contract.js";
import {
  decodeIndexBuild,
  decodeIndexCoverage,
  decodeIndexRetirement,
  decodeIndexStatistics,
  decodeIndexValidation,
} from "./flow-query-index-lifecycle-response.js";
import {
  decodeUniqueTextArray,
  positiveIndexInteger,
  requiredChoice,
  unsignedIndexInteger,
  type FlowQueryResponseMap,
} from "./flow-query-index-decode.js";
import type {
  FlowQueryIndex,
  FlowQueryIndexField,
  FlowQueryIndexFormat,
  FlowQueryIndexServices,
  FlowQueryIndexStatus,
} from "./flow-query-types.js";

export const FLOW_QUERY_INDEXES_CONTRACT = "ferric.flow.query.indexes/v1";

export function decodeFlowQueryIndexStatus(
  value: unknown,
  expectedId?: string,
): FlowQueryIndexStatus {
  const mapping = requiredMap(value, "FLOW.QUERY.INDEXES");
  requireContract(
    mapping,
    "contract_version",
    FLOW_QUERY_INDEXES_CONTRACT,
    "FLOW.QUERY.INDEXES",
  );
  const registry = requiredMap(
    field(mapping, "registry"),
    "FLOW.QUERY.INDEXES registry",
  );
  const services = decodeIndexServices(field(mapping, "services"));
  const rawIndexes = field(mapping, "indexes");
  if (!Array.isArray(rawIndexes) || rawIndexes.length > 32) {
    throw decodeError(
      "FLOW.QUERY.INDEXES indexes must contain at most 32 entries",
      value,
    );
  }
  const indexes = new Array<FlowQueryIndex>(rawIndexes.length);
  for (let index = 0; index < rawIndexes.length; index += 1) {
    if (!Object.hasOwn(rawIndexes, index)) {
      throw decodeError(
        "FLOW.QUERY.INDEXES indexes must be a dense array",
        value,
      );
    }
    indexes[index] = decodeIndex(rawIndexes[index], index);
  }
  const status: FlowQueryIndexStatus = Object.freeze({
    contractVersion: FLOW_QUERY_INDEXES_CONTRACT,
    observedAtMs: unsignedIndexInteger(
      field(mapping, "observed_at_ms"),
      "FLOW.QUERY.INDEXES observed_at_ms",
    ),
    statisticsMaxAgeMs: unsignedIndexInteger(
      field(mapping, "statistics_max_age_ms"),
      "FLOW.QUERY.INDEXES statistics_max_age_ms",
    ),
    registry: Object.freeze({
      epoch: unsignedIndexInteger(
        field(registry, "epoch"),
        "FLOW.QUERY.INDEXES epoch",
      ),
      catalogVersion: positiveIndexInteger(
        field(registry, "catalog_version"),
        "FLOW.QUERY.INDEXES catalog_version",
      ),
    }),
    services,
    indexes: Object.freeze(indexes),
    raw: freezeMap(mapping),
  });
  validateFlowQueryIndexContract(status, expectedId);
  return status;
}

function decodeIndex(value: unknown, index: number): FlowQueryIndex {
  const mapping = requiredMap(value, `FLOW.QUERY.INDEXES index ${index}`);
  const fields = decodeIndexFields(field(mapping, "fields"));
  return Object.freeze({
    id: requiredBoundedText(mapping, "id", "FLOW.QUERY.INDEXES index", 64),
    version: positiveIndexInteger(
      field(mapping, "version"),
      "FLOW.QUERY.INDEXES index version",
    ),
    buildId: requiredBoundedText(
      mapping,
      "build_id",
      "FLOW.QUERY.INDEXES index",
      128,
    ),
    source: requiredChoice(mapping, "source", "FLOW.QUERY.INDEXES index", [
      "runs",
    ]),
    state: requiredChoice(mapping, "state", "FLOW.QUERY.INDEXES index", [
      "building",
      "validating",
      "active",
      "retiring",
      "failed",
    ]),
    queryable: requiredBoolean(
      mapping,
      "queryable",
      "FLOW.QUERY.INDEXES index",
    ),
    fields,
    workloads: decodeUniqueTextArray(
      field(mapping, "workloads"),
      "FLOW.QUERY.INDEXES index workloads",
      16,
      64,
    ),
    countPrefixes: decodeCountPrefixes(
      field(mapping, "count_prefixes"),
      fields.length,
    ),
    coveringFields: decodeCoveringFields(mapping),
    format: decodeIndexFormat(mapping),
    coverage: decodeIndexCoverage(field(mapping, "coverage")),
    build: decodeIndexBuild(field(mapping, "build")),
    validation: decodeIndexValidation(field(mapping, "validation")),
    retirement: decodeIndexRetirement(field(mapping, "retirement")),
    statistics: decodeIndexStatistics(field(mapping, "statistics")),
    raw: freezeMap(mapping),
  });
}

function decodeIndexServices(value: unknown): FlowQueryIndexServices {
  const mapping = requiredMap(value, "FLOW.QUERY.INDEXES services");
  const choices = ["ready", "unavailable"] as const;
  return Object.freeze({
    registry: requiredChoice(
      mapping,
      "registry",
      "FLOW.QUERY.INDEXES services",
      choices,
    ),
    lifecycleWorker: requiredChoice(
      mapping,
      "lifecycle_worker",
      "FLOW.QUERY.INDEXES services",
      choices,
    ),
    statisticsStore: requiredChoice(
      mapping,
      "statistics_store",
      "FLOW.QUERY.INDEXES services",
      choices,
    ),
    statisticsWorker: requiredChoice(
      mapping,
      "statistics_worker",
      "FLOW.QUERY.INDEXES services",
      choices,
    ),
    raw: freezeMap(mapping),
  });
}

function decodeIndexFields(value: unknown): readonly FlowQueryIndexField[] {
  const context = "FLOW.QUERY.INDEXES index fields";
  if (!Array.isArray(value) || value.length < 2 || value.length > 8) {
    throw decodeError(`${context} must contain 2 to 8 entries`, value);
  }
  const fields = new Array<FlowQueryIndexField>(value.length);
  const names = new Set<string>();
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw decodeError(`${context} must be dense`, value);
    }
    const mapping = requiredMap(
      value[index],
      `FLOW.QUERY.INDEXES index field ${index}`,
    );
    const name = requiredBoundedText(
      mapping,
      "name",
      "FLOW.QUERY.INDEXES index field",
      512,
    );
    if (names.has(name)) {
      throw decodeError(`${context} contain duplicates`, value);
    }
    names.add(name);
    fields[index] = Object.freeze({
      name,
      direction: requiredChoice(
        mapping,
        "direction",
        "FLOW.QUERY.INDEXES index field",
        ["asc", "desc"],
      ),
      encoding: requiredChoice(
        mapping,
        "encoding",
        "FLOW.QUERY.INDEXES index field",
        ["hashed", "ordered"],
      ),
      raw: freezeMap(mapping),
    });
  }
  return Object.freeze(fields);
}

function decodeCountPrefixes(
  value: unknown,
  fieldCount: number,
): readonly number[] {
  const context = "FLOW.QUERY.INDEXES index count_prefixes";
  if (!Array.isArray(value) || value.length > fieldCount) {
    throw decodeError(`${context} are invalid`, value);
  }
  const prefixes = new Array<number>(value.length);
  let previous = 0;
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw decodeError(`${context} must be dense`, value);
    }
    const prefix = positiveInteger(value[index], `${context} entry`);
    if (prefix > fieldCount || prefix <= previous) {
      throw decodeError(`${context} are invalid`, value);
    }
    prefixes[index] = prefix;
    previous = prefix;
  }
  return Object.freeze(prefixes);
}

function decodeCoveringFields(
  mapping: FlowQueryResponseMap,
): readonly string[] {
  const raw = field(mapping, "covering_fields");
  if (!Array.isArray(raw) || raw.length > 32) {
    throw decodeError(
      "FLOW.QUERY.INDEXES index covering_fields must contain at most 32 entries",
      raw,
    );
  }
  const fields = new Array<string>(raw.length);
  const seen = new Set<string>();
  for (let index = 0; index < raw.length; index += 1) {
    if (!Object.hasOwn(raw, index)) {
      throw decodeError(
        "FLOW.QUERY.INDEXES index covering_fields must be dense",
        raw,
      );
    }
    const value = boundedText(
      raw[index],
      `FLOW.QUERY.INDEXES index covering_fields entry ${index}`,
      512,
    );
    if (seen.has(value)) {
      throw decodeError(
        "FLOW.QUERY.INDEXES index covering_fields contains duplicates",
        raw,
      );
    }
    seen.add(value);
    fields[index] = value;
  }
  return Object.freeze(fields);
}

function decodeIndexFormat(
  mapping: FlowQueryResponseMap,
): FlowQueryIndexFormat {
  const raw = requiredMap(
    field(mapping, "format"),
    "FLOW.QUERY.INDEXES index format",
  );
  if (!hasKey(raw, "counter")) {
    throw decodeError(
      "FLOW.QUERY.INDEXES index format counter is missing",
      raw,
    );
  }
  const counterValue = field(raw, "counter");
  const counter =
    counterValue === null
      ? undefined
      : boundedText(
          counterValue,
          "FLOW.QUERY.INDEXES index format counter",
          128,
        );
  return Object.freeze({
    queryRow: requiredBoundedText(
      raw,
      "query_row",
      "FLOW.QUERY.INDEXES index format",
      128,
    ),
    key: requiredBoundedText(
      raw,
      "key",
      "FLOW.QUERY.INDEXES index format",
      128,
    ),
    entry: requiredBoundedText(
      raw,
      "entry",
      "FLOW.QUERY.INDEXES index format",
      128,
    ),
    reverse: requiredBoundedText(
      raw,
      "reverse",
      "FLOW.QUERY.INDEXES index format",
      128,
    ),
    counter,
    raw: freezeMap(raw),
  });
}
