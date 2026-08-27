import { field } from "./internal.js";
import {
  boundedText,
  decodeError,
  freezeMap,
  hasKey,
  requiredMap,
} from "./flow-query-response-validation.js";
import {
  FLOW_QUERY_BUILD_PHASES,
  FLOW_QUERY_RETIREMENT_PHASES,
  FLOW_QUERY_VALIDATION_PHASES,
} from "./flow-query-index-contract.js";
import {
  decodeUniqueTextArray,
  indexCounter,
  indexInteger,
  optionalBoundedText,
  optionalIndexInteger,
  positiveIndexInteger,
  requiredChoice,
  unsignedIndexInteger,
  type FlowQueryResponseMap,
} from "./flow-query-index-decode.js";
import type {
  FlowQueryIndexBuild,
  FlowQueryIndexCoverage,
  FlowQueryIndexProgress,
  FlowQueryIndexRetirement,
  FlowQueryIndexStatistics,
  FlowQueryIndexValidation,
  FlowQueryInteger,
} from "./flow-query-types.js";

export function decodeIndexCoverage(value: unknown): FlowQueryIndexCoverage {
  const context = "FLOW.QUERY.INDEXES index coverage";
  const mapping = requiredMap(value, context);
  const completeShards = unsignedIndexInteger(
    field(mapping, "complete_shards"),
    `${context} complete_shards`,
  );
  const totalShards = positiveIndexInteger(
    field(mapping, "total_shards"),
    `${context} total_shards`,
  );
  if (indexInteger(completeShards) > indexInteger(totalShards)) {
    throw decodeError(`${context} complete_shards exceeds total_shards`, value);
  }
  return Object.freeze({
    completeShards,
    totalShards,
    validation: requiredChoice(mapping, "validation", context, [
      "pending",
      "passed",
      "failed",
    ]),
    raw: freezeMap(mapping),
  });
}

export function decodeIndexBuild(value: unknown): FlowQueryIndexBuild {
  const mapping = requiredMap(value, "FLOW.QUERY.INDEXES index build");
  const progress = decodeIndexProgress(
    mapping,
    "build",
    FLOW_QUERY_BUILD_PHASES,
  );
  return Object.freeze({
    ...progress,
    scannedRecords: indexCounter(mapping, "scanned_records", "build"),
    writtenEntries: indexCounter(mapping, "written_entries", "build"),
    writtenBytes: indexCounter(mapping, "written_bytes", "build"),
  });
}

export function decodeIndexValidation(
  value: unknown,
): FlowQueryIndexValidation {
  const mapping = requiredMap(value, "FLOW.QUERY.INDEXES index validation");
  if (
    !hasKey(mapping, "failure_reason") ||
    !hasKey(mapping, "validated_at_ms")
  ) {
    throw decodeError(
      "FLOW.QUERY.INDEXES index validation is missing nullable fields",
      value,
    );
  }
  const progress = decodeIndexProgress(
    mapping,
    "validation",
    FLOW_QUERY_VALIDATION_PHASES,
  );
  return Object.freeze({
    ...progress,
    status: requiredChoice(
      mapping,
      "status",
      "FLOW.QUERY.INDEXES index validation",
      ["pending", "passed", "failed"],
    ),
    checkedRecords: indexCounter(mapping, "checked_records", "validation"),
    checkedEntries: indexCounter(mapping, "checked_entries", "validation"),
    mismatches: indexCounter(mapping, "mismatches", "validation"),
    failureReason: optionalBoundedText(
      mapping,
      "failure_reason",
      "FLOW.QUERY.INDEXES index validation",
      128,
    ),
    validatedAtMs: optionalIndexInteger(
      mapping,
      "validated_at_ms",
      "FLOW.QUERY.INDEXES index validation",
    ),
  });
}

export function decodeIndexRetirement(
  value: unknown,
): FlowQueryIndexRetirement {
  const context = "FLOW.QUERY.INDEXES index retirement";
  const mapping = requiredMap(value, context);
  const status = requiredChoice(mapping, "status", context, [
    "not_applicable",
    "pending",
    "complete",
  ]);
  if (status === "not_applicable") {
    return Object.freeze({ status, raw: freezeMap(mapping) });
  }
  const completedShards = unsignedIndexInteger(
    field(mapping, "completed_shards"),
    `${context} completed_shards`,
  );
  const totalShards = positiveIndexInteger(
    field(mapping, "total_shards"),
    `${context} total_shards`,
  );
  if (indexInteger(completedShards) > indexInteger(totalShards)) {
    throw decodeError(
      `${context} completed_shards exceeds total_shards`,
      mapping,
    );
  }
  return Object.freeze({
    status,
    phaseCounts: decodePhaseCounts(
      field(mapping, "phase_counts"),
      "retirement",
    ),
    currentPhases: decodeAllowedPhases(
      field(mapping, "current_phases"),
      "retirement",
      FLOW_QUERY_RETIREMENT_PHASES,
    ),
    completedShards,
    totalShards,
    deletedEntries: indexCounter(mapping, "deleted_entries", "retirement"),
    deletedBytes: indexCounter(mapping, "deleted_bytes", "retirement"),
    rewrittenReverseRows: indexCounter(
      mapping,
      "rewritten_reverse_rows",
      "retirement",
    ),
    raw: freezeMap(mapping),
  });
}

export function decodeIndexStatistics(
  value: unknown,
): FlowQueryIndexStatistics {
  const context = "FLOW.QUERY.INDEXES index statistics";
  const mapping = requiredMap(value, context);
  for (const name of [
    "oldest_collected_at_ms",
    "newest_collected_at_ms",
    "oldest_age_ms",
    "newest_age_ms",
  ]) {
    if (!hasKey(mapping, name)) {
      throw decodeError(
        `${context} is missing required nullable fields`,
        value,
      );
    }
  }
  const samples = indexCounter(mapping, "samples", "statistics");
  const freshSamples = indexCounter(mapping, "fresh_samples", "statistics");
  const staleSamples = indexCounter(mapping, "stale_samples", "statistics");
  const futureSamples = indexCounter(mapping, "future_samples", "statistics");
  if (
    indexInteger(freshSamples) + indexInteger(staleSamples) !==
      indexInteger(samples) ||
    indexInteger(futureSamples) > indexInteger(staleSamples)
  ) {
    throw decodeError(`${context} counters are inconsistent`, value);
  }
  return Object.freeze({
    status: requiredChoice(mapping, "status", context, [
      "fresh",
      "stale",
      "future",
      "mixed",
      "missing",
      "unavailable",
    ]),
    samples,
    freshSamples,
    staleSamples,
    futureSamples,
    oldestCollectedAtMs: optionalIndexInteger(
      mapping,
      "oldest_collected_at_ms",
      context,
    ),
    newestCollectedAtMs: optionalIndexInteger(
      mapping,
      "newest_collected_at_ms",
      context,
    ),
    oldestAgeMs: optionalIndexInteger(mapping, "oldest_age_ms", context),
    newestAgeMs: optionalIndexInteger(mapping, "newest_age_ms", context),
    raw: freezeMap(mapping),
  });
}

function decodeIndexProgress(
  mapping: FlowQueryResponseMap,
  section: string,
  allowedPhases: readonly string[],
): FlowQueryIndexProgress {
  const context = `FLOW.QUERY.INDEXES index ${section}`;
  const completedShards = unsignedIndexInteger(
    field(mapping, "completed_shards"),
    `${context} completed_shards`,
  );
  const totalShards = positiveIndexInteger(
    field(mapping, "total_shards"),
    `${context} total_shards`,
  );
  if (indexInteger(completedShards) > indexInteger(totalShards)) {
    throw decodeError(
      `${context} completed_shards exceeds total_shards`,
      mapping,
    );
  }
  return Object.freeze({
    scope: requiredChoice(mapping, "scope", context, ["catalog_build"]),
    phaseCounts: decodePhaseCounts(field(mapping, "phase_counts"), section),
    currentPhases: decodeAllowedPhases(
      field(mapping, "current_phases"),
      section,
      allowedPhases,
    ),
    completedShards,
    totalShards,
    raw: freezeMap(mapping),
  });
}

function decodePhaseCounts(
  value: unknown,
  section: string,
): Readonly<Record<string, FlowQueryInteger>> {
  const context = `FLOW.QUERY.INDEXES index ${section} phase_counts`;
  const mapping = requiredMap(value, context);
  const entries =
    mapping instanceof Map ? [...mapping.entries()] : Object.entries(mapping);
  if (entries.length > 16) {
    throw decodeError(`${context} contains too many entries`, value);
  }
  const result = Object.create(null) as Record<string, FlowQueryInteger>;
  for (const [rawPhase, rawCount] of entries) {
    const phase = boundedText(rawPhase, `${context} phase`, 64);
    if (Object.hasOwn(result, phase)) {
      throw decodeError(`${context} contains duplicate phases`, value);
    }
    result[phase] = unsignedIndexInteger(rawCount, `${context} ${phase}`);
  }
  return Object.freeze(result);
}

function decodeAllowedPhases(
  value: unknown,
  section: string,
  allowed: readonly string[],
): readonly string[] {
  const phases = decodeUniqueTextArray(
    value,
    `FLOW.QUERY.INDEXES index ${section} current_phases`,
    allowed.length,
    64,
  );
  if (phases.some((phase) => !allowed.includes(phase))) {
    throw decodeError(
      `FLOW.QUERY.INDEXES index ${section} current_phases are invalid`,
      value,
    );
  }
  return phases;
}
