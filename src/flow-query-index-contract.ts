import { Buffer } from "node:buffer";
import { decodeError, hasKey } from "./flow-query-response-validation.js";
import type {
  FlowQueryIndex,
  FlowQueryIndexProgress,
  FlowQueryIndexStatus,
  FlowQueryInteger,
} from "./flow-query-types.js";

type ValidatableProgress = Pick<
  FlowQueryIndexProgress,
  "phaseCounts" | "currentPhases" | "completedShards" | "totalShards" | "raw"
>;

export const FLOW_QUERY_BUILD_PHASES = ["pending", "snapshot", "backfill", "done"] as const;
export const FLOW_QUERY_VALIDATION_PHASES = [
  "pending",
  "source",
  "index",
  "counter",
  "cleanup",
  "done",
] as const;
export const FLOW_QUERY_RETIREMENT_PHASES = [
  "pending",
  "fence",
  "index",
  "counter",
  "reverse",
  "cleanup",
  "done",
] as const;

const IDENTIFIER = /^[A-Za-z0-9_.:-]+$/u;
const UNQUOTED_METADATA = /^[A-Za-z0-9_-]+$/u;
const ATTRIBUTE_SELECTOR = /^attribute\['((?:[^']|'')*)'\]$/u;
const STATE_META_SELECTOR = /^state_meta\['((?:[^']|'')*)'\]\['((?:[^']|'')*)'\]$/u;
const INTEGER_FIELDS = new Set([
  "version",
  "priority",
  "created_at_ms",
  "updated_at_ms",
  "next_run_at_ms",
  "lease_deadline_ms",
  "attempts",
  "max_active_ms",
]);
const BUILTIN_FIELDS = new Set([
  ...INTEGER_FIELDS,
  "partition_key",
  "run_id",
  "event_id",
  "type",
  "state",
  "run_state",
  "parent_flow_id",
  "root_flow_id",
  "correlation_id",
]);
const RETIREMENT_PROGRESS_FIELDS = [
  "phase_counts",
  "current_phases",
  "completed_shards",
  "total_shards",
  "deleted_entries",
  "deleted_bytes",
  "rewritten_reverse_rows",
] as const;

export function validateFlowQueryIndexContract(
  status: FlowQueryIndexStatus,
  expectedId?: string,
): void {
  let previousId: string | undefined;
  let previousVersion: bigint | undefined;
  for (const index of status.indexes) {
    const version = BigInt(index.version);
    if (
      previousId != null &&
      (index.id < previousId ||
        (index.id === previousId && previousVersion != null && version <= previousVersion))
    ) {
      fail("indexes must be uniquely sorted by id and version", status.raw);
    }
    if (expectedId != null && index.id !== expectedId) {
      fail("filtered indexes do not match the requested id", status.raw);
    }
    previousId = index.id;
    previousVersion = version;
    validateIndex(index, status);
  }
  if (expectedId != null && status.indexes.length === 0) {
    fail("filtered indexes do not match the requested id", status.raw);
  }

  const hasUnavailableStatistics = status.indexes.some(
    (index) => index.statistics.status === "unavailable",
  );
  if (status.services.statisticsStore === "unavailable") {
    if (status.indexes.some((index) => index.statistics.status !== "unavailable")) {
      fail("statistics must be unavailable when the service is unavailable", status.raw);
    }
  } else if (hasUnavailableStatistics) {
    fail("statistics cannot be unavailable while the service is ready", status.raw);
  }
}

function validateIndex(index: FlowQueryIndex, status: FlowQueryIndexStatus): void {
  if (!IDENTIFIER.test(index.id) || index.workloads.some((workload) => !IDENTIFIER.test(workload))) {
    fail("index identity contains invalid characters", index.raw);
  }
  const first = index.fields[0];
  if (first?.name !== "partition_key" || first.direction !== "asc" || first.encoding !== "hashed") {
    fail("index must begin with partition_key asc hashed", index.raw);
  }

  let attributeFields = 0;
  for (const indexField of index.fields) {
    if (indexField.encoding === "hashed" && indexField.direction !== "asc") {
      fail("hashed index fields must be ascending", index.raw);
    }
    const kind = fieldKind(indexField.name);
    if (kind == null) fail("index contains an unsupported field selector", index.raw);
    if (indexField.encoding === "ordered" && kind !== "integer") {
      fail("ordered index fields must be integers", index.raw);
    }
    if (kind === "attribute") attributeFields += 1;
  }
  if (attributeFields > 1) fail("index may contain at most one attribute field", index.raw);

  for (const prefix of index.countPrefixes) {
    if (index.fields.slice(0, prefix).some((field) => field.encoding !== "hashed")) {
      fail("count prefixes may cover only hashed fields", index.raw);
    }
  }
  if (index.coveringFields.length > 0) {
    if (index.coveringFields.some((field) => fieldKind(field) == null)) {
      fail("index contains an unsupported covering field selector", index.raw);
    }
    const required = new Set(["run_id", "version", ...index.fields.map((field) => field.name)]);
    if ([...required].some((field) => !index.coveringFields.includes(field))) {
      fail("index covering fields omit an identity or index field", index.raw);
    }
  }
  if ((index.countPrefixes.length > 0) !== (index.format.counter != null)) {
    fail("index counter format is inconsistent with count prefixes", index.raw);
  }

  validateProgress(index.build, FLOW_QUERY_BUILD_PHASES, "build");
  validateProgress(index.validation, FLOW_QUERY_VALIDATION_PHASES, "validation");
  if (index.retirement.status === "not_applicable") {
    if (RETIREMENT_PROGRESS_FIELDS.some((name) => hasKey(index.retirement.raw, name))) {
      fail("not_applicable retirement must not contain progress", index.retirement.raw);
    }
  } else {
    const retirement = index.retirement;
    if (
      retirement.phaseCounts == null ||
      retirement.currentPhases == null ||
      retirement.completedShards == null ||
      retirement.totalShards == null
    ) {
      fail("retirement progress is incomplete", retirement.raw);
    }
    validateProgress(
      {
        phaseCounts: retirement.phaseCounts,
        currentPhases: retirement.currentPhases,
        completedShards: retirement.completedShards,
        totalShards: retirement.totalShards,
        raw: retirement.raw,
      },
      FLOW_QUERY_RETIREMENT_PHASES,
      "retirement",
    );
  }

  const totals = new Set(
    [
      index.coverage.totalShards,
      index.build.totalShards,
      index.validation.totalShards,
      ...(index.retirement.totalShards == null
        ? []
        : [index.retirement.totalShards]),
    ].map(indexInteger),
  );
  if (totals.size !== 1) fail("index shard totals are inconsistent", index.raw);
  if (!sameIndexInteger(index.coverage.completeShards, index.build.completedShards)) {
    fail("coverage and build completion are inconsistent", index.raw);
  }
  if (index.coverage.validation !== index.validation.status) {
    fail("coverage and validation status are inconsistent", index.raw);
  }
  const queryable =
    index.state === "active" &&
    sameIndexInteger(
      index.coverage.completeShards,
      index.coverage.totalShards,
    ) &&
    index.coverage.validation === "passed";
  if (index.queryable !== queryable) fail("index queryable flag is inconsistent", index.raw);

  validateLifecycle(index);
  validateValidation(index);
  validateStatistics(index, status);
}

function validateProgress(
  progress: ValidatableProgress,
  phases: readonly string[],
  section: string,
): void {
  const entries = Object.entries(progress.phaseCounts);
  if (
    entries.length === 0 ||
    entries.some(
      ([phase, count]) => !phases.includes(phase) || indexInteger(count) <= 0n,
    ) ||
    entries.reduce((sum, [, count]) => sum + indexInteger(count), 0n) !==
      indexInteger(progress.totalShards)
  ) {
    fail(`${section} phase_counts are invalid`, progress.raw);
  }
  const current = phases.filter((phase) => Object.hasOwn(progress.phaseCounts, phase));
  if (
    current.length !== progress.currentPhases.length ||
    current.some((phase, position) => phase !== progress.currentPhases[position])
  ) {
    fail(`${section} current_phases are inconsistent`, progress.raw);
  }
  if (
    !sameIndexInteger(
      progress.completedShards,
      progress.phaseCounts.done ?? 0,
    )
  ) {
    fail(`${section} completed_shards is inconsistent`, progress.raw);
  }
}

function validateLifecycle(index: FlowQueryIndex): void {
  const built = sameIndexInteger(
    index.build.completedShards,
    index.build.totalShards,
  );
  const validation = index.validation.status;
  const retirement = index.retirement.status;
  const valid =
    (index.state === "building" && !built && validation === "pending" && retirement === "not_applicable") ||
    (index.state === "validating" && built && (validation === "pending" || validation === "passed") && retirement === "not_applicable") ||
    (index.state === "active" && built && validation === "passed" && retirement === "not_applicable") ||
    (index.state === "retiring" && built && validation !== "pending" && retirement !== "not_applicable") ||
    (index.state === "failed" && validation !== "pending" && retirement !== "not_applicable");
  if (!valid) fail("index lifecycle fields are inconsistent", index.raw);
}

function validateValidation(index: FlowQueryIndex): void {
  const validation = index.validation;
  const valid =
    (validation.status === "pending" && indexInteger(validation.mismatches) === 0n && validation.failureReason == null && validation.validatedAtMs == null) ||
    (validation.status === "passed" && indexInteger(validation.mismatches) === 0n && validation.failureReason == null && validation.validatedAtMs != null) ||
    (validation.status === "failed" && indexInteger(validation.mismatches) > 0n && Boolean(validation.failureReason) && validation.validatedAtMs != null);
  if (!valid) fail("validation status fields are inconsistent", validation.raw);
}

function validateStatistics(index: FlowQueryIndex, status: FlowQueryIndexStatus): void {
  const statistics = index.statistics;
  const times = [
    statistics.oldestCollectedAtMs,
    statistics.newestCollectedAtMs,
    statistics.oldestAgeMs,
    statistics.newestAgeMs,
  ];
  if (indexInteger(statistics.samples) === 0n) {
    if ((statistics.status !== "missing" && statistics.status !== "unavailable") || times.some((value) => value != null)) {
      fail("empty statistics fields are inconsistent", statistics.raw);
    }
    return;
  }
  if (times.some((value) => value == null)) {
    fail("sampled statistics require timestamps and ages", statistics.raw);
  }
  const [oldest, newest, oldestAge, newestAge] = times as [
    FlowQueryInteger,
    FlowQueryInteger,
    FlowQueryInteger,
    FlowQueryInteger,
  ];
  if (
    indexInteger(oldest) > indexInteger(newest) ||
    indexInteger(oldestAge) !== indexAge(status.observedAtMs, oldest) ||
    indexInteger(newestAge) !== indexAge(status.observedAtMs, newest)
  ) {
    fail("statistics timestamps or ages are inconsistent", statistics.raw);
  }
  const expected = sameIndexInteger(statistics.freshSamples, statistics.samples)
    ? ["fresh"]
    : indexInteger(statistics.freshSamples) === 0n
      ? indexInteger(statistics.futureSamples) > 0n ? ["stale", "future"] : ["stale"]
      : ["mixed"];
  if (!expected.includes(statistics.status)) {
    fail("statistics status does not match sample counters", statistics.raw);
  }
}

function fieldKind(name: string): "integer" | "keyword" | "attribute" | "state_meta" | undefined {
  if (BUILTIN_FIELDS.has(name)) return INTEGER_FIELDS.has(name) ? "integer" : "keyword";
  const parts = name.split(".");
  if (parts.length === 2 && parts[0] === "attribute" && validUnquoted(parts[1] ?? "")) return "attribute";
  if (parts.length === 3 && parts[0] === "state_meta" && validUnquoted(parts[1] ?? "") && validUnquoted(parts[2] ?? "")) return "state_meta";

  const attribute = ATTRIBUTE_SELECTOR.exec(name);
  if (attribute != null) {
    const metadata = (attribute[1] ?? "").replaceAll("''", "'");
    if (validMetadata(metadata, true) && name === externalSelector("attribute", metadata)) return "attribute";
  }
  const stateMeta = STATE_META_SELECTOR.exec(name);
  if (stateMeta != null) {
    const state = (stateMeta[1] ?? "").replaceAll("''", "'");
    const metadata = (stateMeta[2] ?? "").replaceAll("''", "'");
    if (validMetadata(state, false) && validMetadata(metadata, true) && name === externalSelector("state_meta", state, metadata)) return "state_meta";
  }
  return undefined;
}

function validUnquoted(value: string): boolean {
  return !value.startsWith("__") && UNQUOTED_METADATA.test(value) && Buffer.byteLength(value, "ascii") <= 64;
}

function validMetadata(value: string, rejectReserved: boolean): boolean {
  return value.length > 0 && Buffer.byteLength(value, "utf8") <= 64 && (!rejectReserved || !value.startsWith("__"));
}

function externalSelector(root: string, ...segments: string[]): string {
  return segments.every(validUnquoted)
    ? [root, ...segments].join(".")
    : root + segments.map((segment) => `['${segment.replaceAll("'", "''")}']`).join("");
}

function sameIndexInteger(
  left: FlowQueryInteger,
  right: FlowQueryInteger,
): boolean {
  return indexInteger(left) === indexInteger(right);
}

function indexAge(
  observedAtMs: FlowQueryInteger,
  collectedAtMs: FlowQueryInteger,
): bigint {
  const age = indexInteger(observedAtMs) - indexInteger(collectedAtMs);
  return age < 0n ? 0n : age;
}

function indexInteger(value: FlowQueryInteger): bigint {
  return BigInt(value);
}

function fail(message: string, raw: unknown): never {
  throw decodeError(`FLOW.QUERY.INDEXES ${message}`, raw);
}
