import type {
  ScheduleFireDueOptions,
  ScheduleFireOptions,
  ScheduleKind,
  ScheduleListOptions,
  ScheduleOptions
} from "./client-options.js";

const MAX_EXACT_INTEGER = Number.MAX_SAFE_INTEGER;
const SCHEDULE_KINDS = new Set(["one_shot", "delay", "interval", "cron"]);
const RECURRING_KINDS = new Set<ScheduleKind>(["interval", "cron"]);
const OVERLAP_POLICIES = new Set([
  "allow",
  "skip",
  "queue_after_previous",
  "fail_schedule"
]);
const SCHEDULE_STATES = new Set([
  "active",
  "paused",
  "running",
  "completed",
  "failed",
  "cancelled",
  "all"
]);
const TARGET_FIELDS = new Set([
  "correlation_id",
  "id",
  "id_prefix",
  "parent_flow_id",
  "partition_key",
  "payload",
  "payload_ref",
  "priority",
  "root_flow_id",
  "run_at_ms",
  "state",
  "type",
  "value_refs",
  "values"
]);
const TARGET_TEXT_FIELDS = [
  "state",
  "id",
  "id_prefix",
  "partition_key",
  "correlation_id",
  "parent_flow_id",
  "root_flow_id",
  "payload_ref"
] as const;
const INTERNAL_SCHEDULE_TYPE = "__ferricstore_schedule";
const INTERNAL_SCHEDULE_ID_PREFIX = "__ferricstore_schedule__:";

export function validateScheduleCreate(id: unknown, options: ScheduleOptions): void {
  requiredText(id, "id");
  if (!mapping(options)) throw new TypeError("schedule options must be a mapping");

  const kind = effectiveKind(options);
  validateTiming(kind, options);
  const recurring = RECURRING_KINDS.has(kind);
  validateTarget(options.target, recurring);

  nonNegativeInteger(options.atMs, "at_ms");
  nonNegativeInteger(options.delayMs, "delay_ms");
  nonNegativeInteger(options.startAtMs, "start_at_ms");
  positiveInteger(options.everyMs, "every_ms");
  positiveInteger(options.overlapRetryMs, "overlap_retry_ms");
  positiveInteger(options.maxFires, "max_fires");
  nonNegativeInteger(options.endAtMs, "end_at_ms");
  nonNegativeInteger(options.nowMs, "now_ms");
  optionalBoolean(options.overwrite, "overwrite");

  if (kind === "delay") {
    if (options.delayMs == null) throw new TypeError("delay_ms is required for delay schedules");
    if (options.nowMs != null && options.nowMs > MAX_EXACT_INTEGER - options.delayMs) {
      throw new TypeError("now_ms plus delay_ms exceeds the exact integer range");
    }
  }
  if (kind === "interval" && options.everyMs == null) {
    throw new TypeError("every_ms is required for interval schedules");
  }
  if (kind === "cron") {
    requiredText(options.cron, "cron");
    optionalText(options.timezone, "timezone");
  } else if (options.timezone != null) {
    throw new TypeError("timezone is only supported for cron schedules");
  }

  if (kind === "interval") {
    if (options.catchupPolicy != null && options.catchupPolicy !== "fire_once") {
      throw new TypeError("catchup_policy must be fire_once");
    }
  } else if (options.catchupPolicy != null) {
    throw new TypeError("catchup_policy is only supported for interval schedules");
  }

  if (recurring) {
    if (options.overlapPolicy != null && !OVERLAP_POLICIES.has(options.overlapPolicy)) {
      throw new TypeError(
        "overlap_policy must be allow, skip, queue_after_previous, or fail_schedule"
      );
    }
    if (options.overlapRetryMs != null && options.overlapPolicy !== "queue_after_previous") {
      throw new TypeError("overlap_retry_ms requires overlap_policy queue_after_previous");
    }
    const firstRun = knownFirstRun(kind, options);
    if (options.endAtMs != null && firstRun != null && options.endAtMs < firstRun) {
      throw new TypeError("end_at_ms must be at or after first run");
    }
  } else {
    if (options.overlapPolicy != null) {
      throw new TypeError("overlap_policy is only supported for recurring schedules");
    }
    if (options.overlapRetryMs != null) {
      throw new TypeError("overlap_retry_ms requires overlap_policy queue_after_previous");
    }
    if (options.maxFires != null) {
      throw new TypeError("max_fires is only supported for recurring schedules");
    }
    if (options.endAtMs != null) {
      throw new TypeError("end_at_ms is only supported for recurring schedules");
    }
  }
}

export function validateScheduleID(id: unknown): void {
  requiredText(id, "id");
}

export function validateScheduleFire(id: unknown, options: ScheduleFireOptions): void {
  validateScheduleID(id);
  nonNegativeInteger(options.fireAtMs, "fire_at_ms");
  nonNegativeInteger(options.nowMs, "now_ms");
}

export function validateScheduleStatus(id: unknown, nowMs: unknown): void {
  validateScheduleID(id);
  nonNegativeInteger(nowMs, "now_ms");
}

export function validateScheduleFireDue(options: ScheduleFireDueOptions): void {
  if (!mapping(options)) throw new TypeError("schedule fire-due options must be a mapping");
  nonNegativeInteger(options.nowMs, "now_ms");
  optionalText(options.worker, "worker");
  positiveInteger(options.leaseMs, "lease_ms");
  nonNegativeInteger(options.blockMs, "block_ms");
  positiveInteger(options.limit, "limit");

  const leaseMs = options.leaseMs ?? 30_000;
  if (options.nowMs != null && options.nowMs > MAX_EXACT_INTEGER - leaseMs) {
    throw new TypeError("now_ms plus lease_ms exceeds the exact integer range");
  }
}

export function validateScheduleList(options: ScheduleListOptions): void {
  if (!mapping(options)) throw new TypeError("schedule list options must be a mapping");
  if (options.kind != null && !SCHEDULE_KINDS.has(options.kind)) {
    throw new TypeError("kind must be one_shot, delay, interval, or cron");
  }
  if (options.state != null && !SCHEDULE_STATES.has(options.state)) {
    throw new TypeError("state is invalid");
  }
  optionalText(options.timezone, "timezone");
  optionalText(options.targetType, "target_type");
  nonNegativeInteger(options.fromMs, "from_ms");
  nonNegativeInteger(options.toMs, "to_ms");
  if (options.fromMs != null && options.toMs != null && options.fromMs > options.toMs) {
    throw new TypeError("from_ms must not exceed to_ms");
  }
  positiveInteger(options.count, "count");
  optionalBoolean(options.rev, "rev");
}

function effectiveKind(options: ScheduleOptions): ScheduleKind {
  if (options.kind != null) {
    if (!SCHEDULE_KINDS.has(options.kind)) {
      throw new TypeError("kind must be one_shot, delay, interval, or cron");
    }
    return options.kind;
  }
  if (options.cron != null) return "cron";
  if (options.everyMs != null) return "interval";
  if (options.delayMs != null) return "delay";
  return "one_shot";
}

function validateTiming(kind: ScheduleKind, options: ScheduleOptions): void {
  if (options.atMs != null && options.startAtMs != null) {
    throw new TypeError("cannot set both at_ms and start_at_ms");
  }
  if (options.delayMs != null && kind !== "delay") {
    throw new TypeError("delay_ms is only supported for delay schedules");
  }
  if (options.everyMs != null && kind !== "interval") {
    throw new TypeError("every_ms is only supported for interval schedules");
  }
  if (options.cron != null && kind !== "cron") {
    throw new TypeError("cron is only supported for cron schedules");
  }
  if (kind === "delay" && (options.atMs != null || options.startAtMs != null)) {
    throw new TypeError("at_ms and start_at_ms are not supported for delay schedules");
  }
}

function validateTarget(value: unknown, recurring: boolean): void {
  if (!mapping(value)) throw new TypeError("target must be a mapping with a non-empty type");
  const target = value as Record<string, unknown>;
  for (const key of Object.keys(target)) {
    if (!TARGET_FIELDS.has(key)) throw new TypeError(`target contains unknown field ${key}`);
  }

  const type = own(target, "type");
  requiredText(type, "target type");
  if (type === INTERNAL_SCHEDULE_TYPE) {
    throw new TypeError("target type is reserved for internal use");
  }
  for (const key of TARGET_TEXT_FIELDS) optionalText(own(target, key), `target ${key}`);

  const id = own(target, "id");
  const idPrefix = own(target, "id_prefix");
  if (id != null && idPrefix != null) {
    throw new TypeError("target cannot set both id and id_prefix");
  }
  if (recurring && id != null) {
    throw new TypeError("target id is not supported for recurring schedules; use id_prefix");
  }
  if (typeof id === "string" && id.startsWith(INTERNAL_SCHEDULE_ID_PREFIX)) {
    throw new TypeError("target id is reserved for internal use");
  }
  if (typeof idPrefix === "string" && idPrefix.startsWith(INTERNAL_SCHEDULE_ID_PREFIX)) {
    throw new TypeError("target id_prefix is reserved for internal use");
  }

  nonNegativeInteger(own(target, "run_at_ms"), "target run_at_ms");
  const priority = own(target, "priority");
  if (
    priority != null &&
    (typeof priority !== "number" || !Number.isSafeInteger(priority) || priority < 0 || priority > 2)
  ) {
    throw new TypeError("target priority must be an integer between 0 and 2");
  }
}

function knownFirstRun(kind: ScheduleKind, options: ScheduleOptions): number | undefined {
  if (kind === "delay") {
    if (options.nowMs == null || options.delayMs == null) return undefined;
    return options.nowMs + options.delayMs;
  }
  if (kind === "cron") return options.startAtMs ?? options.atMs ?? options.nowMs;
  return options.atMs ?? options.startAtMs ?? options.nowMs;
}

function requiredText(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}

function optionalText(value: unknown, name: string): void {
  if (value != null) requiredText(value, name);
}

function nonNegativeInteger(value: unknown, name: string): void {
  if (value != null && (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)) {
    throw new TypeError(`${name} must be a non-negative exact integer`);
  }
}

function positiveInteger(value: unknown, name: string): void {
  if (value != null && (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0)) {
    throw new TypeError(`${name} must be a positive exact integer`);
  }
}

function optionalBoolean(value: unknown, name: string): void {
  if (value != null && typeof value !== "boolean") {
    throw new TypeError(`${name} must be a boolean`);
  }
}

function mapping(value: unknown): boolean {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function own(mapping: Record<string, unknown>, key: string): unknown {
  return Object.prototype.hasOwnProperty.call(mapping, key) ? mapping[key] : undefined;
}
