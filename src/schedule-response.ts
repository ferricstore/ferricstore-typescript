import { arrayResponse } from "./internal.js";
import { adminRecordResponse } from "./client-helpers.js";
import type {
  FlowAdminRecord,
  ScheduleFireDueResult,
  ScheduleFireResult,
  ScheduleRecord
} from "./client-admin-options.js";

const SCHEDULE_INTEGER_FIELDS = [
  "end_at_ms",
  "last_catchup_at_ms",
  "last_fire_at_ms",
  "last_overlap_at_ms",
  "last_skipped_at_ms",
  "max_fires",
  "next_run_at_ms",
  "overlap_queued_due_at_ms"
] as const;

const REQUIRED_SCHEDULE_INTEGER_FIELDS = [
  "attempts",
  "coalesced_count",
  "fire_count",
  "last_coalesced_count",
  "skipped_count"
] as const;

const OPTIONAL_SCHEDULE_TEXT_FIELDS = [
  "cron",
  "end_reason",
  "flow_id",
  "last_overlap_reason",
  "last_overlap_target_id",
  "last_planning_error",
  "last_target_id",
  "timezone"
] as const;

const SCHEDULE_KINDS = new Set(["one_shot", "delay", "interval", "cron"]);
const SCHEDULE_STATES = new Set([
  "active", "paused", "running", "completed", "failed", "cancelled"
]);
const SCHEDULE_OVERLAP_POLICIES = new Set([
  "allow", "skip", "queue_after_previous", "fail_schedule"
]);

export function scheduleRecordResponse(value: unknown, context: string): ScheduleRecord {
  const record = adminRecordResponse(value, context);
  if (typeof record.id !== "string" || record.id.length === 0) {
    throw new TypeError(`${context} response missing id`);
  }
  if (typeof record.kind !== "string" || !SCHEDULE_KINDS.has(record.kind)) {
    throw new TypeError(`${context} response missing or invalid kind`);
  }
  if (typeof record.state !== "string" || !SCHEDULE_STATES.has(record.state)) {
    throw new TypeError(`${context} response missing or invalid state`);
  }
  if (typeof record.target !== "object" || record.target == null || Array.isArray(record.target)) {
    throw new TypeError(`${context} response missing or invalid target`);
  }
  const target = record.target as FlowAdminRecord;
  if (typeof target.type !== "string" || target.type.length === 0) {
    throw new TypeError(`${context} response missing or invalid target type`);
  }
  if (!("catchup_policy" in record) ||
    (record.kind === "interval" && record.catchup_policy !== "fire_once") ||
    (record.kind !== "interval" && record.catchup_policy != null)) {
    throw new TypeError(`${context} returned an invalid catchup_policy`);
  }
  if (typeof record.overlap_policy !== "string" ||
    !SCHEDULE_OVERLAP_POLICIES.has(record.overlap_policy)) {
    throw new TypeError(`${context} response missing or invalid overlap_policy`);
  }
  safeNonNegativeInteger(record, "created_at_ms", context, true);
  const everyMs = requiredNullableNonNegativeInteger(record, "every_ms", context);
  const cron = requiredNullableResponseText(record, "cron", context);
  const timezone = requiredNullableResponseText(record, "timezone", context);
  const overlapRetryMs = requiredNullableNonNegativeInteger(
    record,
    "overlap_retry_ms",
    context
  );
  validateRecurrence(record, context, everyMs, cron, timezone, overlapRetryMs);
  let coalescedCount = 0;
  let lastCoalescedCount = 0;
  for (const field of REQUIRED_SCHEDULE_INTEGER_FIELDS) {
    const value = safeNonNegativeInteger(record, field, context, true);
    if (field === "coalesced_count") coalescedCount = value;
    if (field === "last_coalesced_count") lastCoalescedCount = value;
  }
  for (const field of SCHEDULE_INTEGER_FIELDS) {
    if (record[field] != null) safeNonNegativeInteger(record, field, context);
  }
  for (const field of OPTIONAL_SCHEDULE_TEXT_FIELDS) {
    optionalResponseText(record, field, context);
  }
  validateCatchupState(record, context, coalescedCount, lastCoalescedCount);
  return record as ScheduleRecord;
}

function validateRecurrence(
  record: FlowAdminRecord,
  context: string,
  everyMs: number | null,
  cron: string | null,
  timezone: string | null,
  overlapRetryMs: number | null
): void {
  if (record.kind === "interval") {
    if (everyMs == null || everyMs <= 0) {
      throw new TypeError(`${context} interval every_ms must be positive`);
    }
  } else if (everyMs != null) {
    throw new TypeError(`${context} every_ms is only valid for interval schedules`);
  }

  if (record.kind === "cron") {
    if (cron == null) throw new TypeError(`${context} cron schedule is missing cron`);
    if (timezone == null) throw new TypeError(`${context} cron schedule is missing timezone`);
  } else if (cron != null) {
    throw new TypeError(`${context} cron is only valid for cron schedules`);
  } else if (timezone != null) {
    throw new TypeError(`${context} timezone is only valid for cron schedules`);
  }

  if (record.kind !== "interval" && record.kind !== "cron" &&
    record.overlap_policy !== "allow") {
    throw new TypeError(`${context} overlap_policy is only valid for recurring schedules`);
  }

  if (overlapRetryMs != null) {
    if (overlapRetryMs <= 0) {
      throw new TypeError(`${context} overlap_retry_ms must be positive`);
    }
    if (record.overlap_policy !== "queue_after_previous") {
      throw new TypeError(`${context} overlap_retry_ms requires queue_after_previous`);
    }
  }
}

function validateCatchupState(
  record: FlowAdminRecord,
  context: string,
  coalescedCount: number,
  lastCoalescedCount: number
): void {
  const lastCatchupAt = record.last_catchup_at_ms;
  if (record.kind !== "interval") {
    if (coalescedCount !== 0) {
      throw new TypeError(`${context} non-interval coalesced_count must be zero`);
    }
    if (lastCoalescedCount !== 0) {
      throw new TypeError(`${context} non-interval last_coalesced_count must be zero`);
    }
    if (lastCatchupAt != null) {
      throw new TypeError(`${context} non-interval last_catchup_at_ms must be null`);
    }
    return;
  }
  if (lastCoalescedCount > coalescedCount) {
    throw new TypeError(`${context} last_coalesced_count exceeds coalesced_count`);
  }
  if (lastCoalescedCount > 0 && lastCatchupAt == null) {
    throw new TypeError(`${context} response missing last_catchup_at_ms after catch-up`);
  }
  if (lastCoalescedCount === 0 && lastCatchupAt != null) {
    throw new TypeError(`${context} last_catchup_at_ms requires a catch-up`);
  }
}

export function optionalScheduleRecord(value: unknown, context: string): ScheduleRecord | null {
  return value == null ? null : scheduleRecordResponse(value, context);
}

export function scheduleRecordList(value: unknown, context: string): ScheduleRecord[] {
  return arrayResponse(value).map((item) => scheduleRecordResponse(item, context));
}

export function scheduleFireDueResponse(value: unknown): ScheduleFireDueResult {
  const context = "FLOW.SCHEDULE.FIRE_DUE";
  const record = adminRecordResponse(value, context);
  const claimed = safeNonNegativeInteger(record, "claimed", context, true);
  const fired = safeNonNegativeInteger(record, "fired", context, true);
  const skipped = safeNonNegativeInteger(record, "skipped", context, true);
  const coalesced = safeNonNegativeInteger(record, "coalesced", context, true);

  if (!Array.isArray(record.errors)) {
    throw new TypeError(`${context} response missing errors`);
  }
  for (const error of record.errors) {
    if (!Array.isArray(error) || error.length !== 2 ||
      typeof error[0] !== "string" || error[0].length === 0 ||
      typeof error[1] !== "string" || error[1].length === 0) {
      throw new TypeError(`${context} returned an invalid error entry`);
    }
  }
  if (fired + skipped + record.errors.length !== claimed) {
    throw new TypeError(`${context} outcomes do not equal claimed`);
  }
  optionalResponseText(record, "claim_error", context);
  const lastTargetId = optionalResponseText(record, "last_target_id", context);
  const lastSkipReason = optionalResponseText(record, "last_skip_reason", context);
  if (fired > 0 && lastTargetId == null) {
    throw new TypeError(`${context} response missing last_target_id`);
  }
  if (fired === 0 && lastTargetId != null) {
    throw new TypeError(`${context} last_target_id requires a fired outcome`);
  }
  if (skipped > 0 && lastSkipReason == null) {
    throw new TypeError(`${context} response missing last_skip_reason`);
  }
  if (skipped === 0 && lastSkipReason != null) {
    throw new TypeError(`${context} last_skip_reason requires a skipped outcome`);
  }
  if (coalesced > 0 && fired + skipped === 0) {
    throw new TypeError(`${context} coalesced count requires a fired or skipped outcome`);
  }
  return record as ScheduleFireDueResult;
}

export function scheduleFireResponse(value: unknown): ScheduleFireResult {
  const context = "FLOW.SCHEDULE.FIRE";
  const record = adminRecordResponse(value, context);
  const schedule = scheduleRecordResponse(record.schedule, `${context} schedule`);
  const fired = safeNonNegativeInteger(record, "fired", context, true);
  const skipped = record.skipped == null
    ? 0
    : safeNonNegativeInteger(record, "skipped", context);

  if (fired > 1 || skipped > 1 || fired + skipped !== 1) {
    throw new TypeError(`${context} outcomes must equal one`);
  }
  if (fired === 1 && (typeof record.target_id !== "string" || record.target_id.length === 0)) {
    throw new TypeError(`${context} response missing target_id`);
  }
  if (skipped === 1 && (typeof record.reason !== "string" || record.reason.length === 0)) {
    throw new TypeError(`${context} response missing reason`);
  }
  if (fired === 0 && record.target_id != null) {
    throw new TypeError(`${context} target_id requires a fired outcome`);
  }
  if (skipped === 0 && record.reason != null) {
    throw new TypeError(`${context} reason requires a skipped outcome`);
  }

  return { ...record, fired, skipped, schedule } as ScheduleFireResult;
}

function optionalResponseText(
  record: FlowAdminRecord,
  field: string,
  context: string
): string | undefined {
  const value = record[field];
  if (value == null) return undefined;
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${context} ${field} must be non-empty text`);
  }
  return value;
}

function requiredNullableResponseText(
  record: FlowAdminRecord,
  field: string,
  context: string
): string | null {
  if (!(field in record)) throw new TypeError(`${context} response missing ${field}`);
  if (record[field] === undefined) {
    throw new TypeError(`${context} response missing ${field}`);
  }
  return optionalResponseText(record, field, context) ?? null;
}

function requiredNullableNonNegativeInteger(
  record: FlowAdminRecord,
  field: string,
  context: string
): number | null {
  if (!(field in record)) throw new TypeError(`${context} response missing ${field}`);
  if (record[field] === undefined) {
    throw new TypeError(`${context} response missing ${field}`);
  }
  if (record[field] === null) return null;
  return safeNonNegativeInteger(record, field, context);
}

function safeNonNegativeInteger(
  record: FlowAdminRecord,
  field: string,
  context: string,
  required = false
): number {
  if (!(field in record) || record[field] == null) {
    if (required) throw new TypeError(`${context} response missing ${field}`);
    return 0;
  }
  const value = record[field];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${context} ${field} must be a safe non-negative integer`);
  }
  return value;
}
