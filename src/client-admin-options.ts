import type { CommandArgument } from "./internal.js";
import type { FencingToken } from "./types.js";

export type FlowAdminRecord = Record<string, unknown>;

/** Bounded interval recovery; missed occurrences are coalesced into one fire. */
export type ScheduleCatchupPolicy = "fire_once";
export type ScheduleKind = "one_shot" | "delay" | "interval" | "cron";
export type ScheduleState = "active" | "paused" | "running" | "completed" | "failed" | "cancelled";
export type ScheduleOverlapPolicy = "allow" | "skip" | "queue_after_previous" | "fail_schedule";

/** Canonical durable schedule returned by create, get, pause, resume, and list. */
export type ScheduleRecord = FlowAdminRecord & {
  id: string;
  kind: ScheduleKind;
  state: ScheduleState;
  target: Record<string, unknown>;
  /** `fire_once` for interval schedules and `null` for other kinds. */
  catchup_policy: ScheduleCatchupPolicy | null;
  /** Cumulative elapsed interval occurrences intentionally not replayed. */
  coalesced_count: number;
  /** Wall-clock recovery time of the latest bounded catch-up. */
  last_catchup_at_ms?: number | null;
  /** Occurrences coalesced by the latest recovery fire. */
  last_coalesced_count: number;
  /** Actionable recurrence error when `end_reason` is `planning_failed`. */
  last_planning_error?: string | null;
  overlap_policy: ScheduleOverlapPolicy;
  next_run_at_ms?: number | null;
  /** Targets actually created; coalesced occurrences are excluded. */
  fire_count: number;
  attempts: number;
  skipped_count: number;
};

/** Aggregate outcome from one bounded due-schedule claim batch. */
export type ScheduleFireDueResult = FlowAdminRecord & {
  claimed: number;
  fired: number;
  skipped: number;
  coalesced: number;
  errors: [string, string][];
  /** Batch-level failure encountered while requesting a later claim wave. */
  claim_error?: string | null;
  last_target_id?: string;
  last_skip_reason?: string;
};

/** Outcome from one explicit schedule fire and its canonical schedule state. */
export type ScheduleFireResult = FlowAdminRecord & {
  fired: 0 | 1;
  skipped: 0 | 1;
  target_id?: string;
  reason?: string;
  schedule: ScheduleRecord;
};

export interface FlowStatsOptions {
  state?: string;
  partitionKey?: string;
  count?: number;
  attributes?: Record<string, CommandArgument>;
  consistentProjection?: boolean;
}

export interface AttributeQueryOptions {
  state?: string;
  partitionKey?: string;
  count?: number;
  consistentProjection?: boolean;
}

export interface ScheduleOptions {
  target: Record<string, unknown>;
  kind?: ScheduleKind;
  atMs?: number;
  delayMs?: number;
  startAtMs?: number;
  everyMs?: number;
  cron?: string;
  timezone?: string;
  /** Interval-only recovery policy. Defaults to `fire_once`. */
  catchupPolicy?: ScheduleCatchupPolicy;
  overlapPolicy?: ScheduleOverlapPolicy;
  overlapRetryMs?: number;
  maxFires?: number;
  endAtMs?: number;
  overwrite?: boolean;
  nowMs?: number;
  extraOptions?: Record<string, CommandArgument>;
}

export interface ScheduleListOptions {
  kind?: ScheduleKind;
  state?: ScheduleState | "all";
  timezone?: string;
  targetType?: string;
  fromMs?: number;
  toMs?: number;
  count?: number;
  rev?: boolean;
}

export interface ScheduleFireDueOptions {
  nowMs?: number;
  worker?: string;
  leaseMs?: number;
  blockMs?: number;
  limit?: number;
}

export interface ScheduleFireOptions {
  fireAtMs?: number;
  nowMs?: number;
}

export interface EffectReserveOptions {
  partitionKey?: string;
  leaseToken?: Buffer;
  fencingToken?: FencingToken;
  operationDigest: string;
  idempotencyKey?: string;
  governanceScope?: string;
  nowMs?: number;
}

export interface EffectStatusOptions {
  partitionKey?: string;
  leaseToken?: Buffer;
  fencingToken?: FencingToken;
  nowMs?: number;
}

export interface EffectConfirmOptions extends EffectStatusOptions {
  externalId?: string;
  latencyMs?: number;
}

export interface EffectFailOptions extends EffectStatusOptions {
  error?: string;
  reason?: string;
  latencyMs?: number;
}

export interface EffectCompensateOptions extends EffectStatusOptions {
  externalId?: string;
  reason?: string;
}

export interface ApprovalRequestOptions {
  flowId: string;
  scope: string;
  reason?: string;
  requestedBy?: string;
  assignees?: readonly string[];
  policyHash?: string;
  policyVersion?: string | number;
  timeoutMs?: number;
  expiresAtMs?: number;
  nowMs?: number;
}

export interface ApprovalListOptions {
  status?: string;
  scope?: string;
  partitionKey?: string;
  flowId?: string;
  limit?: number;
}

export interface CircuitOpenOptions {
  openMs?: number;
  failureThreshold?: number;
  nowMs?: number;
}

export interface BudgetReserveOptions {
  limit?: number;
  windowMs?: number;
  reservationId?: string;
  nowMs?: number;
}

export interface BudgetCommitOptions {
  usage?: Record<string, unknown>;
  nowMs?: number;
}

export interface AdminListOptions {
  scope?: string;
  partitionKey?: string;
  limit?: number;
}

export interface LimitListOptions extends AdminListOptions {
  nowMs?: number;
}

export interface LimitLeaseOptions {
  shardId: number;
  amount: number;
  ttlMs: number;
  limit?: number;
  nowMs?: number;
}

export interface LimitAmountOptions {
  shardId: number;
  amount: number;
  nowMs?: number;
}

export interface LimitReleaseOptions {
  shardId: number;
  reservationIds: readonly string[];
  amount?: number;
}

export interface GovernanceLedgerOptions {
  partitionKey?: string;
  limit?: number;
  fromMs?: number;
  toMs?: number;
  rev?: boolean;
}
