import type { CommandArgument } from "./internal.js";
import type { FencingToken } from "./types.js";

export type FlowAdminRecord = Record<string, unknown>;

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
  kind?: string;
  atMs?: number;
  delayMs?: number;
  startAtMs?: number;
  everyMs?: number;
  cron?: string;
  timezone?: string;
  overlapPolicy?: string;
  overlapRetryMs?: number;
  maxFires?: number;
  endAtMs?: number;
  overwrite?: boolean;
  nowMs?: number;
  extraOptions?: Record<string, CommandArgument>;
}

export interface ScheduleListOptions {
  kind?: string;
  state?: string;
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
  blockMs?: number;
  limit?: number;
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
