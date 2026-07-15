import type { Codec } from "./codecs.js";
import type { NativeClientOptions, ReconnectOptions } from "./adapters.js";
import type { CommandArgument } from "./internal.js";
import type {
  BackpressurePolicy,
  ClaimedItem,
  FencingToken,
  FlowRecord,
  MaxActiveMs,
  RetryPolicy,
  StateMeta
} from "./types.js";
export type {
  AdminListOptions,
  ApprovalListOptions,
  ApprovalRequestOptions,
  AttributeQueryOptions,
  BudgetCommitOptions,
  BudgetReserveOptions,
  CircuitOpenOptions,
  EffectCompensateOptions,
  EffectConfirmOptions,
  EffectFailOptions,
  EffectReserveOptions,
  EffectStatusOptions,
  FlowAdminRecord,
  FlowStatsOptions,
  GovernanceLedgerOptions,
  LimitAmountOptions,
  LimitLeaseOptions,
  LimitListOptions,
  LimitReleaseOptions,
  ScheduleFireDueOptions,
  ScheduleListOptions,
  ScheduleOptions
} from "./client-admin-options.js";

export interface FlowBatchCompletedItem {
  readonly index: number;
  readonly value: unknown;
}

export interface ClaimHydrationItem {
  readonly index: number;
  readonly record: FlowRecord;
}

/** A legacy compact claim leased jobs successfully, but one or more fallback reads failed. */

export interface AutoBatchOptions {
  enabled?: boolean;
  maxCommands?: number;
  maxDelayMs?: number;
  mode?: "safe" | "all";
}

export interface FerricStoreClientOptions {
  autoBatch?: boolean | AutoBatchOptions;
  codec?: Codec;
  backpressure?: BackpressurePolicy;
  /** Maximum items per Flow `*_MANY` server request. Must match `flow_max_batch_items`; defaults to 1,000. */
  flowManyBatchLimit?: number;
  /** Maximum concurrent FLOW.GET fallbacks for legacy compact full-claim responses. Defaults to 16. */
  legacyClaimHydrationConcurrency?: number;
}

export interface FerricStoreClientFromUrlOptions extends FerricStoreClientOptions {
  nativeOptions?: NativeClientOptions;
  reconnect?: boolean | ReconnectOptions;
}

export interface CreateOptions {
  type: string;
  state?: string;
  payload?: unknown;
  partitionKey?: string;
  parentFlowId?: string;
  rootFlowId?: string;
  correlationId?: string;
  runAtMs?: number;
  nowMs?: number;
  priority?: number;
  idempotent?: boolean;
  retentionTtlMs?: number;
  /** Maximum active lifetime from creation, or "infinity" to disable a type policy timeout. */
  maxActiveMs?: MaxActiveMs;
  /** Attribute values stored on the Flow and available to configured attribute indexes. */
  attributes?: Record<string, CommandArgument>;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  stateMeta?: StateMeta;
  returnRecord?: boolean;
}

export interface CreateManyOptions extends Omit<CreateOptions, "payload" | "partitionKey" | "returnRecord"> {
  partitionKey?: string;
  independent?: boolean;
  /** Maximum auto-partition batches sent concurrently. Defaults to 8. */
  autoPartitionConcurrency?: number;
  /** Maximum items per auto-partition request, capped by the client's `flowManyBatchLimit`. */
  autoPartitionBatchSize?: number;
}

export interface StartAndClaimOptions {
  type: string;
  initialState: string;
  worker: string;
  leaseMs?: number;
  payload?: unknown;
  partitionKey?: string;
  parentFlowId?: string;
  rootFlowId?: string;
  correlationId?: string;
  nowMs?: number;
  priority?: number;
  retentionTtlMs?: number;
  attributes?: Record<string, CommandArgument>;
  stateMeta?: StateMeta;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
}

export interface ClaimDueOptions {
  state?: string;
  states?: string[];
  worker: string;
  partitionKey?: string;
  partitionKeys?: string[];
  leaseMs?: number;
  limit?: number;
  priority?: number;
  nowMs?: number;
  blockMs?: number;
  reclaimExpired?: boolean;
  reclaimRatio?: number;
  jobOnly?: boolean;
  payload?: boolean;
  payloadMaxBytes?: number;
  values?: readonly string[];
  valueMaxBytes?: number;
  includeState?: boolean;
  /** Include Flow attributes in compact job-only claims. Defaults to false to minimize wire size. */
  includeAttributes?: boolean;
}

export interface CompleteManyOptions {
  result?: unknown;
  payload?: unknown;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  dropValues?: string[];
  overrideValues?: string[];
  attributesMerge?: Record<string, CommandArgument>;
  attributesDelete?: string[];
  stateMeta?: StateMeta;
  ttlMs?: number;
  nowMs?: number;
  independent?: boolean;
  returnOkOnSuccess?: boolean;
}

export interface CompleteJobsAndClaimJobsResult {
  /** Raw FLOW.COMPLETE_MANY result, retained even when an item failed. */
  readonly completion: unknown;
  /** Any replacement leases returned by the claim half of the operation. */
  readonly claimed: (FlowRecord | ClaimedItem)[];
  /** Completion validation failure; replacement leases must still be handled. */
  readonly completionError?: Error;
  /** Claim command failure after the completion command was submitted. */
  readonly claimError?: Error;
  /** True when both commands shared one ordered transport pipeline. */
  readonly fused: boolean;
}

export interface ReclaimOptions extends Omit<ClaimDueOptions, "state" | "states" | "blockMs" | "reclaimExpired" | "reclaimRatio" | "includeState"> {
  state?: "running";
}

export type FlowStateMode = "fifo" | "parallel";

export interface FlowStatePolicy {
  mode?: FlowStateMode;
  retry?: RetryPolicy;
}

export type FlowStatePolicyLike = FlowStatePolicy | RetryPolicy;

export interface FlowPolicyOptions {
  state?: string;
  mode?: FlowStateMode;
  retry?: RetryPolicy;
  states?: Record<string, FlowStatePolicyLike>;
  retentionTtlMs?: number;
  /** Type-level maximum active lifetime for newly created Flows. */
  maxActiveMs?: MaxActiveMs;
  /** Type-level attribute names projected for FLOW.SEARCH. An empty array clears the index list. */
  indexedAttributes?: readonly string[];
  indexedStateMeta?: string;
}

export interface RequestContext {
  subject?: string;
  tenant?: string;
  scopes?: string | readonly string[];
}

export interface RequestContextOptions {
  requestContext?: RequestContext;
}

export interface InvocationCreateOptions extends RequestContextOptions {
  context?: Record<string, unknown>;
  idempotencyKey?: string;
}

export interface MutateOptions {
  partitionKey?: string;
  payload?: unknown;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  dropValues?: string[];
  overrideValues?: string[];
  attributesMerge?: Record<string, CommandArgument>;
  attributesDelete?: string[];
  stateMeta?: StateMeta;
  nowMs?: number;
  returnRecord?: boolean;
}

export interface LeaseMutationOptions extends MutateOptions {
  leaseToken: Buffer;
  fencingToken: FencingToken;
}

export interface ExtendLeaseOptions {
  leaseToken: Buffer;
  fencingToken: FencingToken;
  leaseMs: number;
  partitionKey?: string;
  nowMs?: number;
  returnOkOnSuccess?: boolean;
}

export interface TransitionOptions extends LeaseMutationOptions {
  fromState: string;
  toState: string;
  runAtMs?: number;
  priority?: number;
}

export interface StepContinueOptions extends Omit<LeaseMutationOptions, "returnRecord"> {
  fromState: string;
  toState: string;
  leaseMs?: number;
  worker?: string;
  returnJob?: boolean;
  /** Flow type to retain when returnJob requests a compact response. */
  type?: string;
}

export interface RunStepsItem {
  id: string;
  partitionKey?: string;
}

export interface RunStepsManyOptions {
  type: string;
  states?: readonly string[];
  steps?: number;
  worker: string;
  leaseMs?: number;
  nowMs?: number;
  payload?: unknown;
  result?: unknown;
  partitionKey?: string;
  retentionTtlMs?: number;
}

export interface CompleteOptions extends LeaseMutationOptions {
  result?: unknown;
  ttlMs?: number;
}

export interface RetryOptions extends LeaseMutationOptions {
  error?: unknown;
  runAtMs?: number;
}

export interface FailOptions extends LeaseMutationOptions {
  error?: unknown;
  ttlMs?: number;
}

export interface CancelOptions extends Omit<MutateOptions, "payload"> {
  fencingToken: FencingToken;
  leaseToken?: Buffer;
  reason?: unknown;
  ttlMs?: number;
}

export interface ReadOptions {
  partitionKey?: string;
  count?: number;
  fromMs?: number;
  toMs?: number;
  rev?: boolean;
  state?: string;
  terminalOnly?: boolean;
  includeCold?: boolean;
  consistentProjection?: boolean;
}

export interface HistoryOptions {
  partitionKey?: string;
  count?: number;
  fromEvent?: string;
  toEvent?: string;
  fromMs?: number;
  toMs?: number;
  fromVersion?: number;
  toVersion?: number;
  rev?: boolean;
  event?: string;
  worker?: string;
  includeCold?: boolean;
  consistentProjection?: boolean;
  values?: boolean;
  payloadMaxBytes?: number;
}

export type SearchStateMeta = StateMeta | Record<string, StateMeta>;

export interface SearchOptions extends ReadOptions {
  attributes?: Record<string, CommandArgument>;
  stateMeta?: SearchStateMeta;
}

export type ManagementPairs = Record<string, CommandArgument>;
