import { FerricStoreError } from "./errors.js";

export type FlowQueryParameter = string | Buffer | boolean | number | bigint;
export type FlowQueryParameters = Readonly<Record<string, FlowQueryParameter>>;
export type FlowQueryRecord = Readonly<Record<string, unknown>>;
export type FlowQueryInteger = number | bigint;

export interface FlowQueryPage {
  readonly hasMore: boolean;
  readonly cursor?: string;
}

export interface FlowQueryQuality {
  readonly exactness: "authoritative" | "projected_exact" | "exact" | "not_applicable";
  readonly freshness: "current" | "projection_watermark" | "not_applicable";
  readonly coverage: "complete" | "unavailable";
  readonly pagination: "none" | "complete" | "authenticated_seek" | "live_seek";
}

export interface FlowQueryUsage {
  readonly rangeSeeks: number;
  readonly rangePages: number;
  readonly scannedEntries: number;
  readonly scannedBytes: number;
  readonly hydratedRecords: number;
  readonly residualChecks: number;
  readonly duplicateEntries: number;
  readonly resultRecords: number;
  readonly responseBytes: number;
  readonly memoryHighWaterBytes: number;
  readonly wallTimeUs: number;
}

interface FlowQueryResultBase {
  readonly version: "ferric.flow.query.result/v1";
  readonly quality: FlowQueryQuality;
  readonly usage: FlowQueryUsage;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryRecordsResult extends FlowQueryResultBase {
  readonly kind: "records";
  readonly records: readonly FlowQueryRecord[];
  readonly page: FlowQueryPage;
}

export interface FlowQueryCountResult extends FlowQueryResultBase {
  readonly kind: "count";
  readonly count: FlowQueryInteger;
}

export type FlowQueryResult = FlowQueryRecordsResult | FlowQueryCountResult;

export interface FlowQueryErrorPosition {
  readonly byte: number;
  readonly line: number;
  readonly column: number;
}

export class FlowQueryError extends FerricStoreError {
  override readonly code: string;
  readonly detail?: string;
  readonly hint?: string;
  readonly position?: FlowQueryErrorPosition;
  readonly context?: Readonly<Record<string, unknown>>;

  constructor(options: {
    readonly code: string;
    readonly message: string;
    readonly detail?: string;
    readonly hint?: string;
    readonly retryable: boolean;
    readonly safeToRetry: boolean;
    readonly retryAfterMs: number;
    readonly position?: FlowQueryErrorPosition;
    readonly context?: Readonly<Record<string, unknown>>;
    readonly raw: unknown;
    readonly cause?: unknown;
  }) {
    super(options.message, {
      cause: options.cause,
      raw: options.raw,
      retryable: options.retryable,
      safeToRetry: options.safeToRetry,
      retryAfterMs: options.retryAfterMs
    });
    this.code = options.code;
    this.detail = options.detail;
    this.hint = options.hint;
    this.position = options.position;
    this.context = options.context;
  }
}

export interface FlowExplainCapabilities {
  readonly requested: readonly string[];
  readonly available: readonly string[];
  readonly missing: readonly string[];
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowExplainResult {
  readonly version: "ferric.flow.explain/v1";
  readonly queryFingerprint: string;
  readonly status: "planned" | "rejected" | "executed";
  readonly plan: Readonly<Record<string, unknown>>;
  readonly estimate: Readonly<Record<string, unknown>>;
  readonly stats?: Readonly<Record<string, unknown>>;
  readonly quality?: FlowQueryQuality;
  readonly bounds: Readonly<Record<string, unknown>>;
  readonly pressure?: Readonly<Record<string, unknown>>;
  readonly decision?: Readonly<Record<string, unknown>>;
  readonly alternatives: readonly Readonly<Record<string, unknown>>[];
  readonly capabilities?: FlowExplainCapabilities;
  readonly actual?: FlowQueryUsage;
  readonly diagnostic?: FlowQueryError;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexRegistry {
  readonly epoch: FlowQueryInteger;
  readonly catalogVersion: FlowQueryInteger;
}

export type FlowQueryIndexServiceState = "ready" | "unavailable";

export interface FlowQueryIndexServices {
  readonly registry: FlowQueryIndexServiceState;
  readonly lifecycleWorker: FlowQueryIndexServiceState;
  readonly statisticsStore: FlowQueryIndexServiceState;
  readonly statisticsWorker: FlowQueryIndexServiceState;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexField {
  readonly name: string;
  readonly direction: "asc" | "desc";
  readonly encoding: "hashed" | "ordered";
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexFormat {
  readonly queryRow: string;
  readonly key: string;
  readonly entry: string;
  readonly reverse: string;
  readonly counter?: string;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexCoverage {
  readonly completeShards: FlowQueryInteger;
  readonly totalShards: FlowQueryInteger;
  readonly validation: "pending" | "passed" | "failed";
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexProgress {
  readonly scope: "catalog_build";
  readonly phaseCounts: Readonly<Record<string, FlowQueryInteger>>;
  readonly currentPhases: readonly string[];
  readonly completedShards: FlowQueryInteger;
  readonly totalShards: FlowQueryInteger;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexBuild extends FlowQueryIndexProgress {
  readonly scannedRecords: FlowQueryInteger;
  readonly writtenEntries: FlowQueryInteger;
  readonly writtenBytes: FlowQueryInteger;
}

export interface FlowQueryIndexValidation extends FlowQueryIndexProgress {
  readonly status: "pending" | "passed" | "failed";
  readonly checkedRecords: FlowQueryInteger;
  readonly checkedEntries: FlowQueryInteger;
  readonly mismatches: FlowQueryInteger;
  readonly failureReason?: string;
  readonly validatedAtMs?: FlowQueryInteger;
}

export interface FlowQueryIndexRetirement {
  readonly status: "not_applicable" | "pending" | "complete";
  readonly phaseCounts?: Readonly<Record<string, FlowQueryInteger>>;
  readonly currentPhases?: readonly string[];
  readonly completedShards?: FlowQueryInteger;
  readonly totalShards?: FlowQueryInteger;
  readonly deletedEntries?: FlowQueryInteger;
  readonly deletedBytes?: FlowQueryInteger;
  readonly rewrittenReverseRows?: FlowQueryInteger;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexStatistics {
  readonly status: "fresh" | "stale" | "future" | "mixed" | "missing" | "unavailable";
  readonly samples: FlowQueryInteger;
  readonly freshSamples: FlowQueryInteger;
  readonly staleSamples: FlowQueryInteger;
  readonly futureSamples: FlowQueryInteger;
  readonly oldestCollectedAtMs?: FlowQueryInteger;
  readonly newestCollectedAtMs?: FlowQueryInteger;
  readonly oldestAgeMs?: FlowQueryInteger;
  readonly newestAgeMs?: FlowQueryInteger;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndex {
  readonly id: string;
  readonly version: FlowQueryInteger;
  readonly buildId: string;
  readonly source: "runs";
  readonly state: "building" | "validating" | "active" | "retiring" | "failed";
  readonly queryable: boolean;
  readonly fields: readonly FlowQueryIndexField[];
  readonly workloads: readonly string[];
  readonly countPrefixes: readonly number[];
  readonly coveringFields: readonly string[];
  readonly format: FlowQueryIndexFormat;
  readonly coverage: FlowQueryIndexCoverage;
  readonly build: FlowQueryIndexBuild;
  readonly validation: FlowQueryIndexValidation;
  readonly retirement: FlowQueryIndexRetirement;
  readonly statistics: FlowQueryIndexStatistics;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexStatus {
  readonly contractVersion: "ferric.flow.query.indexes/v1";
  readonly observedAtMs: FlowQueryInteger;
  readonly statisticsMaxAgeMs: FlowQueryInteger;
  readonly registry: FlowQueryIndexRegistry;
  readonly services: FlowQueryIndexServices;
  readonly indexes: readonly FlowQueryIndex[];
  readonly raw: Readonly<Record<string, unknown>>;
}
