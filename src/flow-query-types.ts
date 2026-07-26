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
  readonly exactness: string;
  readonly freshness: string;
  readonly coverage: string;
  readonly pagination: string;
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

export interface FlowExplainResult {
  readonly version: "ferric.flow.explain/v1";
  readonly queryFingerprint: string;
  readonly status: "planned" | "rejected" | "executed";
  readonly plan: Readonly<Record<string, unknown>>;
  readonly estimate: Readonly<Record<string, unknown>>;
  readonly bounds: Readonly<Record<string, unknown>>;
  readonly actual?: FlowQueryUsage;
  readonly diagnostic?: FlowQueryError;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexRegistry {
  readonly epoch: FlowQueryInteger;
  readonly catalogVersion: FlowQueryInteger;
}

export interface FlowQueryIndexFormat {
  readonly queryRow: string;
  readonly key: string;
  readonly entry: string;
  readonly reverse: string;
  readonly counter?: string;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndex {
  readonly id: string;
  readonly version: FlowQueryInteger;
  readonly buildId: string;
  readonly state: string;
  readonly queryable: boolean;
  readonly coveringFields: readonly string[];
  readonly format: FlowQueryIndexFormat;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface FlowQueryIndexStatus {
  readonly contractVersion: "ferric.flow.query.indexes/v1";
  readonly observedAtMs: number;
  readonly statisticsMaxAgeMs: number;
  readonly registry: FlowQueryIndexRegistry;
  readonly services: Readonly<Record<string, unknown>>;
  readonly indexes: readonly FlowQueryIndex[];
  readonly raw: Readonly<Record<string, unknown>>;
}
