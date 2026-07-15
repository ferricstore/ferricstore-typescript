export type ExceptionPolicy = "retry" | "fail" | "raise";
export type BackoffKind = "fixed" | "linear" | "exponential";
export type WorkerProfile = "latency" | "throughput";
export type WorkerRefillStrategy = "continuous" | "wave";

export interface RetryPolicy {
  maxRetries?: number;
  backoff?: BackoffKind | string;
  baseMs?: number;
  maxMs?: number;
  jitterPct?: number;
  exhaustedTo?: string;
}

export interface BackpressurePolicy {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterPct?: number;
}

export interface ValueConfig {
  valueMaxBytes?: number;
  localCache?: boolean;
}

export interface WorkerConfig {
  profile?: WorkerProfile;
  /** Alias for concurrency when concurrency is omitted. */
  workers?: number;
  /** Maximum simultaneously running per-job handlers and claim credit. */
  concurrency?: number;
  /** Refill free per-job slots continuously or wait for the current wave. Defaults to continuous in run(). */
  refillStrategy?: WorkerRefillStrategy;
  /** Additional slot-refill coalescing delay in milliseconds. Zero still coalesces one event-loop turn. */
  refillDelayMs?: number;
  /** Pipeline batchable completions with replacement claims when routes match. Defaults to true. */
  fuseCompleteClaim?: boolean;
  /** Claim and terminal mutation batch size, capped at the server Flow limit of 1,000. */
  batchSize?: number;
  leaseMs?: number;
  /** Automatically extend leases while handlers run. Defaults to true. */
  leaseRenewal?: boolean;
  /** Lease-renewal interval in milliseconds. Defaults to half of leaseMs. */
  leaseRenewIntervalMs?: number;
  priority?: number;
  nowMs?: number;
  partitionKey?: string;
  partitionKeys?: string[];
  reclaimExpired?: boolean;
  reclaimRatio?: number;
  claimPayload?: boolean;
  claimValues?: string[];
  /** Include Flow attributes in compact worker claims. Defaults to false. */
  claimAttributes?: boolean;
  valueMaxBytes?: number;
  blockMs?: number;
  /** Maximum server block interval while signal is present, bounding safe shutdown latency. */
  abortPollMs?: number;
  idleSleepMs?: number;
  maxIdleSleepMs?: number;
  exceptionPolicy?: ExceptionPolicy;
  /** Persist handler Error.stack for diagnostics. Defaults to false because stacks can expose paths or secrets. */
  includeErrorStack?: boolean;
  /** Maximum asynchronous terminal batches; non-finite values use the worker-mode default. */
  completeAsyncDepth?: number;
  completeIndependent?: boolean;
  /** Maximum sequential non-blocking claims performed by one runOnce call. */
  claimDrainBatches?: number;
  signal?: AbortSignal;
  worker?: string;
}
