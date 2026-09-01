import { createHash } from "node:crypto";
import type { FerricStoreClient } from "./client.js";
import type { AdvanceOptions, StepOptions, StepResult } from "./client-options.js";
import {
  ConnectionClosedError,
  FerricStoreError,
  FlowNotFoundError,
  FlowWrongStateError,
  HTTPTransportError,
  InvalidCommandError,
  RequestTimeoutError,
  StaleLeaseError
} from "./errors.js";
import { valueMGetEntries } from "./client-values.js";
import { CLAIMED_ITEM_WIRE, type ClaimedItem, type FlowRecord } from "./types.js";
import { valueRefToString } from "./workflow-utilities.js";

const DURABLE_STEP_VALUE_PREFIX = "__ferricstore_step__:sha256:";

/** @internal Hooks used by workflow workers to hand renewal to a rotated lease. */
export interface DurableStepHooks {
  beforeCommit?: () => Promise<void>;
  commitFailed?: (error: unknown) => void;
  committed?: (job: ClaimedItem) => void;
  replayed?: (job: ClaimedItem) => void;
}

/** @internal Whether a failed lease-rotating command may have committed. */
export function durableMutationMayHaveCommitted(error: unknown): boolean {
  if (error instanceof ConnectionClosedError || error instanceof RequestTimeoutError) {
    return error.requestDisposition === "possibly_sent";
  }
  if (error instanceof HTTPTransportError) return error.safeToRetry !== true;
  if (
    error instanceof FlowNotFoundError ||
    error instanceof FlowWrongStateError ||
    error instanceof InvalidCommandError ||
    error instanceof StaleLeaseError
  ) {
    return false;
  }
  if (error instanceof FerricStoreError && error.safeToRetry === true) return false;
  return true;
}

/** @internal */
export async function advanceClaim(
  client: FerricStoreClient,
  job: FlowRecord | ClaimedItem,
  options: AdvanceOptions
): Promise<ClaimedItem> {
  assertDurableClaim(job);
  assertNonEmptyString(options.toState, "toState");
  const partitionKey = job.partitionKey === "" ? undefined : job.partitionKey;
  // This helper is the implementation behind the preferred high-level API.
  // eslint-disable-next-line @typescript-eslint/no-deprecated
  const response = await client.stepContinue(job.id, {
    attributesDelete: options.attributesDelete,
    attributesMerge: options.attributesMerge,
    dropValues: options.dropValues,
    fencingToken: job.fencingToken,
    fromState: job.runState,
    leaseMs: options.leaseMs,
    leaseToken: job.leaseToken,
    nowMs: options.nowMs,
    overrideValues: options.overrideValues,
    partitionKey,
    payload: options.payload,
    returnJob: true,
    stateMeta: options.stateMeta,
    type: job.type,
    toState: options.toState,
    valueRefs: options.valueRefs,
    values: options.values
  });
  const refreshed = response as ClaimedItem;
  if (refreshed[CLAIMED_ITEM_WIRE] != null) {
    if (refreshed.runState != null && refreshed.runState !== options.toState) {
      throw new FerricStoreError("compact continuation response returned the wrong runState");
    }
    refreshed.runState ??= options.toState;
    if (refreshed.type == null && job.type != null) refreshed.type = job.type;
  }
  assertContinuationClaim(job, refreshed, options.toState);
  return refreshed;
}

/** @internal */
export async function runDurableStep<TResult>(
  client: FerricStoreClient,
  job: FlowRecord | ClaimedItem,
  options: StepOptions<TResult>,
  hooks: DurableStepHooks = {}
): Promise<StepResult<TResult>> {
  assertDurableClaim(job);
  const name = options.name;
  const run = options.run;
  const continuation = snapshotAdvanceOptions(options);
  assertNonEmptyString(name, "name");
  if (name.trim().length === 0) throw new TypeError("name must not be blank");
  if (!name.isWellFormed()) throw new TypeError("name must contain valid Unicode");
  assertNonEmptyString(continuation.toState, "toState");
  if (typeof run !== "function") throw new TypeError("run must be a function");

  const resultName = durableStepValueName(name);
  assertStepValueIsReserved(continuation, resultName);
  const leaseMs = continuation.leaseMs ?? 30_000;
  const partitionKey = job.partitionKey === "" ? undefined : job.partitionKey;
  const validated = await client.extendLease(job.id, {
    fencingToken: job.fencingToken,
    leaseMs,
    leaseToken: job.leaseToken,
    nowMs: continuation.nowMs,
    partitionKey
  });
  assertValidatedLease(job, validated);

  if (validated.valueRefs != null && Object.hasOwn(validated.valueRefs, resultName)) {
    if (validated.runState !== continuation.toState) {
      throw new FerricStoreError("committed durable step result does not match the requested target state");
    }
    const ref = valueRefToString(validated.valueRefs[resultName]);
    if (ref == null) {
      throw new FerricStoreError("committed durable step result has invalid reference metadata");
    }
    const entry = (await valueMGetEntries(client, [ref]))[0];
    if (entry?.found !== true) {
      throw new FerricStoreError("committed durable step result is missing");
    }
    const replayedJob = claimedItemFromRecord(validated);
    notifyWithoutReplacingOutcome(hooks.replayed, replayedJob);
    return { job: replayedJob, result: entry.value as TResult };
  }

  const result = normalizeStepResult(client, await run()) as TResult;
  await hooks.beforeCommit?.();
  let refreshed: ClaimedItem;
  try {
    refreshed = await advanceClaim(client, validated, {
      ...continuation,
      leaseMs,
      values: { ...continuation.values, [resultName]: result }
    });
  } catch (error) {
    notifyWithoutReplacingOutcome(hooks.commitFailed, error);
    throw error;
  }
  notifyWithoutReplacingOutcome(hooks.committed, refreshed);
  return { job: refreshed, result };
}

function notifyWithoutReplacingOutcome<T>(callback: ((value: T) => void) | undefined, value: T): void {
  try {
    callback?.(value);
  } catch {
    // The command outcome is authoritative. A local continuation notification
    // must never turn a confirmed commit, replay, or mutation failure into a
    // different result.
  }
}

function normalizeStepResult(client: FerricStoreClient, value: unknown): unknown {
  return client.codec.decode(client.codec.encode(value));
}

function durableStepValueName(name: string): string {
  const digest = createHash("sha256").update(name, "utf8").digest("hex");
  return `${DURABLE_STEP_VALUE_PREFIX}${digest}`;
}

function snapshotAdvanceOptions(options: AdvanceOptions): AdvanceOptions {
  return {
    ...(options.attributesDelete == null ? {} : { attributesDelete: [...options.attributesDelete] }),
    ...(options.attributesMerge == null ? {} : { attributesMerge: { ...options.attributesMerge } }),
    ...(options.dropValues == null ? {} : { dropValues: [...options.dropValues] }),
    ...(options.leaseMs == null ? {} : { leaseMs: options.leaseMs }),
    ...(options.nowMs == null ? {} : { nowMs: options.nowMs }),
    ...(options.overrideValues == null ? {} : { overrideValues: [...options.overrideValues] }),
    ...(options.payload === undefined ? {} : { payload: options.payload }),
    ...(options.stateMeta == null ? {} : { stateMeta: { ...options.stateMeta } }),
    toState: options.toState,
    ...(options.valueRefs == null ? {} : { valueRefs: { ...options.valueRefs } }),
    ...(options.values == null ? {} : { values: { ...options.values } })
  };
}

function assertDurableClaim(
  job: FlowRecord | ClaimedItem
): asserts job is (FlowRecord | ClaimedItem) & { runState: string } {
  if (typeof job !== "object" || job == null) throw new TypeError("job must be a claimed Flow");
  assertNonEmptyString(job.id, "job.id");
  assertNonEmptyString(job.runState, "job.runState");
  if (!Buffer.isBuffer(job.leaseToken) || job.leaseToken.byteLength === 0) {
    throw new TypeError("job.leaseToken must be a non-empty Buffer");
  }
  if (
    (typeof job.fencingToken !== "number" || !Number.isSafeInteger(job.fencingToken)) &&
    typeof job.fencingToken !== "bigint"
  ) {
    throw new TypeError("job.fencingToken must be an integer");
  }
  if (fencingValue(job.fencingToken) <= 0n) {
    throw new TypeError("job.fencingToken must be a positive integer");
  }
  if (job.state !== "running") {
    throw new TypeError("job.state must be running");
  }
}

function assertValidatedLease(
  expected: (FlowRecord | ClaimedItem) & { runState: string },
  actual: FlowRecord
): void {
  try {
    assertDurableClaim(actual);
  } catch (error) {
    throw new FerricStoreError("lease validation returned an invalid claim", { cause: error });
  }
  if (actual.id !== expected.id) throw leaseValidationMismatch("id");
  if (normalizedPartition(actual.partitionKey) !== normalizedPartition(expected.partitionKey)) {
    throw leaseValidationMismatch("partition");
  }
  if (!actual.leaseToken.equals(expected.leaseToken)) throw leaseValidationMismatch("lease token");
  if (fencingValue(actual.fencingToken) !== fencingValue(expected.fencingToken)) {
    throw leaseValidationMismatch("fencing token");
  }
  if (actual.state !== expected.state) throw leaseValidationMismatch("physical state");
  if (actual.runState !== expected.runState) throw leaseValidationMismatch("runState");
}

function assertContinuationClaim(
  previous: (FlowRecord | ClaimedItem) & { runState: string },
  refreshed: ClaimedItem,
  toState: string
): void {
  try {
    assertDurableClaim(refreshed);
  } catch (error) {
    throw new FerricStoreError("continuation response returned an invalid claim", { cause: error });
  }
  if (refreshed.id !== previous.id) throw continuationMismatch("id");
  if (normalizedPartition(refreshed.partitionKey) !== normalizedPartition(previous.partitionKey)) {
    throw continuationMismatch("partition");
  }
  if (refreshed.leaseToken.equals(previous.leaseToken)) {
    throw continuationMismatch("lease token did not change");
  }
  if (fencingValue(refreshed.fencingToken) <= fencingValue(previous.fencingToken)) {
    throw continuationMismatch("fencing token did not increase");
  }
  if (refreshed.state !== "running") throw continuationMismatch("physical state");
  if (refreshed.runState !== toState) throw continuationMismatch("runState");
}

function leaseValidationMismatch(field: string): FerricStoreError {
  return new FerricStoreError(`lease validation returned a mismatched ${field}`);
}

function continuationMismatch(field: string): FerricStoreError {
  return new FerricStoreError(`continuation response returned a mismatched ${field}`);
}

function normalizedPartition(value: string | undefined): string | undefined {
  return value == null || value === "" ? undefined : value;
}

function fencingValue(value: number | bigint): bigint {
  return typeof value === "bigint" ? value : BigInt(value);
}

function assertNonEmptyString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}

function assertStepValueIsReserved(options: AdvanceOptions, resultName: string): void {
  if (options.values != null && Object.hasOwn(options.values, resultName)) {
    throw new TypeError("values cannot mutate the reserved durable step result");
  }
  if (options.valueRefs != null && Object.hasOwn(options.valueRefs, resultName)) {
    throw new TypeError("valueRefs cannot mutate the reserved durable step result");
  }
  if (options.dropValues?.includes(resultName) === true) {
    throw new TypeError("dropValues cannot mutate the reserved durable step result");
  }
  if (options.overrideValues?.includes(resultName) === true) {
    throw new TypeError("overrideValues cannot mutate the reserved durable step result");
  }
}

function claimedItemFromRecord(record: FlowRecord): ClaimedItem {
  return {
    ...(record.attributes == null ? {} : { attributes: record.attributes }),
    fencingToken: record.fencingToken,
    id: record.id,
    leaseToken: Buffer.from(record.leaseToken),
    ...(record.partitionKey === "" ? {} : { partitionKey: record.partitionKey }),
    ...(record.payload === undefined ? {} : { payload: record.payload }),
    ...(record.runState == null ? {} : { runState: record.runState }),
    state: record.state,
    type: record.type
  };
}
