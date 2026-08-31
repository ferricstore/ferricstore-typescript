import type { ClaimedItem, StateMeta } from "./types.js";
import type { CommandArgument } from "./internal.js";

const OUTCOME_BRAND = Symbol("ferricstore.outcome");
const ALREADY_APPLIED_BRAND = Symbol("ferricstore.alreadyApplied");
const REPLAYED_STEP_BRAND = Symbol("ferricstore.replayedStep");

export type Outcome = TransitionOutcome | CompleteOutcome | RetryOutcome | FailOutcome;

/** A workflow handler mutation already committed with this refreshed claim. */
export interface AlreadyAppliedOutcome {
  readonly job: ClaimedItem;
  readonly kind: "already_applied";
}

/** A durable workflow step result that is also an already-applied outcome. */
export interface AppliedStepOutcome<TResult> extends AlreadyAppliedOutcome {
  readonly applied: true;
  readonly result: TResult;
}

/** A stored durable-step result recovered without a mutation in this invocation. */
export interface ReplayedStepResult<TResult> {
  readonly applied: false;
  readonly job: ClaimedItem;
  readonly result: TResult;
}

/** Result of a context durable step, distinguishing a new commit from replay. */
export type WorkflowStepResult<TResult> = AppliedStepOutcome<TResult> | ReplayedStepResult<TResult>;

export interface NamedValueMutation {
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  dropValues?: string[];
  overrideValues?: string[];
  attributesMerge?: Record<string, CommandArgument>;
  attributesDelete?: string[];
  stateMeta?: StateMeta;
}

export interface TransitionOutcome extends NamedValueMutation {
  readonly kind: "transition";
  readonly toState: string;
  readonly payload?: unknown;
  readonly priority?: number;
  readonly runAtMs?: number;
}

export interface CompleteOutcome extends NamedValueMutation {
  readonly kind: "complete";
  readonly payload?: unknown;
  readonly result?: unknown;
  readonly ttlMs?: number;
}

export interface RetryOutcome extends NamedValueMutation {
  readonly kind: "retry";
  readonly error?: unknown;
  readonly payload?: unknown;
  readonly runAtMs?: number;
}

export interface FailOutcome extends NamedValueMutation {
  readonly kind: "fail";
  readonly error?: unknown;
  readonly payload?: unknown;
  readonly ttlMs?: number;
}

export function transition(
  toState: string,
  options: Omit<TransitionOutcome, "kind" | "toState"> = {}
): TransitionOutcome {
  return brandOutcome({ ...options, kind: "transition", toState });
}

export function complete(options: Omit<CompleteOutcome, "kind"> = {}): CompleteOutcome {
  return brandOutcome({ ...options, kind: "complete" });
}

export function retry(options: Omit<RetryOutcome, "kind"> = {}): RetryOutcome {
  return brandOutcome({ ...options, kind: "retry" });
}

export function fail(options: Omit<FailOutcome, "kind"> = {}): FailOutcome {
  return brandOutcome({ ...options, kind: "fail" });
}

/** @internal Create a marker only after the corresponding mutation commits. */
export function alreadyApplied(job: ClaimedItem): AlreadyAppliedOutcome {
  return brandAlreadyApplied({ job, kind: "already_applied" });
}

/** @internal Create a durable result marker only after a new commit. */
export function appliedStep<TResult>(job: ClaimedItem, result: TResult): AppliedStepOutcome<TResult> {
  return brandAlreadyApplied({ applied: true, job, kind: "already_applied", result });
}

/** @internal Create a replay result that must not suppress the handler's next outcome. */
export function replayedStep<TResult>(job: ClaimedItem, result: TResult): ReplayedStepResult<TResult> {
  const replayed: ReplayedStepResult<TResult> = { applied: false, job, result };
  Object.defineProperty(replayed, REPLAYED_STEP_BRAND, {
    configurable: false,
    enumerable: false,
    value: true,
    writable: false
  });
  return replayed;
}

export function isOutcome(value: unknown): value is Outcome {
  if (
    typeof value !== "object" ||
    value == null ||
    !Object.hasOwn(value, OUTCOME_BRAND) ||
    (value as Record<PropertyKey, unknown>)[OUTCOME_BRAND] !== true
  ) {
    return false;
  }
  const outcome = value as Partial<Outcome>;
  if (outcome.kind === "transition") {
    return typeof outcome.toState === "string";
  }
  return outcome.kind === "complete" || outcome.kind === "retry" || outcome.kind === "fail";
}

export function isAlreadyAppliedOutcome(value: unknown): value is AlreadyAppliedOutcome {
  return typeof value === "object" && value != null &&
    Object.hasOwn(value, ALREADY_APPLIED_BRAND) &&
    (value as Record<PropertyKey, unknown>)[ALREADY_APPLIED_BRAND] === true &&
    (value as Partial<AlreadyAppliedOutcome>).kind === "already_applied";
}

/** @internal */
export function isReplayedStepResult(value: unknown): value is ReplayedStepResult<unknown> {
  return typeof value === "object" && value != null &&
    Object.hasOwn(value, REPLAYED_STEP_BRAND) &&
    (value as Record<PropertyKey, unknown>)[REPLAYED_STEP_BRAND] === true &&
    (value as Partial<ReplayedStepResult<unknown>>).applied === false;
}

function brandOutcome<T extends Outcome>(outcome: T): T {
  Object.defineProperty(outcome, "kind", {
    configurable: false,
    enumerable: true,
    value: outcome.kind,
    writable: false
  });
  if (outcome.kind === "transition") {
    Object.defineProperty(outcome, "toState", {
      configurable: false,
      enumerable: true,
      value: outcome.toState,
      writable: false
    });
  }
  Object.defineProperty(outcome, OUTCOME_BRAND, {
    configurable: false,
    enumerable: false,
    value: true,
    writable: false
  });
  return outcome;
}

function brandAlreadyApplied<T extends AlreadyAppliedOutcome>(outcome: T): T {
  Object.defineProperty(outcome, "kind", {
    configurable: false,
    enumerable: true,
    value: "already_applied",
    writable: false
  });
  Object.defineProperty(outcome, ALREADY_APPLIED_BRAND, {
    configurable: false,
    enumerable: false,
    value: true,
    writable: false
  });
  return outcome;
}
