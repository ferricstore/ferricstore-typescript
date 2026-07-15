import type { StateMeta } from "./types.js";
import type { CommandArgument } from "./internal.js";

const OUTCOME_BRAND = Symbol("ferricstore.outcome");

export type Outcome = TransitionOutcome | CompleteOutcome | RetryOutcome | FailOutcome;

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
