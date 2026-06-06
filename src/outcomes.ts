export type Outcome = TransitionOutcome | CompleteOutcome | RetryOutcome | FailOutcome;

export interface NamedValueMutation {
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  dropValues?: string[];
  overrideValues?: string[];
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
  return { kind: "transition", toState, ...options };
}

export function complete(options: Omit<CompleteOutcome, "kind"> = {}): CompleteOutcome {
  return { kind: "complete", ...options };
}

export function retry(options: Omit<RetryOutcome, "kind"> = {}): RetryOutcome {
  return { kind: "retry", ...options };
}

export function fail(options: Omit<FailOutcome, "kind"> = {}): FailOutcome {
  return { kind: "fail", ...options };
}

export function isOutcome(value: unknown): value is Outcome {
  if (typeof value !== "object" || value == null || !("kind" in value)) {
    return false;
  }
  return ["transition", "complete", "retry", "fail"].includes(String((value as { kind: unknown }).kind));
}
