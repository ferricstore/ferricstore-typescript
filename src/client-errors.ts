import type { ClaimHydrationItem, FlowBatchCompletedItem } from "./client-options.js";
import { FerricStoreError } from "./errors.js";
import type { ClaimedItem } from "./types.js";

export class ClaimHydrationError extends FerricStoreError {
  override readonly code = "flow_claim_hydration_partial";
  readonly claimed: readonly ClaimedItem[];
  readonly failedIndex: number;
  readonly hydratedItems: readonly ClaimHydrationItem[];

  constructor(
    cause: unknown,
    claimed: readonly ClaimedItem[],
    hydratedItems: readonly ClaimHydrationItem[],
    failedIndex: number
  ) {
    const detail = cause instanceof Error
      ? cause.message
      : typeof cause === "string"
        ? cause
        : "fallback read failed";
    super(
      `Legacy claim hydration failed after ${hydratedItems.length} of ${claimed.length} record(s): ${detail}`,
      { cause }
    );
    this.claimed = claimed;
    this.failedIndex = failedIndex;
    this.hydratedItems = hydratedItems;
  }
}

/** A chunked independent Flow operation failed after zero or more item results were confirmed. */
export class FlowBatchError extends FerricStoreError {
  override readonly code = "flow_batch_partial";
  readonly completed: number;
  readonly completedItems: readonly FlowBatchCompletedItem[];
  readonly operation: string;

  constructor(operation: string, cause: unknown, completedItems: readonly FlowBatchCompletedItem[]) {
    const detail = cause instanceof Error
      ? cause.message
      : typeof cause === "string"
        ? cause
        : "batch execution failed";
    super(
      completedItems.length === 0
        ? `${operation} failed before any item result was confirmed: ${detail}`
        : `${operation} failed after ${completedItems.length} confirmed item result(s): ${detail}`,
      { cause }
    );
    this.completed = completedItems.length;
    this.completedItems = completedItems;
    this.operation = operation;
  }
}

export function confirmedFlowBatchItems(results: readonly unknown[]): FlowBatchCompletedItem[] {
  const completed: FlowBatchCompletedItem[] = [];
  for (let index = 0; index < results.length; index += 1) {
    if (Object.hasOwn(results, index)) {
      completed.push({ index, value: results[index] });
    }
  }
  return completed;
}
