import { ClaimHydrationError } from "./client-errors.js";
import { errorFromUnknown } from "./client-helpers.js";
import type { ClaimHydrationItem } from "./client-options.js";
import type { ClaimedItem, FlowRecord } from "./types.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";

interface ClaimOptionsSnapshotSource {
  partitionKeys?: string[];
  states?: string[];
  values?: readonly string[];
}

export function snapshotClaimOptions<T extends ClaimOptionsSnapshotSource>(
  options: T,
  reusableAfterAwait = false
): T {
  const snapshot = { ...options };
  if (reusableAfterAwait && snapshot.partitionKeys != null) {
    snapshot.partitionKeys = snapshotOwnStringArray(snapshot.partitionKeys, "partitionKeys") as string[];
  }
  if (reusableAfterAwait && snapshot.states != null) {
    snapshot.states = snapshotOwnStringArray(snapshot.states, "states") as string[];
  }
  if (snapshot.values != null) {
    snapshot.values = snapshotOwnStringArray(snapshot.values, "values");
  }
  return Object.freeze(snapshot);
}

export async function hydrateClaimedRecords(
  claimed: readonly ClaimedItem[],
  concurrency: number,
  hydrate: (item: ClaimedItem) => Promise<FlowRecord>
): Promise<FlowRecord[]> {
  const results = new Array<FlowRecord>(claimed.length);
  let cursor = 0;
  let failure: { readonly error: unknown; readonly index: number } | undefined;
  const run = async (): Promise<void> => {
    while (failure == null) {
      const index = cursor;
      cursor += 1;
      const item = claimed[index];
      if (item == null) return;
      try {
        results[index] = await hydrate(item);
      } catch (error) {
        failure ??= { error, index };
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(concurrency, claimed.length) }, run));
  if (failure != null) {
    const hydratedItems: ClaimHydrationItem[] = [];
    for (let index = 0; index < results.length; index += 1) {
      const record = results[index];
      if (record != null) hydratedItems.push({ index, record });
    }
    throw new ClaimHydrationError(failure.error, claimed, hydratedItems, failure.index);
  }
  return results;
}

export function claimFailure(error: unknown): {
  readonly claimed: (FlowRecord | ClaimedItem)[];
  readonly claimError: Error;
} {
  const claimError = errorFromUnknown(error);
  return {
    claimed: claimError instanceof ClaimHydrationError ? [...claimError.claimed] : [],
    claimError
  };
}
