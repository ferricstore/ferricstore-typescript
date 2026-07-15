import type { WorkerConfig } from "./types.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";

type WorkflowWorkerConfig = WorkerConfig & { states?: string[] };

/** Captures the complete mutable worker input once, before asynchronous work begins. */
export function snapshotWorkerConfig<T extends WorkflowWorkerConfig>(options: T): T {
  const snapshot = { ...options };
  if (snapshot.partitionKeys != null) {
    snapshot.partitionKeys = snapshotOwnStringArray(snapshot.partitionKeys, "partitionKeys") as string[];
  }
  if (snapshot.claimValues != null) {
    snapshot.claimValues = snapshotOwnStringArray(snapshot.claimValues, "claimValues") as string[];
  }
  if (snapshot.states != null) {
    snapshot.states = snapshotOwnStringArray(snapshot.states, "states") as string[];
  }
  return Object.freeze(snapshot);
}
