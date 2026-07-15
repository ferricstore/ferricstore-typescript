import { normalizeExceptionPolicy } from "./types.js";
import type {
  StateOptions,
  StateRegistration,
  WorkflowHandler
} from "./workflow-types.js";
import { normalizeStateMode } from "./workflow-utilities.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";

/** Snapshot state configuration once so workers share a stable registration. */
export function createWorkflowStateRegistration(
  name: string,
  handler: WorkflowHandler,
  options: StateOptions,
  defaultValueMaxBytes: number | undefined
): StateRegistration {
  const own = { ...options };
  const claimValues = own.claimValues == null
    ? undefined
    : snapshotOwnStringArray(own.claimValues, "claimValues");
  const retryPolicy = own.retryPolicy == null
    ? undefined
    : Object.freeze({ ...own.retryPolicy });
  return Object.freeze({
    claimPayload: own.claimPayload ?? true,
    claimRecord: own.claimRecord ?? true,
    exceptionPolicy: normalizeExceptionPolicy(own.exceptionPolicy),
    handler,
    leaseMs: own.leaseMs ?? 30_000,
    ...(own.mode == null ? {} : { mode: normalizeStateMode(own.mode) }),
    name,
    returnRecord: own.returnRecord ?? false,
    valueMaxBytes: own.valueMaxBytes ?? defaultValueMaxBytes,
    ...(claimValues == null ? {} : { claimValues }),
    ...(retryPolicy == null ? {} : { retryPolicy })
  });
}
