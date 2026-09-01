import type { Workflow } from "./workflow.js";
import {
  complete,
  fail,
  isAlreadyAppliedOutcome,
  isOutcome,
  isReplayedStepResult,
  retry
} from "./outcomes.js";
import type { ClaimedItem, FlowRecord, WorkerConfig } from "./types.js";
import { LeaseRenewalGuard, workerErrorPayload } from "./worker-internal.js";
import type { WorkflowContext } from "./workflow-context.js";
import { applyWorkflowOutcome } from "./workflow-outcome-application.js";
import type { StateRegistration } from "./workflow-types.js";
import { WorkerWorkflowContext } from "./workflow-worker-context.js";

/** @internal Execute one workflow handler with lease-safe durable-mutation handoff. */
export async function executeWorkflowJob(
  workflow: Workflow,
  workerOptions: WorkerConfig,
  job: FlowRecord | ClaimedItem,
  registration: StateRegistration,
  guard: LeaseRenewalGuard
): Promise<void> {
  guard.assertActive();
  const ctx = new WorkerWorkflowContext(workflow, job, registration.name, guard.job, {
    pause: async () => await guard.pauseForLeaseMutation(),
    resume: (refreshed) => guard.resumeWith(refreshed)
  });
  let value: unknown;
  try {
    value = await registration.handler(ctx);
  } catch (error) {
    await guard.stop();
    if (ctx.hasUncertainMutation) throw asError(error, "durable workflow mutation result is uncertain");
    await applyHandlerError(workflow, workerOptions, ctx, registration, error);
    return;
  }

  await guard.stop();
  if (ctx.hasUncertainMutation) {
    throw asError(ctx.uncertainMutationFailure, "durable workflow mutation result is uncertain");
  }
  if (isAlreadyAppliedOutcome(value)) {
    ctx.assertAppliedOutcome(value);
    await releaseAppliedMutation(workflow, ctx);
    return;
  }
  if (isReplayedStepResult(value)) {
    await applyWorkflowOutcome(
      workflow.client,
      ctx,
      complete({ result: value.result }),
      registration.returnRecord
    );
    return;
  }
  if (isOutcome(value)) {
    await applyWorkflowOutcome(workflow.client, ctx, value, registration.returnRecord);
    return;
  }
  if (ctx.hasAppliedMutation) {
    await releaseAppliedMutation(workflow, ctx);
    return;
  }
  await applyWorkflowOutcome(
    workflow.client,
    ctx,
    complete({ result: value }),
    registration.returnRecord
  );
}

function asError(value: unknown, message: string): Error {
  return value instanceof Error ? value : new Error(message, { cause: value });
}

async function releaseAppliedMutation(workflow: Workflow, ctx: WorkflowContext): Promise<void> {
  await workflow.client.transition(ctx.id, {
    fencingToken: ctx.fencingToken,
    fromState: ctx.state,
    leaseToken: ctx.leaseToken,
    partitionKey: ctx.partitionKey,
    toState: ctx.logicalState
  });
}

async function applyHandlerError(
  workflow: Workflow,
  workerOptions: WorkerConfig,
  ctx: WorkflowContext,
  registration: StateRegistration,
  error: unknown
): Promise<void> {
  const policy = workerOptions.exceptionPolicy ?? registration.exceptionPolicy;
  if (policy === "raise") throw error;
  const encodedError = workerErrorPayload(error, workerOptions, workflow.client.codec);
  await applyWorkflowOutcome(
    workflow.client,
    ctx,
    policy === "fail" ? fail({ error: encodedError }) : retry({ error: encodedError }),
    registration.returnRecord
  );
}
