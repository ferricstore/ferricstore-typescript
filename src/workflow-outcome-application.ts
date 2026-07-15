import type { FerricStoreClient } from "./client.js";
import type { CompleteOutcome, FailOutcome, Outcome, RetryOutcome, TransitionOutcome } from "./outcomes.js";
import { WorkflowContext } from "./workflow-context.js";

export async function applyWorkflowOutcome(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: Outcome,
  returnRecord: boolean
): Promise<void> {
  switch (outcome.kind) {
    case "transition":
      await applyTransition(client, ctx, outcome, returnRecord);
      return;
    case "complete":
      await applyComplete(client, ctx, outcome, returnRecord);
      return;
    case "retry":
      await applyRetry(client, ctx, outcome, returnRecord);
      return;
    case "fail":
      await applyFail(client, ctx, outcome, returnRecord);
      return;
  }
}

async function applyTransition(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: TransitionOutcome,
  returnRecord: boolean
): Promise<void> {
  const target = ctx.workflow.stateRegistration(outcome.toState);
  if (target?.mode === "fifo" && outcome.priority != null) {
    throw new Error("priority is not supported for fifo state");
  }
  await client.transition(ctx.id, {
    attributesDelete: outcome.attributesDelete,
    attributesMerge: outcome.attributesMerge,
    dropValues: outcome.dropValues,
    fencingToken: ctx.fencingToken,
    fromState: ctx.state,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    priority: outcome.priority,
    returnRecord,
    runAtMs: outcome.runAtMs,
    stateMeta: outcome.stateMeta,
    toState: outcome.toState,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

async function applyComplete(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: CompleteOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.complete(ctx.id, {
    attributesDelete: outcome.attributesDelete,
    attributesMerge: outcome.attributesMerge,
    dropValues: outcome.dropValues,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    result: outcome.result,
    returnRecord,
    stateMeta: outcome.stateMeta,
    ttlMs: outcome.ttlMs,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

async function applyRetry(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: RetryOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.retry(ctx.id, {
    attributesDelete: outcome.attributesDelete,
    attributesMerge: outcome.attributesMerge,
    dropValues: outcome.dropValues,
    error: outcome.error,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    returnRecord,
    runAtMs: outcome.runAtMs,
    stateMeta: outcome.stateMeta,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

async function applyFail(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: FailOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.fail(ctx.id, {
    attributesDelete: outcome.attributesDelete,
    attributesMerge: outcome.attributesMerge,
    dropValues: outcome.dropValues,
    error: outcome.error,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    returnRecord,
    stateMeta: outcome.stateMeta,
    ttlMs: outcome.ttlMs,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}
