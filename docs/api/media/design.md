# SDK Design

FerricFlow is centered on durable state-machine records.

The TypeScript SDK keeps that visible. A workflow has a `type`, `id`, current `state`, payload/value refs, lease/fencing data, retry metadata, history, and terminal status. Workers claim a state, execute normal TypeScript code, then explicitly write one of four outcomes:

```ts
return transition("charged");
return complete({ result: { ok: true } });
return retry({ error: "rate limited" });
return fail({ error: "bad input" });
```

## What The SDK Does

- Builds typed `FLOW.*` commands over RESP.
- Uses node-redis with RESP3 and Buffer blob-string mapping.
- Provides a low-level `FlowClient` for direct command control.
- Provides `QueueClient` for durable queue-shaped workloads.
- Provides `WorkflowClient` for explicit state-machine workflows.
- Keeps payload serialization in an SDK codec, not in FerricStore.

## What The SDK Does Not Do

- It does not replay workflow code.
- It does not require decorators.
- It does not instrument user functions.
- It does not require a TypeScript service to own every workflow state.

This matters because a Flow can move between services. For example, one TypeScript worker can handle `created`, a Go service can handle `charged`, and a Python service can handle `receipt`. FerricFlow stores the durable state between them.

## Relation To Temporal And DBOS Examples

Temporal’s TypeScript SDK separates clients, workers, workflow code, and activities. DBOS TypeScript examples register workflows and steps. Those are useful API references for how TypeScript users expect to organize workflow code.

FerricFlow’s runtime model is different. The important unit is not a replayed TypeScript function. The important unit is the Flow record and its explicit state transitions.

That is why this SDK uses ordinary handler registration:

```ts
order.state("created", async (ctx) => {
  await chargeCard(ctx.payload);
  return transition("charged");
});
```

The handler is just code. The durable result is the `FLOW.TRANSITION` command.

## Throughput-Oriented Choices

The SDK keeps the hot path thin:

- no local replay sandbox;
- no generated wrappers around handler code;
- RESP commands use existing node-redis pooling and auto-pipelining behavior;
- batch APIs such as `enqueueMany`, `completeMany`, `retryMany`, and `failMany` are exposed directly;
- value refs let workers hydrate only the named values they need.

The storage throughput story belongs mostly to FerricStore itself: FerricStore owns the storage path and FerricFlow state is stored inside FerricStore, not through an external workflow database client from this SDK.
