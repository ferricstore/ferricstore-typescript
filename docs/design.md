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

- Builds typed `FLOW.*` commands over FerricStore's native protocol.
- Uses the SDK native adapter with Buffer-safe binary values.
- Provides a low-level `FerricStoreClient` for direct command control.
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

The handler is just code. Ordinary outcomes become explicit Flow mutations.
For an operation that needs replayable output, `step()` validates the current
lease, runs the closure in the caller's JavaScript execution context, and
atomically stores its result with `FLOW.STEP_CONTINUE`. Recovery reads a
committed result without rerunning the closure; a closure whose result was not
committed may run again, so external providers still require stable
idempotency keys.

A timer, signal, approval, or scheduled state is persisted before the current
claim is released. Waiting therefore does not consume worker concurrency. Once
the condition becomes runnable, any available worker can claim a fresh lease
and continue from the durable state; completed durable steps replay their
stored results instead of running again.

`stepContinue()` is retained only as a deprecated low-level migration API.
Applications should use chainable `advance()` for state-only transitions and
`step()` for a journaled closure plus transition.

The SDK does not use a global executor or thread pool for step closures. It
awaits the value returned by the caller or worker-owned callback. CPU-bound
closures therefore need the application's normal Node.js worker-thread or
service isolation strategy.

## Throughput-Oriented Choices

The SDK keeps the hot path thin:

- no local replay sandbox;
- no generated wrappers around handler code;
- Native commands use FerricStore's multiplexed `ferric://` protocol;
- batch APIs such as `enqueueMany`, `completeMany`, `retryMany`, and `failMany` are exposed directly;
- value refs let workers hydrate only the named values they need.

The storage throughput story belongs mostly to FerricStore itself: FerricStore owns the storage path and FerricFlow state is stored inside FerricStore, not through an external workflow database client from this SDK.
