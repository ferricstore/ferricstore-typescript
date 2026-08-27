# Agent framework persistence

FerricStore's TypeScript package has optional adapters for LangGraph.js and the
OpenAI Agents SDK. They live in separate package entry points, so the base SDK
does not load either framework.

## Install

For LangGraph.js:

```bash
npm install @ferricstore/ferricstore @langchain/langgraph @langchain/core
```

For the OpenAI Agents SDK:

```bash
npm install @ferricstore/ferricstore @openai/agents
```

Both adapters accept the normal `FerricStoreClient`. Their serialization is
independent of the client's configured codec.

## LangGraph.js checkpoints

`FerricStoreSaver` implements LangGraph's `BaseCheckpointSaver` contract:

```ts
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { FerricStoreClient } from "@ferricstore/ferricstore";
import { FerricStoreSaver } from "@ferricstore/ferricstore/langgraph";

const client = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388");
const saver = new FerricStoreSaver(client);

const State = Annotation.Root({ count: Annotation<number>() });
const graph = new StateGraph(State)
  .addNode("increment", ({ count }) => ({ count: count + 1 }))
  .addEdge(START, "increment")
  .addEdge("increment", END)
  .compile({ checkpointer: saver });

await graph.invoke(
  { count: 0 },
  { configurable: { thread_id: "agent-42" } }
);
```

The saver supports named checkpoint namespaces, latest and exact reads,
ordered and filtered listing, parent chains, pending writes, retry-safe write
indexes, global listing, and complete thread deletion. It uses LangGraph's
serializer, so framework-specific values round-trip correctly.

Checkpoint mutations are serialized per thread with renewable,
ownership-checked locks. Indexes are published before the final checkpoint
record; readers validate each record and skip incomplete entries. This makes a
process failure during publication invisible and a retry safe.

## LangGraph.js long-term memory

`FerricStoreStore` implements `BaseStore`:

```ts
import { FerricStoreStore } from "@ferricstore/ferricstore/langgraph";

const store = new FerricStoreStore(client);

await store.put(["users", "u-42"], "preferences", {
  language: "en",
  notifications: true
});

const memories = await store.search(["users", "u-42"], {
  filter: { notifications: true }
});
```

It supports hierarchical namespaces, atomic per-item mutation ordering,
batched operations, exact and comparison filters (`$eq`, `$ne`, `$gt`, `$gte`,
`$lt`, `$lte`, `$in`, `$nin`), ordered pagination, namespace listing, updates,
and deletion.
Semantic `query` search currently throws a clear error because no vector index
is configured; it never silently returns unranked data.

## Run LangGraph inside FerricFlow

The checkpointer makes graph steps resumable. `LangGraphFlow` adds the durable
outer lifecycle: leases and fencing, retries, scheduled work, signals,
approvals, workflow history, and terminal state.

```ts
import { LangGraphFlow } from "@ferricstore/ferricstore/langgraph";

const agentFlow = new LangGraphFlow(graph, {
  interruptState: "waiting_for_approval"
});

workflow.state("running", agentFlow.handler.bind(agentFlow));
```

By default, the bridge derives a stable LangGraph `thread_id` from the Flow
type, partition, and ID. It sends the Flow payload on the first invocation and
uses `null` input when a checkpoint already exists. LangGraph runtime context
includes the active `WorkflowContext`. Completed graphs become `complete()`
outcomes; interrupts can transition to a chosen Flow state or use a custom
outcome mapper. Call `resume(flowContext, value)` from a handler to send a
LangGraph `Command({ resume: value })`.

The graph checkpointer and FerricFlow solve different layers and are intended
to be used together:

```text
FerricFlow durable run lifecycle
        ↓
LangGraphFlow invocation bridge
        ↓
LangGraph graph + FerricStoreSaver
        ↓
FerricStore
```

## OpenAI Agents SDK Session

`FerricStoreSession` implements the base `Session` contract plus the optional
history rewrite and atomic transaction capabilities used by the current
OpenAI Agents SDK:

```ts
import { Agent, run } from "@openai/agents";
import { FerricStoreSession } from "@ferricstore/ferricstore/openai-agents";

const session = new FerricStoreSession(client, {
  sessionId: "customer-42"
});
const agent = new Agent({ name: "Support", instructions: "Be helpful." });

await run(agent, "Where is my order?", { session });
```

The adapter provides chronological reads with tail limits, append, pop,
clear, compaction replacement, function-call history rewrites, and atomic
`append_items` / `replace_suffix` transactions. A transaction stores its
operation ID and history mutation in one atomic record. Repeating the same
operation is a no-op; reusing its ID for different content or replacing a
non-matching suffix fails without changing history.

All session mutations use a renewable FerricStore lock. Reads see either the
old or new complete session record, never a partial history. `clearSession()`
also clears transaction receipts. Session persistence stores conversation
history; put the overall agent run in FerricFlow when it also needs durable
leases, retries, timers, signals, or multi-step business state.

## Operational options

All three adapters accept `keyPrefix`, `lockTtlMs`, `lockWaitMs`, and
`lockRetryMs`. The saver and store also accept `scanCount`; the saver accepts a
custom LangGraph serializer. Defaults are suitable for ordinary use. Give
different applications or environments different prefixes when they share a
FerricStore deployment.
