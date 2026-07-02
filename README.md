# FerricStore TypeScript SDK

TypeScript SDK for FerricStore and FerricFlow.

FerricFlow is an explicit durable state-machine layer over FerricStore. Your application runs normal TypeScript code. FerricFlow stores the durable workflow state, leases, retry data, values, history, signals, and terminal status.

```text
FLOW.CREATE -> FLOW.CLAIM_DUE -> handler -> FLOW.TRANSITION / COMPLETE / FAIL / RETRY
```

## Install

```bash
npm install @ferricstore/ferricstore
```

Requires Node.js 24 or newer. The SDK is ESM-only and tested with Node 24 and 26.

## Run FerricStore Locally

```bash
docker run -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ghcr.io/ferricstore/ferricstore:0.6.0
```

## Durable Queue

```ts
import { FerricStoreClient, JsonCodec, QueueClient } from "@ferricstore/ferricstore";

const flow = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const emails = new QueueClient(flow).queue("email");

await emails.enqueue("email-1", {
  idempotent: true,
  payload: { template: "welcome", userId: "user-1" }
});

await emails.worker({ batchSize: 100, worker: "email-worker-1" }).run(async (job) => {
  console.log(job.id, job.payload);
  return { sent: true };
});
```

## Explicit Workflow

```ts
import { FerricStoreClient, JsonCodec, WorkflowClient, complete, transition } from "@ferricstore/ferricstore";

const flow = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const order = new WorkflowClient(flow).workflow({
  initialState: "created",
  type: "order"
});

order.state("created", async (ctx) => {
  await chargeCard(ctx.payload);
  return transition("charged");
});

order.state("charged", async (ctx) => {
  await sendReceipt(ctx.id);
  return complete({ result: { ok: true } });
});

await order.start("order-1", {
  idempotent: true,
  payload: { amount: 42, userId: "user-1" }
});

await order.worker({
  batchSize: 50,
  states: ["created", "charged"],
  worker: "order-worker-1"
}).run();
```

Handlers return explicit durable outcomes:

- `transition("next_state")`
- `complete({ result })`
- `retry({ error })`
- `fail({ error })`

FerricFlow does not replay TypeScript handler code. Workers claim a durable state, run normal code, then write the next state through the FerricFlow API.

## Low-Level Flow Commands

```ts
const flow = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388");

await flow.create("order-1", {
  type: "order",
  state: "created",
  payload: Buffer.from("order payload"),
  idempotent: true
});

const jobs = await flow.claimDue("order", {
  state: "created",
  worker: "worker-1",
  leaseMs: 30_000,
  limit: 10,
  payload: true
});

for (const job of jobs) {
  await flow.transition(job.id, {
    fromState: job.state,
    toState: "charged",
    leaseToken: job.leaseToken,
    fencingToken: job.fencingToken,
    partitionKey: job.partitionKey
  });
}
```

## FerricStore KV And Data Structures

The same client exposes typed helpers for FerricStore's Redis-compatible store commands:

```ts
const client = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

await client.kv.set("user:1", { name: "Ada" }, { px: 60_000 });
const user = await client.kv.get("user:1");

await client.hash.hset("user:1:profile", { email: "ada@example.com" });
await client.lists.lpush("jobs", { id: "job-1" });
await client.sets.sadd("seen-users", "user:1");
await client.zset.zadd("leaderboard", [{ score: 42, member: "user:1" }]);
await client.stream.xadd("events", "*", { type: "created", id: "user:1" });
await client.json.set("user:1:json", "$", { name: "Ada" });
await client.bloom.add("seen-filter", "user:1");
```

Available store helpers:

- `client.kv` — strings, key expiry, key management, scans.
- `client.hash` — hashes and Redis 7.4 hash-field TTL commands.
- `client.lists` — lists.
- `client.sets` — sets.
- `client.zset` — sorted sets.
- `client.stream` — streams and consumer groups.
- `client.bitmap` — bitmap commands.
- `client.hyperloglog` — HyperLogLog commands.
- `client.geo` — geospatial commands.
- `client.bloom`, `client.cuckoo`, `client.cms`, `client.topk`, `client.tdigest` — probabilistic data structures.
- `client.json` — RedisJSON-compatible JSON commands.

For connection-mode commands such as raw subscription flows or transactions, use `client.command(...)` directly so protocol behavior stays explicit.

## Auto-Batching

The default client is latency-first: each SDK call sends its own native request.

For high-throughput services that issue many independent calls concurrently, enable SDK auto-batching:

```ts
const client = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388", {
  autoBatch: {
    enabled: true,
    maxCommands: 512,
    maxDelayMs: 0
  }
});

await Promise.all([
  client.kv.set("a", "1"),
  client.kv.set("b", "2"),
  client.hash.hset("user:1", { email: "ada@example.com" })
]);
```

Auto-batching groups eligible concurrent commands into native `PIPELINE` frames and resolves each original promise independently. Blocking/session commands such as `FLOW.CLAIM_DUE`, `AUTH`, `QUIT`, `SUBSCRIBE`, and client-control commands bypass auto-batching.

Queue workers are latency-first by default. For high-throughput queue workers, use one profile flag:

```ts
await emails.worker({ profile: "throughput" }).run(async (job) => {
  await sendEmail(job.id);
});
```

The throughput profile uses compact claims, larger claim batches, and async completion batching. Explicit worker options still override the profile.

## Examples

Runnable examples live in the `examples/` directory:

- [durable-queue.ts](examples/durable-queue.ts)
- [order-workflow.ts](examples/order-workflow.ts)
- [fanout.ts](examples/fanout.ts)
- [signals.ts](examples/signals.ts)
- [value-refs.ts](examples/value-refs.ts)
- [kv-store.ts](examples/kv-store.ts)

## Codecs

`RawCodec` is the default and works with `Buffer`, `Uint8Array`, and strings. Use `JsonCodec` for language-neutral structured payloads and results.

```ts
const flow = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});
```

## Design Notes

The SDK borrows a familiar TypeScript registration style from workflow libraries, but the model is FerricFlow’s explicit state machine:

- handlers are normal application code;
- workflow progress is stored as state transitions, not as a replayed execution stack;
- current state, owner, lease token, fencing token, retry data, history, values, and next claimable state are workflow data;
- the same Flow can be processed by services in different languages through FerricStore's native protocol.

See [docs/design.md](docs/design.md) for more detail.

## Development

```bash
npm install
npm run check
```

`npm run check` runs strict TypeScript, ESLint, Vitest, and the package build.

Use Docker Compose for local integration testing:

```bash
npm run integration:up
FERRICSTORE_INTEGRATION=1 npm run test:integration
npm run integration:down
```

Generate API docs with:

```bash
npm run docs
```
