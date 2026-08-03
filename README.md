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

Requires Node.js 22.22 or newer. The SDK ships ESM and CommonJS builds and is tested with Node 22, 24, and 26.

## Compatibility

TypeScript SDK `0.11.5` requires FerricStore server `0.11.4` or newer. With
FerricStore 0.11.5 it negotiates compact Stream mode 34 for homogeneous auto-ID
`XADD` pipelines and compact Pub/Sub mode 35 for homogeneous `PUBLISH`
pipelines. Native wire protocol v1 and the generic fallback are unchanged.
Capabilities and response-size limits are negotiated
per connection from the HELLO-shaped startup response rather than inferred from
a server version table.

ESM:

```ts
import { FerricStoreClient, JsonCodec } from "@ferricstore/ferricstore";
```

CommonJS:

```js
const { FerricStoreClient, JsonCodec } = require("@ferricstore/ferricstore");
```

## Run FerricStore Locally

```bash
docker run -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ghcr.io/ferricstore/ferricstore:0.11.5
```

## Query durable runs

Use parameterized FQL for bounded, partition-scoped reads. Cursors are opaque
and must be reused with the same query and parameters.

```ts
const client = await FerricStoreClient.fromUrl("ferric://127.0.0.1:6388");
const query = `FROM runs
WHERE partition_key = @partition AND type = @type AND state = @state
ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS`;
const params = { partition: "partition-a", type: "invoice", state: "queued" };

const result = await client.query(query, params);
const plan = await client.explain(query, params);
const indexes = await client.queryIndexes();
```

Each index reports `coveringFields`, which identifies the built-in and dynamic
`attribute.*` or `state_meta.*` fields that it can return without record
hydration. Its `format` values are opaque storage-generation identifiers; use
them to detect a rebuild requirement, not to decode server storage. The
`counter` format is absent for indexes without counters.

Select a sparse result map by adding up to 32 source-specific fields after
`RETURN RECORD` or `RETURN RECORDS`, for example
`RETURN RECORDS (run_id, state, attribute['customer'])`. A bare return keeps the
complete public record. Projection runs after authorization, authoritative
recheck, ordering, and cursor calculation: it reduces retained result data,
encoding, network, and client decoding work, but not index scans or hydration.

Use the source-aware builder to avoid hand-quoting result selectors:

```ts
const projected = projectFlowQuery(
  "FROM runs WHERE partition_key = @partition AND run_id = @run",
  "record",
  FlowProjection.run.id,
  FlowProjection.run.state,
  FlowProjection.run.attribute("customer")
);
const result = await client.query(projected, { partition: "partition-a", run: "run-1" });
```

## Cluster-aware client

For a single node, use `fromUrl`. For a FerricStore cluster, pass multiple seed URLs. The SDK fetches the server `SHARDS` topology, routes keyed commands to the current shard leader, and refuses learned hosts outside the seed-host trust set by default. The creation promise resolves only after startup and authentication succeed; cluster creation also waits for the initial topology, so connection failures reject the corresponding `await` directly.

Cross-shard pipelines are grouped into one native pipeline per leader/lane and
merged back into caller order. Decomposable multi-key commands (`MGET`,
`EXISTS`, `DEL`, `UNLINK`, and `FLOW.VALUE.MGET`) use the same parallel shard
fan-out; atomic multi-key commands are never split client-side. Pass
`{ ordered: true }` as the second argument to `client.pipeline()` when later
commands depend on earlier ones and the transport may need an individual or
cross-route fallback.

```ts
const flow = await FerricStoreClient.fromUrls(
  [
    "ferric://fs0.example.com:6388",
    "ferric://fs1.example.com:6388",
    "ferric://fs2.example.com:6388"
  ],
  {
    codec: new JsonCodec(),
    nativeOptions: {
      // Use "any" only inside a trusted private network.
      endpointPolicy: "seed_hosts",
      // Bound client-side route fan-out; freed slots refill immediately.
      topologyConcurrency: 16,
      warmConnections: true
    }
  }
);

await flow.refreshTopology();
console.log(await flow.route("tenant-a:order-1"));
```

Learned topology endpoints are checked before connection. The default
`"seed_hosts"` policy permits exact seed endpoints plus `trustedHosts`;
`"none"` permits exact seed endpoints only. Use `"any"` only when every
server-advertised endpoint is already inside a trusted network boundary.
All HA seed URLs must use the same `ferric://` or `ferrics://` transport;
`tlsOptions` configures a secure transport but does not change a URL's scheme.
Each seed connection uses only the credentials embedded in its own URL. The
first complete seed credential pair is reused for learned cluster endpoints,
but never overrides another seed URL, even when that seed is first reached
through learned topology. Duplicate seed endpoints with conflicting effective
credentials are rejected. Explicit `nativeOptions.username` and `password`
remain an intentional cluster-wide override.

The option types follow ownership: `NativeAdapterOptions` contains direct
connection settings, `TopologyNativeAdapterOptions` adds learned-endpoint
policy, and `NativeClientOptions` adds reconnect/HA seed selection. Passing a
higher-layer option to `NativeAdapter.fromUrl()` is rejected instead of being
silently ignored.

`topologyConcurrency` defaults to 16 and bounds client-side per-route work such
as cross-shard fan-out, split pipelines, warm-up, and shutdown. It uses
continuous slot filling: when one route finishes, the next waiting route starts
without waiting for the rest of the current group. This setting is local to one
client process or pod; it does not configure FerricStore server concurrency.

Automatic reconnect retries only an operation rejected before it could be
written. If a connection closes while a request is in flight, the SDK surfaces
that error because the server may already have applied the command; it does not
replay an uncertain mutation. A later request reconnects normally. Use
FerricFlow fencing or command-level idempotency when the caller needs safe
application retries. `ConnectionClosedError.requestDisposition` exposes this
decision as `"unsent"` or `"possibly_sent"` when the transport can classify it;
unclassified failures are treated conservatively as possibly sent.
`RequestTimeoutError.requestDisposition` provides the same retry-safety signal:
timeouts while waiting for a local flow-control or write-queue slot are
`"unsent"`, while a request whose frame entered the socket is
`"possibly_sent"`. Do not automatically retry a possibly-sent mutation.
`autoReconnect` accepts `maxRetries`, `baseDelayMs`, `maxDelayMs`, and
`jitterPct`; backoff is applied only after a reconnect attempt itself fails.

Topology-aware clients retry one routed command or one physical fused pipeline
after a successful topology refresh only when the server's typed reroute error
explicitly reports both `retryable: true` and `safe_to_retry: true`. Producer
backpressure follows the same flags and honors `retry_after_ms` within the
configured delay cap. Split or scattered pipelines and uncertain connection
failures are never replayed.

Connection-local state mutations (`AUTH`, `CLIENT SETNAME`, `QUIT`, `RESET`,
and related native controls) are rejected on reconnecting and topology clients
because they cannot be applied atomically to every current and future socket.
Configure `nativeOptions.username`, `password`, `clientName`, and `events` when
creating those clients, and use `close()` for shutdown. A directly managed
single `NativeAdapter` retains connection-local command semantics. Native
`CLIENT TRACKING` and `CLIENT CACHING` are unsupported; use native event
subscriptions instead.

### Deployment integration tests

The default integration suite targets one local development server. Real HA,
TLS, and authentication deployments can be verified with the opt-in deployment
suite:

```bash
FERRICSTORE_HA_URLS=ferric://fs0:6388,ferric://fs1:6388 npm run test:integration:deployment

FERRICSTORE_TLS_URL=ferrics://fs0:6389 \
FERRICSTORE_TLS_CA_FILE=/path/to/ca.pem \
npm run test:integration:deployment

FERRICSTORE_AUTH_URL=ferric://app:secret@fs0:6388 \
npm run test:integration:deployment
```

The HA fixture must advertise at least two reachable leader endpoints. The auth
fixture must require credentials even for the default user. Set
`FERRICSTORE_TLS_PLAINTEXT_URL` as well to verify that a TLS-only deployment
rejects its plaintext listener. HA TLS/auth options are available through
`FERRICSTORE_HA_TLS_CA_FILE`, `FERRICSTORE_HA_TLS_SERVERNAME`,
`FERRICSTORE_HA_USERNAME`, and `FERRICSTORE_HA_PASSWORD`.

You can also keep one primary URL and add seeds:

```ts
const flow = await FerricStoreClient.fromUrl("ferric://fs0.example.com:6388", {
  nativeOptions: {
    haRouting: true,
    seeds: ["ferric://fs1.example.com:6388", "ferric://fs2.example.com:6388"]
  }
});
```

Native connections honor the flow-control windows advertised by the
HELLO-shaped `STARTUP` response and
`WINDOW_UPDATE`. Available data-request slots are refilled immediately as
responses finish; waiting requests are scheduled fairly across protocol lanes.
The adapter also caps automatic lanes and same-lane work to the advertised lane
queue, and caps ordered pipeline chunks and outbound frame bodies to the limits
negotiated during startup. Compact response codecs and aggregate response sizes
are enabled only when advertised for that connection.
Set `nativeOptions.maxQueuedRequests` to bound the local waiter queue (default
`65_536`, or `0` to reject immediately when all advertised slots are occupied).
Queue waiting counts toward `nativeOptions.timeoutMs`.

Control requests do not consume server data credits. Set
`nativeOptions.maxPendingControlRequests` to bound correlated control requests
awaiting responses on each connection (default `4_096`).

If Node reports socket backpressure, later encoded frames wait for `drain` in a
bounded client queue. Set `nativeOptions.maxQueuedWriteBytes` to control that
queue (default 64 MiB, or `0` to reject subsequent writes immediately). Healthy
socket writes still go directly to `socket.write` without entering the queue.

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

await emails.enqueueMany([{ id: "email-2", payload: { template: "receipt" } }], {
  autoPartitionBatchSize: 1_000,
  autoPartitionConcurrency: 8
});

await emails.worker({ batchSize: 100, concurrency: 16, worker: "email-worker-1" }).run(async (job) => {
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
  concurrency: 8,
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

Per-job `run()` workers cap every claim to currently available concurrency. They continuously refill slots by default, so if five of ten jobs finish and their terminal writes are acknowledged, one client-side `claim(limit: 5)` can start five replacements while the other five continue. Queue completions produced in the same event-loop turn remain batched. A slot stays occupied through its `complete`, `retry`, or `fail` acknowledgement; the worker never exceeds its local concurrency limit.

Full-record claims across multiple states are returned by the claim command itself, including requested payloads and named values. `FLOW.GET` hydration is retained only for compatibility with a server that unexpectedly returns legacy compact tuples. That fallback preserves result order and is bounded to 16 concurrent reads by default; set `legacyClaimHydrationConcurrency` on the client to tune it. If a fallback read fails, `ClaimHydrationError.claimed` contains every already-leased job, while `hydratedItems` contains the indexed records that finished successfully; `failedIndex` and `cause` identify the first observed failure. Set `jobOnly: true` when compact claim metadata is sufficient; compact state metadata is decoded directly from the claim response without per-job `FLOW.GET` calls.

Use `refillStrategy: "wave"` to wait for the entire current claim to settle before claiming again. `refillDelayMs` adds a small coalescing window before a continuous refill; the default `0` still coalesces completions for one event-loop turn. Batchable queue completions and same-route replacement claims share one ordered native pipeline by default; set `fuseCompleteClaim: false` to keep them as separate requests. `runOnce()` and batch-handler APIs remain finite and wave-oriented. These controls are local to one worker instance or pod: two pods configured with `concurrency: 10` can execute up to twenty jobs collectively.

Worker claim and terminal-write batches use the client's `flowManyBatchLimit`,
which defaults to FerricStore's standard 1,000-item limit and should match the
server's `flow_max_batch_items` setting. Higher handler concurrency remains
supported and is filled through multiple bounded claims. `completeAsyncDepth`
is normalized to a finite, non-negative integer; non-finite values use the safe
worker-mode default. If an asynchronous completion fails after earlier writes
succeed, `QueueCompletionError.completed` reports every successful completion
drained by the same call, regardless of where the failed completion appeared.

When a worker combines `blockMs` with an `AbortSignal`, native long polls are
bounded by `abortPollMs` (default `1_000`) so shutdown is observed without
abandoning an in-flight claim that may already have leased work. Finite server
blocking time is added to the transport timeout rather than consuming it.

Unpartitioned `enqueueMany` calls group items in linear time, preserve caller
result order, keep chunks for the same auto-partition sequential, and dispatch
different partitions with bounded concurrency. The defaults above match the
server's standard 1,000-item Flow batch limit while avoiding unbounded requests.
Explicit and mixed-partition independent batches use the same hard request cap;
all Flow many mutations split larger inputs only when `independent: true`.
`independent: false` is never silently split and rejects oversized inputs before
dispatch. Set `flowManyBatchLimit` on the client when the server uses a custom
`flow_max_batch_items` value. If a later independent chunk fails,
`FlowBatchError.completedItems` reports the exact input indices and values whose
results were already confirmed; the original failure remains available as
`cause`.

Workers renew active leases every half lease by default and stop renewal before the fenced terminal write. Set `leaseRenewal: false` only when the handler is guaranteed to finish comfortably inside `leaseMs`; use `leaseRenewIntervalMs` to override the renewal interval.

`ctx.valueMany(names)` deduplicates shared references and fetches every missing referenced value with one `FLOW.VALUE.MGET`. Inline and locally cached values do not consume network work, and a stored JSON `null` remains distinct from a missing reference.

Flow fencing tokens remain `number` values while safe and are returned as
`bigint` once they exceed JavaScript's safe integer range. Pass the token back
unchanged; all fenced mutation APIs accept the exported `FencingToken` type,
and native compact claim and batch paths preserve its signed 64-bit value
exactly.

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

Flow attributes can be returned without hydrating each record and can be
updated atomically with the fenced state mutation:

```ts
const attributed = await flow.claimDue("order", {
  includeAttributes: true,
  jobOnly: true,
  state: "created",
  worker: "worker-1"
});

await flow.transition(attributed[0]!.id, {
  attributesDelete: ["temporary"],
  attributesMerge: { processor: "payments-v2" },
  fencingToken: attributed[0]!.fencingToken,
  fromState: "created",
  leaseToken: attributed[0]!.leaseToken,
  toState: "charged"
});
```

The low-level client also exposes the fused `startAndClaim`, `stepContinue`,
and `runStepsMany` operations, schedule administration, Flow statistics and
attribute queries, effects, approvals, circuits, budgets, and distributed
limits. History supports the complete server filter surface, including event,
time, and version bounds plus cold/consistent reads and payload hydration:

```ts
const events = await flow.history("order-1", {
  consistentProjection: true,
  fromVersion: 2,
  includeCold: true,
  payloadMaxBytes: 64_000,
  toVersion: 8,
  values: true
});

await flow.scheduleCreate("orders-every-five-minutes", {
  cron: "*/5 * * * *",
  kind: "cron",
  target: { state: "created", type: "order" },
  timezone: "UTC"
});
```

Overdue interval schedules use bounded `fire_once` catch-up. Recovery creates
one target, coalesces additional elapsed periods in constant time, and sets the
next run one full interval after recovery:

```ts
const schedule = await flow.scheduleCreate("billing-sweep", {
  catchupPolicy: "fire_once",
  everyMs: 60_000,
  kind: "interval",
  overlapPolicy: "queue_after_previous",
  target: { id_prefix: "billing-sweep", type: "billing" }
});
```

`ScheduleRecord` exposes the complete recurrence configuration through
`created_at_ms`, `every_ms`, `cron`, `timezone`, `overlap_policy`, and
`overlap_retry_ms`, in addition to `catchup_policy`, `coalesced_count`,
`last_coalesced_count`, `last_catchup_at_ms`, and `last_planning_error`, using
the server's canonical field names. Non-applicable recurrence fields are
`null`, not omitted. `scheduleFireDue()` returns
`ScheduleFireDueResult`, including the
batch `coalesced` total. Its `errors` entries correspond to claimed schedules;
`claim_error` separately reports a failure to request a later wave after
completed outcomes were preserved. Catch-up handles scheduler delay; overlap
policy separately handles a previous target that is still active. `fire_once` is the
default and only catch-up policy for intervals; other schedule kinds reject it.
The built-in server scheduler normally owns due execution. Call
`scheduleFireDue()` only for tests, administration, or a deployment that
deliberately disables the built-in runner and supplies a custom one.

Recurring targets reject a fixed `id`. Set `id_prefix` to choose their
generated prefix, or omit it to use the schedule ID. `ScheduleState` includes
the transient `"running"` state used while the server holds a due-execution
lease. Bounded catch-up is interval-only; overdue cron schedules advance one
matching occurrence per successful automatic fire.
When planning fails, `state` is `"failed"`, `end_reason` is
`"planning_failed"`, and `last_planning_error` contains the actionable error.
`scheduleDelete()` resolves to `undefined` only after an `OK` server reply.

FIFO Flow state policy is opt-in per state:

```ts
const policy = await flow.installPolicy("email", {
  states: {
    queued: { mode: "fifo" }
  }
});

// Direct writes deep-patch by default. Fence concurrent editors with generation CAS.
const updated = await flow.installPolicy("email", {
  expectedGeneration: policy.generation,
  maxActiveMs: 300_000
});

// Full replacement is explicit on the client API.
await flow.installPolicy("email", {
  expectedGeneration: updated.generation,
  replace: true,
  states: { queued: { mode: "fifo" } }
});

await flow.create("email-3", {
  partitionKey: "tenant-a:email",
  payload: Buffer.from("welcome"),
  state: "queued",
  type: "email"
});
```

FIFO states require a `partitionKey`; priority is for parallel states.
`Workflow.installPolicy()` defaults to full replacement because workflow declarations
describe a complete policy. Pass `replace: false` when a workflow install should patch.
FIFO ordering is enforced by the server per `(type, state, partitionKey)`; worker
concurrency remains available across different partitions.

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
await client.bloom.add("seen-filter", "user:1");
```

Large unambiguous scalar batches accept an array without spreading, for example
`client.kv.del(keys)` and `client.tdigest.add(key, values)`. Codec-backed APIs,
where an array may itself be one stored value, expose explicit methods such as
`lpushMany`, `saddMany`, `zremMany`, `maddMany`, and `queryMany`. These forms
avoid JavaScript's variadic-call limit and build the command in one linear pass;
the existing rest-argument forms retain their original meaning.

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

`JsonCodec` serializes ordinary FerricStore values as JSON; FerricStore does not expose RedisJSON `JSON.*` commands.

Transactions and raw subscription flows require an exclusive pinned connection
session, which the multiplexed native client does not currently expose. The SDK
rejects those commands before dispatch so a failed transaction cannot partially
apply mutations.

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

Auto-batching groups eligible concurrent commands into native `PIPELINE` frames and resolves each original promise independently. Across frames and individual-request fallbacks, same-key write dependencies retain invocation order, while read-only and disjoint-key work remains concurrent. Commands whose direct native representation uses a custom binary body are safely wrapped as typed `COMMAND_EXEC` pipeline items, preserving one pipeline request instead of issuing each command separately. Blocking/session and control commands such as `FLOW.CLAIM_DUE`, `BLPOP`, `XREAD`, `AUTH`, `PING`, `OPTIONS`, and `QUIT` bypass auto-batching. Explicit pipelines issue unsupported or connection-blocking items individually; blocking fallbacks and state-changing controls are sequenced with dependent data commands. Other fallbacks remain concurrent unless `client.pipeline(commands, { ordered: true })` is requested. Individual fallbacks continuously refill a bounded pool instead of starting every request at once; the default limit is 64 and `fallbackConcurrency` on the pipeline options can tune it per call. Native pipeline paths are unchanged. Reconnecting and topology executors reject connection-local mutations before dispatch, and an uncertain native pipeline is never replayed automatically.

Queue workers are latency-first by default. For high-throughput queue workers, use one profile flag:

```ts
await emails.worker({ profile: "throughput", concurrency: 32 }).run(async (job) => {
  await sendEmail(job.id);
});
```

The throughput profile uses compact claims, a larger batch ceiling, and concurrent completion batching. Per-job claim credit follows currently available `concurrency` (or its `workers` alias), capped by `batchSize`; explicit worker options override profile defaults.

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

Benchmark raw FQL and the record convenience layer against a live server. The default
comparison interleaves both paths and fails if either performs more than one `FLOW.QUERY`
or any `FLOW.GET` hydration per operation:

```bash
npm run build
npm run bench:flow-query -- --requests 500 --concurrency 2 --rows 100 --pretty
```

Generate API docs with:

```bash
npm run docs
```
