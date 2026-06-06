# Python SDK Parity

This checklist tracks TypeScript SDK parity against `ferricstore-python`.

Legend:

- Done: typed TS API exists.
- Partial: usable through TS, but Python has a richer helper or worker feature.
- Raw: use `client.command(...)`.

## Core Client

| Python SDK Surface | TypeScript Status | Notes |
| --- | --- | --- |
| `FlowClient.from_url` | Done | `FlowClient.fromUrl` over node-redis RESP3. |
| `command` | Done | Raw RESP escape hatch. |
| `pipeline` | Done | Takes command arrays. |
| `close` | Done | Closes owned adapter. |
| `RawCodec` / `JsonCodec` / custom codec | Done | `Codec` interface. |
| Typed server error mapping | Done | Common FerricStore errors mapped. |
| Backpressure on producer writes | Done | Retry on overload for create/enqueue paths. |
| `autobatch` | Partial | Not implemented as a separate TS helper yet. Use `enqueueMany` / batch mutation helpers. |

## Server/RESP Commands

| Command Family | TypeScript Status | Notes |
| --- | --- | --- |
| `PING`, `ECHO`, `INFO` | Done | `ping`, `echo`, `serverInfo`. `FlowClient.info(type)` remains the `FLOW.INFO` helper. |
| `CONFIG` | Done | `configGet`, `configSet`, `configGetLocal`, `configResetStat`, `configRewrite`. |
| `SLOWLOG`, `COMMAND`, `CLIENT` introspection | Done | Thin typed helpers for supported subcommands. |
| `PUBLISH`, `PUBSUB` introspection | Done | Subscription-mode commands stay raw because they change connection behavior. |
| `ACL`, `AUTH` | Done | Thin typed helpers. |
| `MULTI`, `EXEC`, `WATCH`, subscriber mode | Raw | Use `client.command(...)`; these are connection-state flows. |

## FerricStore Native Commands

| Command Family | TypeScript Status |
| --- | --- |
| `CAS` | Done |
| `LOCK`, `UNLOCK`, `EXTEND` | Done |
| `RATELIMIT.ADD` | Done |
| `FETCH_OR_COMPUTE*` | Done |
| `FERRICSTORE.KEY_INFO` | Done |
| `FERRICSTORE.CONFIG`, `HOTNESS`, `METRICS`, `BLOBGC`, `DOCTOR` | Done |
| `CLUSTER.HEALTH`, `STATS`, `KEYSLOT`, `SLOTS`, `STATUS`, `ROLE`, `JOIN`, `LEAVE`, `FAILOVER`, `PROMOTE`, `DEMOTE` | Done |

## FerricFlow Commands

| Command | TypeScript Status |
| --- | --- |
| `FLOW.CREATE` | Done |
| `FLOW.CREATE_MANY` | Done |
| `FLOW.VALUE.PUT` | Done |
| `FLOW.VALUE.MGET` | Done |
| `FLOW.SIGNAL` | Done |
| `FLOW.CLAIM_DUE` | Done |
| `FLOW.RECLAIM` | Done |
| `FLOW.EXTEND_LEASE` | Done |
| `FLOW.TRANSITION`, `FLOW.TRANSITION_MANY` | Done |
| `FLOW.COMPLETE`, `FLOW.COMPLETE_MANY` | Done |
| `FLOW.RETRY`, `FLOW.RETRY_MANY` | Done |
| `FLOW.FAIL`, `FLOW.FAIL_MANY` | Done |
| `FLOW.CANCEL`, `FLOW.CANCEL_MANY` | Done |
| `FLOW.REWIND` | Done |
| `FLOW.GET`, `FLOW.LIST`, `FLOW.TERMINALS`, `FLOW.FAILURES`, `FLOW.STUCK` | Done |
| `FLOW.BY_PARENT`, `FLOW.BY_ROOT`, `FLOW.BY_CORRELATION` | Done |
| `FLOW.INFO`, `FLOW.HISTORY` | Done |
| `FLOW.SPAWN_CHILDREN` | Done |
| `FLOW.POLICY.SET`, `FLOW.POLICY.GET` | Done |
| `FLOW.RETENTION_CLEANUP` | Done |

## Queue API

| Python SDK Surface | TypeScript Status | Notes |
| --- | --- | --- |
| `QueueClient.queue` | Done | `new QueueClient(flow).queue(...)`. |
| `Queue.enqueue`, `enqueue_many` | Done | `enqueue`, `enqueueMany`. |
| Worker `run_once`, `run` | Done | Async TS worker loop. |
| Exception policy retry/fail/raise | Done | Covered by tests. |
| Batch handler, start/stop/join/stats | Partial | Python worker has a richer threaded lifecycle and batch scheduler. TS has async `run`/`runOnce`. |
| Advanced partition scanning/cooldowns | Partial | Python has more throughput scheduler controls. |

## Workflow API

| Python SDK Surface | TypeScript Status | Notes |
| --- | --- | --- |
| `WorkflowClient.workflow` | Done | Explicit state-machine workflow builder. |
| `Workflow.start`, `start_many` | Done | `start`, `startMany`. |
| `state(...)` registration | Done | TS uses `workflow.state(name, handler, opts)`. |
| Outcomes `transition`, `complete`, `retry`, `fail` | Done | Same conceptual model. |
| `WorkflowContext.flow.*` helpers | Done | Current-flow helper surface exists. |
| Lazy value refs | Done | `ctx.value`, `ctx.valueMany`. |
| Spawn children / fanout | Done | `ctx.flow.spawnChildren`. |
| Class/decorator workflow style | Partial | Python has class/decorator ergonomics; TS currently uses registration. |
| Batch apply optimization | Partial | Python has richer uniform batch apply paths. TS has low-level many commands. |
| Worker lifecycle start/stop/join/stats | Partial | TS worker is async-loop based. |

## KV/Data-Structure Commands

| Redis-Compatible Family | TypeScript Status |
| --- | --- |
| String/key/TTL/object commands | Done through `client.kv`. |
| Hash commands including hash field TTL | Done through `client.hash`. |
| List commands including blocking variants | Done through `client.lists`. |
| Set commands | Done through `client.sets`. |
| Sorted set commands | Done through `client.zset`; commands not supported by FerricStore are intentionally absent. |
| Stream commands and consumer groups | Done through `client.stream` for the supported FerricStore stream surface. |
| Bitmap, HyperLogLog, Geo | Done through `client.bitmap`, `client.hyperloglog`, `client.geo`. |
| Bloom, Cuckoo, Count-Min, TopK, TDigest | Done through `client.bloom`, `client.cuckoo`, `client.cms`, `client.topk`, `client.tdigest`. |
| JSON | Done through `client.json`. |

## Main Remaining Gaps

- Live integration parity against a running FerricStore server.
- Advanced worker scheduling parity with Python.
- Class/decorator workflow ergonomics.
- Full generated command matrix tests for every Redis-compatible helper and edge option.
