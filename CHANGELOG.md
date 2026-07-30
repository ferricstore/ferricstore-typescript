# Changelog

All notable changes to the FerricStore TypeScript SDK will be documented here.

The format is based on Keep a Changelog, and this project follows semver once it reaches `1.0`.

## [Unreleased]

## [0.11.5] - 2026-07-30

### Changed

- Negotiate FerricStore 0.11.5's compact Stream producer capability and encode
  homogeneous `XADD key * field value...` pipelines with mode 34. Legacy
  servers, explicit IDs, trimming, `NOMKSTREAM`, malformed pairs, and
  unsupported values retain the generic pipeline path.
- Retain FerricStore 0.11.4 as the minimum server and native wire protocol v1.

## [0.11.4] - 2026-07-28

### Added

- Decode and validate the complete durable-schedule recurrence response,
  including creation time, interval period, cron expression, timezone, and
  overlap retry configuration.

### Changed

- Require FerricStore 0.11.4 while retaining native wire protocol v1.

## [0.5.2] - 2026-07-27

### Added

- Expose typed specialized-plan capabilities and the complete query-index
  service, field, lifecycle, validation, retirement, and statistics status.

### Changed

- Propagate the client timeout into the server-side query deadline for direct
  and pipelined native requests without adding another timer or payload copy.
- Reject oversized or malformed query parameters, non-canonical compact
  cursors, unsupported quality values, and inconsistent usage counters before
  exposing a result.
- Pin live integration and core parity to FerricStore 0.11.3 while retaining
  FerricStore 0.11.0 as the minimum compatible server and native wire v1.

## [0.5.1] - 2026-07-26

### Changed

- Validate the unchanged compact FQL1 query/result contract against
  FerricStore 0.11.2's fused index execution and corrected compact
  `EXPLAIN ANALYZE` response path.
- Pin live integration and core parity to FerricStore 0.11.2 while retaining
  FerricStore 0.11.0 as the minimum compatible server and native wire v1.

## [0.5.0] - 2026-07-26

### Changed

- Require FerricStore 0.11.0 while retaining native wire protocol v1 and the
  existing FQL1 query/result contracts.
- Reject missing, duplicate, oversized, invalid UTF-8, and malformed nullable
  query-index generation metadata before returning it to callers.

### Added

- Expose bounded `coveringFields` and opaque per-generation `format` codec
  identities from `queryIndexes()`, with live OSS catalog coverage.

## [0.4.1] - 2026-07-24

### Changed

- Require FerricStore 0.10.3 for result projections and the negotiated compact
  FQL1 result codec while retaining native wire protocol v1.
- Require the custom-payload frame flag for every negotiated compact response,
  and reject malformed Unicode scalar text before native encoding.

### Added

- Add source-aware `FlowProjection` selectors and `projectFlowQuery` for
  bounded sparse run/event results, plus a shared server/SDK codec golden corpus.

## [0.4.0] - 2026-07-23

### Added

- Typed `query`, `explain`, `explainAnalyze`, and `queryIndexes` APIs for the
  OSS FQL1 planner, including opaque pagination cursors, count results,
  actionable diagnostics, and exact unsigned 64-bit index metadata.
- Live pagination, count, explain/analyze, index-status, eventual-projection,
  convenience, and scoped query ACL integration coverage.

### Changed

- Require FerricStore 0.10.0 and negotiate the complete FQL request, result,
  explain, index-status, capability, shape, and schema contracts during HELLO.
- Pin live integration to the immutable FerricStore 0.10.2 release and exact
  OSS core parity commit while retaining 0.10.0 as the minimum server contract.
- Compile collection convenience methods to bounded, partition-scoped FQL and
  remove the superseded collection opcodes from the native command surface.
- Reject incompatible index-status contracts during HELLO and preserve the
  server's 64-byte metadata and normalized-state domains before query I/O.
- Reject collection shapes without a bounded server index and explicit empty
  index identifiers before transport, with locale-independent metadata order.
- Reject malformed Unicode query response text while retaining the existing
  64-byte quality-label response bound.
- Update vulnerable transitive development dependencies and enforce a
  high-severity `npm audit` gate in test and release workflows.

## [0.3.0] - 2026-07-19

### Added

- FerricStore 0.9.1 Flow policy generation CAS through `expectedGeneration`, explicit full replacement through `replace`, and typed `FlowPolicySnapshot` results from policy reads and writes.
- Dedicated `StalePolicyGenerationError` mapping and strict nonnegative safe-integer generation validation across the full `0..9_007_199_254_740_991` range.
- Live conformance coverage for HELLO policy fields, deep patch, replacement, successful and stale CAS, and FIFO concurrency across partitions.

### Changed

- Require FerricStore server 0.9.1 while retaining the native v1 magic, framing, headers, and opcode numbers.
- Send representable Flow policy reads and writes through their existing structured native opcodes instead of the generic command fallback.
- Keep direct policy updates as deep patches by default; workflow policy installation now defaults to full replacement.
- Never automatically replay CAS mutations after reconnect or reroute failures, including outcomes known to be unsent.

## [0.2.0] - 2026-07-18

### Added

- FerricStore 0.8 `max_active_ms` support across Flow creation, create-many, start-and-claim, child spawning, type policies, records, and timeout failure details, including `infinity` inputs.
- Strict `SET` support for `EXAT`, `PXAT`, and `KEEPTTL`, plus local same-slot validation for atomic `MSET` and `MSETNX`.
- Exported SDK, minimum-server (`0.8.0`), and native-protocol (`v1`) compatibility constants and package metadata.

### Changed

- Treat FerricStore 0.8 as a breaking beta contract while retaining the v1 native magic, headers, flags, and opcode table.
- Negotiate compact response opcode support and `max_response_bytes` from each connection's HELLO-shaped startup response; chunked replies are correlated by lane, opcode, and request id and bounded while assembling.
- Require three-field `FETCH_OR_COMPUTE` compute replies and ownership tokens on both completion commands.
- Use canonical `parent_flow_id` and `root_flow_id`, the 0.8 `FLOW.SIGNAL` `id`/`signal` schema, explicit Flow effect routing, and the decay-free `TOPK.RESERVE` grammar.
- Follow server `retryable`, `safe_to_retry`, and `retry_after_ms` metadata; uncertain sent mutations remain non-replayable.
- Keep unauthenticated outbound frames within 64 KiB until authentication succeeds and avoid constructing reserved Flow storage keys for client routing.

### Fixed

- Preserve unknown compact Flow record extensions, decode `max_active_ms` failures, and require lease plus fencing tokens on fenced mutations (fencing alone for cancel).
- Reassemble chunked compact `MGET` and `FLOW.VALUE.MGET` responses without intermediate payload copies and enforce negotiated aggregate response limits.

### Removed

- Removed tokenless fetch completion APIs, TopK decay, and `parent_id`/`root_id` compatibility aliases.

## [0.1.7] - 2026-07-15

### Added

- Automatic queue/workflow lease renewal with explicit disable and interval controls.
- Bounded native frame, response, container-depth, and container-item decoding.
- Native management-event callbacks, `GOAWAY` draining, and topology-change refresh subscriptions.
- Client-side native flow-control enforcement with bounded, fair per-lane waiting and dynamic `WINDOW_UPDATE` limits.
- Opt-in live deployment coverage for multi-node HA routing, TLS-only transports, and authenticated connections.
- Typed `maxActiveMs` support for Flow creation, batch creation, child spawning, type policies, and decoded records.
- Opt-in worker `includeErrorStack` diagnostics; persisted handler errors omit stack traces by default.
- Typed fused Flow step operations, schedule administration, Flow statistics and attribute queries, effects, approvals, governance, circuits, budgets, and distributed limits.
- Full `FLOW.HISTORY` event, time, version, worker, projection, values, and payload-size filters.
- Opt-in attribute-bearing compact claims and atomic attribute merge/delete options on fenced Flow mutations.
- `FlowBatchError` partial-progress details for chunked independent Flow operations.
- `ClaimHydrationError` lease and partial-record details when a legacy compact claim fallback read fails.
- `ConnectionClosedError.requestDisposition` for distinguishing definitely-unsent requests from operations that may have reached the server.
- Array-native overloads for large scalar batches and explicit `*Many` methods where an array can itself be a codec-backed value.
- Exact `FencingToken` (`number | bigint`) support across Flow records, claims, workflows, mutations, and compact batches.
- Configurable per-connection `maxPendingControlRequests` protection for correlated native control traffic.

### Changed

- Full-record multi-state claims now use one generic command response; compact tuple hydration remains only as a compatibility fallback.
- Legacy compact full-claim hydration uses bounded, ordered concurrency with a configurable client limit.
- Flow many-request limits are client-configurable and shared by producers, terminal mutations, and Queue/Workflow workers; oversized independent mutations chunk safely.
- Custom-binary commands are wrapped as `COMMAND_EXEC` items inside one native pipeline instead of being issued individually.
- `FLOW.CREATE`, `FLOW.VALUE.MGET`, and rich single-item Flow mutations use their typed native payloads, including named values, value references, state metadata, and lineage.
- Rich `FLOW.CREATE_MANY`, terminal-many, transition-many, `FLOW.GET`, `FLOW.VALUE.PUT`, and `FLOW.SPAWN_CHILDREN` calls use typed native payloads without dropping parent/item value options.
- `WorkflowContext.valueMany()` deduplicates references and hydrates all missing named values with one `FLOW.VALUE.MGET` request.
- Long-running per-job workers continuously refill acknowledged free slots by default, with configurable wave scheduling and refill coalescing; finite `runOnce` and batch APIs retain wave semantics.
- Continuous queue workers fuse batchable completions with same-route replacement claims by default, with an explicit client-side opt-out and lease-preserving error handling.
- Per-job workers cap claims to handler concurrency, honor `workers`, `concurrency`, `claimDrainBatches`, and workflow throughput settings, and stop renewal before terminal writes.
- Topology connections now use transport-aware endpoint identities and coalesce concurrent connection creation.
- Abortable blocking workers use bounded safe long polls, and finite server block durations extend rather than consume the transport response timeout.
- Auto-partitioned `enqueueMany` producers group in linear time, preserve result order, chunk batches at 1,000 items by default, and dispatch partitions with bounded configurable concurrency.
- Native connections honor STARTUP lane, lane-queue, pipeline-command, and outbound-frame limits; oversized pipelines are split into ordered byte-fitting chunks.
- Auto-batch boundaries and individual-request fallbacks preserve same-key write dependencies while retaining read-only and disjoint-key concurrency.
- Nonblocking fused Flow calls remain eligible for safe auto-batching, while blocking schedule polls always bypass shared batches.
- Flow history, schedule, query, effect, and governance helpers use their dedicated native opcodes.
- Custom topology host allowlists are normalized once at pool construction instead of once per routed request.
- Native request encoding preflights generic and custom-binary bodies, then writes directly into the final frame without intermediate full-body copies.
- Representable `FLOW.VALUE.MGET`, `FLOW.LIST`, `FLOW.CANCEL_MANY`, and `FLOW.TRANSITION_MANY` requests use the server's compact binary formats; rich or embedded shapes retain their typed/generic fallbacks.
- Tagged releases run the live FerricStore integration suite before npm publishing and GitHub release creation.
- Core routing and native wire-ABI parity now run in CI against a pinned FerricStore revision and fail instead of silently skipping when the checkout is unavailable.

### Fixed

- Default governance effect reads to the Flow auto-partition, fall back from unsupported compact request shapes to typed native bodies, and retain topology changes that arrive during an active refresh.
- Preserve multi-state claims, server reclaim defaults, custom-payload pipeline commands, binary routing keys, queue outcome value mutations, and chunked management events.
- Close failed heartbeats and explicit client shutdowns promptly, reject response correlation mismatches, and prevent topology pools from reconnecting after close.
- Wait for initial startup, authentication, and topology discovery before `fromUrl()` or `fromUrls()` resolves, while retaining reconnect support afterward.
- Preserve successful per-item results when auto-batching through an executor that only implements `executeCommand`.
- Start handling partial multi-state workflow claims before probing another state, so a later claim failure cannot discard already leased jobs.
- Normalize non-finite loop and delay controls, preserve exact signed 64-bit native integers as `bigint`, and reject unsafe number coercion.
- Distinguish missing lazy value references from stored values that decode to JSON `null`.
- Decode string-backed payloads and named Flow values through the configured codec instead of exposing their encoded representation.
- Surface per-item queue completion failures, force full claims when payloads or named values are requested, and validate value multi-get response shape and cardinality.
- Preserve exact bigint counter replies in typed increment helpers and reject unsafe coercion in number-only helpers.
- Fan out decomposable cross-shard multi-key operations by leader/lane, preserve pipeline order, and route Flow value writes by partition/owner even when user data resembles option tokens.
- Enforce worker concurrency when a server over-returns, cache missing values without caching caller defaults, and parse fragmented native frames with linear copying.
- Preserve exact int64 module counts and ranks, including bigint CMS/TopK increments, without changing safe-number replies.
- Reject malformed pipeline cardinality, Flow records and collections, array responses, and non-integer numeric replies instead of returning empty/default/`NaN` values.
- Append native response chunks in O(1) metadata time and perform only one final payload copy.
- Decode zero-width compact `MGET` values and flat-pair `KEY_INFO` replies correctly; classify structured native busy responses without misclassifying `BUSYGROUP` errors.
- Normalize bracketed IPv6 endpoints, enforce connect timeouts through TLS handshakes, and gracefully retire topology connections removed by refreshes.
- Reject malformed management, fetch, rate-limit, key-info, Flow, and many-result responses instead of silently coercing them.
- Refill advertised native request slots as soon as responses settle, honor reduced or zero windows, and bound local waiters with `maxQueuedRequests`.
- Reject positional multi-result replies whose cardinality differs from their MGET/hash/module/geo inputs.
- Pause and bound encoded writes after socket backpressure with `maxQueuedWriteBytes`, without adding a queue to healthy writes.
- Wait for in-flight auto-batches before explicit pipelines and shutdown, remove settled queue-completion promises correctly, and decode CAS replies without JavaScript truthiness.
- Chunk SDK timers beyond Node's maximum delay, preserving long request, connect, heartbeat, batching, sleep, and worker durations.
- Reject overlapping, incomplete, or internally inconsistent SHARDS topologies before replacing active routes.
- Send every client-split pipeline chunk before surfacing the first item error, and keep blocking/session commands out of `autoBatch: "all"`.
- Fail closed on malformed success, boolean, and key/value replies; expose `FERRICSTORE.METRICS` as its Prometheus text response.
- Preserve exact `SET` expiry semantics and fall back for invalid/conflicting option combinations instead of emitting a different direct payload.
- Accumulate sequential blocking durations for pipeline response deadlines, force-close half-open sockets, and derive TLS SNI only from DNS hosts.
- Keep multi-state `runOnce()` finite, validate text-only introspection replies, and expose only server-valid option sets on Flow administration helpers.
- Reject conflicting `GETEX`/`HGETEX` expiry modes before dispatch and model their mutual exclusivity in TypeScript.
- Preserve invalid `FLOW.SEARCH` boolean tokens for server-side validation instead of coercing them to `false` in the native fast path.
- Lock down reconnect semantics so uncertain in-flight operations surface without replay, while later requests can reconnect safely.
- Preserve completion-before-claim ordering when native pipelines are unavailable, map collected pipeline item errors to typed SDK errors, and cap worker Flow batches at the server's 1,000-item limit.
- Fall back safely for explicit control-command pipelines, keep control commands out of all-mode auto-batches, preserve opt-in ordering across topology route splits, and normalize non-finite completion depths.
- Reject transaction and subscription commands that require a pinned connection before native dispatch, preventing failed transaction pipelines from partially mutating data.
- Sequence state-changing control fallbacks, await every concurrently launched fallback or topology group before surfacing errors, and preserve arbitrary JavaScript rejection reasons.
- Treat zero partition claim credit as no work, validate workflow worker exception policies at construction, and cap every producer many-request at 1,000 items without splitting non-independent semantics.
- Validate Queue worker exception policies before claiming, preserve non-independent AUTO batch semantics, and report durable async completion successes through `QueueCompletionError` when a later write fails.
- Reject connection-local mutations on reconnecting and topology executors, and fail unsupported native `CLIENT TRACKING`/`CACHING` helpers before network dispatch.
- Enforce the configured Flow many limit across create, complete, transition, retry, fail, and cancel operations without changing single-request behavior below the limit.
- Preserve compact pipeline list values that begin with status-like data, keep connection-blocking commands out of native pipeline execution, and never replay an uncertain composite pipeline during reconnect.
- Recover topology refresh and route helpers after idle closes, and replace closed or draining cached topology adapters before reuse.
- Preserve arbitrary non-`Error` rejection reasons through command-only auto-batches and expose ordered fallback execution through `client.pipeline(commands, { ordered: true })`.
- Drain all required asynchronous queue completions before reporting a failure, including successes that appear later in the pending list.
- Reject partial per-item `createMany` metadata before dispatch, and isolate every connection-scoped command from all-mode auto-batches.
- Preserve every leased claim and successfully hydrated record when legacy compact-claim hydration fails; stop new auto-partition chunks after the first observed failure.
- Bound incomplete-frame fragment metadata while preserving complete-frame zero-copy decoding, and enforce compact GET framing plus nested Flow value-reference decode budgets.
- Reject keyless or multi-source `BITOP NOT` calls and invalid `CMS.MERGE` source/weight shapes before network dispatch.
- Await reconnect replacement cleanup during shutdown, recover read-only topology operations from wrapped idle closes, revalidate coalesced adapters before use, and drain blocking async completions in linear scans.
- Route Flow commands with the core SHA-256 partition tags and CRC32 auto-partition buckets, including command-specific claim selectors; VM-specific schedule ids remain on the control path.
- Drain replacement leases that arrive after worker shutdown begins, and preserve compact replacement leases alongside hydration failures.
- Batch Queue batch-handler completion/retry/fail outcomes with independent per-item validation instead of sequential fail-fast terminal writes.
- Bound stalled socket write tombstones, index chunk cleanup by request, prefer the last healthy topology refresh endpoint, and make concurrent shutdown callers join one close operation.
- Decode codec-backed composite hash, list, set, sorted-set, stream, and geo replies, including flat native stream entries and `SET GET`; add `TOPK.COUNT` parity.
- Separate direct connection, topology, and client-side HA native option types, rejecting options at layers where they would otherwise be ignored.
- Recognize worker control outcomes only when created by the SDK helpers, while keeping helper discriminants immutable at runtime.
- Preserve missing `MEMORY USAGE` replies as `null` instead of coercing them to zero.
- Route partitioned Flow commands from the shared protocol grammar, including named-value reads whose data resembles option tokens, without disabling compact claim payloads.
- Reject contradictory `SET`, `ZADD`, and `GEOADD` options before dispatch and model the same exclusions in TypeScript.
- Never reconnect-replay requests that entered a socket before `GOAWAY`, retirement, or connection failure; definitely-unsent queued and synchronously rejected writes remain safely retryable.
- Retry one explicitly safe routed command or fused pipeline after topology refresh, and include final response chunks in global assembly accounting without adding payload copies.
- Keep state-only compact Flow claims on the direct custom-response path and correlate each compact response with its requested tuple mode instead of guessing from mixed shapes.
- Reject unsafe overload retry hints and cap the complete server-hint, backoff, and jitter delay at the configured producer maximum.
- Reject sparse public argument arrays before dispatch, avoiding silent argument compaction while retaining linear bulk-command construction.
- Preserve fencing tokens beyond JavaScript's safe range as `bigint` without moving representable many-item mutations off compact native paths.

### Removed

- Remove the unsupported RedisJSON helper surface and routing metadata; `JsonCodec` remains available for ordinary value serialization.

### Security

- Give `endpointPolicy: "none"` strict seed-endpoint-only semantics instead of treating it as unrestricted.
- Reject mixed plaintext/TLS topology seed sets and derive learned-node transport strictly from the configured URL scheme.
- Keep URL-derived topology credentials scoped to their own seed connection even when reached through learned topology, reject conflicting duplicate seed credentials, and reuse one complete pair only for non-seed learned endpoints.
- Validate learned topology endpoints before every connection, including refresh fallbacks.
- Reject malformed or misspelled request-context authorization metadata instead of silently removing it from `COMMAND_EXEC`.
- Avoid prototype mutation when decoding or building maps with user/server-controlled keys.
- Bound incomplete chunk accumulation and reject oversized frame declarations before buffering their bodies.
- Bound incomplete response chunk counts, including zero-byte frames, per response and across the connection.
- Reject outbound request bodies that exceed the server-advertised native frame limit before writing them to the socket.
- Apply one cumulative allocation budget across nested and compact response values instead of resetting the item limit per child container.
- Avoid persisting local paths or secrets from handler stack traces unless explicitly requested.
- Pin privileged release actions to immutable commits and scope read, npm provenance, and GitHub release permissions to separate jobs.
- Pin every third-party action in test and security workflows to an immutable commit.

## [0.1.6] - 2026-07-08

### Added

- Flow state mode policy support for FerricStore `0.7.5`, including opt-in FIFO/PARALLEL state policy maps.
- Workflow state `mode` registration and policy installation for FIFO states.
- Invocation helper commands over the public native command contract.
- Native `COMMAND_EXEC` request-context payload support.

### Changed

- Updated local Docker guidance and integration compose image to FerricStore `0.7.5`.
- Workflow workers now apply exception policy only to handler execution; state mutation and SDK validation errors propagate.

## [0.1.5] - 2026-07-06

### Added

- Exported native command opcode table and direct native builders for current control-plane opcodes.
- `FLOW.SEARCH` client helper with attribute and state metadata filters.
- FerricStore management helpers for capabilities, ACL list alias, namespace, quota, and telemetry commands.
- Live integration guard comparing server `OPTIONS` opcodes to SDK constants.

### Changed

- Updated local Docker guidance and integration compose image to FerricStore `0.7.2`.

### Fixed

- Kept unpartitioned Flow reads on the topology control path while routing explicitly partitioned Flow commands to shards.
- Routed `BITOP`, `XREAD`/`XREADGROUP`, and `RENAME`/`RENAMENX` by their real key positions under topology routing.
- Restricted default learned endpoint trust to exact seed endpoints unless a host is explicitly added to `trustedHosts`.

## [0.1.4] - 2026-07-04

### Added

- Native adapter TCP keepalive options and default idle heartbeat pings, disableable with `heartbeatIntervalMs: 0`.
- Reconnecting executor for stale native connections, enabled by default from `FerricStoreClient.fromUrl()`.

### Fixed

- Reconnect and retry once when a native connection was already closed before a new command was sent.

## [0.1.3] - 2026-07-03

### Added

- CommonJS package export alongside the existing ESM export.
- Package export smoke test covering both `import()` and `require()`.

### Changed

- Lowered the Node.js support floor to Node.js 22.22.
- Expanded CI coverage to Node.js 22.22, 24, and 26.

## [0.1.0] - Unreleased

### Added

- Initial TypeScript SDK package.
- Low-level `FlowClient` for FerricFlow `FLOW.*` commands.
- Queue API for durable queue-shaped workloads.
- Workflow API for explicit state-machine workflows.
- Typed FerricStore KV/data-structure helpers.
- `RawCodec` and `JsonCodec`.
- Unit tests, examples, TypeDoc config, npm packaging, and CI workflows.
