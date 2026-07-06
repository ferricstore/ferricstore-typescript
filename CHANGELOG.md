# Changelog

All notable changes to the FerricStore TypeScript SDK will be documented here.

The format is based on Keep a Changelog, and this project follows semver once it reaches `1.0`.

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
