# Changelog

All notable changes to the FerricStore TypeScript SDK will be documented here.

The format is based on Keep a Changelog, and this project follows semver once it reaches `1.0`.

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
