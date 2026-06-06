# Contributing

Thanks for helping improve the FerricStore TypeScript SDK.

## Development Setup

Use Node.js 24 or newer.

```bash
npm install
npm run check
```

`npm run check` runs:

- strict TypeScript type checking;
- ESLint;
- Vitest unit tests;
- package build and declaration generation;
- TypeDoc generation.

## Local FerricStore

For examples and integration testing:

```bash
docker compose up -d ferricstore
npm run test:integration
docker compose down -v
```

## Design Rules

- Keep the SDK thin over FerricStore RESP commands.
- Prefer explicit FerricFlow outcomes over hidden replay or instrumentation.
- Preserve the escape hatch: anything missing from typed helpers must still work through `client.command(...)`.
- Add tests for command shape when adding a typed wrapper.
- Keep examples runnable against local FerricStore.

## Pull Request Checklist

- Add or update tests.
- Update README/docs when changing public API.
- Run `npm run check`.
- Run `npm run pack:dry-run` for packaging changes.
