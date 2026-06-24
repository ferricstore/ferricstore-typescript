# FerricStore TypeScript SDK benchmark results

Environment:
- Machine: local macOS development machine
- Server: `ghcr.io/ferricstore/ferricstore:0.5.2` via `docker compose`
- Protocol: native `ferric://127.0.0.1:6388`
- SDK transport: native `ferric://` sockets with multiplexed protocol lanes
- Date: 2026-06-23

## KV throughput

Command:

```bash
npm run bench:kv -- \
  --command set \
  --seconds 10 \
  --pipeline 500 \
  --inflight-batches 64 \
  --clients 1 \
  --key-count 100000 \
  --value-bytes 16 \
  --pretty
```

Result:

```text
SET: 1,159,974 requests/s
batch p50: 25.12 ms
batch p95: 31.66 ms
batch p99: 40.86 ms
errors: 0
```

Command:

```bash
npm run bench:kv -- \
  --command get \
  --seconds 10 \
  --pipeline 1000 \
  --inflight-batches 64 \
  --clients 1 \
  --key-count 100000 \
  --value-bytes 16 \
  --pretty
```

Result:

```text
GET: 2,500,330 requests/s
batch p50: 24.22 ms
batch p95: 30.71 ms
batch p99: 37.65 ms
errors: 0
```

## DBOS-style Flow throughput

Best live run with workers active while producers run:

```bash
npm run bench:dbos -- \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --create-batch-size 500 \
  --create-async-depth 4 \
  --claim-batch-size 500 \
  --clients 1 \
  --protocol-lanes 64 \
  --pretty
```

Result:

```text
create: 124,683 flows/s
end-to-end: 47,187 flows/s
claim calls: 240
empty claims: 32
completed: 100000 / 100000
errors: 0
```

Higher logical partition/worker shape:

```bash
npm run bench:dbos -- \
  --flows 100000 \
  --workers 64 \
  --producers 4 \
  --partitions 64 \
  --create-batch-size 500 \
  --create-async-depth 4 \
  --worker-start-backlog 100000 \
  --claim-batch-size 500 \
  --clients 2 \
  --protocol-lanes 64 \
  --pretty
```

Result:

```text
create: 243,714 flows/s
end-to-end: 69,825 flows/s
claim calls: 320
empty claims: 64
completed: 100000 / 100000
errors: 0
```

## Notes

- `--claim-block-ms` is intentionally omitted by default. In FerricStore, `BLOCK 0` means wait forever, so sending `BLOCK 0` from a benchmark can create SDK request timeouts on drained partitions.
- Two native client sockets with 64 protocol lanes each performed best for the TypeScript DBOS-style run in this Docker setup. One socket reached roughly 57k-62k/s; four sockets were slower than two.
- Compact native request bodies are enabled for `FLOW.CREATE_MANY` and compact job-only `FLOW.CLAIM_DUE`.
- `FLOW.COMPLETE_MANY` stays on the typed direct native opcode and worker completion uses `RETURN OK_ON_SUCCESS` to avoid materializing per-item success responses.
