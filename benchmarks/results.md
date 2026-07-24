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
SET: 1,315,294 requests/s
batch p50: 22.49 ms
batch p95: 26.93 ms
batch p99: 31.62 ms
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
GET: 4,473,717 requests/s
batch p50: 13.62 ms
batch p95: 20.23 ms
batch p99: 22.66 ms
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
  --workers 128 \
  --producers 4 \
  --partitions 128 \
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
create: 233,040-245,105 flows/s
end-to-end: 63,240-73,714 flows/s
claim calls: 384
empty claims: 128
completed: 100000 / 100000
errors: 0
```

Sustained 1M live run on the same local Docker setup:

```bash
npm run bench:dbos -- \
  --flows 1000000 \
  --workers 128 \
  --producers 4 \
  --partitions 128 \
  --create-batch-size 500 \
  --create-async-depth 4 \
  --worker-start-backlog 0 \
  --claim-batch-size 500 \
  --clients 2 \
  --protocol-lanes 64 \
  --pretty
```

Result:

```text
create: 91,385 flows/s
end-to-end: 44,427 flows/s
claim calls: 2541
empty claims: 370
completed: 1000000 / 1000000
errors: 0
```

## FQL1 raw versus typed convenience queries

Environment for these measurements:

- Server: disposable image built from the matching local FerricStore workspace
- Runtime: Node.js 25.2.1
- Date: 2026-07-24
- Shape: clean local Docker server per run, concurrency 2, 20 warmups per mode,
  1,000 measured calls per mode, and interleaved/alternating raw and convenience calls

Command:

```bash
npm run bench:flow-query -- \
  --requests 1000 \
  --concurrency 2 \
  --rows 100 \
  --warmup 20 \
  --pretty
```

Baseline from three clean-server runs with 100 records (about 59 KiB) per response:

```text
combined throughput: 386.5-414.4 requests/s (38,653-41,441 records/s)
raw p50:             3.92-4.19 ms
convenience p50:     4.25-4.54 ms
convenience p50 cost: 7.75%-8.37%
client CPU:          37.8%-40.4% of one core
wire commands/run:   2,000 FLOW.QUERY, 0 FLOW.GET
```

After changing convenience queries to decode native object records directly,
three equivalent clean-server runs produced:

```text
combined throughput: 387.0-422.0 requests/s (38,697-42,199 records/s)
raw p50:             4.07-4.23 ms
convenience p50:     4.29-4.48 ms
convenience p50 cost: 5.43%-6.02%
client CPU/run:      1,872-1,895 ms
wire commands/run:   2,000 FLOW.QUERY, 0 FLOW.GET
```

The median convenience p50 gap fell from 8.36% to 5.66%, a 32% reduction in
the additional typed-decoding cost. Median total client CPU fell from 1,956 ms
to 1,875 ms (4.1%), while median combined throughput remained effectively flat
at 388.6 versus 391.1 requests/s. This isolates the gain to client work rather
than claiming a noisy server-throughput improvement.

The 1-record control remained within measurement noise: optimized convenience
p50 was 0.03 ms above raw. The larger-page cost is typed `FlowRecord`
materialization, not additional network requests. After forced collection, V8
heap and external-buffer use returned to baseline; RSS retained the allocator
high-water mark.

Final matching-contract validation (500 interleaved calls per mode, 100 rows)
produced 501.8 combined requests/s and 50,183 records/s. Raw and convenience
p50 were 3.56 ms and 3.78 ms respectively (6.3% convenience overhead), with
exactly 1,000 `FLOW.QUERY` commands and no `FLOW.GET` hydration. Forced GC
returned heap and external-buffer usage to below their pre-measurement values.

The bounded direct-request CI shape was also exercised with 10,000 `SET`
requests, four clients and eight in-flight requests per client: all 10,000
were acknowledged with zero errors at 556.4 requests/s, above the conservative
100 requests/s regression floor.

## Notes

- `--claim-block-ms` is intentionally omitted by default. In FerricStore, `BLOCK 0` means wait forever, so sending `BLOCK 0` from a benchmark can create SDK request timeouts on drained partitions.
- Two native client sockets with 64 protocol lanes each performed best for the TypeScript DBOS-style run in this Docker setup. One socket reached roughly 57k-62k/s; four sockets were slower than two.
- Compact native request bodies are enabled for `FLOW.CREATE_MANY` and compact job-only `FLOW.CLAIM_DUE`.
- `FLOW.COMPLETE_MANY` stays on the typed direct native opcode and worker completion uses `RETURN OK_ON_SUCCESS` to avoid materializing per-item success responses.
- Homogeneous native pipeline encoding writes one compact request buffer per batch for `GET`/`SET`, and compact pipeline success responses unwrap without per-item status tuple allocation.
