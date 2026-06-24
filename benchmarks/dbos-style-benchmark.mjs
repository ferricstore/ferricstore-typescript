#!/usr/bin/env node
import { FlowClient, RawCodec } from '../dist/index.js';
import { performance } from 'node:perf_hooks';
import { randomUUID } from 'node:crypto';

const args = parseArgs(process.argv.slice(2));
const url = arg(args, 'url', 'ferric://127.0.0.1:6388');
const flows = Number(arg(args, 'flows', '100000'));
const producers = Number(arg(args, 'producers', '4'));
const workers = Number(arg(args, 'workers', '16'));
const partitions = Number(arg(args, 'partitions', '16'));
const createBatchSize = Number(arg(args, 'create-batch-size', '500'));
const createAsyncDepth = Number(arg(args, 'create-async-depth', '4'));
const claimBatchSize = Number(arg(args, 'claim-batch-size', '500'));
const claimPartitionBatchSize = Number(arg(args, 'claim-partition-batch-size', '1'));
const claimBlockMs = has(args, 'claim-block-ms') ? Number(arg(args, 'claim-block-ms', '0')) : undefined;
const workerStartBacklog = Number(arg(args, 'worker-start-backlog', '0'));
const completeAsyncDepth = Number(arg(args, 'complete-async-depth', '1'));
const clientCount = Number(arg(args, 'clients', '2'));
const protocolLanes = Number(arg(args, 'protocol-lanes', '64'));
const pretty = has(args, 'pretty');
const type = arg(args, 'type', `ts-dbos-${randomUUID()}`);

const clients = await Promise.all(Array.from({ length: clientCount }, () => FlowClient.fromUrl(url, {
  codec: new RawCodec(),
  nativeOptions: { protocolLanes }
})));
let created = 0;
let completed = 0;
let claimed = 0;
let claimCalls = 0;
let emptyClaims = 0;

try {
  const started = performance.now();
  const producerTasks = Array.from({ length: producers }, (_, index) => producer(index, clients[index % clients.length]));
  const workerTasks = Array.from({ length: workers }, (_, index) => worker(index, clients[(producers + index) % clients.length]));
  await Promise.all(producerTasks);
  const createDone = performance.now();
  await Promise.all(workerTasks);
  const done = performance.now();
  const totalSeconds = (done - started) / 1000;
  const createSeconds = (createDone - started) / 1000;
  const output = {
    benchmark: 'typescript_dbos_style', url, type, flows, producers, workers, partitions,
    create_batch_size: createBatchSize, create_async_depth: createAsyncDepth,
    claim_batch_size: claimBatchSize, claim_partition_batch_size: claimPartitionBatchSize,
    claim_block_ms: claimBlockMs ?? null,
    worker_start_backlog: workerStartBacklog,
    complete_async_depth: completeAsyncDepth,
    clients: clientCount, protocol_lanes: protocolLanes,
    created, claimed, completed, claim_calls: claimCalls, empty_claims: emptyClaims,
    create_seconds: createSeconds, total_seconds: totalSeconds,
    create_flows_per_sec: created / createSeconds,
    end_to_end_flows_per_sec: completed / totalSeconds
  };
  console.log(pretty ? JSON.stringify(output, null, 2) : JSON.stringify(output));
} finally {
  await Promise.allSettled(clients.map((client) => client.close()));
}

async function producer(producerIndex, client) {
  const pending = new Set();
  for (let base = producerIndex * createBatchSize; base < flows; base += createBatchSize * producers) {
    const items = [];
    for (let offset = 0; offset < createBatchSize && base + offset < flows; offset += 1) {
      const n = base + offset;
      items.push({ id: `${type}:${n}`, partitionKey: `p${n % partitions}` });
    }
    if (items.length === 0) continue;
    const promise = client.createMany(undefined, items, { type, state: 'queued', independent: true })
      .then(() => { created += items.length; })
      .catch((error) => {
        throw new Error(`producer ${producerIndex} create batch ${base} failed: ${error?.message ?? error}`);
      });
    pending.add(promise.finally(() => pending.delete(promise)));
    if (pending.size >= createAsyncDepth) await Promise.race(pending);
  }
  await Promise.allSettled([...pending]);
}

async function worker(workerIndex, client) {
  const partitionKeys = workerPartitionKeys(workerIndex);
  const pending = new Set();
  while (created < workerStartBacklog && completed < flows) {
    await sleep(1);
  }
  while (completed < flows || created < flows) {
    const jobs = await client.claimDue(type, {
      state: 'queued',
      ...(partitionKeys.length === 1 ? { partitionKey: partitionKeys[0] } : { partitionKeys }),
      worker: `ts-worker-${workerIndex}`,
      leaseMs: 30000,
      limit: claimBatchSize,
      jobOnly: true,
      blockMs: claimBlockMs
    }).catch((error) => {
      throw new Error(`worker ${workerIndex} claim ${partitionKeys.join(',')} failed: ${error?.message ?? error}`);
    });
    claimCalls += 1;
    if (jobs.length === 0) {
      emptyClaims += 1;
      if (created >= flows) break;
      await sleep(1);
      continue;
    }
    claimed += jobs.length;
    const promise = client.completeJobs(jobs, { independent: true })
      .then(() => { completed += jobs.length; })
      .catch((error) => {
        throw new Error(`worker ${workerIndex} complete ${jobs.length} ${partitionKeys.join(',')} failed: ${error?.message ?? error}`);
      });
    pending.add(promise.finally(() => pending.delete(promise)));
    if (pending.size >= completeAsyncDepth) await Promise.race(pending);
  }
  await Promise.allSettled([...pending]);
}

function workerPartitionKeys(workerIndex) {
  const keys = [];
  const start = workerIndex * claimPartitionBatchSize;
  for (let offset = 0; offset < claimPartitionBatchSize && start + offset < partitions; offset += 1) {
    keys.push(`p${start + offset}`);
  }
  return keys.length === 0 ? [`p${workerIndex % partitions}`] : keys;
}

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function parseArgs(argv) {
  const out = new Map();
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (next == null || next.startsWith('--')) out.set(key, true);
    else { out.set(key, next); i += 1; }
  }
  return out;
}
function arg(args, key, fallback) { return String(args.get(key) ?? fallback); }
function has(args, key) { return args.has(key); }
