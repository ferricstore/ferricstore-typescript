#!/usr/bin/env node
import { FerricStoreClient, QueueClient, RawCodec } from '../dist/index.js';
import { performance } from 'node:perf_hooks';
import { randomUUID } from 'node:crypto';
import { clearTimeout } from 'node:timers';

const args = parseArgs(process.argv.slice(2));
const url = arg(args, 'url', 'ferric://127.0.0.1:6388');
const requestMode = arg(args, 'request-mode', 'many');
const flows = Number(arg(args, 'flows', '100000'));
const producers = Number(arg(args, 'producers', '4'));
const workers = Number(arg(args, 'workers', '16'));
const partitions = Number(arg(args, 'partitions', '16'));
const createBatchSize = Number(arg(args, 'create-batch-size', '500'));
const createAsyncDepth = Number(arg(args, 'create-async-depth', '4'));
const claimBatchSize = Number(arg(args, 'claim-batch-size', '500'));
const claimPartitionBatchSize = Number(arg(args, 'claim-partition-batch-size', '1'));
const claimDrainBatches = Number(arg(args, 'claim-drain-batches', '1'));
const claimBlockMs = has(args, 'claim-block-ms') ? Number(arg(args, 'claim-block-ms', '0')) : undefined;
const workerStartBacklog = Number(arg(args, 'worker-start-backlog', '0'));
const completeAsyncDepth = Number(arg(args, 'complete-async-depth', '1'));
const clientCount = Number(arg(args, 'clients', '2'));
const protocolLanes = Number(arg(args, 'protocol-lanes', '64'));
const autoBatchMaxCommands = Number(arg(args, 'auto-batch-max-commands', String(Math.max(createBatchSize, claimBatchSize))));
const autoBatchMaxDelayMs = Number(arg(args, 'auto-batch-max-delay-ms', '0'));
const trackDuplicates = has(args, 'track-duplicates');
const pretty = has(args, 'pretty');
const type = arg(args, 'type', `ts-dbos-${randomUUID()}`);
const autoBatch = requestMode === 'auto-batch';
const wakeCoordinator = requestMode === 'queue-worker' && has(args, 'wake-credits')
  ? createPartitionWakeCoordinator(partitions)
  : undefined;

const clients = await Promise.all(Array.from({ length: clientCount }, () => FerricStoreClient.fromUrl(url, {
  codec: new RawCodec(),
  nativeOptions: { protocolLanes },
  ...(autoBatch ? {
    autoBatch: {
      enabled: true,
      maxCommands: autoBatchMaxCommands,
      maxDelayMs: autoBatchMaxDelayMs
    }
  } : {})
})));
let created = 0;
let completed = 0;
let claimed = 0;
let claimCalls = 0;
let emptyClaims = 0;
let duplicateCompletions = 0;
const completedIds = trackDuplicates ? new Set() : undefined;

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
    request_mode: requestMode,
    create_batch_size: createBatchSize, create_async_depth: createAsyncDepth,
    claim_batch_size: claimBatchSize, claim_partition_batch_size: claimPartitionBatchSize,
    claim_drain_batches: claimDrainBatches,
    claim_block_ms: claimBlockMs ?? null,
    worker_start_backlog: workerStartBacklog,
    complete_async_depth: completeAsyncDepth,
    clients: clientCount, protocol_lanes: protocolLanes,
    track_duplicates: trackDuplicates,
    wake_credits: wakeCoordinator != null,
    auto_batch: autoBatch ? {
      max_commands: autoBatchMaxCommands,
      max_delay_ms: autoBatchMaxDelayMs
    } : null,
    created, claimed, completed, duplicate_completions: duplicateCompletions, claim_calls: claimCalls, empty_claims: emptyClaims,
    create_seconds: createSeconds, total_seconds: totalSeconds,
    create_flows_per_sec: created / createSeconds,
    end_to_end_flows_per_sec: completed / totalSeconds
  };
  console.log(pretty ? JSON.stringify(output, null, 2) : JSON.stringify(output));
  if (completed !== flows) {
    process.exitCode = 1;
  }
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
    const promise = createItems(client, items)
      .then(() => {
        created += items.length;
        wakeCoordinator?.addItems(items);
      })
      .catch((error) => {
        throw new Error(`producer ${producerIndex} create batch ${base} failed: ${error?.message ?? error}`);
      });
    pending.add(promise.finally(() => pending.delete(promise)));
    if (pending.size >= createAsyncDepth) await Promise.race(pending);
  }
  await Promise.all([...pending]);
}

async function worker(workerIndex, client) {
  const partitionKeys = workerPartitionKeys(workerIndex);
  if (partitionKeys.length === 0 && wakeCoordinator == null) {
    return;
  }
  if (requestMode === 'queue-worker') {
    return await queueWorker(workerIndex, client, partitionKeys);
  }
  const pending = new Set();
  while (created < workerStartBacklog && completed < flows) {
    await sleep(1);
  }
  while (completed < flows || created < flows) {
    let drained = false;
    for (let drain = 0; drain < claimDrainBatches && (completed < flows || created < flows); drain += 1) {
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
        break;
      }
      drained = true;
      claimed += jobs.length;
      const promise = completeJobs(client, jobs)
        .then(() => {
          if (completedIds == null) {
            completed += jobs.length;
          } else {
            let unique = 0;
            for (const job of jobs) {
              if (completedIds.has(job.id)) {
                duplicateCompletions += 1;
              } else {
                completedIds.add(job.id);
                unique += 1;
              }
            }
            completed += unique;
          }
        })
        .catch((error) => {
          throw new Error(`worker ${workerIndex} complete ${jobs.length} ${partitionKeys.join(',')} failed: ${error?.message ?? error}`);
        });
      pending.add(promise.finally(() => pending.delete(promise)));
      if (pending.size >= completeAsyncDepth) await Promise.race(pending);
      if (jobs.length < claimBatchSize) break;
    }
    if (!drained) {
      if (created >= flows) break;
      await sleep(1);
      continue;
    }
  }
  await Promise.all([...pending]);
}

async function createItems(client, items) {
  if (requestMode === 'many' || requestMode === 'queue-worker') {
    return await client.createMany(undefined, items, { type, state: 'queued', independent: true });
  }
  if (requestMode === 'auto-batch' || requestMode === 'direct') {
    return await Promise.all(items.map((item) => client.create(item.id, {
      partitionKey: item.partitionKey,
      state: 'queued',
      type
    })));
  }
  throw new Error(`unsupported --request-mode ${requestMode}; expected many, queue-worker, auto-batch, or direct`);
}

async function completeJobs(client, jobs) {
  if (requestMode === 'many') {
    return await client.completeJobs(jobs, { independent: true });
  }
  if (requestMode === 'auto-batch' || requestMode === 'direct') {
    return await Promise.all(jobs.map((job) => client.complete(job.id, {
      fencingToken: job.fencingToken,
      leaseToken: job.leaseToken,
      partitionKey: job.partitionKey
    })));
  }
  throw new Error(`unsupported --request-mode ${requestMode}; expected many, queue-worker, auto-batch, or direct`);
}

async function queueWorker(workerIndex, client, partitionKeys) {
  const queue = new QueueClient(client).queue({ type, state: 'queued', worker: `ts-worker-${workerIndex}` });
  const worker = queue.worker({
    batchSize: claimBatchSize,
    blockMs: claimBlockMs,
    claimPayload: false,
    completeAsyncDepth,
    completeIndependent: true,
    leaseMs: 30000,
    ...(wakeCoordinator == null ? (partitionKeys.length === 1 ? { partitionKey: partitionKeys[0] } : { partitionKeys }) : {}),
    worker: `ts-worker-${workerIndex}`
  });
  while (created < workerStartBacklog && completed < flows) {
    await sleep(1);
  }
  while (completed < flows || created < flows) {
    const selected = wakeCoordinator?.takePartitions(workerIndex, Math.max(claimPartitionBatchSize, 1), claimBatchSize);
    if (wakeCoordinator != null && selected == null) {
      if (created >= flows && wakeCoordinator.totalCredit() === 0) break;
      await wakeCoordinator.wait(1);
      continue;
    }
    const result = selected == null
      ? await worker.runBatchOnce(() => undefined)
      : await worker.runBatchOnceForPartitionKeys(() => undefined, selected.partitionKeys, { claimCredit: selected.credit });
    if (selected != null && result.claimed < selected.credit) {
      wakeCoordinator?.returnCredit(selected.taken, selected.credit - result.claimed);
    }
    claimCalls += 1;
    completed += result.completed;
    if (result.claimed === 0) {
      emptyClaims += 1;
      if (created >= flows && (wakeCoordinator == null || wakeCoordinator.totalCredit() === 0)) break;
      await sleep(1);
      continue;
    }
    claimed += result.claimed;
  }
  const flushed = await worker.flush();
  completed += flushed;
}

function workerPartitionKeys(workerIndex) {
  const keys = [];
  if (claimPartitionBatchSize <= 1) {
    for (let partition = workerIndex; partition < partitions; partition += workers) {
      keys.push(`p${partition}`);
    }
    return keys.length === 0 ? [`p${workerIndex % partitions}`] : keys;
  }

  const start = workerIndex * claimPartitionBatchSize;
  for (let offset = 0; offset < claimPartitionBatchSize && start + offset < partitions; offset += 1) {
    keys.push(`p${start + offset}`);
  }
  return keys;
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

function createPartitionWakeCoordinator(partitions) {
  const credits = Array.from({ length: partitions }, () => 0);
  const waiters = new Set();
  let cursor = 0;

  function notify() {
    const pending = [...waiters];
    waiters.clear();
    for (const waiter of pending) waiter();
  }

  return {
    addItems(items) {
      for (const item of items) {
        this.add(partitionIndexFromKey(item.partitionKey), 1);
      }
    },

    add(index, count) {
      if (!Number.isInteger(index) || index < 0 || index >= partitions || count <= 0) {
        return;
      }
      credits[index] += count;
      notify();
    },

    totalCredit() {
      let total = 0;
      for (const credit of credits) total += credit;
      return total;
    },

    takePartitions(workerIndex, maxPartitions, maxCredit) {
      if (partitions <= 0 || maxPartitions <= 0 || maxCredit <= 0) {
        return null;
      }
      const indices = [];
      const takenPartitions = [];
      let credit = 0;
      const start = (cursor + workerIndex) % partitions;
      for (let attempt = 0; attempt < partitions && indices.length < maxPartitions && credit < maxCredit; attempt += 1) {
        const index = (start + attempt) % partitions;
        const available = credits[index];
        if (available <= 0) continue;
        const taken = Math.min(available, maxCredit - credit);
        credits[index] -= taken;
        credit += taken;
        indices.push(index);
        takenPartitions.push({ index, credit: taken });
      }
      if (credit <= 0) {
        return null;
      }
      cursor = (start + 1) % partitions;
      return {
        credit,
        taken: takenPartitions,
        partitionKeys: indices.map((index) => `p${index}`)
      };
    },

    returnCredit(takenPartitions, credit) {
      let remaining = credit;
      for (let index = takenPartitions.length - 1; index >= 0 && remaining > 0; index -= 1) {
        const taken = takenPartitions[index];
        const returned = Math.min(taken.credit, remaining);
        credits[taken.index] += returned;
        remaining -= returned;
      }
      if (credit > remaining) {
        notify();
      }
    },

    async wait(ms) {
      await new Promise((resolve) => {
        const waiter = () => {
          clearTimeout(timeout);
          resolve();
        };
        const timeout = setTimeout(() => {
          waiters.delete(waiter);
          resolve();
        }, ms);
        waiters.add(waiter);
      });
    }
  };
}

function partitionIndexFromKey(key) {
  if (typeof key !== 'string' || !key.startsWith('p')) {
    return -1;
  }
  const index = Number(key.slice(1));
  return Number.isInteger(index) ? index : -1;
}
