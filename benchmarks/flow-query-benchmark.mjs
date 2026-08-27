#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { clearInterval, setImmediate, setInterval } from 'node:timers';
import {
  FerricStoreClient,
  NativeAdapter,
  RawCodec
} from '../dist/index.js';

const args = parseArgs(process.argv.slice(2));
const url = arg(args, 'url', 'ferric://127.0.0.1:6388');
const requests = positiveInteger(args, 'requests', 500);
const concurrency = positiveInteger(args, 'concurrency', 2);
const rows = boundedInteger(args, 'rows', 100, 1, 100);
const warmup = nonNegativeInteger(args, 'warmup', 20);
const protocolLanes = positiveInteger(args, 'protocol-lanes', 8);
const mode = arg(args, 'mode', 'both');
const pretty = has(args, 'pretty');
if (mode !== 'raw' && mode !== 'convenience' && mode !== 'both') {
  throw new TypeError('--mode must be raw, convenience, or both');
}

const runId = randomUUID();
const partitionKey = `ts-sdk:flow-query-benchmark:${runId}:partition`;
const type = `ts-sdk-flow-query-benchmark-${runId}`;
const state = 'ready';
const now = Date.now();
const query =
  'FROM runs WHERE partition_key = @partition AND type = @type AND state = @state ' +
  `ORDER BY updated_at_ms DESC LIMIT ${rows} RETURN RECORDS`;
const params = { partition: partitionKey, state, type };
const commandCounts = new Map();
const adapter = await NativeAdapter.fromUrl(url, { protocolLanes });
const client = new FerricStoreClient({
  async close() {
    await adapter.close();
  },
  async executeCommand(...command) {
    countCommand(command);
    return await adapter.executeCommand(...command);
  },
  async executeCommandArgs(command) {
    countCommand(command);
    return await adapter.executeCommandArgs(command);
  }
}, { codec: new RawCodec() });

try {
  await client.createMany(
    partitionKey,
    Array.from({ length: rows }, (_, index) => ({
      id: `ts-sdk:flow-query-benchmark:${runId}:${index}`
    })),
    { idempotent: true, nowMs: now, runAtMs: now, state, type }
  );
  const sample = await waitForProjection();
  const explained = await client.explain(query, params);
  if (explained.plan.order !== 'native') {
    throw new Error(`FLOW.QUERY benchmark requires native ordering, got ${String(explained.plan.order)}`);
  }
  const operations = {
    raw: async () => {
      const result = await client.query(query, params);
      if (result.kind !== 'records' || result.records.length !== rows) {
        throw new Error('raw FLOW.QUERY returned an unexpected result');
      }
    },
    convenience: async () => {
      const result = await client.list(type, { count: rows, partitionKey, state });
      if (result.length !== rows) {
        throw new Error('Flow list convenience returned an unexpected result');
      }
    }
  };

  await warmOperations(operations);

  let result;
  if (mode === 'both') {
    commandCounts.clear();
    globalThis.gc?.();
    result = { comparison: await runComparison(operations) };
    assertCommandShape(requests * 2, 'comparison');
    result.comparison.commands = sortedRecord(commandCounts);
  } else {
    commandCounts.clear();
    globalThis.gc?.();
    const measured = await runLoad(operations[mode]);
    assertCommandShape(requests, mode);
    measured.commands = sortedRecord(commandCounts);
    result = { modes: { [mode]: measured } };
  }

  const output = {
    benchmark: 'typescript_flow_query',
    concurrency,
    ...result,
    protocol_lanes: protocolLanes,
    requests_per_mode: requests,
    plan_order: explained.plan.order,
    response_bytes: sample.usage.responseBytes,
    rows_per_response: rows,
    server_url: url,
    warmup_per_mode: warmup
  };
  console.log(pretty ? JSON.stringify(output, null, 2) : JSON.stringify(output));
} finally {
  await client.close();
}

async function waitForProjection() {
  const deadline = performance.now() + 30_000;
  let last;
  while (performance.now() < deadline) {
    last = await client.query(query, params);
    if (last.kind === 'records' && last.records.length === rows) return last;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('FLOW.QUERY benchmark projection did not become ready', { cause: last });
}

async function warmOperations(operations) {
  for (let index = 0; index < warmup; index += 1) {
    if (mode === 'raw') await operations.raw();
    else if (mode === 'convenience') await operations.convenience();
    else if (index % 2 === 0) {
      await operations.raw();
      await operations.convenience();
    } else {
      await operations.convenience();
      await operations.raw();
    }
  }
}

async function runLoad(operation) {
  const latencies = new Array(requests);
  let next = 0;
  const measured = await measureLoad(async () => {
    await Promise.all(Array.from(
      { length: Math.min(concurrency, requests) },
      async () => {
        while (next < requests) {
          const index = next;
          next += 1;
          const requestStarted = performance.now();
          await operation();
          latencies[index] = performance.now() - requestStarted;
        }
      }
    ));
  });
  return {
    ...measured,
    latency_ms: statistics(latencies),
    records_per_second: requests * rows / measured.elapsed_seconds,
    requests_per_second: requests / measured.elapsed_seconds
  };
}

async function runComparison(operations) {
  const latencies = {
    convenience: new Array(requests),
    raw: new Array(requests)
  };
  let next = 0;
  const measured = await measureLoad(async () => {
    await Promise.all(Array.from(
      { length: Math.min(concurrency, requests) },
      async () => {
        while (next < requests) {
          const index = next;
          next += 1;
          const order = index % 2 === 0
            ? ['raw', 'convenience']
            : ['convenience', 'raw'];
          for (const selected of order) {
            const requestStarted = performance.now();
            await operations[selected]();
            latencies[selected][index] = performance.now() - requestStarted;
          }
        }
      }
    ));
  });
  const raw = statistics(latencies.raw);
  const convenience = statistics(latencies.convenience);
  return {
    ...measured,
    combined_records_per_second: requests * 2 * rows / measured.elapsed_seconds,
    combined_requests_per_second: requests * 2 / measured.elapsed_seconds,
    convenience_latency_ms: convenience,
    convenience_overhead_percent: {
      average: relativePercent(convenience.average, raw.average),
      p50: relativePercent(convenience.p50, raw.p50),
      p95: relativePercent(convenience.p95, raw.p95)
    },
    raw_latency_ms: raw
  };
}

async function measureLoad(load) {
  const before = process.memoryUsage();
  const beforeCpu = process.cpuUsage();
  let peakHeap = before.heapUsed;
  let peakRss = before.rss;
  let peakExternal = before.external;
  const sampleMemory = () => {
    const memory = process.memoryUsage();
    peakHeap = Math.max(peakHeap, memory.heapUsed);
    peakRss = Math.max(peakRss, memory.rss);
    peakExternal = Math.max(peakExternal, memory.external);
  };
  const memoryTimer = setInterval(sampleMemory, 5);
  memoryTimer.unref();
  const started = performance.now();
  try {
    await load();
  } finally {
    clearInterval(memoryTimer);
    sampleMemory();
  }
  const elapsedSeconds = (performance.now() - started) / 1_000;
  const cpu = process.cpuUsage(beforeCpu);
  const after = process.memoryUsage();
  if (globalThis.gc != null) {
    globalThis.gc();
    await new Promise((resolve) => setImmediate(resolve));
    globalThis.gc();
  }
  const retained = process.memoryUsage();
  return {
    cpu: {
      system_ms: cpu.system / 1_000,
      total_ms: (cpu.user + cpu.system) / 1_000,
      utilization_percent: (cpu.user + cpu.system) / (elapsedSeconds * 10_000),
      user_ms: cpu.user / 1_000
    },
    elapsed_seconds: elapsedSeconds,
    memory: {
      external_after_bytes: after.external,
      external_before_bytes: before.external,
      external_peak_bytes: peakExternal,
      external_peak_delta_bytes: peakExternal - before.external,
      external_retained_bytes: retained.external,
      external_retained_delta_bytes: retained.external - before.external,
      heap_after_bytes: after.heapUsed,
      heap_before_bytes: before.heapUsed,
      heap_delta_bytes: after.heapUsed - before.heapUsed,
      heap_peak_bytes: peakHeap,
      heap_peak_delta_bytes: peakHeap - before.heapUsed,
      heap_retained_bytes: retained.heapUsed,
      heap_retained_delta_bytes: retained.heapUsed - before.heapUsed,
      rss_after_bytes: after.rss,
      rss_before_bytes: before.rss,
      rss_delta_bytes: after.rss - before.rss,
      rss_peak_bytes: peakRss,
      rss_peak_delta_bytes: peakRss - before.rss,
      rss_retained_bytes: retained.rss,
      rss_retained_delta_bytes: retained.rss - before.rss
    }
  };
}

function assertCommandShape(expectedQueries, label) {
  const queryCalls = commandCounts.get('FLOW.QUERY') ?? 0;
  const getCalls = commandCounts.get('FLOW.GET') ?? 0;
  if (queryCalls !== expectedQueries || getCalls !== 0) {
    throw new Error(
      `${label} performed ${queryCalls} FLOW.QUERY and ${getCalls} FLOW.GET calls for ${expectedQueries} operations`
    );
  }
}

function countCommand(command) {
  const first = command[0];
  const name = Buffer.isBuffer(first)
    ? first.toString('utf8').toUpperCase()
    : String(first).toUpperCase();
  commandCounts.set(name, (commandCounts.get(name) ?? 0) + 1);
}

function statistics(values) {
  if (values.some((value) => !Number.isFinite(value))) {
    throw new Error('benchmark latency samples are incomplete');
  }
  const sorted = [...values].sort((left, right) => left - right);
  const pick = (percentile) => sorted[
    Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * percentile) - 1))
  ];
  return {
    average: sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
    maximum: sorted[sorted.length - 1],
    p50: pick(0.5),
    p95: pick(0.95),
    p99: pick(0.99),
    samples: sorted.length
  };
}

function relativePercent(value, baseline) {
  return (value / baseline - 1) * 100;
}

function sortedRecord(values) {
  return Object.fromEntries([...values].sort(([left], [right]) => left.localeCompare(right)));
}

function parseArgs(argv) {
  const output = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (next == null || next.startsWith('--')) output.set(key, true);
    else {
      output.set(key, next);
      index += 1;
    }
  }
  return output;
}

function boundedInteger(values, key, fallback, minimum, maximum) {
  const value = Number(arg(values, key, String(fallback)));
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`--${key} must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}

function positiveInteger(values, key, fallback) {
  return boundedInteger(values, key, fallback, 1, Number.MAX_SAFE_INTEGER);
}

function nonNegativeInteger(values, key, fallback) {
  return boundedInteger(values, key, fallback, 0, Number.MAX_SAFE_INTEGER);
}

function arg(values, key, fallback) {
  return String(values.get(key) ?? fallback);
}

function has(values, key) {
  return values.has(key);
}
