#!/usr/bin/env node
import { FerricStoreClient, RawCodec } from '../dist/index.js';
import { readFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const args = parseArgs(process.argv.slice(2));
const url = arg(args, 'url', 'ferric://127.0.0.1:6388');
const command = arg(args, 'command', 'get');
const requestMode = arg(args, 'request-mode', 'pipeline');
const seconds = positiveNumber(args, 'seconds', 30);
const pipeline = positiveInteger(args, 'pipeline', command === 'get' ? 1000 : 500);
const inflightBatches = positiveInteger(args, 'inflight-batches', 64);
const clients = positiveInteger(args, 'clients', 1);
const keyCount = positiveInteger(args, 'key-count', 100000);
const valueBytes = nonNegativeInteger(args, 'value-bytes', 256);
const autoBatchMaxCommands = positiveInteger(args, 'auto-batch-max-commands', pipeline);
const autoBatchMaxDelayMs = nonNegativeNumber(args, 'auto-batch-max-delay-ms', 0);
const requestedRequests = optionalPositiveInteger(args, 'requests');
const minThroughput = optionalNonNegativeNumber(args, 'min-throughput');
const prefix = arg(args, 'prefix', `ts-kv-${Date.now()}`);
const pretty = has(args, 'pretty');
const httpTarget = /^https?:\/\//u.test(url);
const http2 = has(args, 'http2');
const httpMaxConnections = positiveInteger(args, 'http-max-connections', 100);
const caFile = optionalArg(args, 'ca-file');

const value = Buffer.alloc(valueBytes, 120);
const clientOptions = {
  codec: new RawCodec(),
  ...(httpTarget ? {
    httpOptions: {
      http2,
      maxConnections: httpMaxConnections,
      ...(process.env.FERRICSTORE_BEARER_TOKEN == null
        ? {}
        : { bearerToken: process.env.FERRICSTORE_BEARER_TOKEN }),
      ...(process.env.FERRICSTORE_PASSWORD == null
        ? {}
        : {
            password: process.env.FERRICSTORE_PASSWORD,
            username: process.env.FERRICSTORE_USERNAME ?? 'default'
          }),
      ...(caFile == null ? {} : { tlsOptions: { ca: readFileSync(caFile) } })
    }
  } : {}),
  ...(requestMode === 'auto-batch' ? {
    autoBatch: {
      enabled: true,
      maxCommands: autoBatchMaxCommands,
      maxDelayMs: autoBatchMaxDelayMs
    }
  } : {})
};
const conns = await Promise.all(Array.from({ length: clients }, () => FerricStoreClient.fromUrl(url, clientOptions)));

try {
  if (command === 'get') {
    await warm(conns[0], prefix, keyCount, pipeline, value);
  }
  const result = await run(conns, {
    command,
    requestMode,
    seconds,
    pipeline,
    inflightBatches,
    keyCount,
    prefix,
    requestedRequests,
    value
  });
  const output = {
    benchmark: 'typescript_protocol_kv',
    transport: httpTarget ? (http2 ? 'http2' : 'http1') : 'native',
    url,
    command,
    request_mode: requestMode,
    seconds,
    pipeline,
    inflight_batches: inflightBatches,
    clients,
    http_max_connections: httpTarget ? httpMaxConnections : null,
    key_count: keyCount,
    value_bytes: valueBytes,
    requested_requests: requestedRequests ?? null,
    min_throughput: minThroughput ?? null,
    auto_batch: requestMode === 'auto-batch' ? {
      max_commands: autoBatchMaxCommands,
      max_delay_ms: autoBatchMaxDelayMs
    } : null,
    ...result
  };
  console.log(pretty ? JSON.stringify(output, null, 2) : JSON.stringify(output));
  assertBenchmark(result, requestedRequests, minThroughput);
} finally {
  await Promise.allSettled(conns.map((client) => client.close()));
}

async function warm(client, prefix, keyCount, pipelineDepth, value) {
  for (let base = 0; base < keyCount; base += pipelineDepth) {
    const commands = [];
    for (let i = 0; i < pipelineDepth && base + i < keyCount; i += 1) {
      commands.push(['SET', `${prefix}:${base + i}`, value]);
    }
    await client.pipeline(commands);
  }
}

async function run(conns, options) {
  let sequence = 0;
  let submittedRequests = 0;
  let requests = 0;
  let errors = 0;
  const latencies = [];
  const deadline = performance.now() + options.seconds * 1000;
  const fixedRequestCount = options.requestedRequests != null;

  async function worker(client) {
    const pending = new Set();
    const submit = () => {
      if (!fixedRequestCount && performance.now() >= deadline) return false;
      const commandCount = fixedRequestCount
        ? Math.min(options.pipeline, options.requestedRequests - submittedRequests)
        : options.pipeline;
      if (commandCount <= 0) return false;
      const commands = [];
      const startSeq = sequence;
      sequence += commandCount;
      submittedRequests += commandCount;
      for (let i = 0; i < commandCount; i += 1) {
        const n = (startSeq + i) % options.keyCount;
        const key = `${options.prefix}:${n}`;
        commands.push(options.command === 'get' ? ['GET', key] : ['SET', key, options.value]);
      }
      const started = performance.now();
      const promise = executeBatch(client, commands, options.requestMode)
        .then(() => {
          requests += commands.length;
          latencies.push(performance.now() - started);
        })
        .catch(() => { errors += commands.length; })
        .finally(() => pending.delete(promise));
      pending.add(promise);
      return true;
    };

    while (true) {
      while (pending.size < options.inflightBatches && submit()) {
        // Fill only the configured number of acknowledged in-flight batches.
      }
      const allSubmitted = fixedRequestCount
        ? submittedRequests >= options.requestedRequests
        : performance.now() >= deadline;
      if (pending.size === 0 && allSubmitted) break;
      if (pending.size > 0) await Promise.race(pending);
    }
  }

  const started = performance.now();
  await Promise.all(conns.map(worker));
  const elapsed = (performance.now() - started) / 1000;
  return {
    submitted_requests: submittedRequests,
    requests,
    errors,
    requests_per_sec: requests / elapsed,
    elapsed_seconds: elapsed,
    batch_latency_ms: stats(latencies)
  };
}

function assertBenchmark(result, requestedRequests, minThroughput) {
  if (requestedRequests != null && result.submitted_requests !== requestedRequests) {
    throw new Error(`benchmark submitted ${result.submitted_requests} requests; expected ${requestedRequests}`);
  }
  if (result.errors > 0 || (requestedRequests != null && result.requests !== requestedRequests)) {
    throw new Error(`benchmark completed ${result.requests} requests with ${result.errors} errors`);
  }
  if (minThroughput != null && result.requests_per_sec < minThroughput) {
    throw new Error(
      `throughput regression: ${result.requests_per_sec.toFixed(2)} requests/sec is below ${minThroughput}`
    );
  }
}

async function executeBatch(client, commands, requestMode) {
  if (requestMode === 'pipeline') {
    return await client.pipeline(commands);
  }
  if (requestMode === 'auto-batch' || requestMode === 'direct') {
    return await Promise.all(commands.map((command) => client.command(...command)));
  }
  throw new Error(`unsupported --request-mode ${requestMode}; expected pipeline, auto-batch, or direct`);
}

function stats(values) {
  if (values.length === 0) return { avg: 0, p50: 0, p95: 0, p99: 0, max: 0, samples: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const pick = (p) => sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1))];
  return { avg: sorted.reduce((a, b) => a + b, 0) / sorted.length, p50: pick(0.5), p95: pick(0.95), p99: pick(0.99), max: sorted[sorted.length - 1], samples: sorted.length };
}

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
function optionalArg(args, key) { return has(args, key) ? String(args.get(key)) : undefined; }
function has(args, key) { return args.has(key); }
function numberArg(args, key, fallback) {
  const value = Number(args.get(key) ?? fallback);
  if (!Number.isFinite(value)) throw new Error(`--${key} must be a finite number`);
  return value;
}
function positiveNumber(args, key, fallback) {
  const value = numberArg(args, key, fallback);
  if (value <= 0) throw new Error(`--${key} must be positive`);
  return value;
}
function nonNegativeNumber(args, key, fallback) {
  const value = numberArg(args, key, fallback);
  if (value < 0) throw new Error(`--${key} must be nonnegative`);
  return value;
}
function positiveInteger(args, key, fallback) {
  const value = positiveNumber(args, key, fallback);
  if (!Number.isSafeInteger(value)) throw new Error(`--${key} must be a safe integer`);
  return value;
}
function nonNegativeInteger(args, key, fallback) {
  const value = nonNegativeNumber(args, key, fallback);
  if (!Number.isSafeInteger(value)) throw new Error(`--${key} must be a safe integer`);
  return value;
}
function optionalPositiveInteger(args, key) {
  return has(args, key) ? positiveInteger(args, key, 0) : undefined;
}
function optionalNonNegativeNumber(args, key) {
  return has(args, key) ? nonNegativeNumber(args, key, 0) : undefined;
}
