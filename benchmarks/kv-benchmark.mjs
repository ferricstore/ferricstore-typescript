#!/usr/bin/env node
import { FlowClient, RawCodec } from '../dist/index.js';
import { performance } from 'node:perf_hooks';

const args = parseArgs(process.argv.slice(2));
const url = arg(args, 'url', 'ferric://127.0.0.1:6388');
const command = arg(args, 'command', 'get');
const seconds = Number(arg(args, 'seconds', '30'));
const pipeline = Number(arg(args, 'pipeline', command === 'get' ? '1000' : '500'));
const inflightBatches = Number(arg(args, 'inflight-batches', '64'));
const clients = Number(arg(args, 'clients', '1'));
const keyCount = Number(arg(args, 'key-count', '100000'));
const valueBytes = Number(arg(args, 'value-bytes', '256'));
const prefix = arg(args, 'prefix', `ts-kv-${Date.now()}`);
const pretty = has(args, 'pretty');

const value = Buffer.alloc(valueBytes, 120);
const conns = await Promise.all(Array.from({ length: clients }, () => FlowClient.fromUrl(url, { codec: new RawCodec() })));

try {
  if (command === 'get') {
    await warm(conns[0], prefix, keyCount, pipeline, value);
  }
  const result = await run(conns, { command, seconds, pipeline, inflightBatches, keyCount, prefix, value });
  const output = { benchmark: 'typescript_protocol_kv', url, command, seconds, pipeline, inflight_batches: inflightBatches, clients, key_count: keyCount, value_bytes: valueBytes, ...result };
  console.log(pretty ? JSON.stringify(output, null, 2) : JSON.stringify(output));
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
  let requests = 0;
  let errors = 0;
  let stopped = false;
  const latencies = [];
  const deadline = performance.now() + options.seconds * 1000;

  async function worker(client) {
    const pending = new Set();
    const submit = () => {
      const commands = [];
      const startSeq = sequence;
      sequence += options.pipeline;
      for (let i = 0; i < options.pipeline; i += 1) {
        const n = (startSeq + i) % options.keyCount;
        const key = `${options.prefix}:${n}`;
        commands.push(options.command === 'get' ? ['GET', key] : ['SET', key, options.value]);
      }
      const started = performance.now();
      const promise = client.pipeline(commands)
        .then(() => {
          requests += commands.length;
          latencies.push(performance.now() - started);
        })
        .catch(() => { errors += commands.length; })
        .finally(() => pending.delete(promise));
      pending.add(promise);
    };

    while (!stopped) {
      while (pending.size < options.inflightBatches && performance.now() < deadline) submit();
      if (performance.now() >= deadline) stopped = true;
      if (pending.size > 0) await Promise.race(pending);
    }
    await Promise.allSettled([...pending]);
  }

  const started = performance.now();
  await Promise.all(conns.map(worker));
  const elapsed = (performance.now() - started) / 1000;
  return { requests, errors, requests_per_sec: requests / elapsed, elapsed_seconds: elapsed, batch_latency_ms: stats(latencies) };
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
function has(args, key) { return args.has(key); }
