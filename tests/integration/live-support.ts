import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { expect } from "vitest";
import {
  FerricStoreClient,
  type FerricStoreClientFromUrlOptions,
  type ClaimedItem,
  type FencedItem,
  type FlowRecord
} from "../../src/index.js";

export function url(): string {
  return process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388";
}

export async function integrationClient(
  options: FerricStoreClientFromUrlOptions = {}
): Promise<FerricStoreClient> {
  const target = url();
  if (!target.startsWith("http://") && !target.startsWith("https://")) {
    return await FerricStoreClient.fromUrl(target, options);
  }
  const password = process.env.FERRICSTORE_PASSWORD;
  if (password == null || password === "") throw new Error("FERRICSTORE_PASSWORD is required for HTTP integration");
  const caFile = process.env.FERRICSTORE_CA_FILE;
  return await FerricStoreClient.fromUrl(target, {
    ...options,
    reconnect: false,
    httpOptions: {
      ...(options.httpOptions ?? {}),
      http2: process.env.FERRICSTORE_HTTP2 !== "false",
      password,
      tlsOptions: {
        ...(options.httpOptions?.tlsOptions ?? {}),
        ...(caFile == null || caFile === "" ? {} : { ca: readFileSync(caFile) })
      },
      username: process.env.FERRICSTORE_USERNAME ?? "default"
    }
  });
}

export function suffix(): string {
  return randomUUID();
}

export async function eventually<T>(
  operation: () => Promise<T>,
  ready: (value: T) => boolean,
  message: string,
  options: { readonly intervalMs?: number; readonly timeoutMs?: number } = {}
): Promise<T> {
  const deadline = performance.now() + (options.timeoutMs ?? 30_000);
  const intervalMs = options.intervalMs ?? 50;
  let lastValue: T | undefined;
  let lastError: unknown;

  while (performance.now() < deadline) {
    try {
      lastValue = await operation();
      lastError = undefined;
      if (ready(lastValue)) return lastValue;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(message, { cause: lastError ?? lastValue });
}

export async function waitForAclProjection<T>(
  operation: () => Promise<T>,
  options: { readonly intervalMs?: number; readonly timeoutMs?: number } = {}
): Promise<T> {
  const deadline = performance.now() + (options.timeoutMs ?? 5_000);
  const intervalMs = options.intervalMs ?? 25;
  let lastError: Error | undefined;

  while (performance.now() < deadline) {
    try {
      return await operation();
    } catch (error) {
      if (!(error instanceof Error) ||
          !/^(?:NOPERM|LOADING) ACL catalog projection unavailable$/i.test(error.message)) {
        throw error;
      }
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error("ACL catalog projection did not become ready", { cause: lastError });
}

export function text(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  throw new Error(`expected text-compatible value, got ${typeof value}`);
}

export function ok(value: unknown): boolean {
  return value === true || value === 1 || value === "OK" || (Buffer.isBuffer(value) && value.toString("utf8") === "OK");
}

export async function expectSupportedOrKnownServerError<T>(
  promise: Promise<T>,
  pattern = /unsupported|unknown|not supported|not enabled|invalid|password|cluster|no config file|shard index/i
): Promise<T | undefined> {
  try {
    return await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(pattern);
    return undefined;
  }
}

export function field(source: unknown, name: string): unknown {
  if (source instanceof Map) {
    if (source.has(name)) return source.get(name);
    const expected = Buffer.from(name);
    for (const [key, value] of source.entries()) {
      if ((Buffer.isBuffer(key) || key instanceof Uint8Array) && Buffer.from(key).equals(expected)) return value;
    }
    return undefined;
  }
  if (typeof source === "object" && source != null) {
    const record = source as Record<string, unknown>;
    return record[name];
  }
  return undefined;
}

export function isReadonlyArray(value: unknown): value is readonly unknown[] {
  return Array.isArray(value);
}

export function eventId(event: unknown): string {
  if (Array.isArray(event) && event.length > 0) {
    return text(event[0]);
  }
  const value = field(event, "event_id") ?? field(event, "id");
  if (value == null) {
    throw new Error(`history event did not contain an event id: ${JSON.stringify(event)}`);
  }
  return text(value);
}

export function fenced(job: ClaimedItem): FencedItem {
  return {
    fencingToken: job.fencingToken,
    id: job.id,
    leaseToken: job.leaseToken,
    partitionKey: job.partitionKey
  };
}

export function expectStateMeta(record: FlowRecord | undefined, state: string, expected: Record<string, unknown>): void {
  expect(record?.stateMeta).toMatchObject({ [state]: binaryStateMeta(expected) });
}

export function binaryStateMeta(expected: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(expected).map(([key, value]) => [
    key,
    typeof value === "string" ? Buffer.from(value) : value
  ]));
}

export async function deletePrefixedKeys(flow: FerricStoreClient, prefix: string): Promise<void> {
  const keys = (await flow.kv.keys(`${prefix}*`)).map(text);
  if (keys.length > 0) {
    await flow.kv.del(keys);
  }
}

export async function claimOne(
  flow: FerricStoreClient,
  type: string,
  state: string,
  partitionKey: string,
  options: {
    leaseMs?: number;
    nowMs?: number;
    worker?: string;
  } = {}
): Promise<ClaimedItem> {
  const deadline = performance.now() + 5_000;
  const claim = async (): Promise<ClaimedItem[]> => await flow.claimJobs(type, {
    leaseMs: options.leaseMs ?? 30_000,
    limit: 1,
    nowMs: options.nowMs,
    partitionKey,
    state,
    worker: options.worker ?? "ts-sdk-integration-worker"
  });
  let jobs = await claim();
  while (jobs.length === 0 && performance.now() < deadline) {
    // A successful empty claim has no mutation to replay. Errors propagate above.
    await new Promise((resolve) => setTimeout(resolve, 25));
    jobs = await claim();
  }
  expect(jobs).toHaveLength(1);
  const job = jobs[0];
  if (job == null) {
    throw new Error("expected a claimed job");
  }
  return job;
}

export async function createAndClaim(
  flow: FerricStoreClient,
  type: string,
  runId: string,
  name: string,
  options: { nowMs?: number; leaseMs?: number; state?: string } = {}
): Promise<{ readonly id: string; readonly partitionKey: string; readonly job: ClaimedItem }> {
  const id = `ts-sdk:${name}:${runId}`;
  const partitionKey = `${id}:partition`;
  const state = options.state ?? "queued";
  const nowMs = options.nowMs ?? Date.now();
  await flow.create(id, {
    idempotent: true,
    nowMs,
    partitionKey,
    payload: { name },
    runAtMs: nowMs,
    state,
    type
  });
  const job = await claimOne(flow, type, state, partitionKey, {
    leaseMs: options.leaseMs,
    nowMs
  });
  return { id, job, partitionKey };
}

export async function createManyAndClaim(
  flow: FerricStoreClient,
  type: string,
  runId: string,
  name: string,
  state: string,
  now: number
): Promise<{ readonly ids: readonly string[]; readonly jobs: ClaimedItem[]; readonly partitionKey: string }> {
  const ids = [`ts-sdk:${name}:${runId}:a`, `ts-sdk:${name}:${runId}:b`];
  const partitionKey = `ts-sdk:${name}:${runId}:partition`;
  await flow.createMany(partitionKey, ids.map((id) => ({ id })), {
    nowMs: now,
    runAtMs: now,
    state,
    type
  });
  const jobs = await flow.claimJobs(type, {
    limit: ids.length,
    nowMs: now,
    partitionKey,
    state,
    worker: `ts-sdk-${name}-worker`
  });
  expect(jobs).toHaveLength(ids.length);
  return { ids, jobs, partitionKey };
}
