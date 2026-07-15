import { randomUUID } from "node:crypto";
import type { FerricStoreClient } from "./client.js";
import {
  type ClaimedItem,
  type CreateItem,
  type FlowRecord,
  type MaxActiveMs,
  type StateMeta,
  type WorkerConfig
} from "./types.js";
import { type CommandArgument } from "./internal.js";
import { QueueWorker } from "./queue-worker.js";
import { snapshotFlowClientOptions } from "./flow-client-options.js";
export { QueueWorker } from "./queue-worker.js";
export { QueueCompletionError } from "./queue-completion-error.js";

export type QueueJob = FlowRecord | ClaimedItem;
export type QueueHandler = (job: QueueJob) => Promise<unknown> | unknown;
export type QueueBatchHandler = (jobs: QueueJob[]) => Promise<unknown> | unknown;

export interface QueueOptions {
  type: string;
  state?: string;
  worker?: string;
}

export interface QueueWorkerResult {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
}

export class QueueClient {
  readonly flow: FerricStoreClient;

  constructor(flow: FerricStoreClient) {
    this.flow = flow;
  }

  queue(options: QueueOptions | string): Queue {
    return new Queue(this.flow, typeof options === "string" ? { type: options } : options);
  }
}

export class Queue {
  readonly client: FerricStoreClient;
  readonly type: string;
  readonly state: string;
  readonly defaultWorker: string;

  constructor(client: FerricStoreClient, options: QueueOptions) {
    const captured = snapshotFlowClientOptions(options, "Queue");
    this.client = client;
    this.type = captured.type;
    this.state = captured.state ?? "queued";
    this.defaultWorker = captured.worker ?? `${this.type}-${randomUUID()}`;
  }

  async enqueue(id: string, options: {
    attributes?: Record<string, CommandArgument>;
    payload?: unknown;
    partitionKey?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    retentionTtlMs?: number;
    maxActiveMs?: MaxActiveMs;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
    returnRecord?: boolean;
  } = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.client.enqueue(id, {
      ...options,
      state: this.state,
      type: this.type
    });
  }

  async enqueueMany(items: CreateItem[], options: {
    attributes?: Record<string, CommandArgument>;
    partitionKey?: string;
    autoPartitionBatchSize?: number;
    autoPartitionConcurrency?: number;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    independent?: boolean;
    retentionTtlMs?: number;
    maxActiveMs?: MaxActiveMs;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
  } = {}): Promise<unknown[] | unknown> {
    return await this.client.enqueueMany(items, {
      ...options,
      state: this.state,
      type: this.type
    });
  }

  worker(options: WorkerConfig = {}): QueueWorker {
    return new QueueWorker(this, options);
  }
}
