import { randomUUID } from "node:crypto";
import type {
  FerricStoreClient,
  FlowPolicyOptions,
  FlowStateMode,
  FlowStatePolicyLike
} from "./client.js";
import type { CompleteOutcome, FailOutcome, Outcome, RetryOutcome, TransitionOutcome } from "./outcomes.js";
import { complete, fail, isOutcome, retry } from "./outcomes.js";
import {
  normalizeExceptionPolicy,
  type ChildSpec,
  type ClaimedItem,
  type ExceptionPolicy,
  type FlowRecord,
  type RetryPolicy,
  type StateMeta,
  type ValueConfig,
  type WorkerConfig
} from "./types.js";
import { sleep } from "./internal.js";

export type WorkflowHandler = (ctx: WorkflowContext) => Promise<Outcome | void | unknown> | Outcome | void | unknown;

export interface StateOptions {
  mode?: FlowStateMode;
  leaseMs?: number;
  claimPayload?: boolean;
  claimRecord?: boolean;
  claimValues?: string[];
  valueMaxBytes?: number;
  exceptionPolicy?: ExceptionPolicy;
  retryPolicy?: RetryPolicy;
  returnRecord?: boolean;
}

export interface WorkflowOptions {
  type: string;
  initialState?: string;
  valueConfig?: ValueConfig;
  worker?: string;
}

export interface WorkflowWorkerResult {
  claimed: number;
  applied: number;
  claimCalls: number;
  emptyClaims: number;
}

export interface StateRegistration {
  claimValues?: string[];
  claimPayload: boolean;
  claimRecord: boolean;
  exceptionPolicy: ExceptionPolicy;
  handler: WorkflowHandler;
  leaseMs: number;
  mode?: FlowStateMode;
  name: string;
  returnRecord: boolean;
  retryPolicy?: RetryPolicy;
  valueMaxBytes?: number;
}

export class WorkflowClient {
  readonly flow: FerricStoreClient;

  constructor(flow: FerricStoreClient) {
    this.flow = flow;
  }

  workflow(options: WorkflowOptions): Workflow {
    return new Workflow(this.flow, options);
  }
}

export class Workflow {
  readonly client: FerricStoreClient;
  readonly type: string;
  readonly initialState: string;
  readonly valueConfig: ValueConfig & { localCache: boolean };
  readonly defaultWorker: string;
  private readonly states = new Map<string, StateRegistration>();

  constructor(client: FerricStoreClient, options: WorkflowOptions) {
    this.client = client;
    this.type = options.type;
    this.initialState = options.initialState ?? "queued";
    this.valueConfig = {
      localCache: options.valueConfig?.localCache ?? false,
      valueMaxBytes: options.valueConfig?.valueMaxBytes
    };
    this.defaultWorker = options.worker ?? `${this.type}-${randomUUID()}`;
  }

  state(name: string, handler: WorkflowHandler, options: StateOptions = {}): this {
    this.states.set(name, {
      claimPayload: options.claimPayload ?? true,
      claimRecord: options.claimRecord ?? true,
      exceptionPolicy: normalizeExceptionPolicy(options.exceptionPolicy),
      handler,
      leaseMs: options.leaseMs ?? 30_000,
      ...(options.mode == null ? {} : { mode: normalizeStateMode(options.mode) }),
      name,
      returnRecord: options.returnRecord ?? false,
      valueMaxBytes: options.valueMaxBytes ?? this.valueConfig.valueMaxBytes,
      ...(options.claimValues == null ? {} : { claimValues: options.claimValues }),
      ...(options.retryPolicy == null ? {} : { retryPolicy: options.retryPolicy })
    });
    return this;
  }

  async start(id: string, options: {
    payload?: unknown;
    state?: string;
    partitionKey?: string;
    parentFlowId?: string;
    rootFlowId?: string;
    correlationId?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    retentionTtlMs?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
    returnRecord?: boolean;
  } = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.client.create(id, {
      ...options,
      state: options.state ?? this.initialState,
      type: this.type
    });
  }

  async startMany(items: {
    id: string;
    payload?: unknown;
    partitionKey?: string;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
  }[], options: {
    state?: string;
    partitionKey?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    idempotent?: boolean;
    independent?: boolean;
    retentionTtlMs?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
  } = {}): Promise<unknown[] | unknown> {
    return await this.client.createMany(options.partitionKey, items, {
      ...options,
      state: options.state ?? this.initialState,
      type: this.type
    });
  }

  async signal(id: string, options: Parameters<FerricStoreClient["signal"]>[1]): Promise<unknown> {
    return await this.client.signal(id, options);
  }

  async get(id: string, options: Parameters<FerricStoreClient["get"]>[1] = {}): Promise<FlowRecord | undefined> {
    return await this.client.get(id, options);
  }

  async history(id: string, options: Parameters<FerricStoreClient["history"]>[1] = {}): Promise<unknown[]> {
    return await this.client.history(id, options);
  }

  worker(options: WorkerConfig & { states?: string[] } = {}): WorkflowWorker {
    return new WorkflowWorker(this, options);
  }

  async installPolicy(options: Omit<FlowPolicyOptions, "mode" | "state" | "states"> = {}): Promise<unknown> {
    const states: Record<string, FlowStatePolicyLike> = {};
    for (const registration of this.states.values()) {
      if (registration.mode != null || registration.retryPolicy != null) {
        states[registration.name] = {
          ...(registration.mode == null ? {} : { mode: registration.mode }),
          ...(registration.retryPolicy == null ? {} : { retry: registration.retryPolicy })
        };
      }
    }
    return await this.client.installPolicy(this.type, {
      ...options,
      states
    });
  }

  stateNames(): string[] {
    return [...this.states.keys()];
  }

  stateRegistration(name: string): StateRegistration | undefined {
    return this.states.get(name);
  }
}

export class WorkflowContext {
  readonly workflow: Workflow;
  readonly job: FlowRecord | ClaimedItem;
  readonly stateName: string;
  readonly flow: WorkflowFlowCommands;
  private readonly valueCache = new Map<string, unknown>();

  constructor(workflow: Workflow, job: FlowRecord | ClaimedItem, stateName: string) {
    this.workflow = workflow;
    this.job = job;
    this.stateName = stateName;
    this.flow = new WorkflowFlowCommands(this);
  }

  get client(): FerricStoreClient {
    return this.workflow.client;
  }

  get id(): string {
    return this.job.id;
  }

  get type(): string {
    return this.job.type || this.workflow.type;
  }

  get state(): string {
    return this.job.state;
  }

  get logicalState(): string {
    return this.stateName;
  }

  get partitionKey(): string | undefined {
    return this.job.partitionKey;
  }

  get payload(): unknown {
    return this.job.payload;
  }

  get values(): Record<string, unknown> {
    return "values" in this.job && this.job.values != null ? this.job.values : {};
  }

  get stateMeta(): Record<string, unknown> {
    return "stateMeta" in this.job && this.job.stateMeta != null ? this.job.stateMeta : {};
  }

  get indexedStateMeta(): string | undefined {
    return "indexedStateMeta" in this.job ? this.job.indexedStateMeta : undefined;
  }

  get valueRefs(): Record<string, unknown> {
    return "valueRefs" in this.job && this.job.valueRefs != null ? this.job.valueRefs : {};
  }

  get leaseToken(): Buffer {
    return this.job.leaseToken;
  }

  get fencingToken(): number {
    return this.job.fencingToken;
  }

  get version(): number {
    return "version" in this.job ? this.job.version : 0;
  }

  get parentFlowId(): string | undefined {
    return "parentFlowId" in this.job ? this.job.parentFlowId : undefined;
  }

  get rootFlowId(): string | undefined {
    return "rootFlowId" in this.job ? this.job.rootFlowId : undefined;
  }

  get correlationId(): string | undefined {
    return "correlationId" in this.job ? this.job.correlationId : undefined;
  }

  async value(name: string, defaultValue?: unknown, options: { localCache?: boolean } = {}): Promise<unknown> {
    const useLocalCache = options.localCache ?? this.workflow.valueConfig.localCache;
    if (useLocalCache && this.valueCache.has(name)) {
      return this.valueCache.get(name);
    }

    if (name in this.values) {
      const value = this.values[name];
      if (useLocalCache) {
        this.valueCache.set(name, value);
      }
      return value;
    }

    const ref = valueRefToString(this.valueRefs[name]);
    if (ref == null) {
      return defaultValue;
    }

    const values = await this.client.valueMGet([ref], { maxBytes: this.valueMaxBytes() });
    const value = values[0] ?? defaultValue;
    if (useLocalCache) {
      this.valueCache.set(name, value);
    }
    return value;
  }

  async valueMany(names: string[], options: { localCache?: boolean } = {}): Promise<Record<string, unknown>> {
    const result: Record<string, unknown> = {};
    await Promise.all(
      names.map(async (name) => {
        result[name] = await this.value(name, undefined, options);
      })
    );
    return result;
  }

  private valueMaxBytes(): number | undefined {
    return this.workflow.stateRegistration(this.stateName)?.valueMaxBytes;
  }
}

export class WorkflowFlowCommands {
  constructor(private readonly ctx: WorkflowContext) {}

  async get(id = this.ctx.id, options: { partitionKey?: string; full?: boolean } = {}): Promise<FlowRecord | undefined> {
    return await this.ctx.client.get(id, {
      ...options,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async history(id = this.ctx.id, options: Parameters<FerricStoreClient["history"]>[1] = {}): Promise<unknown[]> {
    return await this.ctx.client.history(id, {
      ...options,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async create(id: string, options: Omit<Parameters<FerricStoreClient["create"]>[1], "type"> & { type?: string } = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.ctx.client.create(id, {
      ...options,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey,
      state: options.state ?? this.ctx.workflow.initialState,
      type: options.type ?? this.ctx.workflow.type
    });
  }

  async transition(toState: string, options: Partial<Omit<Parameters<FerricStoreClient["transition"]>[1], "toState" | "fromState" | "leaseToken" | "fencingToken">> & { fromState?: string } = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.ctx.client.transition(this.ctx.id, {
      ...options,
      fencingToken: this.ctx.fencingToken,
      fromState: options.fromState ?? this.ctx.state,
      leaseToken: this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey,
      toState
    });
  }

  async complete(options: Partial<Omit<Parameters<FerricStoreClient["complete"]>[1], "leaseToken" | "fencingToken">> = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.ctx.client.complete(this.ctx.id, {
      ...options,
      fencingToken: this.ctx.fencingToken,
      leaseToken: this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async retry(options: Partial<Omit<Parameters<FerricStoreClient["retry"]>[1], "leaseToken" | "fencingToken">> = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.ctx.client.retry(this.ctx.id, {
      ...options,
      fencingToken: this.ctx.fencingToken,
      leaseToken: this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async fail(options: Partial<Omit<Parameters<FerricStoreClient["fail"]>[1], "leaseToken" | "fencingToken">> = {}): Promise<FlowRecord | Buffer | unknown> {
    return await this.ctx.client.fail(this.ctx.id, {
      ...options,
      fencingToken: this.ctx.fencingToken,
      leaseToken: this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async signal(signal: string, options: Omit<Parameters<FerricStoreClient["signal"]>[1], "signal"> = {}): Promise<unknown> {
    return await this.ctx.client.signal(this.ctx.id, {
      ...options,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey,
      signal
    });
  }

  async putValue(name: string, value: unknown, options: Parameters<FerricStoreClient["valuePut"]>[1] = {}): Promise<unknown> {
    return await this.ctx.client.valuePut(value, {
      ...options,
      name,
      ownerFlowId: options.ownerFlowId ?? this.ctx.id,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }

  async value(name: string, defaultValue?: unknown, options: { localCache?: boolean } = {}): Promise<unknown> {
    return await this.ctx.value(name, defaultValue, options);
  }

  async values(names: string[], options: { localCache?: boolean } = {}): Promise<Record<string, unknown>> {
    return await this.ctx.valueMany(names, options);
  }

  async spawnChildren(children: ChildSpec[], options: Parameters<FerricStoreClient["spawnChildren"]>[2] = {}): Promise<unknown> {
    return await this.ctx.client.spawnChildren(this.ctx.id, children, {
      ...options,
      fencingToken: options.fencingToken ?? this.ctx.fencingToken,
      leaseToken: options.leaseToken ?? this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey
    });
  }
}

export class WorkflowWorker {
  readonly workflow: Workflow;
  readonly options: WorkerConfig & { states?: string[] };

  constructor(workflow: Workflow, options: WorkerConfig & { states?: string[] }) {
    this.workflow = workflow;
    this.options = options;
  }

  async runOnce(): Promise<WorkflowWorkerResult> {
    const states = this.options.states ?? this.workflow.stateNames();
    let claimed = 0;
    let applied = 0;
    let claimCalls = 0;
    let emptyClaims = 0;

    for (const stateName of states) {
      const registration = this.workflow.stateRegistration(stateName);
      if (registration == null) {
        throw new Error(`No handler registered for workflow state '${stateName}'`);
      }

      claimCalls += 1;
      const jobs = await this.workflow.client.claimDue(this.workflow.type, {
        blockMs: this.options.blockMs,
        includeState: !registration.claimRecord,
        jobOnly: !registration.claimRecord,
        leaseMs: this.options.leaseMs ?? registration.leaseMs,
        limit: this.options.batchSize ?? 10,
        nowMs: this.options.nowMs,
        partitionKey: this.options.partitionKey,
        partitionKeys: this.options.partitionKeys,
        payload: registration.claimPayload,
        priority: this.options.priority,
        reclaimExpired: this.options.reclaimExpired,
        reclaimRatio: this.options.reclaimRatio,
        state: stateName,
        valueMaxBytes: registration.valueMaxBytes,
        values: this.options.claimValues ?? registration.claimValues,
        worker: this.options.worker ?? this.workflow.defaultWorker
      });

      if (jobs.length === 0) {
        emptyClaims += 1;
        continue;
      }

      claimed += jobs.length;
      for (const job of jobs) {
        await this.applyJob(job, registration);
        applied += 1;
      }
    }

    return { applied, claimCalls, claimed, emptyClaims };
  }

  async run(): Promise<void> {
    const idleSleepMs = this.options.idleSleepMs ?? 250;
    let currentIdleSleepMs = idleSleepMs;
    const maxIdleSleepMs = this.options.maxIdleSleepMs ?? 5_000;

    while (this.options.signal?.aborted !== true) {
      const result = await this.runOnce();
      if (result.claimed === 0) {
        await sleep(currentIdleSleepMs, this.options.signal);
        currentIdleSleepMs = Math.min(maxIdleSleepMs, currentIdleSleepMs * 2);
      } else {
        currentIdleSleepMs = idleSleepMs;
      }
    }
  }

  private async applyJob(job: FlowRecord | ClaimedItem, registration: StateRegistration): Promise<void> {
    const ctx = new WorkflowContext(this.workflow, job, registration.name);
    let outcome: Outcome;
    try {
      const value = await registration.handler(ctx);
      outcome = isOutcome(value) ? value : complete({ result: value });
    } catch (error) {
      await this.applyHandlerError(ctx, registration, error);
      return;
    }
    await applyOutcome(this.workflow.client, ctx, outcome, registration.returnRecord);
  }

  private async applyHandlerError(ctx: WorkflowContext, registration: StateRegistration, error: unknown): Promise<void> {
    const policy = this.options.exceptionPolicy ?? registration.exceptionPolicy;
    if (policy === "raise") {
      throw error;
    }
    if (policy === "fail") {
      await applyOutcome(this.workflow.client, ctx, fail({ error: errorToPayload(error) }), registration.returnRecord);
      return;
    }
    await applyOutcome(this.workflow.client, ctx, retry({ error: errorToPayload(error) }), registration.returnRecord);
  }
}

async function applyOutcome(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: Outcome,
  returnRecord: boolean
): Promise<void> {
  switch (outcome.kind) {
    case "transition":
      await applyTransition(client, ctx, outcome, returnRecord);
      return;
    case "complete":
      await applyComplete(client, ctx, outcome, returnRecord);
      return;
    case "retry":
      await applyRetry(client, ctx, outcome, returnRecord);
      return;
    case "fail":
      await applyFail(client, ctx, outcome, returnRecord);
      return;
  }
}

async function applyTransition(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: TransitionOutcome,
  returnRecord: boolean
): Promise<void> {
  validateTransitionPolicy(ctx, outcome);
  await client.transition(ctx.id, {
    dropValues: outcome.dropValues,
    fencingToken: ctx.fencingToken,
    fromState: ctx.state,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    priority: outcome.priority,
    returnRecord,
    runAtMs: outcome.runAtMs,
    stateMeta: outcome.stateMeta,
    toState: outcome.toState,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

function validateTransitionPolicy(ctx: WorkflowContext, outcome: TransitionOutcome): void {
  const target = ctx.workflow.stateRegistration(outcome.toState);
  if (target?.mode === "fifo" && outcome.priority != null) {
    throw new Error("priority is not supported for fifo state");
  }
}

async function applyComplete(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: CompleteOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.complete(ctx.id, {
    dropValues: outcome.dropValues,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    result: outcome.result,
    returnRecord,
    stateMeta: outcome.stateMeta,
    ttlMs: outcome.ttlMs,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

async function applyRetry(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: RetryOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.retry(ctx.id, {
    dropValues: outcome.dropValues,
    error: outcome.error,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    returnRecord,
    runAtMs: outcome.runAtMs,
    stateMeta: outcome.stateMeta,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

async function applyFail(
  client: FerricStoreClient,
  ctx: WorkflowContext,
  outcome: FailOutcome,
  returnRecord: boolean
): Promise<void> {
  await client.fail(ctx.id, {
    dropValues: outcome.dropValues,
    error: outcome.error,
    fencingToken: ctx.fencingToken,
    leaseToken: ctx.leaseToken,
    overrideValues: outcome.overrideValues,
    partitionKey: ctx.partitionKey,
    payload: outcome.payload,
    returnRecord,
    stateMeta: outcome.stateMeta,
    ttlMs: outcome.ttlMs,
    valueRefs: outcome.valueRefs,
    values: outcome.values
  });
}

function errorToPayload(error: unknown): unknown {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack
    };
  }
  return error;
}

function normalizeStateMode(mode: FlowStateMode): FlowStateMode {
  if (mode === "fifo" || mode === "parallel") {
    return mode;
  }
  throw new Error("state mode must be 'fifo' or 'parallel'");
}

function valueRefToString(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (typeof value === "object" && value != null && "ref" in value) {
    const ref = (value as { ref?: unknown }).ref;
    return typeof ref === "string" ? ref : Buffer.isBuffer(ref) ? ref.toString("utf8") : undefined;
  }
  return undefined;
}
