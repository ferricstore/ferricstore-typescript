import { randomUUID } from "node:crypto";
import {
  type FerricStoreClient,
  type FlowPolicyOptions,
  type FlowStatePolicyLike,
  type RunStepsItem,
  type RunStepsManyOptions,
  type StartAndClaimOptions
} from "./client.js";
import {
  type ClaimedItem,
  type FlowRecord,
  type MaxActiveMs,
  type StateMeta,
  type ValueConfig,
  type WorkerConfig
} from "./types.js";
import { setOwnValue, sleep, type CommandArgument } from "./internal.js";
import {
  LeaseRenewalGuard,
  nextWorkerIdleSleepMs,
  normalizeWorkflowWorkerConfig,
  runContinuousWorkerPool,
  workerBatchSize,
  workerClaimBlockMs,
  workerClaimLimit,
  workerConcurrency,
  workerDrainBatches,
  workerIdleSleepMs,
  workerLeaseMs,
  workerMaxIdleSleepMs,
  workerRefillStrategy,
  workerSignalAborted
} from "./worker-internal.js";
import { createWorkflowStateRegistration } from "./workflow-registration.js";
import { snapshotFlowClientOptions } from "./flow-client-options.js";
export { WorkflowContext, WorkflowFlowCommands } from "./workflow-context.js";
import { executeWorkflowJob } from "./workflow-job-execution.js";
import { resolveWorkflowStates, type ResolvedWorkflowState } from "./workflow-utilities.js";
import type {
  ContinuousWorkflowJob,
  StateOptions,
  StateRegistration,
  WorkflowHandler,
  WorkflowOptions,
  WorkflowWorkerResult
} from "./workflow-types.js";

type GuardedContinuousWorkflowJob = ContinuousWorkflowJob & {
  readonly guard: LeaseRenewalGuard;
};
export type {
  StateOptions,
  StateRegistration,
  WorkflowHandler,
  WorkflowOptions,
  WorkflowWorkerResult
} from "./workflow-types.js";

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
    const captured = snapshotFlowClientOptions(options, "Workflow");
    this.client = client;
    this.type = captured.type;
    this.initialState = captured.initialState ?? "queued";
    this.valueConfig = {
      localCache: captured.valueConfig?.localCache ?? false,
      valueMaxBytes: captured.valueConfig?.valueMaxBytes
    };
    this.defaultWorker = captured.worker ?? `${this.type}-${randomUUID()}`;
  }

  state(name: string, handler: WorkflowHandler, options: StateOptions = {}): this {
    this.states.set(name, createWorkflowStateRegistration(
      name,
      handler,
      options,
      this.valueConfig.valueMaxBytes
    ));
    return this;
  }

  async start(id: string, options: {
    attributes?: Record<string, CommandArgument>;
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
    maxActiveMs?: MaxActiveMs;
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

  async startAndClaim(
    id: string,
    options: Omit<StartAndClaimOptions, "type" | "initialState"> & { initialState?: string }
  ): Promise<FlowRecord> {
    return await this.client.startAndClaim(id, {
      ...options,
      initialState: options.initialState ?? this.initialState,
      type: this.type
    });
  }

  async runStepsMany(
    items: readonly (string | RunStepsItem)[],
    options: Omit<RunStepsManyOptions, "type">
  ): Promise<unknown> {
    return await this.client.runStepsMany(items, { ...options, type: this.type });
  }

  async startMany(items: {
    id: string;
    payload?: unknown;
    partitionKey?: string;
    attributes?: Record<string, CommandArgument>;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    stateMeta?: StateMeta;
  }[], options: {
    attributes?: Record<string, CommandArgument>;
    state?: string;
    partitionKey?: string;
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

  async installPolicy(options: Omit<FlowPolicyOptions, "mode" | "state" | "states"> = {}) {
    const states: Record<string, FlowStatePolicyLike> = {};
    for (const registration of this.states.values()) {
      if (registration.mode != null || registration.retryPolicy != null) {
        setOwnValue(states, registration.name, {
          ...(registration.mode == null ? {} : { mode: registration.mode }),
          ...(registration.retryPolicy == null ? {} : { retry: { ...registration.retryPolicy } })
        });
      }
    }
    return await this.client.installPolicy(this.type, {
      ...options,
      replace: Object.hasOwn(options, "replace") ? options.replace ?? true : true,
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

export class WorkflowWorker {
  readonly workflow: Workflow;
  readonly options: WorkerConfig & { states?: string[] };

  constructor(workflow: Workflow, options: WorkerConfig & { states?: string[] }) {
    this.workflow = workflow;
    this.options = normalizeWorkflowWorkerConfig(options);
  }

  async runOnce(): Promise<WorkflowWorkerResult> {
    const states = this.resolveStates();
    let claimed = 0;
    let applied = 0;
    let claimCalls = 0;
    let emptyClaims = 0;

    for (const { registration, stateName } of states) {
      const leaseMs = workerLeaseMs(this.options, registration.leaseMs);
      const concurrency = workerConcurrency(this.options);
      for (let batch = 0; batch < workerDrainBatches(this.options); batch += 1) {
        claimCalls += 1;
        const jobs = await this.claimState(
          stateName,
          registration,
          workerClaimLimit(this.options, this.workflow.client.flowManyBatchLimit),
          batch === 0 && states.length === 1
        );

        if (jobs.length === 0) {
          emptyClaims += 1;
          break;
        }

        claimed += jobs.length;
        const guards = jobs.map((job) => new LeaseRenewalGuard(this.workflow.client, job, leaseMs, this.options));
        let cursor = 0;
        let batchFailure: { error: unknown } | undefined;
        const recordFailure = (error: unknown): void => {
          batchFailure ??= { error };
        };
        const runNext = async (): Promise<void> => {
          while (cursor < jobs.length) {
            const index = cursor;
            cursor += 1;
            const job = jobs[index];
            const guard = guards[index];
            if (job == null || guard == null) continue;
            try {
              await this.applyJob(job, registration, guard);
              applied += 1;
            } catch (error) {
              recordFailure(error);
            } finally {
              try {
                await guard.stop();
              } catch (error) {
                recordFailure(error);
              }
            }
          }
        };
        try {
          await Promise.all(
            Array.from({ length: Math.min(concurrency, jobs.length) }, () => runNext())
          );
          if (batchFailure != null) throwError(batchFailure.error);
        } finally {
          // A non-conforming server or custom executor can return more jobs than
          // requested. Explicitly stop guards that were never assigned; assigned
          // guards are stopped by runNext even when another item fails.
          await Promise.allSettled(
            guards.slice(cursor).map(async (guard) => await guard.stop())
          );
        }
      }
    }

    return { applied, claimCalls, claimed, emptyClaims };
  }

  async run(): Promise<void> {
    const states = this.resolveStates();
    if (workerRefillStrategy(this.options) === "continuous") {
      await this.runContinuously(states);
      return;
    }

    await this.runInWaves();
  }

  private async runInWaves(): Promise<void> {
    const idleSleepMs = workerIdleSleepMs(this.options);
    let currentIdleSleepMs = idleSleepMs;
    const maxIdleSleepMs = workerMaxIdleSleepMs(this.options, idleSleepMs);

    while (!workerSignalAborted(this.options.signal)) {
      const result = await this.runOnce();
      if (result.claimed === 0) {
        if (workerSignalAborted(this.options.signal)) break;
        try {
          await sleep(currentIdleSleepMs, this.options.signal);
        } catch (error) {
          if (!workerSignalAborted(this.options.signal)) throw error;
          break;
        }
        currentIdleSleepMs = nextWorkerIdleSleepMs(currentIdleSleepMs, maxIdleSleepMs);
      } else {
        currentIdleSleepMs = idleSleepMs;
      }
    }
  }

  private async runContinuously(registrations: readonly ResolvedWorkflowState[]): Promise<void> {
    let stateCursor = 0;

    await runContinuousWorkerPool<GuardedContinuousWorkflowJob>({
      claim: async (limit, useBlocking) => {
        let remainingStates = registrations.length;
        while (remainingStates > 0) {
          remainingStates -= 1;
          const selected = registrations[stateCursor];
          stateCursor = registrations.length === 0 ? 0 : (stateCursor + 1) % registrations.length;
          if (selected == null) break;
          const leaseMs = workerLeaseMs(this.options, selected.registration.leaseMs);
          const jobs = await this.claimState(
            selected.stateName,
            selected.registration,
            limit,
            useBlocking && registrations.length === 1
          );
          if (jobs.length > 0) {
            return jobs.map((job) => ({
              guard: new LeaseRenewalGuard(this.workflow.client, job, leaseMs, this.options),
              job,
              leaseMs,
              registration: selected.registration
            }));
          }
        }
        return [];
      },
      concurrency: workerConcurrency(this.options),
      handle: async ({ guard, job, registration }) => {
        try {
          await this.applyJob(job, registration, guard);
        } finally {
          await guard.stop();
        }
      },
      idleSleepMs: this.options.idleSleepMs,
      maxClaimSize: workerBatchSize(this.options, this.workflow.client.flowManyBatchLimit),
      maxIdleSleepMs: this.options.maxIdleSleepMs,
      refillPartialClaims: true,
      refillDelayMs: this.options.refillDelayMs,
      signal: this.options.signal
    });
  }

  private resolveStates(): ResolvedWorkflowState[] {
    return resolveWorkflowStates(
      this.options.states ?? this.workflow.stateNames(),
      (stateName) => this.workflow.stateRegistration(stateName)
    );
  }

  private async claimState(
    stateName: string,
    registration: StateRegistration,
    limit: number,
    useBlocking: boolean
  ): Promise<(FlowRecord | ClaimedItem)[]> {
    const preferCompact = !registration.claimRecord || this.options.profile === "throughput";
    return await this.workflow.client.claimDue(this.workflow.type, {
      blockMs: workerClaimBlockMs(this.options, useBlocking),
      includeState: preferCompact,
      includeAttributes: this.options.claimAttributes,
      jobOnly: preferCompact,
      leaseMs: workerLeaseMs(this.options, registration.leaseMs),
      limit,
      nowMs: this.options.nowMs,
      partitionKey: this.options.partitionKey,
      partitionKeys: this.options.partitionKeys,
      payload: this.options.claimPayload ?? registration.claimPayload,
      priority: this.options.priority,
      reclaimExpired: this.options.reclaimExpired,
      reclaimRatio: this.options.reclaimRatio,
      state: stateName,
      valueMaxBytes: this.options.valueMaxBytes ?? registration.valueMaxBytes,
      values: this.options.claimValues ?? registration.claimValues,
      worker: this.options.worker ?? this.workflow.defaultWorker
    });
  }

  private async applyJob(
    job: FlowRecord | ClaimedItem,
    registration: StateRegistration,
    guard: LeaseRenewalGuard
  ): Promise<void> {
    await executeWorkflowJob(this.workflow, this.options, job, registration, guard);
  }
}

function throwError(value: unknown): never {
  throw value instanceof Error ? value : new Error("workflow worker failed", { cause: value });
}
