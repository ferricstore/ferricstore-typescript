import {
  valueMGetEntries,
  type AdvanceOptions,
  type FerricStoreClient,
  type StepContinueOptions,
  type StepOptions
} from "./client.js";
import { advanceClaim, durableMutationMayHaveCommitted, runDurableStep } from "./client-durable-step.js";
import { setOwnValue } from "./internal.js";
import {
  alreadyApplied,
  appliedStep,
  replayedStep,
  type AlreadyAppliedOutcome,
  type WorkflowStepResult
} from "./outcomes.js";
import {
  CLAIMED_ITEM_WIRE,
  type ChildSpec,
  type ClaimedItem,
  type FencingToken,
  type FlowRecord
} from "./types.js";
import type { Workflow } from "./workflow.js";
import type { MutationCoordinator } from "./workflow-worker-context.js";
import { valueRefToString } from "./workflow-utilities.js";

const MISSING_VALUE = Symbol("ferricstore.missingValue");

export class WorkflowContext {
  readonly workflow: Workflow;
  readonly job: FlowRecord | ClaimedItem;
  readonly flow: WorkflowFlowCommands;
  private appliedMutation = false;
  private currentStateName: string;
  private leaseJob: ClaimedItem;
  private mutationFailure?: unknown;
  private mutationPhase: "idle" | "committing" | "uncertain" = "idle";
  private mutationCoordinator?: MutationCoordinator;
  private readonly valueCache = new Map<string, unknown>();

  constructor(
    workflow: Workflow,
    job: FlowRecord | ClaimedItem,
    stateName: string
  ) {
    this.workflow = workflow;
    this.job = job;
    this.leaseJob = job;
    this.currentStateName = stateName;
    this.flow = new WorkflowFlowCommands(this);
  }

  /** @internal */
  protected configureWorkerMutation(leaseJob: ClaimedItem, coordinator: MutationCoordinator): void {
    this.leaseJob = leaseJob;
    this.mutationCoordinator = coordinator;
  }

  get client(): FerricStoreClient {
    return this.workflow.client;
  }

  get stateName(): string {
    return this.currentStateName;
  }

  get id(): string {
    return this.leaseJob.id;
  }

  get type(): string {
    return this.leaseJob.type ?? this.workflow.type;
  }

  get state(): string {
    return this.leaseJob.state;
  }

  get logicalState(): string {
    return this.currentStateName;
  }

  get partitionKey(): string | undefined {
    return this.leaseJob.partitionKey;
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
    return Buffer.from(this.leaseJob.leaseToken);
  }

  get fencingToken(): FencingToken {
    return this.leaseJob.fencingToken;
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

  async advance(
    toState: string,
    options: Omit<AdvanceOptions, "toState"> = {}
  ): Promise<AlreadyAppliedOutcome> {
    if (typeof toState !== "string" || toState.length === 0) {
      throw new TypeError("toState must be a non-empty string");
    }
    const current = this.durableClaim();
    await this.beforeCommit();
    let refreshed: ClaimedItem;
    try {
      refreshed = await advanceClaim(this.client, current, { ...options, toState });
    } catch (error) {
      this.commitFailed(error);
      throw error;
    }
    this.committed(refreshed);
    return alreadyApplied(refreshed);
  }

  async step<TResult>(options: StepOptions<TResult>): Promise<WorkflowStepResult<TResult>> {
    let applied = false;
    const stepped = await runDurableStep(this.client, this.durableClaim(), options, {
      beforeCommit: async () => await this.beforeCommit(),
      commitFailed: (error) => this.commitFailed(error),
      committed: (job) => {
        applied = true;
        this.committed(job);
      },
      replayed: (job) => this.replayed(job)
    });
    return applied
      ? appliedStep(stepped.job, stepped.result)
      : replayedStep(stepped.job, stepped.result);
  }

  /** @internal */
  get hasAppliedMutation(): boolean {
    return this.appliedMutation;
  }

  /** @internal */
  get uncertainMutationFailure(): unknown {
    return this.mutationPhase === "uncertain" ? this.mutationFailure : undefined;
  }

  /** @internal */
  get hasUncertainMutation(): boolean {
    return this.mutationPhase === "uncertain";
  }

  /** @internal */
  assertAppliedOutcome(outcome: AlreadyAppliedOutcome): void {
    if (!this.appliedMutation || !outcome.job.leaseToken.equals(this.leaseJob.leaseToken) ||
        outcome.job.fencingToken !== this.leaseJob.fencingToken) {
      throw new Error("already-applied outcome does not match the workflow's refreshed claim");
    }
  }

  async value(name: string, defaultValue?: unknown, options: { localCache?: boolean } = {}): Promise<unknown> {
    const useLocalCache = options.localCache ?? this.workflow.valueConfig.localCache;
    if (useLocalCache && this.valueCache.has(name)) {
      const cached = this.valueCache.get(name);
      return cached === MISSING_VALUE ? defaultValue : cached;
    }

    if (Object.hasOwn(this.values, name)) {
      const value = this.values[name];
      if (useLocalCache) {
        this.valueCache.set(name, value);
      }
      return value;
    }

    const ref = valueRefToString(Object.hasOwn(this.valueRefs, name) ? this.valueRefs[name] : undefined);
    if (ref == null) {
      if (useLocalCache) this.valueCache.set(name, MISSING_VALUE);
      return defaultValue;
    }

    const entries = await valueMGetEntries(this.client, [ref], { maxBytes: this.valueMaxBytes() });
    const entry = entries[0];
    if (entry?.found === true) {
      if (useLocalCache) this.valueCache.set(name, entry.value);
      return entry.value;
    }
    if (useLocalCache) this.valueCache.set(name, MISSING_VALUE);
    return defaultValue;
  }

  async valueMany(names: string[], options: { localCache?: boolean } = {}): Promise<Record<string, unknown>> {
    const result: Record<string, unknown> = {};
    const useLocalCache = options.localCache ?? this.workflow.valueConfig.localCache;
    const namesByRef = new Map<string, string[]>();
    const uniqueNames = new Set<string>();
    for (let index = 0; index < names.length; index += 1) {
      const name = names[index];
      if (!Object.hasOwn(names, index) || typeof name !== "string") {
        throw new TypeError("names must be a dense array of own strings");
      }
      uniqueNames.add(name);
    }

    for (const name of uniqueNames) {
      if (useLocalCache && this.valueCache.has(name)) {
        const cached = this.valueCache.get(name);
        setOwnValue(result, name, cached === MISSING_VALUE ? undefined : cached);
        continue;
      }
      if (Object.hasOwn(this.values, name)) {
        const value = this.values[name];
        setOwnValue(result, name, value);
        if (useLocalCache) this.valueCache.set(name, value);
        continue;
      }
      const ref = valueRefToString(Object.hasOwn(this.valueRefs, name) ? this.valueRefs[name] : undefined);
      if (ref == null) {
        setOwnValue(result, name, undefined);
        if (useLocalCache) this.valueCache.set(name, MISSING_VALUE);
        continue;
      }
      const refNames = namesByRef.get(ref);
      if (refNames == null) {
        namesByRef.set(ref, [name]);
      } else {
        refNames.push(name);
      }
    }

    const refs = [...namesByRef.keys()];
    const entries = await valueMGetEntries(this.client, refs, { maxBytes: this.valueMaxBytes() });
    for (let index = 0; index < refs.length; index += 1) {
      const ref = refs[index];
      if (ref == null) continue;
      const entry = entries[index];
      const value = entry?.found === true ? entry.value : undefined;
      for (const name of namesByRef.get(ref) ?? []) {
        setOwnValue(result, name, value);
        if (useLocalCache) this.valueCache.set(name, entry?.found === true ? value : MISSING_VALUE);
      }
    }
    return result;
  }

  private valueMaxBytes(): number | undefined {
    return this.workflow.stateRegistration(this.currentStateName)?.valueMaxBytes;
  }

  private durableClaim(): ClaimedItem {
    const runState = this.leaseJob.runState ?? (
      this.leaseJob[CLAIMED_ITEM_WIRE] == null ? undefined : this.currentStateName
    );
    if (typeof runState !== "string" || runState.length === 0) {
      throw new TypeError("job.runState must be a non-empty string");
    }
    return {
      ...this.leaseJob,
      leaseToken: Buffer.from(this.leaseJob.leaseToken),
      runState
    };
  }

  private async beforeCommit(): Promise<void> {
    if (this.mutationPhase !== "idle") {
      throw new Error("a durable workflow mutation is already in progress or has an uncertain result");
    }
    await this.mutationCoordinator?.pause();
    this.mutationPhase = "committing";
  }

  private commitFailed(error: unknown): void {
    if (!durableMutationMayHaveCommitted(error)) {
      this.mutationFailure = undefined;
      this.mutationPhase = "idle";
      this.mutationCoordinator?.resume(this.leaseJob);
      return;
    }
    this.mutationFailure = error;
    this.mutationPhase = "uncertain";
  }

  private committed(job: ClaimedItem): void {
    this.acceptJob(job);
    this.appliedMutation = true;
    this.mutationCoordinator?.resume(job);
  }

  private replayed(job: ClaimedItem): void {
    this.acceptJob(job);
  }

  private acceptJob(job: ClaimedItem): void {
    this.leaseJob = job;
    this.currentStateName = job.runState ?? this.currentStateName;
    this.mutationFailure = undefined;
    this.mutationPhase = "idle";
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

  /** @deprecated Use `ctx.advance(toState)`. */
  async stepContinue(
    toState: string,
    options: Partial<Omit<StepContinueOptions, "toState" | "fromState" | "leaseToken" | "fencingToken">> & {
      fromState?: string;
    } = {}
  ): Promise<FlowRecord | ClaimedItem> {
    // The context method intentionally preserves the deprecated low-level surface.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    return await this.ctx.client.stepContinue(this.ctx.id, {
      ...options,
      fencingToken: this.ctx.fencingToken,
      fromState: options.fromState ?? this.ctx.state,
      leaseToken: this.ctx.leaseToken,
      partitionKey: options.partitionKey ?? this.ctx.partitionKey,
      type: options.type ?? this.ctx.type,
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
