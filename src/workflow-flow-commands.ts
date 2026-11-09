import type { FerricStoreClient, StepContinueOptions } from "./client.js";
import { FerricStoreError } from "./errors.js";
import type { ChildGroupResults } from "./flow-child-results.js";
import type { ChildSpec, ClaimedItem, FlowRecord } from "./types.js";
import type { WorkflowContext } from "./workflow-context.js";

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

  /** Reads one of this Flow's child groups; throws if the Flow has no such group. */
  async childResults<TResult = unknown>(
    groupId = "default",
    options: { maxBytes?: number } = {}
  ): Promise<ChildGroupResults<TResult>> {
    const group = await this.ctx.client.childResults<TResult>(this.ctx.id, {
      ...options,
      groupId,
      partitionKey: this.ctx.partitionKey
    });
    if (group == null) throw new FerricStoreError(`Flow ${this.ctx.id} has no child group ${groupId}`);
    return group;
  }
}
