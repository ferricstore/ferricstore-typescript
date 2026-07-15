import {
  append,
  appendBool,
  appendEncoded,
  appendNamedValues,
  nowMs,
  type CommandArgument
} from "./internal.js";
import { NativeAdapter } from "./adapters.js";
import { ReconnectingExecutor } from "./reconnecting-executor.js";
import { TopologyNativeAdapterPool } from "./topology.js";
import {
  claimedItemFromResp,
  type ClaimedItem,
  type FencedItem,
  type FlowRecord,
  type StateMeta
} from "./types.js";

import type {
  FerricStoreClientFromUrlOptions,
  StartAndClaimOptions,
  StepContinueOptions,
  RunStepsItem,
  RunStepsManyOptions
} from "./client-options.js";
export type {
  FlowBatchCompletedItem,
  ClaimHydrationItem,
  AutoBatchOptions,
  FerricStoreClientOptions,
  FerricStoreClientFromUrlOptions,
  CreateOptions,
  CreateManyOptions,
  StartAndClaimOptions,
  ClaimDueOptions,
  CompleteManyOptions,
  CompleteJobsAndClaimJobsResult,
  ReclaimOptions,
  FlowStateMode,
  FlowStatePolicy,
  FlowStatePolicyLike,
  FlowPolicyOptions,
  RequestContext,
  RequestContextOptions,
  InvocationCreateOptions,
  MutateOptions,
  LeaseMutationOptions,
  ExtendLeaseOptions,
  TransitionOptions,
  StepContinueOptions,
  RunStepsItem,
  RunStepsManyOptions,
  FlowAdminRecord,
  FlowStatsOptions,
  AttributeQueryOptions,
  ScheduleOptions,
  ScheduleListOptions,
  ScheduleFireDueOptions,
  EffectReserveOptions,
  EffectStatusOptions,
  EffectConfirmOptions,
  EffectFailOptions,
  EffectCompensateOptions,
  ApprovalRequestOptions,
  ApprovalListOptions,
  CircuitOpenOptions,
  BudgetReserveOptions,
  BudgetCommitOptions,
  AdminListOptions,
  LimitListOptions,
  LimitLeaseOptions,
  LimitAmountOptions,
  LimitReleaseOptions,
  GovernanceLedgerOptions,
  CompleteOptions,
  RetryOptions,
  FailOptions,
  CancelOptions,
  ReadOptions,
  HistoryOptions,
  SearchStateMeta,
  SearchOptions,
  ManagementPairs
} from "./client-options.js";

import {
  appendStateMeta,
  appendAttributes,
  appendAttributeMutations,
  appendFencedItems,
  assertManyPartitionMatches
} from "./client-helpers.js";
export { completeJobsResultError } from "./client-helpers.js";
export { ClaimHydrationError, FlowBatchError } from "./client-errors.js";
export { groupAutoPartitionItems } from "./client-grouping.js";
import { valueMGetEntries } from "./client-values.js";
export { valueMGetEntries } from "./client-values.js";
import {
  nativeAdapterOptions,
  snapshotFerricUrls,
  snapshotNativeClientOptions,
  topologyNativeOptions
} from "./client-native-options.js";
import { FerricStoreProducerClient } from "./client-producer.js";
import { snapshotClientOptions } from "./client-config.js";
import { snapshotFencedItem, snapshotFlowManyOptions } from "./flow-many-snapshot.js";

export class FerricStoreClient extends FerricStoreProducerClient {
  static async fromUrl(url: string, options: FerricStoreClientFromUrlOptions = {}): Promise<FerricStoreClient> {
    const clientOptions = snapshotClientOptions(options);
    const nativeOptions = snapshotNativeClientOptions(clientOptions.nativeOptions ?? {});
    const reconnectOptions = clientOptions.reconnect ?? nativeOptions.autoReconnect ?? true;
    const seeds = nativeOptions.seeds ?? [];
    const useTopology = nativeOptions.haRouting === true || seeds.length > 0 ||
      nativeOptions.endpointPolicy != null || nativeOptions.endpointValidator != null ||
      nativeOptions.topologyConcurrency != null || nativeOptions.trustedHosts != null ||
      nativeOptions.warmConnections != null;
    const createExecutor = async (signal?: AbortSignal) => {
      const bootstrapOptions = nativeOptionsForBootstrap(nativeOptions, signal);
      return (
        useTopology
          ? await TopologyNativeAdapterPool.fromUrls([url, ...seeds], topologyNativeOptions(bootstrapOptions))
          : await NativeAdapter.fromUrl(url, nativeAdapterOptions(bootstrapOptions))
      );
    };
    const executor = reconnectOptions === false
      ? await createExecutor()
      : new ReconnectingExecutor(createExecutor, reconnectOptions === true ? {} : reconnectOptions);
    if (executor instanceof ReconnectingExecutor) {
      await executor.ready();
    }
    return new FerricStoreClient(executor, clientOptions);
  }

  static async fromUrls(urls: readonly string[], options: FerricStoreClientFromUrlOptions = {}): Promise<FerricStoreClient> {
    const clientOptions = snapshotClientOptions(options);
    if (urls.length === 0) {
      throw new Error("FerricStoreClient.fromUrls requires at least one URL");
    }
    const seedUrls = snapshotFerricUrls(urls);
    const nativeOptions = snapshotNativeClientOptions(clientOptions.nativeOptions ?? {});
    const reconnectOptions = clientOptions.reconnect ?? nativeOptions.autoReconnect ?? true;
    const createExecutor = async (signal?: AbortSignal) => await TopologyNativeAdapterPool.fromUrls(
      seedUrls,
      topologyNativeOptions(nativeOptionsForBootstrap(nativeOptions, signal))
    );
    const executor = reconnectOptions === false
      ? await createExecutor()
      : new ReconnectingExecutor(createExecutor, reconnectOptions === true ? {} : reconnectOptions);
    if (executor instanceof ReconnectingExecutor) {
      await executor.ready();
    }
    return new FerricStoreClient(executor, clientOptions);
  }

  async valuePut(
    value: unknown,
    options: {
      partitionKey?: string;
      ownerFlowId?: string;
      name?: string;
      override?: boolean;
      ttlMs?: number;
      nowMs?: number;
    } = {}
  ): Promise<unknown> {
    const args: CommandArgument[] = ["FLOW.VALUE.PUT", this.codec.encode(value), "NOW", options.nowMs ?? nowMs()];
    append(args, "PARTITION", options.partitionKey);
    append(args, "OWNER_FLOW_ID", options.ownerFlowId);
    append(args, "NAME", options.name);
    appendBool(args, "OVERRIDE", options.override);
    append(args, "TTL", options.ttlMs);
    return await this.commandArgs(args);
  }

  async valueMGet(refs: string[], options: { maxBytes?: number } = {}): Promise<unknown[]> {
    return (await valueMGetEntries(this, refs, options)).map((entry) =>
      entry.found ? entry.value : null
    );
  }

  async signal(id: string, options: {
    signal: string;
    partitionKey?: string;
    idempotencyKey?: string;
    ifState?: string | string[];
    transitionTo?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
  }): Promise<unknown> {
    const args: CommandArgument[] = ["FLOW.SIGNAL", id, "SIGNAL", options.signal];
    append(args, "PARTITION", options.partitionKey);
    append(args, "IDEMPOTENCY", options.idempotencyKey);
    if (Array.isArray(options.ifState)) {
      for (let index = 0; index < options.ifState.length; index += 1) {
        const state = options.ifState[index];
        if (!Object.hasOwn(options.ifState, index) || typeof state !== "string") {
          throw new TypeError("ifState must be a dense array of strings");
        }
        append(args, "IF_STATE", state);
      }
    } else {
      append(args, "IF_STATE", options.ifState);
    }
    append(args, "TRANSITION_TO", options.transitionTo);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    append(args, "PRIORITY", options.priority);
    appendNamedValues(args, this.codec, options);
    return await this.commandArgs(args);
  }

  async flowSignal(id: string, options: Parameters<FerricStoreClient["signal"]>[1]): Promise<unknown> {
    return await this.signal(id, options);
  }

  async startAndClaim(id: string, options: StartAndClaimOptions): Promise<FlowRecord> {
    const partitionKey = options.partitionKey;
    const args: CommandArgument[] = [
      "FLOW.START_AND_CLAIM",
      id,
      "TYPE",
      options.type,
      "INITIAL_STATE",
      options.initialState,
      "WORKER",
      options.worker,
      "LEASE_MS",
      options.leaseMs ?? 30_000,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", partitionKey);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "PARENT_FLOW_ID", options.parentFlowId);
    append(args, "ROOT_FLOW_ID", options.rootFlowId);
    append(args, "CORRELATION_ID", options.correlationId);
    append(args, "PRIORITY", options.priority);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    appendAttributes(args, options.attributes);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    return await this.recordOrGet(await this.commandArgs(args), id, partitionKey);
  }

  async stepContinue(id: string, options: StepContinueOptions): Promise<FlowRecord | ClaimedItem> {
    const partitionKey = options.partitionKey;
    const returnJob = options.returnJob === true;
    const type = options.type;
    const args: CommandArgument[] = [
      "FLOW.STEP_CONTINUE",
      id,
      options.leaseToken,
      options.fromState,
      options.toState,
      "FENCING",
      options.fencingToken,
      "LEASE_MS",
      options.leaseMs ?? 30_000,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", partitionKey);
    append(args, "WORKER", options.worker);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    if (returnJob) args.push("RETURN", "JOBS_COMPACT");
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    return returnJob
      ? claimedItemFromResp(response, this.codec, { type })
      : await this.recordOrGet(response, id, partitionKey);
  }

  async runStepsMany(
    items: readonly (string | RunStepsItem)[],
    options: RunStepsManyOptions
  ): Promise<unknown> {
    if (items.length === 0) return Buffer.from("OK");
    const hasStates = options.states != null;
    const hasSteps = options.steps != null;
    if (hasStates === hasSteps) {
      throw new Error("runStepsMany requires exactly one of states or steps");
    }
    let states: string[] | undefined;
    if (options.states != null) {
      if (options.states.length === 0) throw new Error("runStepsMany states must be non-empty");
      states = new Array<string>(options.states.length);
      for (let index = 0; index < options.states.length; index += 1) {
        const state = options.states[index];
        if (!Object.hasOwn(options.states, index) || typeof state !== "string" || state.length === 0) {
          throw new TypeError("runStepsMany states must be a dense array of non-empty strings");
        }
        states[index] = state;
      }
    }
    if (options.steps != null && (!Number.isSafeInteger(options.steps) || options.steps <= 0)) {
      throw new Error("runStepsMany steps must be a positive safe integer");
    }
    const normalizedItems = new Array<{ id: string; partition_key?: string }>(items.length);
    for (let index = 0; index < items.length; index += 1) {
      const item = items[index];
      if (!Object.hasOwn(items, index) || item === undefined) {
        throw new TypeError("runStepsMany items must be dense");
      }
      const value = typeof item === "string" ? { id: item } : item;
      if (value.id.length === 0) throw new Error("runStepsMany item id must be non-empty");
      const partitionKey = value.partitionKey ?? options.partitionKey;
      normalizedItems[index] = partitionKey == null
        ? { id: value.id }
        : { id: value.id, partition_key: partitionKey };
    }
    const args: CommandArgument[] = ["FLOW.RUN_STEPS_MANY", "TYPE", options.type];
    if (states != null) {
      args.push("STATES", states);
    } else {
      args.push("STEPS", options.steps);
    }
    args.push(
      "WORKER", options.worker,
      "LEASE_MS", options.leaseMs ?? 30_000,
      "NOW", options.nowMs ?? nowMs()
    );
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    appendEncoded(args, "RESULT", this.codec, options.result);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    args.push("ITEMS", normalizedItems);
    return await this.commandArgs(args);
  }

  async transitionMany(partitionKey: string | undefined, options: {
    fromState: string;
    toState: string;
    items: FencedItem[];
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    attributesMerge?: Record<string, CommandArgument>;
    attributesDelete?: string[];
    stateMeta?: StateMeta;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    independent?: boolean;
  }): Promise<unknown[] | unknown> {
    if (options.items.length === 0) {
      return [];
    }
    assertManyPartitionMatches(partitionKey, options.items, "FLOW.TRANSITION_MANY");
    if (options.items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) {
        throw this.flowManyLimitError("transitionMany");
      }
      const currentNowMs = options.nowMs ?? nowMs();
      const capturedOptions = snapshotFlowManyOptions(
        { ...options, nowMs: currentNowMs },
        this.codec
      );
      return await this.executeIndependentManyChunks("transitionMany", options.items, snapshotFencedItem, async (batchItems) => await this.transitionMany(
        partitionKey,
        { ...capturedOptions, items: batchItems }
      ));
    }
    const args: CommandArgument[] = ["FLOW.TRANSITION_MANY", partitionKey ?? "MIXED", options.fromState, options.toState];
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "PRIORITY", options.priority);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    appendFencedItems(args, partitionKey, options.items, "FLOW.TRANSITION_MANY", true);
    return this.recordsOrResponse(await this.commandArgs(args));
  }

}

function nativeOptionsForBootstrap(
  options: ReturnType<typeof snapshotNativeClientOptions>,
  signal: AbortSignal | undefined
): ReturnType<typeof snapshotNativeClientOptions> {
  if (signal == null) return options;
  return {
    ...options,
    signal: options.signal == null ? signal : AbortSignal.any([options.signal, signal])
  };
}
