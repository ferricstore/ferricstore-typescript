import {
  append,
  appendBool,
  appendEncoded,
  appendNamedValues,
  nowMs,
  type CommandArgument
} from "./internal.js";
import type {
  CancelOptions,
  CompleteManyOptions,
  CompleteOptions,
  FailOptions,
  RetryOptions,
  TransitionOptions
} from "./client-options.js";
import {
  appendAttributeMutations,
  appendClaimedItems,
  appendFencedItems,
  appendStateMeta,
  assertManyPartitionMatches
} from "./client-helpers.js";
import { FerricStoreFlowSupportClient } from "./client-flow-support.js";
import type { ClaimedItem, FencedItem, FlowRecord, StateMeta } from "./types.js";

export class FerricStoreMutationClient extends FerricStoreFlowSupportClient {
  async transition(id: string, options: TransitionOptions): Promise<FlowRecord | Buffer | unknown> {
    const currentNowMs = options.nowMs ?? nowMs();
    const args: CommandArgument[] = [
      "FLOW.TRANSITION",
      id,
      options.fromState,
      options.toState,
      "LEASE_TOKEN",
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      currentNowMs
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async complete(id: string, options: CompleteOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.COMPLETE",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "RESULT", this.codec, options.result);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async completeMany(
    partitionKey: string | undefined,
    items: ClaimedItem[],
    options: CompleteManyOptions = {}
  ): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];
    assertManyPartitionMatches(partitionKey, items, "FLOW.COMPLETE_MANY");
    if (items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) throw this.flowManyLimitError("completeMany");
      const currentNowMs = options.nowMs ?? nowMs();
      return await this.executeIndependentManyChunks("completeMany", items, async (batchItems) => await this.completeMany(
        partitionKey,
        batchItems,
        { ...options, nowMs: currentNowMs }
      ));
    }
    return this.recordsOrResponse(await this.commandArgs(this.completeManyRequest(partitionKey, items, options)));
  }

  protected completeManyRequest(
    partitionKey: string | undefined,
    items: ClaimedItem[],
    options: CompleteManyOptions
  ): CommandArgument[] {
    const args: CommandArgument[] = ["FLOW.COMPLETE_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "RESULT", this.codec, options.result);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    if (options.returnOkOnSuccess === true) args.push("RETURN", "OK_ON_SUCCESS");
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.COMPLETE_MANY");
    return args;
  }

  async completeJobs(jobs: ClaimedItem[], options: CompleteManyOptions = {}): Promise<unknown[] | unknown> {
    if (jobs.length === 0) return [];
    const firstPartition = jobs[0]?.partitionKey;
    const partitionKey = firstPartition != null && jobs.every((job) => job.partitionKey === firstPartition)
      ? firstPartition
      : undefined;
    return await this.completeMany(partitionKey, jobs, {
      ...options,
      independent: options.independent ?? true,
      returnOkOnSuccess: options.returnOkOnSuccess ?? true
    });
  }

  async retry(id: string, options: RetryOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.RETRY",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    if (options.returnRecord === true) return await this.recordOrGet(response, id, options.partitionKey);
    return response;
  }

  async retryMany(partitionKey: string | undefined, items: ClaimedItem[], options: {
    error?: unknown;
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
    independent?: boolean;
    returnOkOnSuccess?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];
    assertManyPartitionMatches(partitionKey, items, "FLOW.RETRY_MANY");
    if (items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) throw this.flowManyLimitError("retryMany");
      const currentNowMs = options.nowMs ?? nowMs();
      return await this.executeIndependentManyChunks("retryMany", items, async (batchItems) => await this.retryMany(
        partitionKey,
        batchItems,
        { ...options, nowMs: currentNowMs }
      ));
    }
    const args: CommandArgument[] = ["FLOW.RETRY_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    if (options.returnOkOnSuccess === true) args.push("RETURN", "OK_ON_SUCCESS");
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.RETRY_MANY");
    return this.recordsOrResponse(await this.commandArgs(args));
  }

  async fail(id: string, options: FailOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.FAIL",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    if (options.returnRecord === true) return await this.recordOrGet(response, id, options.partitionKey);
    return response;
  }

  async failMany(partitionKey: string | undefined, items: ClaimedItem[], options: {
    error?: unknown;
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    attributesMerge?: Record<string, CommandArgument>;
    attributesDelete?: string[];
    stateMeta?: StateMeta;
    ttlMs?: number;
    nowMs?: number;
    independent?: boolean;
    returnOkOnSuccess?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];
    assertManyPartitionMatches(partitionKey, items, "FLOW.FAIL_MANY");
    if (items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) throw this.flowManyLimitError("failMany");
      const currentNowMs = options.nowMs ?? nowMs();
      return await this.executeIndependentManyChunks("failMany", items, async (batchItems) => await this.failMany(
        partitionKey,
        batchItems,
        { ...options, nowMs: currentNowMs }
      ));
    }
    const args: CommandArgument[] = ["FLOW.FAIL_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    if (options.returnOkOnSuccess === true) args.push("RETURN", "OK_ON_SUCCESS");
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.FAIL_MANY");
    return this.recordsOrResponse(await this.commandArgs(args));
  }

  async cancel(id: string, options: CancelOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = ["FLOW.CANCEL", id, "FENCING", options.fencingToken, "NOW", options.nowMs ?? nowMs()];
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "REASON", this.codec, options.reason);
    append(args, "TTL", options.ttlMs);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    const response = await this.commandArgs(args);
    if (options.returnRecord === true) return await this.recordOrGet(response, id, options.partitionKey);
    return response;
  }

  async cancelMany(partitionKey: string | undefined, items: FencedItem[], options: {
    reason?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    attributesMerge?: Record<string, CommandArgument>;
    attributesDelete?: string[];
    stateMeta?: StateMeta;
    ttlMs?: number;
    nowMs?: number;
    independent?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];
    assertManyPartitionMatches(partitionKey, items, "FLOW.CANCEL_MANY");
    if (items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) throw this.flowManyLimitError("cancelMany");
      const currentNowMs = options.nowMs ?? nowMs();
      return await this.executeIndependentManyChunks("cancelMany", items, async (batchItems) => await this.cancelMany(
        partitionKey,
        batchItems,
        { ...options, nowMs: currentNowMs }
      ));
    }
    const args: CommandArgument[] = ["FLOW.CANCEL_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "REASON", this.codec, options.reason);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);
    appendAttributeMutations(args, options);
    appendFencedItems(args, partitionKey, items, "FLOW.CANCEL_MANY", false);
    return this.recordsOrResponse(await this.commandArgs(args));
  }
}
