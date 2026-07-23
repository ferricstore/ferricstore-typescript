import { Buffer } from "node:buffer";
import {
  append,
  appendBool,
  appendNamedValues,
  appendValueReturn,
  arrayResponse,
  expandManyResponse,
  nowMs,
  parseKvResponse,
  type CommandArgument
} from "./internal.js";
import { flowRecordFromResp, type ChildSpec, type FencingToken, type FlowRecord, type MaxActiveMs } from "./types.js";
import type {
  FlowPolicyOptions,
  HistoryOptions
} from "./client-options.js";
import {
  appendFlowStatePolicy,
  appendNamedCounts,
  appendPayloadRead,
  appendPolicyMode,
  appendRetryPolicy,
  isRecordLike
} from "./client-helpers.js";
import { FlowBatchError, confirmedFlowBatchItems } from "./client-errors.js";
import { FerricStoreFlowQueryClient } from "./client-flow-query.js";
import { executeProducerWriteWithBackpressure } from "./producer-backpressure.js";
import {
  assertFlowPolicyGeneration,
  flowPolicySnapshotFromResp,
  type FlowPolicySnapshot
} from "./flow-policy.js";

/** @internal Read, policy, and shared Flow helpers kept off the primary implementation module. */
export class FerricStoreFlowSupportClient extends FerricStoreFlowQueryClient {
  async rewind(id: string, options: {
    partitionKey?: string;
    toEvent?: string;
    expectState?: string;
    nowMs?: number;
    returnRecord?: boolean;
  } = {}): Promise<FlowRecord | Buffer | unknown> {
    const partitionKey = options.partitionKey;
    const returnRecord = options.returnRecord === true;
    const args: CommandArgument[] = ["FLOW.REWIND", id, "NOW", options.nowMs ?? nowMs()];
    append(args, "PARTITION", partitionKey);
    append(args, "TO_EVENT", options.toEvent);
    append(args, "EXPECT_STATE", options.expectState);
    const response = await this.commandArgs(args);
    if (returnRecord) {
      return await this.recordOrGet(response, id, partitionKey);
    }
    return response;
  }

  async get<TPayload = unknown>(id: string, options: {
    partitionKey?: string;
    full?: boolean;
    payload?: boolean;
    payloadMaxBytes?: number;
    values?: readonly string[];
    valueMaxBytes?: number;
  } = {}): Promise<FlowRecord<TPayload> | undefined> {
    const args: CommandArgument[] = ["FLOW.GET", id];
    append(args, "PARTITION", options.partitionKey);
    appendBool(args, "FULL", options.full);
    appendPayloadRead(args, options.payload, options.payloadMaxBytes);
    appendValueReturn(args, { values: options.values, valueMaxBytes: options.valueMaxBytes });
    const response = await this.commandArgs(args);
    if (response == null) {
      return undefined;
    }
    return flowRecordFromResp<TPayload>(response, this.codec);
  }

  async info(type: string): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FLOW.INFO", type));
  }

  async history(id: string, options: HistoryOptions = {}): Promise<unknown[]> {
    const args: CommandArgument[] = ["FLOW.HISTORY", id];
    append(args, "PARTITION", options.partitionKey);
    append(args, "COUNT", options.count);
    append(args, "FROM_EVENT", options.fromEvent);
    append(args, "TO_EVENT", options.toEvent);
    append(args, "FROM_MS", options.fromMs);
    append(args, "TO_MS", options.toMs);
    append(args, "FROM_VERSION", options.fromVersion);
    append(args, "TO_VERSION", options.toVersion);
    appendBool(args, "REV", options.rev);
    append(args, "EVENT", options.event);
    append(args, "WORKER", options.worker);
    appendBool(args, "INCLUDE_COLD", options.includeCold);
    appendBool(args, "CONSISTENT_PROJECTION", options.consistentProjection);
    appendBool(args, "VALUES", options.values);
    append(args, "PAYLOAD_MAX_BYTES", options.payloadMaxBytes);
    return arrayResponse(await this.commandArgs(args));
  }

   async spawnChildren(parentId: string, children: ChildSpec[], options: {
    groupId?: string;
    partitionKey?: string;
    leaseToken?: Buffer;
    fencingToken?: FencingToken;
    wait?: string;
    waitState?: string;
    success?: string;
    failure?: string;
    fromState?: string;
    nowMs?: number;
    onChildFailed?: string;
    onParentClosed?: string;
    maxActiveMs?: MaxActiveMs;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
  } = {}): Promise<unknown> {
    if (children.length === 0) throw new TypeError("spawnChildren children must be non-empty");
    let extendedItems = false;
    let mixed = false;
    for (let index = 0; index < children.length; index += 1) {
      const child = children[index];
      if (!Object.hasOwn(children, index) || child == null) {
        throw new TypeError("spawnChildren children must be dense");
      }
      if (child.values != null || child.valueRefs != null) extendedItems = true;
      if (child.partitionKey != null) mixed = true;
    }
    const args: CommandArgument[] = [
      "FLOW.SPAWN_CHILDREN",
      parentId,
      "GROUP",
      options.groupId ?? "default",
      "WAIT",
      options.wait ?? "all",
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "FENCING", options.fencingToken);
    append(args, "WAIT_STATE", options.waitState);
    append(args, "SUCCESS", options.success);
    append(args, "FAILURE", options.failure);
    append(args, "FROM_STATE", options.fromState);
    append(args, "ON_CHILD_FAILED", options.onChildFailed);
    append(args, "ON_PARENT_CLOSED", options.onParentClosed);
    append(args, "MAX_ACTIVE_MS", options.maxActiveMs);
    appendNamedValues(args, this.codec, options);
    if (extendedItems) {
      args.push("ITEMS_EXT", children.length);
      for (const child of children) {
        args.push(child.id, child.partitionKey ?? "-", child.type, this.codec.encode(child.payload));
        appendNamedCounts(args, this.codec, child.values ?? {}, child.valueRefs ?? {});
      }
    } else {
      args.push("ITEMS");
      if (mixed) {
        args.push("MIXED");
      }
      for (const child of children) {
        if (mixed) {
          if (child.partitionKey == null) {
            throw new Error("mixed spawnChildren children require partitionKey");
          }
          args.push(child.id, child.partitionKey, child.type, this.codec.encode(child.payload));
        } else {
          args.push(child.id, child.type, this.codec.encode(child.payload));
        }
      }
    }
    return await this.commandArgs(args);
  }

  async installPolicy(type: string, options: FlowPolicyOptions = {}): Promise<FlowPolicySnapshot> {
    const args: CommandArgument[] = ["FLOW.POLICY.SET", type];
    const expectedGeneration = Object.hasOwn(options, "expectedGeneration")
      ? options.expectedGeneration
      : undefined;
    const replace = Object.hasOwn(options, "replace") ? options.replace : undefined;
    if (expectedGeneration != null) assertFlowPolicyGeneration(expectedGeneration);
    if (replace != null && typeof replace !== "boolean") {
      throw new TypeError("replace must be a boolean");
    }
    append(args, "EXPECTED_GENERATION", expectedGeneration);
    appendBool(args, "REPLACE", replace);
    if (options.indexedAttributes != null) {
      args.push("INDEXED_ATTRIBUTES", JSON.stringify(options.indexedAttributes));
    }
    append(args, "INDEXED_STATE_META", options.indexedStateMeta);
    append(args, "MAX_ACTIVE_MS", options.maxActiveMs);
    if (options.state == null) append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    append(args, "STATE", options.state);
    appendPolicyMode(args, options.state, options.mode);
    if (options.retry != null) {
      appendRetryPolicy(args, options.retry);
    }
    if (options.state != null) append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    for (const [state, policy] of Object.entries(options.states ?? {})) {
      args.push("STATE", state);
      appendFlowStatePolicy(args, policy);
    }
    return flowPolicySnapshotFromResp(await this.commandArgs(args));
  }

  async policyGet(type: string, options: { state?: string } = {}): Promise<FlowPolicySnapshot> {
    const args: CommandArgument[] = ["FLOW.POLICY.GET", type];
    append(args, "STATE", options.state);
    return flowPolicySnapshotFromResp(await this.commandArgs(args));
  }

  async retentionCleanup(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FLOW.RETENTION_CLEANUP"));
  }

  protected async executeProducerWrite(args: CommandArgument[]): Promise<unknown> {
    return await executeProducerWriteWithBackpressure(
      async () => await this.commandArgs(args),
      this.backpressure,
      this.closeSignal
    );
  }

  protected async recordOrGet(response: unknown, id: string, partitionKey: string | undefined): Promise<FlowRecord> {
    if (isRecordLike(response)) {
      return flowRecordFromResp(response, this.codec);
    }
    const record = await this.get(id, { partitionKey });
    if (record == null) {
      throw new Error(`Flow ${id} was not returned and could not be loaded`);
    }
    return record;
  }

  protected recordsOrResponse(value: unknown): unknown[] | unknown {
    if (Array.isArray(value) && value.every(isRecordLike)) {
      return this.records(value);
    }
    return value;
  }

  protected flowManyLimitError(operation: string): Error {
    return new Error(
      `${operation} accepts at most ${this.flowManyBatchLimit.toLocaleString("en-US")} items unless independent=true allows safe chunking`
    );
  }

  protected async executeIndependentManyChunks<T>(
    operation: string,
    items: readonly T[],
    capture: (item: T) => T,
    execute: (batchItems: T[]) => Promise<unknown[] | unknown>
  ): Promise<unknown[]> {
    const results: unknown[] = [];
    const captured = new Array<T>(items.length);
    for (let index = 0; index < items.length; index += 1) {
      if (!Object.hasOwn(items, index)) throw new TypeError(`${operation} items must be dense`);
      captured[index] = capture(items[index] as T);
    }
    for (let start = 0; start < captured.length; start += this.flowManyBatchLimit) {
      const batchItems = captured.slice(start, start + this.flowManyBatchLimit);
      try {
        const response = await execute(batchItems);
        for (const result of expandManyResponse(response, batchItems.length)) results.push(result);
      } catch (error) {
        throw new FlowBatchError(operation, error, confirmedFlowBatchItems(results));
      }
    }
    return results;
  }

  protected appendPartitionOptions(
    args: CommandArgument[],
    options: { partitionKey?: string; partitionKeys?: string[] }
  ): void {
    if (options.partitionKey != null && options.partitionKeys != null) {
      throw new Error("partitionKey and partitionKeys are mutually exclusive");
    }
    append(args, "PARTITION", options.partitionKey);
    if (options.partitionKeys != null) {
      if (options.partitionKeys.length === 0) {
        throw new Error("partitionKeys must be non-empty");
      }
      args.push("PARTITIONS", options.partitionKeys.length);
      for (let index = 0; index < options.partitionKeys.length; index += 1) {
        const partitionKey = options.partitionKeys[index];
        if (!Object.hasOwn(options.partitionKeys, index) || typeof partitionKey !== "string") {
          throw new TypeError("partitionKeys must be a dense array of strings");
        }
        args.push(partitionKey);
      }
    }
  }
}
