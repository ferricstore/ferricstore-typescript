import { finitePositiveInteger } from "./auto-batch.js";
import { FerricStoreClaimClient } from "./client-claims.js";
import { confirmedFlowBatchItems, FlowBatchError } from "./client-errors.js";
import { assertDenseCreateItems, groupAutoPartitionItems } from "./client-grouping.js";
import { snapshotCreateItem, snapshotFlowManyOptions } from "./flow-many-snapshot.js";
import { encodeFlowValue } from "./flow-value-snapshot.js";
import {
  appendAttributes,
  appendNamedCounts,
  appendStateMeta,
  sharedCreateManyAttributes,
  sharedCreateManyStateMeta
} from "./client-helpers.js";
import type { CreateManyOptions, CreateOptions } from "./client-options.js";
import {
  append,
  appendBool,
  appendEncoded,
  appendNamedValues,
  expandManyResponse,
  nowMs,
  type CommandArgument
} from "./internal.js";
import type { CreateItem, FlowRecord } from "./types.js";

/** Producer-oriented Flow commands, separated from mutation and claim APIs. */
export class FerricStoreProducerClient extends FerricStoreClaimClient {
  async create(id: string, options: CreateOptions): Promise<FlowRecord | Buffer | unknown> {
    const currentNowMs = options.nowMs ?? nowMs();
    const partitionKey = options.partitionKey;
    const returnRecord = options.returnRecord === true;
    const args: CommandArgument[] = [
      "FLOW.CREATE",
      id,
      "TYPE",
      options.type,
      "STATE",
      options.state ?? "queued",
      "NOW",
      currentNowMs
    ];
    append(args, "PARTITION", partitionKey);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "PARENT_FLOW_ID", options.parentFlowId);
    append(args, "ROOT_FLOW_ID", options.rootFlowId);
    append(args, "CORRELATION_ID", options.correlationId);
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendBool(args, "IDEMPOTENT", options.idempotent);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    append(args, "MAX_ACTIVE_MS", options.maxActiveMs);
    appendAttributes(args, options.attributes);
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, this.codec, options);

    const response = await this.executeProducerWrite(args);
    return returnRecord
      ? await this.recordOrGet(response, id, partitionKey)
      : response;
  }

  async enqueue(
    id: string,
    options: Omit<CreateOptions, "state"> & { state?: string }
  ): Promise<FlowRecord | Buffer | unknown> {
    return await this.create(id, { ...options, state: options.state ?? "queued" });
  }

  async enqueueMany(items: CreateItem[], options: CreateManyOptions): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];

    if (options.independent === false) {
      assertDenseCreateItems(items, "enqueueMany");
      if (items.length > this.flowManyBatchLimit) {
        throw new Error(
          `independent=false enqueueMany accepts at most ${this.flowManyBatchLimit.toLocaleString("en-US")} items because splitting would weaken batch semantics`
        );
      }
      return await this.createMany(options.partitionKey, items, {
        ...options,
        independent: false,
        state: options.state ?? "queued"
      });
    }

    if (options.partitionKey != null || items.some((item) => item?.partitionKey != null)) {
      assertDenseCreateItems(items, "enqueueMany");
      const partitionKey = options.partitionKey;
      const createOptions = snapshotFlowManyOptions({
        ...options,
        independent: options.independent ?? true,
        state: options.state ?? "queued"
      }, this.codec);
      if (items.length <= this.flowManyBatchLimit) {
        return await this.createMany(partitionKey, items, createOptions);
      }
      return await this.executeIndependentManyChunks(
        "enqueueMany",
        items,
        (item) => snapshotCreateItem(item, this.codec),
        async (batchItems) => await this.createMany(partitionKey, batchItems, createOptions)
      );
    }

    const capturedItems = items.map((item) => snapshotCreateItem(item, this.codec));
    const groups = groupAutoPartitionItems(capturedItems);
    const groupedOptions = snapshotFlowManyOptions({
      ...options,
      independent: options.independent ?? true,
      state: options.state ?? "queued"
    }, this.codec);
    const results = Array<unknown>(items.length);
    const batchSize = Math.min(
      this.flowManyBatchLimit,
      finitePositiveInteger(options.autoPartitionBatchSize, this.flowManyBatchLimit)
    );
    const concurrency = Math.min(
      groups.length,
      finitePositiveInteger(options.autoPartitionConcurrency, 8)
    );
    let cursor = 0;
    let failure: { readonly error: unknown } | undefined;

    const dispatch = async (): Promise<void> => {
      while (failure == null) {
        const groupIndex = cursor;
        cursor += 1;
        const group = groups[groupIndex];
        if (group == null) return;
        try {
          for (let start = 0; start < group.items.length; start += batchSize) {
            if (failure != null) return;
            const batchItems = group.items.slice(start, start + batchSize);
            const response = await this.createMany(group.bucket, batchItems, groupedOptions);
            const expanded = expandManyResponse(response, batchItems.length);
            for (let resultIndex = 0; resultIndex < expanded.length; resultIndex += 1) {
              const originalIndex = group.indices[start + resultIndex];
              if (originalIndex != null) results[originalIndex] = expanded[resultIndex];
            }
          }
        } catch (error) {
          failure ??= { error };
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, dispatch));
    if (failure != null) {
      throw new FlowBatchError("enqueueMany", failure.error, confirmedFlowBatchItems(results));
    }
    return results;
  }

  async createMany(
    partitionKey: string | undefined,
    items: CreateItem[],
    options: CreateManyOptions
  ): Promise<unknown[] | unknown> {
    if (items.length === 0) return [];
    assertDenseCreateItems(items, "createMany");

    if (partitionKey != null) {
      for (const item of items) {
        if (item.partitionKey != null && item.partitionKey !== partitionKey) {
          throw new Error("createMany item partitionKey does not match batch partitionKey");
        }
      }
    }
    const currentNowMs = options.nowMs ?? nowMs();
    const attributes = sharedCreateManyAttributes(items, options.attributes);
    const stateMeta = sharedCreateManyStateMeta(items, options.stateMeta);
    if (items.length > this.flowManyBatchLimit) {
      if (options.independent !== true) throw this.flowManyLimitError("createMany");
      const capturedOptions = snapshotFlowManyOptions(
        { ...options, attributes, nowMs: currentNowMs, stateMeta },
        this.codec
      );
      return await this.executeIndependentManyChunks(
        "createMany",
        items,
        (item) => snapshotCreateItem(item, this.codec),
        async (batchItems) => await this.createMany(
          partitionKey,
          batchItems,
          capturedOptions
        )
      );
    }
    const mixed = partitionKey == null && items.some((item) => item.partitionKey != null);
    if (mixed && items.some((item) => item.partitionKey == null)) {
      throw new Error("mixed createMany items require partitionKey");
    }
    const auto = partitionKey == null && !mixed;
    const wirePartition = mixed ? "MIXED" : auto ? "AUTO" : partitionKey;
    const args: CommandArgument[] = [
      "FLOW.CREATE_MANY",
      wirePartition,
      "TYPE",
      options.type,
      "STATE",
      options.state ?? "queued",
      "NOW",
      currentNowMs
    ];
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendBool(args, "IDEMPOTENT", options.idempotent);
    appendBool(args, "INDEPENDENT", options.independent);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    append(args, "MAX_ACTIVE_MS", options.maxActiveMs);
    appendAttributes(args, attributes);
    appendStateMeta(args, stateMeta);

    const extendedItems = items.some((item) => item.values != null || item.valueRefs != null);
    if (extendedItems) {
      args.push("ITEMS_EXT", items.length);
      for (const item of items) {
        const itemValues = { ...(options.values ?? {}), ...(item.values ?? {}) };
        const itemRefs = { ...(options.valueRefs ?? {}), ...(item.valueRefs ?? {}) };
        args.push(item.id, mixed ? item.partitionKey ?? "-" : "-", encodeFlowValue(this.codec, item.payload));
        appendNamedCounts(args, this.codec, itemValues, itemRefs);
      }
    } else {
      appendNamedValues(args, this.codec, options);
      args.push("ITEMS");
      for (const item of items) {
        if (mixed) {
          if (item.partitionKey == null) throw new Error("mixed createMany items require partitionKey");
          args.push(item.id, item.partitionKey, encodeFlowValue(this.codec, item.payload));
        } else {
          args.push(item.id, encodeFlowValue(this.codec, item.payload));
        }
      }
    }

    return this.recordsOrResponse(await this.executeProducerWrite(args));
  }
}
