import { createHash } from "node:crypto";

import {
  BaseCheckpointSaver,
  copyCheckpoint,
  type Checkpoint,
  type CheckpointMetadata,
  type CheckpointTuple
} from "@langchain/langgraph";
import type { RunnableConfig } from "@langchain/core/runnables";

import {
  arrayResponse,
  integerResponse,
  normalizeKeyPrefix,
  positiveInteger,
  textResponse,
  type FerricStoreCommandClient,
  type FerricStoreLockOptions,
  withMutationLocks
} from "../agent-persistence/durability.js";

export type LangGraphPendingWrite = [channel: string, value: unknown];
export type LangGraphChannelVersions = Record<string, string | number>;
type SerializerProtocol = BaseCheckpointSaver["serde"];

const FORMAT_VERSION = 1;
const CHECKPOINT_FIELD_PREFIX = "checkpoint:";
const WRITE_FIELD_PREFIX = "write:";
const WRITES_INDEX: Readonly<Record<string, number>> = {
  __error__: -1,
  __interrupt__: -3,
  __resume__: -4,
  __scheduled__: -2
};

interface CheckpointRecord {
  readonly checkpoint: Checkpoint;
  readonly checkpointId: string;
  readonly checkpointNs: string;
  readonly formatVersion: typeof FORMAT_VERSION;
  readonly metadata: CheckpointMetadata;
  readonly parentCheckpointId?: string;
  readonly threadId: string;
}

interface PendingWriteRecord {
  readonly channel: string;
  readonly formatVersion: typeof FORMAT_VERSION;
  readonly index: number;
  readonly taskId: string;
  readonly value: unknown;
}

export interface FerricStoreSaverOptions extends FerricStoreLockOptions {
  /** FerricStore key prefix. Defaults to `langgraph:checkpoint`. */
  keyPrefix?: string;
  /** Number of catalog/hash entries read per page. Defaults to 256. */
  scanCount?: number;
  /** Optional LangGraph serializer. The framework JSON-plus serializer is used by default. */
  serde?: SerializerProtocol;
}

/** Durable LangGraph.js checkpoint saver backed by FerricStore. */
export class FerricStoreSaver extends BaseCheckpointSaver {
  readonly client: FerricStoreCommandClient;
  readonly keyPrefix: string;
  readonly scanCount: number;
  private readonly lockOptions: FerricStoreLockOptions;

  constructor(client: FerricStoreCommandClient, options: FerricStoreSaverOptions = {}) {
    super(options.serde);
    this.client = client;
    this.keyPrefix = normalizeKeyPrefix(options.keyPrefix ?? "langgraph:checkpoint", "langgraph:checkpoint");
    this.scanCount = positiveInteger(options.scanCount, 256, "scanCount");
    this.lockOptions = {
      lockRetryMs: options.lockRetryMs,
      lockTtlMs: options.lockTtlMs,
      lockWaitMs: options.lockWaitMs
    };
  }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const { checkpointNs, threadId } = identity(config);
    const key = this.threadKey(threadId, checkpointNs);
    let id = checkpointId(config);
    let record: CheckpointRecord | undefined;
    if (id != null) {
      record = await this.readRecord(key, id);
    } else {
      let offset = 0;
      while (true) {
        const values = arrayResponse(
          await this.client.command("ZREVRANGE", this.checkpointIndexKey(key), offset, offset),
          "ZREVRANGE checkpoint response"
        );
        if (values.length === 0) return undefined;
        id = textResponse(values[0], "checkpoint ID");
        record = await this.readRecord(key, id);
        if (record != null) break;
        offset += 1;
      }
    }
    if (id == null || record == null) return undefined;
    return await this.tupleFromRecord(key, record);
  }

  async *list(config: RunnableConfig, options: {
    limit?: number;
    before?: RunnableConfig;
    filter?: Record<string, unknown>;
  } = {}): AsyncGenerator<CheckpointTuple> {
    const limit = options.limit;
    if (limit != null && (!Number.isSafeInteger(limit) || limit < 0)) {
      throw new TypeError("limit must be a non-negative safe integer");
    }
    if (limit === 0) return;
    const beforeId = options.before == null ? undefined : checkpointId(options.before);
    const configurable = config.configurable;
    const threadId = optionalText(configurable?.thread_id, "thread_id");
    const expectedId = optionalText(configurable?.checkpoint_id ?? configurable?.thread_ts, "checkpoint_id");
    const namespaceSpecified = configurable != null && Object.hasOwn(configurable, "checkpoint_ns");
    const checkpointNs = namespaceSpecified ? requiredText(configurable?.checkpoint_ns, "checkpoint_ns", true) : undefined;

    const matches: { key: string; record: CheckpointRecord }[] = [];
    if (threadId == null) {
      await this.collectGlobal(matches, beforeId, expectedId, options.filter, limit);
    } else {
      const keys = checkpointNs == null
        ? await this.threadKeys(threadId)
        : [this.threadKey(threadId, checkpointNs)];
      for (const key of keys) {
        const ids = expectedId == null
          ? await this.checkpointIds(key)
          : [expectedId];
        for (const id of ids) {
          if (beforeId != null && id >= beforeId) continue;
          const record = await this.readRecord(key, id);
          if (record == null || !metadataMatches(record.metadata, options.filter)) continue;
          matches.push({ key, record });
        }
      }
      matches.sort((left, right) => right.record.checkpointId.localeCompare(left.record.checkpointId));
      if (limit != null) matches.splice(limit);
    }

    for (const match of matches) yield await this.tupleFromRecord(match.key, match.record);
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    _newVersions: LangGraphChannelVersions
  ): Promise<RunnableConfig> {
    void _newVersions;
    const { checkpointNs, threadId } = identity(config);
    if (typeof checkpoint.id !== "string" || checkpoint.id.length === 0) {
      throw new TypeError("checkpoint.id must be a non-empty string");
    }
    const parentCheckpointId = checkpointId(config);
    const record: CheckpointRecord = {
      checkpoint: copyCheckpoint(checkpoint),
      checkpointId: checkpoint.id,
      checkpointNs,
      formatVersion: FORMAT_VERSION,
      metadata,
      ...(parentCheckpointId == null ? {} : { parentCheckpointId }),
      threadId
    };
    const key = this.threadKey(threadId, checkpointNs);
    await withMutationLocks(this.client, [this.threadLockKey(threadId)], async () => {
      const locator = checkpointLocator(checkpoint.id, key);
      // Publish discovery paths first. Readers validate the final hash record,
      // so an interrupted write is invisible and a retry can safely finish it.
      await this.client.command("SADD", this.threadCatalogKey(threadId), key);
      await this.client.command("ZADD", this.threadLocatorCatalogKey(threadId), 0, locator);
      await this.client.command("ZADD", this.catalogKey(), 0, locator);
      await this.client.command("ZADD", this.checkpointIndexKey(key), 0, checkpoint.id);
      await this.client.command("HSET", key, checkpointField(checkpoint.id), await this.serialize(record));
    }, this.lockOptions);
    return checkpointConfig(threadId, checkpointNs, checkpoint.id);
  }

  async putWrites(config: RunnableConfig, writes: LangGraphPendingWrite[], taskId: string): Promise<void> {
    const { checkpointNs, threadId } = identity(config);
    const id = checkpointId(config);
    if (id == null) throw new TypeError("putWrites requires configurable.checkpoint_id");
    if (typeof taskId !== "string" || taskId.length === 0) throw new TypeError("taskId must be non-empty text");
    const snapshots = await Promise.all(writes.map(async ([channel, value], fallbackIndex) => {
      if (typeof channel !== "string") throw new TypeError("pending-write channel must be text");
      const index = WRITES_INDEX[channel] ?? fallbackIndex;
      const record: PendingWriteRecord = {
        channel,
        formatVersion: FORMAT_VERSION,
        index,
        taskId,
        value
      };
      return { index, value: await this.serialize(record) };
    }));
    const key = this.threadKey(threadId, checkpointNs);
    await withMutationLocks(this.client, [this.threadLockKey(threadId)], async () => {
      await this.client.command("SADD", this.threadCatalogKey(threadId), key);
      for (const snapshot of snapshots) {
        await this.client.command(
          snapshot.index < 0 ? "HSET" : "HSETNX",
          key,
          writeField(id, taskId, snapshot.index),
          snapshot.value
        );
      }
    }, this.lockOptions);
  }

  async deleteThread(threadId: string): Promise<void> {
    const normalized = requiredText(threadId, "threadId");
    await withMutationLocks(this.client, [this.threadLockKey(normalized)], async () => {
      for (const key of await this.threadKeys(normalized)) {
        await this.client.command("DEL", key, this.checkpointIndexKey(key));
      }
      const locatorKey = this.threadLocatorCatalogKey(normalized);
      while (true) {
        const locators = arrayResponse(
          await this.client.command("ZRANGE", locatorKey, 0, this.scanCount - 1),
          "thread checkpoint locator response"
        );
        if (locators.length === 0) break;
        await this.client.command("ZREM", this.catalogKey(), ...asArguments(locators));
        await this.client.command("ZREM", locatorKey, ...asArguments(locators));
      }
      await this.client.command("DEL", this.threadCatalogKey(normalized), locatorKey);
    }, this.lockOptions);
  }

  private catalogKey(): string {
    return `${this.keyPrefix}:checkpoints`;
  }

  private threadCatalogKey(threadId: string): string {
    const digest = sha256(threadId);
    return `${this.keyPrefix}:{lgt:${digest}}:namespaces`;
  }

  private threadLocatorCatalogKey(threadId: string): string {
    return `${this.threadCatalogKey(threadId)}:checkpoint-locators`;
  }

  private threadLockKey(threadId: string): string {
    return `${this.keyPrefix}:{lgt:${sha256(threadId)}}:mutation-lock`;
  }

  private threadKey(threadId: string, checkpointNs: string): string {
    return `${this.keyPrefix}:{lg:${sha256(lengthPrefixed([threadId, checkpointNs]))}}:thread`;
  }

  private checkpointIndexKey(threadKey: string): string {
    return `${threadKey}:checkpoint-index`;
  }

  private async serialize(value: unknown): Promise<Buffer> {
    const [type, data] = await this.serde.dumpsTyped(value);
    const typeBytes = Buffer.from(type, "utf8");
    if (typeBytes.length > 65_535) throw new TypeError("serialized LangGraph type name is too long");
    const header = Buffer.allocUnsafe(2);
    header.writeUInt16BE(typeBytes.length);
    return Buffer.concat([header, typeBytes, Buffer.from(data)]);
  }

  private async deserialize<T>(value: unknown, name: string): Promise<T> {
    if (!(typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array)) {
      throw new TypeError(`FerricStore returned a non-binary ${name}`);
    }
    const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
    if (bytes.length < 2) throw new Error(`invalid FerricStore ${name}`);
    const typeLength = bytes.readUInt16BE(0);
    if (bytes.length < typeLength + 2) throw new Error(`truncated FerricStore ${name}`);
    const type = bytes.subarray(2, typeLength + 2).toString("utf8");
    return await this.serde.loadsTyped(type, bytes.subarray(typeLength + 2)) as T;
  }

  private async readRecord(key: string, id: string): Promise<CheckpointRecord | undefined> {
    const value = await this.client.command("HGET", key, checkpointField(id));
    if (value == null) return undefined;
    const record = await this.deserialize<CheckpointRecord>(value, "LangGraph checkpoint record");
    if (record.formatVersion !== FORMAT_VERSION || record.checkpointId !== id) {
      throw new Error("unsupported or corrupt FerricStore LangGraph checkpoint record");
    }
    return record;
  }

  private async pendingWrites(key: string, id: string): Promise<[string, string, unknown][]> {
    const fields = await this.scanHash(key, `${WRITE_FIELD_PREFIX}${encodeComponent(id)}:*`);
    const records = await Promise.all(fields.map(async ([, value]) =>
      await this.deserialize<PendingWriteRecord>(value, "LangGraph pending write")));
    for (const record of records) {
      if (record.formatVersion !== FORMAT_VERSION) {
        throw new Error("unsupported FerricStore LangGraph pending-write format");
      }
    }
    records.sort((left, right) => left.taskId.localeCompare(right.taskId) || left.index - right.index);
    return records.map((record) => [record.taskId, record.channel, record.value]);
  }

  private async tupleFromRecord(key: string, record: CheckpointRecord): Promise<CheckpointTuple> {
    return {
      checkpoint: record.checkpoint,
      config: checkpointConfig(record.threadId, record.checkpointNs, record.checkpointId),
      metadata: record.metadata,
      pendingWrites: await this.pendingWrites(key, record.checkpointId),
      ...(record.parentCheckpointId == null
        ? {}
        : { parentConfig: checkpointConfig(record.threadId, record.checkpointNs, record.parentCheckpointId) })
    };
  }

  private async scanHash(key: string, pattern: string): Promise<[unknown, unknown][]> {
    let cursor = 0;
    const result: [unknown, unknown][] = [];
    do {
      const response = arrayResponse(
        await this.client.command("HSCAN", key, cursor, "MATCH", pattern, "COUNT", this.scanCount),
        "HSCAN response"
      );
      if (response.length !== 2) throw new Error("FerricStore HSCAN response must contain cursor and items");
      cursor = integerResponse(response[0], "HSCAN cursor");
      const values = response[1];
      if (values instanceof Map) {
        result.push(...values.entries());
      } else {
        const flat = arrayResponse(values, "HSCAN items");
        if (flat.length % 2 !== 0) throw new Error("FerricStore HSCAN returned an odd item count");
        for (let index = 0; index < flat.length; index += 2) result.push([flat[index], flat[index + 1]]);
      }
    } while (cursor !== 0);
    return result;
  }

  private async threadKeys(threadId: string): Promise<string[]> {
    const values = arrayResponse(
      await this.client.command("SMEMBERS", this.threadCatalogKey(threadId)),
      "thread checkpoint catalog response"
    );
    return values.map((value) => textResponse(value, "thread checkpoint key")).sort();
  }

  private async checkpointIds(key: string): Promise<string[]> {
    const values = arrayResponse(
      await this.client.command("ZREVRANGE", this.checkpointIndexKey(key), 0, -1),
      "checkpoint index response"
    );
    return values.map((value) => textResponse(value, "checkpoint ID"));
  }

  private async collectGlobal(
    result: { key: string; record: CheckpointRecord }[],
    beforeId: string | undefined,
    expectedId: string | undefined,
    filter: Record<string, unknown> | undefined,
    limit: number | undefined
  ): Promise<void> {
    let offset = 0;
    while (limit == null || result.length < limit) {
      const values = arrayResponse(
        await this.client.command("ZREVRANGE", this.catalogKey(), offset, offset + this.scanCount - 1),
        "global checkpoint catalog response"
      );
      if (values.length === 0) break;
      for (const value of values) {
        const { checkpointId: id, threadKey: key } = decodeCheckpointLocator(value);
        if (expectedId != null && id !== expectedId) continue;
        if (beforeId != null && id >= beforeId) continue;
        const record = await this.readRecord(key, id);
        if (record == null || !metadataMatches(record.metadata, filter)) continue;
        result.push({ key, record });
        if (limit != null && result.length >= limit) break;
      }
      offset += values.length;
      if (values.length < this.scanCount) break;
    }
  }
}

function identity(config: RunnableConfig): { checkpointNs: string; threadId: string } {
  if (config.configurable == null || typeof config.configurable !== "object") {
    throw new TypeError("LangGraph config must contain configurable.thread_id");
  }
  return {
    checkpointNs: requiredText(config.configurable.checkpoint_ns ?? "", "checkpoint_ns", true),
    threadId: requiredText(config.configurable.thread_id, "thread_id")
  };
}

function checkpointId(config: RunnableConfig): string | undefined {
  return optionalText(config.configurable?.checkpoint_id ?? config.configurable?.thread_ts, "checkpoint_id");
}

function checkpointConfig(threadId: string, checkpointNs: string, id: string): RunnableConfig {
  return { configurable: { checkpoint_id: id, checkpoint_ns: checkpointNs, thread_id: threadId } };
}

function requiredText(value: unknown, name: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    throw new TypeError(`${name} must be ${allowEmpty ? "text" : "non-empty text"}`);
  }
  return value;
}

function optionalText(value: unknown, name: string): string | undefined {
  if (value == null || value === "") return undefined;
  return requiredText(value, name);
}

function metadataMatches(metadata: CheckpointMetadata, filter: Record<string, unknown> | undefined): boolean {
  return filter == null || Object.entries(filter).every(([key, value]) => {
    const actual = Object.getOwnPropertyDescriptor(metadata, key)?.value as unknown;
    return Object.is(actual, value);
  });
}

function checkpointField(id: string): string {
  return `${CHECKPOINT_FIELD_PREFIX}${encodeComponent(id)}`;
}

function writeField(id: string, taskId: string, index: number): string {
  return `${WRITE_FIELD_PREFIX}${encodeComponent(id)}:${encodeComponent(taskId)}:${index}`;
}

function encodeComponent(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

function checkpointLocator(id: string, threadKey: string): Buffer {
  return Buffer.concat([orderedText(id), Buffer.from(threadKey, "utf8")]);
}

function decodeCheckpointLocator(value: unknown): { checkpointId: string; threadKey: string } {
  if (!(typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array)) {
    throw new TypeError("FerricStore returned a non-binary checkpoint locator");
  }
  const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
  const decoded = decodeOrderedText(bytes, 0);
  const threadKey = bytes.subarray(decoded.offset).toString("utf8");
  if (threadKey.length === 0) throw new Error("FerricStore checkpoint locator has an empty thread key");
  return { checkpointId: decoded.value, threadKey };
}

function orderedText(value: string): Buffer {
  const output: number[] = [];
  for (const byte of Buffer.from(value, "utf8")) {
    if (byte === 0) output.push(0, 255);
    else output.push(byte);
  }
  output.push(0, 0);
  return Buffer.from(output);
}

function decodeOrderedText(bytes: Buffer, start: number): { offset: number; value: string } {
  const output: number[] = [];
  let offset = start;
  while (offset < bytes.length) {
    const byte = bytes[offset];
    if (byte == null) throw new Error("truncated FerricStore checkpoint locator");
    offset += 1;
    if (byte !== 0) {
      output.push(byte);
      continue;
    }
    const escaped = bytes[offset];
    offset += 1;
    if (escaped === 0) return { offset, value: Buffer.from(output).toString("utf8") };
    if (escaped === 255) output.push(0);
    else throw new Error("invalid FerricStore checkpoint locator escape");
  }
  throw new Error("unterminated FerricStore checkpoint locator");
}

function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function lengthPrefixed(values: readonly string[]): Buffer {
  const parts: Buffer[] = [];
  for (const value of values) {
    const payload = Buffer.from(value, "utf8");
    const length = Buffer.allocUnsafe(8);
    length.writeBigUInt64BE(BigInt(payload.length));
    parts.push(length, payload);
  }
  return Buffer.concat(parts);
}

function asArguments(values: readonly unknown[]): (string | number | Buffer)[] {
  return values.map((value) => {
    if (typeof value === "string" || typeof value === "number" || Buffer.isBuffer(value)) return value;
    if (value instanceof Uint8Array) return Buffer.from(value);
    throw new TypeError("FerricStore returned an invalid command argument");
  });
}
