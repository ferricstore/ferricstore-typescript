import { createHash, randomUUID } from "node:crypto";

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
  readAtomicValue,
  textResponse,
  type FerricStoreCommandClient,
  type FerricStoreLockOptions,
  type FerricStoreMutationLease,
  withMutationLocks
} from "../agent-persistence/durability.js";

export type LangGraphPendingWrite = [channel: string, value: unknown];
export type LangGraphChannelVersions = Record<string, string | number>;
type SerializerProtocol = BaseCheckpointSaver["serde"];

const FORMAT_VERSION = 1;
const LEGACY_EPOCH_PREFIX = "legacy:";
const CURRENT_EPOCH_PREFIX = "v2:";
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

interface ThreadEpoch {
  readonly legacyFallback: boolean;
  readonly raw: Buffer | undefined;
  readonly value: string | undefined;
}

export interface FerricStoreSaverOptions extends FerricStoreLockOptions {
  /** FerricStore key prefix. Defaults to `langgraph:checkpoint`. */
  keyPrefix?: string;
  /** Number of catalog/hash entries read per page. Defaults to 256. */
  scanCount?: number;
  /** Optional LangGraph serializer. The framework JSON-plus serializer is used by default. */
  serde?: SerializerProtocol;
}

/** Durable, epoch-fenced LangGraph.js checkpoint saver backed by FerricStore. */
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
    const epoch = await this.readThreadEpoch(threadId);
    let id = checkpointId(config);
    let record: CheckpointRecord | undefined;
    if (id != null) {
      record = await this.readRecordAtEpoch(key, id, epoch);
    } else {
      for (const candidate of await this.checkpointIdsAtEpoch(key, epoch)) {
        record = await this.readRecordAtEpoch(key, candidate, epoch);
        if (record != null) {
          id = candidate;
          break;
        }
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
      await this.collectGlobal(matches, beforeId, expectedId, checkpointNs, options.filter, limit);
    } else {
      const keys = checkpointNs == null
        ? await this.threadKeys(threadId)
        : [this.threadKey(threadId, checkpointNs)];
      for (const key of keys) {
        const epoch = await this.readThreadEpoch(threadId);
        const ids = expectedId == null
          ? await this.checkpointIdsAtEpoch(key, epoch)
          : [expectedId];
        for (const id of ids) {
          if (beforeId != null && id >= beforeId) continue;
          const record = await this.readRecordAtEpoch(key, id, epoch);
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
    await withMutationLocks(this.client, [this.threadLockKey(threadId)], async (lease) => {
      const epoch = await this.ensureThreadEpoch(threadId, lease);
      const locator = checkpointLocator(checkpoint.id, key, threadId);
      // Discovery indexes are append-only and published before the CAS record.
      // Readers validate the record in the current thread epoch, so stale
      // writers and interrupted publications remain invisible.
      await lease.publish("SADD", this.threadCatalogKey(threadId), key);
      await lease.publish("ZADD", this.threadLocatorCatalogKey(threadId), 0, locator);
      await lease.publish("ZADD", this.catalogKey(), 0, locator);
      await lease.publish("ZADD", this.checkpointIndexKey(key), 0, checkpoint.id);
      await lease.publish("ZADD", this.atomicCheckpointIndexKey(key, requireEpoch(epoch)), 0, checkpoint.id);
      const dataKey = this.checkpointRecordKey(key, requireEpoch(epoch), checkpoint.id);
      const value = await this.serialize(record);
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const expected = await readAtomicValue(this.client, dataKey, "LangGraph atomic checkpoint record");
        if (await lease.compareAndSet(dataKey, expected, value)) {
          await this.assertCurrentEpoch(threadId, epoch);
          return;
        }
      }
      throw new Error("concurrent FerricStore LangGraph checkpoint mutation did not converge");
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
    await withMutationLocks(this.client, [this.threadLockKey(threadId)], async (lease) => {
      const epoch = await this.ensureThreadEpoch(threadId, lease);
      const epochValue = requireEpoch(epoch);
      await lease.publish("SADD", this.threadCatalogKey(threadId), key);
      for (const snapshot of snapshots) {
        const dataKey = this.pendingWriteKey(key, epochValue, id, taskId, snapshot.index);
        const indexKey = this.atomicPendingWriteIndexKey(key, epochValue, id);
        await lease.publish("ZADD", indexKey, 0, dataKey);
        for (let attempt = 0; attempt < 8; attempt += 1) {
          const expected = await readAtomicValue(this.client, dataKey, "LangGraph atomic pending write");
          if (snapshot.index >= 0 && expected != null) break;
          if (snapshot.index >= 0 && epoch.legacyFallback) {
            const legacy = await this.client.command("HGET", key, writeField(id, taskId, snapshot.index));
            if (legacy != null) break;
          }
          if (await lease.compareAndSet(dataKey, expected, snapshot.value)) break;
          if (attempt === 7) {
            throw new Error("concurrent FerricStore LangGraph pending-write mutation did not converge");
          }
        }
      }
      await this.assertCurrentEpoch(threadId, epoch);
    }, this.lockOptions);
  }

  async deleteThread(threadId: string): Promise<void> {
    const normalized = requiredText(threadId, "threadId");
    await withMutationLocks(this.client, [this.threadLockKey(normalized)], async (lease) => {
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const epoch = await this.ensureThreadEpoch(normalized, lease);
        const next = Buffer.from(`${CURRENT_EPOCH_PREFIX}${randomUUID()}`, "utf8");
        if (await lease.compareAndSet(this.threadEpochKey(normalized), epoch.raw, next)) return;
      }
      throw new Error("concurrent FerricStore LangGraph thread deletion did not converge");
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

  private threadEpochKey(threadId: string): string {
    return `${this.threadCatalogKey(threadId)}:atomic-epoch`;
  }

  private atomicCheckpointIndexKey(threadKey: string, epoch: string): string {
    return `${threadKey}:atomic:${sha256(epoch)}:checkpoint-index`;
  }

  private atomicPendingWriteIndexKey(threadKey: string, epoch: string, checkpointId: string): string {
    return `${threadKey}:atomic:${sha256(epoch)}:writes:${encodeComponent(checkpointId)}`;
  }

  private checkpointRecordKey(threadKey: string, epoch: string, checkpointId: string): string {
    return `${threadKey}:atomic:${sha256(epoch)}:checkpoint:${encodeComponent(checkpointId)}`;
  }

  private pendingWriteKey(
    threadKey: string,
    epoch: string,
    checkpointId: string,
    taskId: string,
    index: number
  ): string {
    return `${threadKey}:atomic:${sha256(epoch)}:write:${encodeComponent(checkpointId)}:${encodeComponent(taskId)}:${index}`;
  }

  private async readThreadEpoch(threadId: string): Promise<ThreadEpoch> {
    const raw = await readAtomicValue(this.client, this.threadEpochKey(threadId), "LangGraph thread epoch");
    if (raw == null) return { legacyFallback: true, raw, value: undefined };
    const value = raw.toString("utf8");
    if (!value.startsWith(LEGACY_EPOCH_PREFIX) && !value.startsWith(CURRENT_EPOCH_PREFIX)) {
      throw new Error("unsupported or corrupt FerricStore LangGraph thread epoch");
    }
    return { legacyFallback: value.startsWith(LEGACY_EPOCH_PREFIX), raw, value };
  }

  private async ensureThreadEpoch(
    threadId: string,
    lease: FerricStoreMutationLease
  ): Promise<ThreadEpoch> {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const epoch = await this.readThreadEpoch(threadId);
      if (epoch.value != null) return epoch;
      const raw = Buffer.from(`${LEGACY_EPOCH_PREFIX}${randomUUID()}`, "utf8");
      if (await lease.compareAndSet(this.threadEpochKey(threadId), undefined, raw)) {
        return { legacyFallback: true, raw, value: raw.toString("utf8") };
      }
    }
    throw new Error("concurrent FerricStore LangGraph epoch initialization did not converge");
  }

  private async assertCurrentEpoch(threadId: string, expected: ThreadEpoch): Promise<void> {
    const current = await this.readThreadEpoch(threadId);
    if (expected.raw == null || current.raw?.equals(expected.raw) !== true) {
      throw new Error("FerricStore LangGraph thread epoch changed while committing data");
    }
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

  private async readRecordAtEpoch(
    key: string,
    id: string,
    epoch: ThreadEpoch
  ): Promise<CheckpointRecord | undefined> {
    let value: unknown;
    if (epoch.value != null) {
      value = await this.client.command("GET", this.checkpointRecordKey(key, epoch.value, id));
      if (value == null && epoch.legacyFallback) {
        value = await this.client.command("HGET", key, checkpointField(id));
      }
    } else {
      value = await this.client.command("HGET", key, checkpointField(id));
    }
    if (value == null) return undefined;
    const record = await this.deserialize<CheckpointRecord>(value, "LangGraph checkpoint record");
    if (record.formatVersion !== FORMAT_VERSION || record.checkpointId !== id) {
      throw new Error("unsupported or corrupt FerricStore LangGraph checkpoint record");
    }
    return record;
  }

  private async pendingWritesAtEpoch(
    key: string,
    id: string,
    epoch: ThreadEpoch
  ): Promise<[string, string, unknown][]> {
    const records = new Map<string, PendingWriteRecord>();
    if (epoch.value != null) {
      const keys = arrayResponse(
        await this.client.command("ZRANGE", this.atomicPendingWriteIndexKey(key, epoch.value, id), 0, -1),
        "LangGraph atomic pending-write index response"
      );
      for (const rawKey of keys) {
        const dataKey = textResponse(rawKey, "LangGraph pending-write key");
        const value = await this.client.command("GET", dataKey);
        if (value == null) continue;
        const record = await this.deserialize<PendingWriteRecord>(value, "LangGraph pending write");
        records.set(pendingWriteIdentity(record), record);
      }
    }
    if (epoch.legacyFallback) {
      const fields = await this.scanHash(key, `${WRITE_FIELD_PREFIX}${encodeComponent(id)}:*`);
      for (const [, value] of fields) {
        const record = await this.deserialize<PendingWriteRecord>(value, "LangGraph pending write");
        const identity = pendingWriteIdentity(record);
        if (!records.has(identity)) records.set(identity, record);
      }
    }
    const ordered = [...records.values()];
    for (const record of ordered) {
      if (record.formatVersion !== FORMAT_VERSION) {
        throw new Error("unsupported FerricStore LangGraph pending-write format");
      }
    }
    ordered.sort((left, right) => left.taskId.localeCompare(right.taskId) || left.index - right.index);
    return ordered.map((record) => [record.taskId, record.channel, record.value]);
  }

  private async tupleFromRecord(key: string, record: CheckpointRecord): Promise<CheckpointTuple> {
    const epoch = await this.readThreadEpoch(record.threadId);
    return {
      checkpoint: record.checkpoint,
      config: checkpointConfig(record.threadId, record.checkpointNs, record.checkpointId),
      metadata: record.metadata,
      pendingWrites: await this.pendingWritesAtEpoch(key, record.checkpointId, epoch),
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

  private async checkpointIdsAtEpoch(key: string, epoch: ThreadEpoch): Promise<string[]> {
    const ids = new Set<string>();
    if (epoch.value != null) {
      const values = arrayResponse(
        await this.client.command("ZREVRANGE", this.atomicCheckpointIndexKey(key, epoch.value), 0, -1),
        "atomic checkpoint index response"
      );
      for (const value of values) ids.add(textResponse(value, "checkpoint ID"));
    }
    if (epoch.legacyFallback) {
      const values = arrayResponse(
        await this.client.command("ZREVRANGE", this.checkpointIndexKey(key), 0, -1),
        "checkpoint index response"
      );
      for (const value of values) ids.add(textResponse(value, "checkpoint ID"));
    }
    return [...ids].sort((left, right) => right.localeCompare(left));
  }

  private async collectGlobal(
    result: { key: string; record: CheckpointRecord }[],
    beforeId: string | undefined,
    expectedId: string | undefined,
    checkpointNs: string | undefined,
    filter: Record<string, unknown> | undefined,
    limit: number | undefined
  ): Promise<void> {
    let offset = 0;
    const seen = new Set<string>();
    while (limit == null || result.length < limit) {
      const values = arrayResponse(
        await this.client.command("ZREVRANGE", this.catalogKey(), offset, offset + this.scanCount - 1),
        "global checkpoint catalog response"
      );
      if (values.length === 0) break;
      for (const value of values) {
        const locator = decodeCheckpointLocator(value);
        const { checkpointId: id, threadKey: key } = locator;
        if (expectedId != null && id !== expectedId) continue;
        if (beforeId != null && id >= beforeId) continue;
        let record: CheckpointRecord | undefined;
        if (locator.threadId != null) {
          record = await this.readRecordAtEpoch(key, id, await this.readThreadEpoch(locator.threadId));
        } else {
          const legacy = await this.client.command("HGET", key, checkpointField(id));
          if (legacy != null) {
            const candidate = await this.deserialize<CheckpointRecord>(legacy, "LangGraph checkpoint record");
            record = await this.readRecordAtEpoch(key, id, await this.readThreadEpoch(candidate.threadId));
          }
        }
        if (
          record == null ||
          checkpointNs != null && record.checkpointNs !== checkpointNs ||
          !metadataMatches(record.metadata, filter)
        ) continue;
        const identity = lengthPrefixed([record.threadId, record.checkpointNs, record.checkpointId]).toString("base64url");
        if (seen.has(identity)) continue;
        seen.add(identity);
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

function checkpointLocator(id: string, threadKey: string, threadId?: string): Buffer {
  if (threadId == null) return Buffer.concat([orderedText(id), Buffer.from(threadKey, "utf8")]);
  const threadIdBytes = Buffer.from(threadId, "utf8");
  const length = Buffer.allocUnsafe(4);
  length.writeUInt32BE(threadIdBytes.length);
  return Buffer.concat([
    orderedText(id),
    Buffer.from([0, 1]),
    length,
    threadIdBytes,
    Buffer.from(threadKey, "utf8")
  ]);
}

function decodeCheckpointLocator(value: unknown): {
  checkpointId: string;
  threadId?: string;
  threadKey: string;
} {
  if (!(typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array)) {
    throw new TypeError("FerricStore returned a non-binary checkpoint locator");
  }
  const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
  const decoded = decodeOrderedText(bytes, 0);
  let offset = decoded.offset;
  let threadId: string | undefined;
  if (bytes[offset] === 0 && bytes[offset + 1] === 1) {
    offset += 2;
    if (bytes.length < offset + 4) throw new Error("truncated FerricStore checkpoint locator");
    const length = bytes.readUInt32BE(offset);
    offset += 4;
    if (bytes.length < offset + length) throw new Error("truncated FerricStore checkpoint locator thread ID");
    threadId = bytes.subarray(offset, offset + length).toString("utf8");
    offset += length;
    if (threadId.length === 0) throw new Error("FerricStore checkpoint locator has an empty thread ID");
  }
  const threadKey = bytes.subarray(offset).toString("utf8");
  if (threadKey.length === 0) throw new Error("FerricStore checkpoint locator has an empty thread key");
  return { checkpointId: decoded.value, ...(threadId == null ? {} : { threadId }), threadKey };
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

function requireEpoch(epoch: ThreadEpoch): string {
  if (epoch.value == null) throw new Error("FerricStore LangGraph thread epoch is not initialized");
  return epoch.value;
}

function pendingWriteIdentity(record: PendingWriteRecord): string {
  return lengthPrefixed([record.taskId, String(record.index)]).toString("base64url");
}
