import { createHash } from "node:crypto";

import {
  BaseStore,
  type GetOperation,
  type Item,
  type ListNamespacesOperation,
  type MatchCondition,
  type Operation,
  type OperationResults,
  type PutOperation,
  type SearchOperation
} from "@langchain/langgraph";

import {
  arrayResponse,
  normalizeKeyPrefix,
  positiveInteger,
  readAtomicValue,
  type FerricStoreCommandClient,
  type FerricStoreLockOptions,
  type FerricStoreMutationLease,
  withMutationLocks
} from "../agent-persistence/durability.js";

const FORMAT_VERSION = 1;
const ITEM_FIELD_PREFIX = "item:";
const DELETED_ITEM = Buffer.from("ferricstore:langgraph:item:deleted:v2", "utf8");
type SearchItem = Item & { score?: number };

interface StoredItem {
  readonly createdAt: string;
  readonly formatVersion: typeof FORMAT_VERSION;
  readonly key: string;
  readonly namespace: string[];
  readonly updatedAt: string;
  readonly value: Record<string, unknown>;
}

export interface FerricStoreStoreOptions extends FerricStoreLockOptions {
  /** FerricStore key prefix. Defaults to `langgraph:store`. */
  keyPrefix?: string;
  /** Number of catalog entries read per page. Defaults to 256. */
  scanCount?: number;
}

/**
 * LangGraph.js long-term memory store backed by FerricStore.
 *
 * It implements hierarchical namespaces, exact/comparison filters, ordered
 * pagination, batching, CAS-protected item commits, and append-only discovery
 * indexes. Vector search is deliberately rejected until a semantic index is
 * configured rather than silently returning an unranked result.
 */
export class FerricStoreStore extends BaseStore {
  readonly client: FerricStoreCommandClient;
  readonly keyPrefix: string;
  readonly scanCount: number;
  private readonly lockOptions: FerricStoreLockOptions;

  constructor(client: FerricStoreCommandClient, options: FerricStoreStoreOptions = {}) {
    super();
    this.client = client;
    this.keyPrefix = normalizeKeyPrefix(options.keyPrefix ?? "langgraph:store", "langgraph:store");
    this.scanCount = positiveInteger(options.scanCount, 256, "scanCount");
    this.lockOptions = {
      lockRetryMs: options.lockRetryMs,
      lockTtlMs: options.lockTtlMs,
      lockWaitMs: options.lockWaitMs
    };
  }

  async batch<Op extends Operation[]>(operations: Op): Promise<OperationResults<Op>> {
    if (!Array.isArray(operations)) throw new TypeError("operations must be an array");
    const lockKeys = operations
      .filter(isPutOperation)
      .map((operation) => {
        validatePut(operation);
        return this.itemLockKey(operation.namespace, operation.key);
      });

    return await withMutationLocks(this.client, lockKeys, async (lease) => {
      const results: unknown[] = [];
      const puts = new Map<string, PutOperation>();
      for (const operation of operations) {
        if (isPutOperation(operation)) {
          puts.set(JSON.stringify([operation.namespace, operation.key]), operation);
          results.push(undefined);
        } else if (isSearchOperation(operation)) {
          results.push(await this.searchOperation(operation));
        } else if (isGetOperation(operation)) {
          results.push(await this.getOperation(operation));
        } else if (isListNamespacesOperation(operation)) {
          results.push(await this.listNamespacesOperation(operation));
        } else {
          throw new TypeError("unsupported LangGraph store operation");
        }
      }
      for (const operation of puts.values()) await this.putOperation(operation, lease);
      return results as OperationResults<Op>;
    }, this.lockOptions);
  }

  private catalogKey(): string {
    return `${this.keyPrefix}:namespaces`;
  }

  private namespaceKey(namespace: readonly string[]): string {
    return `${this.keyPrefix}:{lgs:${sha256(namespaceIdentity(namespace))}}:namespace`;
  }

  private itemLockKey(namespace: readonly string[], key: string): string {
    return `${this.keyPrefix}:{lgsi:${itemIdentityDigest(namespace, key)}}:mutation-lock`;
  }

  private itemDataKey(namespace: readonly string[], key: string): string {
    return `${this.keyPrefix}:{lgsi:${itemIdentityDigest(namespace, key)}}:atomic-item`;
  }

  private async getOperation(operation: GetOperation): Promise<Item | null> {
    validateNamespace(operation.namespace);
    if (typeof operation.key !== "string") throw new TypeError("store key must be text");
    const snapshot = await this.readItemSnapshot(operation.namespace, operation.key);
    return snapshot.record == null ? null : itemFromRecord(snapshot.record);
  }

  private async putOperation(operation: PutOperation, lease: FerricStoreMutationLease): Promise<void> {
    validatePut(operation);
    const dataKey = this.itemDataKey(operation.namespace, operation.key);
    const locator = catalogMember(operation.namespace, operation.key);
    if (operation.value == null) {
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const snapshot = await this.readItemSnapshot(operation.namespace, operation.key);
        if (snapshot.expected?.equals(DELETED_ITEM) === true) return;
        if (await lease.compareAndSet(dataKey, snapshot.expected, DELETED_ITEM)) return;
      }
      throw new Error("concurrent FerricStore LangGraph store deletion did not converge");
    }
    const storedValue = snapshotJsonValue(operation.value);
    // The catalog is append-only. Readers validate the current CAS-protected
    // item, so a stale or interrupted publication cannot hide a newer value.
    await lease.publish("ZADD", this.catalogKey(), 0, locator);
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const snapshot = await this.readItemSnapshot(operation.namespace, operation.key);
      const now = new Date().toISOString();
      const record: StoredItem = {
        createdAt: snapshot.record?.createdAt ?? now,
        formatVersion: FORMAT_VERSION,
        key: operation.key,
        namespace: [...operation.namespace],
        updatedAt: now,
        value: storedValue
      };
      if (await lease.compareAndSet(dataKey, snapshot.expected, encodeItem(record))) return;
    }
    throw new Error("concurrent FerricStore LangGraph store mutation did not converge");
  }

  private async searchOperation(operation: SearchOperation): Promise<SearchItem[]> {
    validateNamespacePrefix(operation.namespacePrefix);
    if (operation.query != null) {
      throw new Error(
        "FerricStoreStore semantic query search is not configured; use metadata filters or a vector-enabled store"
      );
    }
    const limit = normalizePageNumber(operation.limit, 10, "search limit");
    const offset = normalizePageNumber(operation.offset, 0, "search offset");
    if (limit === 0) return [];
    const matches: SearchItem[] = [];
    for await (const locator of this.catalogLocators()) {
      if (!startsWithNamespace(locator.namespace, operation.namespacePrefix)) continue;
      const item = await this.readCatalogItem(locator.namespace, locator.key);
      if (item == null || !matchesFilter(item.value, operation.filter)) continue;
      matches.push(item);
      if (matches.length >= offset + limit) break;
    }
    return matches.slice(offset, offset + limit);
  }

  private async listNamespacesOperation(operation: ListNamespacesOperation): Promise<string[][]> {
    const limit = normalizePageNumber(operation.limit, 100, "namespace limit");
    const offset = normalizePageNumber(operation.offset, 0, "namespace offset");
    const maxDepth = operation.maxDepth == null
      ? undefined
      : positiveInteger(operation.maxDepth, operation.maxDepth, "maxDepth");
    if (limit === 0) return [];
    const namespaces = new Map<string, string[]>();
    for await (const locator of this.catalogLocators()) {
      const item = await this.readCatalogItem(locator.namespace, locator.key);
      if (item == null) continue;
      if (!matchesConditions(item.namespace, operation.matchConditions)) continue;
      const namespace = maxDepth == null ? item.namespace : item.namespace.slice(0, maxDepth);
      namespaces.set(JSON.stringify(namespace), namespace);
    }
    return [...namespaces.values()]
      .sort(compareNamespaces)
      .slice(offset, offset + limit);
  }

  private async readCatalogItem(namespace: string[], key: string): Promise<Item | null> {
    const snapshot = await this.readItemSnapshot(namespace, key);
    return snapshot.record == null ? null : itemFromRecord(snapshot.record);
  }

  private async readItemSnapshot(
    namespace: readonly string[],
    key: string
  ): Promise<{ expected: Buffer | undefined; record: StoredItem | null }> {
    const expected = await readAtomicValue(
      this.client,
      this.itemDataKey(namespace, key),
      "LangGraph atomic store item"
    );
    if (expected != null) {
      return {
        expected,
        record: expected.equals(DELETED_ITEM) ? null : decodeItemRecord(expected)
      };
    }
    const legacy = await this.client.command("HGET", this.namespaceKey(namespace), itemField(key));
    return { expected, record: legacy == null ? null : decodeItemRecord(legacy) };
  }

  private async *catalogLocators(): AsyncGenerator<{ key: string; namespace: string[] }> {
    let offset = 0;
    while (true) {
      const values = arrayResponse(
        await this.client.command("ZRANGE", this.catalogKey(), offset, offset + this.scanCount - 1),
        "LangGraph store catalog response"
      );
      if (values.length === 0) return;
      for (const value of values) yield decodeCatalogMember(value);
      offset += values.length;
      if (values.length < this.scanCount) return;
    }
  }
}

function isPutOperation(operation: Operation): operation is PutOperation {
  return "namespace" in operation && "key" in operation && "value" in operation;
}

function isGetOperation(operation: Operation): operation is GetOperation {
  return "namespace" in operation && "key" in operation && !("value" in operation);
}

function isSearchOperation(operation: Operation): operation is SearchOperation {
  return "namespacePrefix" in operation;
}

function isListNamespacesOperation(operation: Operation): operation is ListNamespacesOperation {
  return !("namespace" in operation) && !("namespacePrefix" in operation);
}

function validatePut(operation: PutOperation): void {
  validateNamespace(operation.namespace);
  if (typeof operation.key !== "string") throw new TypeError("store key must be text");
  if (operation.value == null) return;
  if (typeof operation.value !== "object" || Array.isArray(operation.value)) {
    throw new TypeError("LangGraph store values must be JSON objects");
  }
  validateJson(operation.value, new WeakSet());
}

function validateNamespace(namespace: readonly string[]): void {
  if (!Array.isArray(namespace) || namespace.length === 0) {
    throw new Error("namespace cannot be empty");
  }
  for (const label of namespace) {
    if (typeof label !== "string" || label.length === 0 || label.includes(".")) {
      throw new Error("namespace labels must be non-empty strings without periods");
    }
  }
  if (namespace[0] === "langgraph") throw new Error('root namespace label cannot be "langgraph"');
}

function validateNamespacePrefix(namespace: readonly string[]): void {
  if (!Array.isArray(namespace)) throw new TypeError("namespacePrefix must be an array");
  if (namespace.length > 0) validateNamespace(namespace);
}

function validateJson(value: unknown, ancestors: WeakSet<object>): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (value === undefined) throw new TypeError("LangGraph store values must not contain undefined");
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("LangGraph store numbers must be finite");
    return;
  }
  if (typeof value !== "object") throw new TypeError("LangGraph store values must be JSON serializable");
  if (ancestors.has(value)) throw new TypeError("LangGraph store values must not be cyclic");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const keys = Reflect.ownKeys(value);
      if (
        keys.length !== value.length + 1 ||
        keys.some((key) => typeof key !== "string" || key !== "length" && !isArrayIndex(key, value.length))
      ) {
        throw new TypeError("LangGraph store arrays must not be sparse or customized");
      }
      for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (descriptor == null || !descriptor.enumerable || !("value" in descriptor)) {
          throw new TypeError("LangGraph store arrays contain an unsupported item");
        }
        validateJson(descriptor.value as unknown, ancestors);
      }
      return;
    }
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("LangGraph store values must contain only JSON objects");
    }
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string") throw new TypeError("LangGraph store objects must not have symbol keys");
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor == null || !descriptor.enumerable || !("value" in descriptor)) {
        throw new TypeError("LangGraph store objects contain an unsupported property");
      }
      validateJson(descriptor.value as unknown, ancestors);
    }
  } finally {
    ancestors.delete(value);
  }
}

function snapshotJsonValue(value: Record<string, unknown>): Record<string, unknown> {
  validateJson(value, new WeakSet());
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function encodeItem(record: StoredItem): Buffer {
  return Buffer.from(JSON.stringify(record), "utf8");
}

function itemFromRecord(record: StoredItem): Item {
  return {
    createdAt: new Date(record.createdAt),
    key: record.key,
    namespace: [...record.namespace],
    updatedAt: new Date(record.updatedAt),
    value: record.value
  };
}

function itemIdentityDigest(namespace: readonly string[], key: string): string {
  const keyBytes = Buffer.from(key, "utf8");
  return sha256(Buffer.concat([
    namespaceIdentity(namespace),
    uint64(keyBytes.length),
    keyBytes
  ]));
}

function decodeItemRecord(value: unknown): StoredItem {
  if (!(typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array)) {
    throw new TypeError("FerricStore returned a non-binary LangGraph store item");
  }
  const text = typeof value === "string" ? value : Buffer.from(value).toString("utf8");
  let record: unknown;
  try {
    record = JSON.parse(text);
  } catch (error) {
    throw new Error("invalid FerricStore LangGraph store item", { cause: error });
  }
  if (
    record == null ||
    typeof record !== "object" ||
    (record as Partial<StoredItem>).formatVersion !== FORMAT_VERSION ||
    typeof (record as Partial<StoredItem>).key !== "string" ||
    !Array.isArray((record as Partial<StoredItem>).namespace) ||
    typeof (record as Partial<StoredItem>).createdAt !== "string" ||
    typeof (record as Partial<StoredItem>).updatedAt !== "string" ||
    (record as Partial<StoredItem>).value == null ||
    typeof (record as Partial<StoredItem>).value !== "object"
  ) {
    throw new Error("unsupported or corrupt FerricStore LangGraph store item");
  }
  return record as StoredItem;
}

function matchesFilter(value: Record<string, unknown>, filter: Record<string, unknown> | undefined): boolean {
  return filter == null || Object.entries(filter).every(([key, expected]) => compareValue(value[key], expected));
}

function compareValue(value: unknown, expected: unknown): boolean {
  if (expected != null && typeof expected === "object" && !Array.isArray(expected)) {
    const entries = Object.entries(expected);
    if (entries.length > 0 && entries.every(([key]) => FILTER_OPERATORS.has(key))) {
      return entries.every(([operator, operand]) => applyOperator(value, operator, operand));
    }
    return value != null && typeof value === "object" && !Array.isArray(value) &&
      entries.every(([key, nested]) => compareValue((value as Record<string, unknown>)[key], nested));
  }
  if (Array.isArray(expected)) {
    return Array.isArray(value) && value.length === expected.length &&
      value.every((item, index) => compareValue(item, expected[index]));
  }
  return Object.is(value, expected);
}

function applyOperator(value: unknown, operator: string, expected: unknown): boolean {
  switch (operator) {
    case "$eq": return Object.is(value, expected);
    case "$ne": return !Object.is(value, expected);
    case "$gt": return comparable(value, expected, (left, right) => left > right);
    case "$gte": return comparable(value, expected, (left, right) => left >= right);
    case "$in": return Array.isArray(expected) && expected.some((item) => Object.is(item, value));
    case "$lt": return comparable(value, expected, (left, right) => left < right);
    case "$lte": return comparable(value, expected, (left, right) => left <= right);
    case "$nin": return !Array.isArray(expected) || expected.every((item) => !Object.is(item, value));
    default: throw new Error(`unsupported filter operator: ${operator}`);
  }
}

function comparable(
  value: unknown,
  expected: unknown,
  compare: (left: number, right: number) => boolean
): boolean {
  try {
    return compare(Number(value), Number(expected));
  } catch {
    return false;
  }
}

const FILTER_OPERATORS = new Set(["$eq", "$gt", "$gte", "$in", "$lt", "$lte", "$ne", "$nin"]);

function matchesConditions(namespace: string[], conditions: MatchCondition[] | undefined): boolean {
  return conditions == null || conditions.every((condition) => {
    if (namespace.length < condition.path.length) return false;
    const actual = condition.matchType === "prefix" ? namespace : [...namespace].reverse();
    const pattern = condition.matchType === "prefix" ? condition.path : [...condition.path].reverse();
    return pattern.every((component, index) => component === "*" || actual[index] === component);
  });
}

function startsWithNamespace(namespace: readonly string[], prefix: readonly string[]): boolean {
  return prefix.length <= namespace.length && prefix.every((value, index) => namespace[index] === value);
}

function normalizePageNumber(value: number | undefined, fallback: number, name: string): number {
  const normalized = value ?? fallback;
  if (!Number.isSafeInteger(normalized) || normalized < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return normalized;
}

function compareNamespaces(left: readonly string[], right: readonly string[]): number {
  const count = Math.min(left.length, right.length);
  for (let index = 0; index < count; index += 1) {
    const compared = (left[index] ?? "").localeCompare(right[index] ?? "");
    if (compared !== 0) return compared;
  }
  return left.length - right.length;
}

function itemField(key: string): string {
  return `${ITEM_FIELD_PREFIX}${Buffer.from(key, "utf8").toString("base64url")}`;
}

function namespaceIdentity(namespace: readonly string[]): Buffer {
  return Buffer.concat(namespace.flatMap((component) => {
    const payload = Buffer.from(component, "utf8");
    return [uint64(payload.length), payload];
  }));
}

function catalogMember(namespace: readonly string[], key: string): Buffer {
  return Buffer.concat([
    ...namespace.map(orderedText),
    Buffer.from([0, 0]),
    orderedText(key)
  ]);
}

function decodeCatalogMember(value: unknown): { key: string; namespace: string[] } {
  if (!(typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array)) {
    throw new TypeError("FerricStore returned a non-binary LangGraph store locator");
  }
  const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
  const namespace: string[] = [];
  let offset = 0;
  while (true) {
    if (bytes[offset] === 0 && bytes[offset + 1] === 0) {
      offset += 2;
      break;
    }
    const decoded = decodeOrderedText(bytes, offset);
    namespace.push(decoded.value);
    offset = decoded.offset;
  }
  const key = decodeOrderedText(bytes, offset);
  if (key.offset !== bytes.length || namespace.length === 0) {
    throw new Error("invalid FerricStore LangGraph store locator");
  }
  return { key: key.value, namespace };
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
    if (byte == null) throw new Error("truncated FerricStore LangGraph store locator");
    offset += 1;
    if (byte !== 0) {
      output.push(byte);
      continue;
    }
    const escaped = bytes[offset];
    offset += 1;
    if (escaped === 0) return { offset, value: Buffer.from(output).toString("utf8") };
    if (escaped === 255) output.push(0);
    else throw new Error("invalid FerricStore LangGraph store locator escape");
  }
  throw new Error("unterminated FerricStore LangGraph store locator");
}

function uint64(value: number): Buffer {
  const result = Buffer.allocUnsafe(8);
  result.writeBigUInt64BE(BigInt(value));
  return result;
}

function sha256(value: Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function isArrayIndex(key: string, length: number): boolean {
  if (!/^(?:0|[1-9]\d*)$/u.test(key)) return false;
  const index = Number(key);
  return Number.isSafeInteger(index) && index >= 0 && index < length;
}
