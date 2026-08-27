import { createHash, randomUUID } from "node:crypto";

import type {
  AgentInputItem,
  Session,
  SessionHistoryRewriteArgs,
  SessionHistoryRewriteAwareSession,
  SessionHistoryTransaction,
  SessionHistoryTransactionArgs,
  SessionHistoryTransactionAwareSession
} from "@openai/agents";

import {
  normalizeKeyPrefix,
  readAtomicValue,
  type FerricStoreCommandClient,
  type FerricStoreLockOptions,
  withMutationLocks
} from "./agent-persistence/durability.js";
import {
  cloneSnapshot,
  decodeSnapshot,
  encodeSnapshot,
  legacySnapshotDigest,
  snapshotDigest,
  snapshotsEqual
} from "./agent-persistence/snapshot.js";

const SESSION_FORMAT_VERSION = 1;
const SESSION_STATE_FIELD = "state";
const RECEIPT_DIGEST_VERSION = "v2:";

interface StoredSessionState {
  readonly formatVersion: typeof SESSION_FORMAT_VERSION;
  readonly items: AgentInputItem[];
  readonly operations: Record<string, string>;
  readonly sessionId: string;
}

export interface FerricStoreSessionOptions extends FerricStoreLockOptions {
  /** Existing conversation identifier. A random UUID is created when omitted. */
  sessionId?: string;
  /** Items used only when this session has not yet been persisted. */
  initialItems?: AgentInputItem[];
  /** FerricStore key prefix. Defaults to `openai:agents:session`. */
  keyPrefix?: string;
  /** Previous worker locales to accept when migrating unversioned operation receipts. */
  legacyReceiptLocales?: string[];
}

/**
 * Durable OpenAI Agents SDK conversation history backed by FerricStore.
 *
 * Renewable locks reduce contention, while compare-and-swap makes every state
 * commit safe even if a writer's lease expires in flight. History transactions
 * and their operation receipts are persisted in one atomic value, implementing
 * the SDK's retry-safe transaction capability in addition to its base Session
 * contract.
 */
export class FerricStoreSession implements
  Session,
  SessionHistoryRewriteAwareSession,
  SessionHistoryTransactionAwareSession {
  readonly client: FerricStoreCommandClient;
  readonly sessionId: string;
  readonly keyPrefix: string;
  private readonly initialItems: AgentInputItem[];
  private readonly lockOptions: FerricStoreLockOptions;
  private readonly legacyReceiptLocales: readonly string[];
  private readonly sessionKey: string;
  private readonly stateKey: string;
  private readonly lockKey: string;

  constructor(client: FerricStoreCommandClient, options: FerricStoreSessionOptions = {}) {
    this.client = client;
    this.sessionId = options.sessionId ?? randomUUID();
    if (typeof this.sessionId !== "string" || this.sessionId.trim().length === 0) {
      throw new TypeError("sessionId must be a non-empty string");
    }
    this.keyPrefix = normalizeKeyPrefix(options.keyPrefix ?? "openai:agents:session", "openai:agents:session");
    this.initialItems = snapshotItems(options.initialItems ?? [], "initialItems");
    this.lockOptions = {
      lockRetryMs: options.lockRetryMs,
      lockTtlMs: options.lockTtlMs,
      lockWaitMs: options.lockWaitMs
    };
    if (options.legacyReceiptLocales != null && !Array.isArray(options.legacyReceiptLocales)) {
      throw new TypeError("legacyReceiptLocales must be an array");
    }
    try {
      this.legacyReceiptLocales = Intl.getCanonicalLocales(options.legacyReceiptLocales ?? []);
    } catch (error) {
      throw new TypeError("legacyReceiptLocales contains an invalid locale", { cause: error });
    }
    const digest = createHash("sha256").update(this.sessionId, "utf8").digest("hex");
    this.sessionKey = `${this.keyPrefix}:{oais:${digest}}:session`;
    this.stateKey = `${this.sessionKey}:atomic-state`;
    this.lockKey = `${this.keyPrefix}:{oais:${digest}}:mutation-lock`;
  }

  async getSessionId(): Promise<string> {
    await this.mutate(async (state) => state);
    return this.sessionId;
  }

  async getItems(limit?: number): Promise<AgentInputItem[]> {
    if (limit != null && limit <= 0) return [];
    if (limit != null && !Number.isSafeInteger(limit)) {
      throw new TypeError("limit must be a safe integer");
    }
    const state = await this.readState();
    const items = limit == null ? state.items : state.items.slice(Math.max(state.items.length - limit, 0));
    return cloneSnapshot(items);
  }

  async addItems(items: AgentInputItem[]): Promise<void> {
    if (items.length === 0) return;
    const additions = snapshotItems(items, "items");
    await this.mutate(async (state) => ({
      ...state,
      items: [...state.items, ...additions]
    }));
  }

  async replaceHistoryWithCompaction(items: AgentInputItem[]): Promise<void> {
    const replacement = snapshotItems(items, "items");
    await this.mutate(async (state) => ({ ...state, items: replacement }));
  }

  async popItem(): Promise<AgentInputItem | undefined> {
    let popped: AgentInputItem | undefined;
    await this.mutate(async (state) => {
      popped = state.items.at(-1);
      return popped == null ? state : { ...state, items: state.items.slice(0, -1) };
    });
    return popped == null ? undefined : cloneSnapshot(popped);
  }

  async clearSession(): Promise<void> {
    await this.mutate(async (state) => ({ ...state, items: [], operations: {} }));
  }

  async applyHistoryMutations(args: SessionHistoryRewriteArgs): Promise<void> {
    if (args == null || !Array.isArray(args.mutations)) {
      throw new TypeError("session history mutations are invalid");
    }
    if (args.mutations.length === 0) return;
    const mutations = cloneSnapshot(args.mutations);
    await this.mutate(async (state) => {
      let items = cloneSnapshot(state.items);
      for (const mutation of mutations) {
        if (mutation.type !== "replace_function_call") {
          throw new TypeError("unsupported session history mutation");
        }
        const replacement = snapshotItem(mutation.replacement, "mutation replacement");
        let keptReplacement = false;
        const next: AgentInputItem[] = [];
        for (const item of items) {
          if (item.type === "function_call" && item.callId === mutation.callId) {
            if (!keptReplacement) {
              next.push(replacement);
              keptReplacement = true;
            }
          } else {
            next.push(item);
          }
        }
        items = next;
      }
      return { ...state, items };
    });
  }

  async applyHistoryTransaction(args: SessionHistoryTransactionArgs): Promise<void> {
    const { operationId, transaction } = snapshotTransactionArgs(args);
    const digest = `${RECEIPT_DIGEST_VERSION}${snapshotDigest(transaction)}`;
    const legacyDigests = new Set([
      snapshotDigest(transaction),
      legacySnapshotDigest(transaction),
      ...this.legacyReceiptLocales.map((locale) => legacySnapshotDigest(transaction, locale))
    ]);
    await this.mutate(async (state) => {
      const existing = Object.getOwnPropertyDescriptor(state.operations, operationId)?.value as unknown;
      if (existing != null) {
        if (typeof existing !== "string") throw new Error("corrupt session history operation receipt");
        if (existing === digest) return state;
        if (!legacyDigests.has(existing)) {
          throw new Error("session history operation was already applied with a different transaction");
        }
        return { ...state, operations: { ...state.operations, [operationId]: digest } };
      }

      let items: AgentInputItem[];
      if (transaction.type === "append_items") {
        items = [...state.items, ...transaction.items];
      } else {
        const suffixStart = state.items.length - transaction.expectedSuffix.length;
        const actualSuffix = suffixStart < 0 ? [] : state.items.slice(suffixStart);
        if (suffixStart < 0 || !snapshotsEqual(actualSuffix, transaction.expectedSuffix)) {
          throw new Error("session history suffix no longer matches the transaction precondition");
        }
        items = [...state.items.slice(0, suffixStart), ...transaction.replacement];
      }
      return {
        ...state,
        items,
        operations: { ...state.operations, [operationId]: digest }
      };
    });
  }

  private async mutate(
    operation: (state: StoredSessionState) => Promise<StoredSessionState>
  ): Promise<void> {
    await withMutationLocks(this.client, [this.lockKey], async (lease) => {
      for (let attempt = 0; attempt < 8; attempt += 1) {
        lease.assertOwned();
        const snapshot = await this.readMutationState();
        const next = await operation(snapshot.state);
        if (await lease.compareAndSet(this.stateKey, snapshot.expected, encodeSnapshot(next))) return;
      }
      throw new Error("concurrent FerricStore OpenAI Agents session mutation did not converge");
    }, this.lockOptions);
  }

  private async readState(): Promise<StoredSessionState> {
    return (await this.readMutationState()).state;
  }

  private async readMutationState(): Promise<{ expected: Buffer | undefined; state: StoredSessionState }> {
    const expected = await readAtomicValue(this.client, this.stateKey, "OpenAI Agents atomic session state");
    const value = expected ?? await this.client.command("HGET", this.sessionKey, SESSION_STATE_FIELD);
    if (value == null) return { expected, state: this.emptyState() };
    const state = decodeSnapshot<StoredSessionState>(value, "OpenAI Agents session state");
    if (
      state == null ||
      typeof state !== "object" ||
      state.formatVersion !== SESSION_FORMAT_VERSION ||
      state.sessionId !== this.sessionId ||
      !Array.isArray(state.items) ||
      state.operations == null ||
      typeof state.operations !== "object" ||
      Array.isArray(state.operations) ||
      Object.values(state.operations).some((digest) => typeof digest !== "string")
    ) {
      throw new Error("unsupported or corrupt FerricStore OpenAI Agents session state");
    }
    return { expected, state };
  }

  private emptyState(): StoredSessionState {
    return {
      formatVersion: SESSION_FORMAT_VERSION,
      items: cloneSnapshot(this.initialItems),
      operations: {},
      sessionId: this.sessionId
    };
  }
}

function snapshotTransactionArgs(args: SessionHistoryTransactionArgs): {
  operationId: string;
  transaction: SessionHistoryTransaction;
} {
  if (args == null || typeof args !== "object") throw new TypeError("session history transaction is invalid");
  if (typeof args.operationId !== "string" || args.operationId.trim().length === 0) {
    throw new TypeError("session history transaction operationId must be a non-empty string");
  }
  const transaction = cloneSnapshot(args.transaction);
  if (transaction == null || typeof transaction !== "object") {
    throw new TypeError("session history transaction must be an object");
  }
  if (transaction.type === "append_items") {
    if (!Array.isArray(transaction.items)) throw new TypeError("session history append items are invalid");
    return {
      operationId: args.operationId,
      transaction: { type: "append_items", items: snapshotItems(transaction.items, "transaction items") }
    };
  }
  if (transaction.type === "replace_suffix") {
    if (!Array.isArray(transaction.expectedSuffix) || !Array.isArray(transaction.replacement)) {
      throw new TypeError("session history suffix transaction is invalid");
    }
    return {
      operationId: args.operationId,
      transaction: {
        type: "replace_suffix",
        expectedSuffix: snapshotItems(transaction.expectedSuffix, "transaction expectedSuffix"),
        replacement: snapshotItems(transaction.replacement, "transaction replacement")
      }
    };
  }
  throw new TypeError("unsupported session history transaction type");
}

function snapshotItems(items: AgentInputItem[], name: string): AgentInputItem[] {
  if (!Array.isArray(items)) throw new TypeError(`${name} must be an array`);
  return items.map((item) => snapshotItem(item, name));
}

function snapshotItem(item: AgentInputItem, name: string): AgentInputItem {
  if (item == null || typeof item !== "object" || Array.isArray(item)) {
    throw new TypeError(`${name} contains an invalid agent item`);
  }
  return cloneSnapshot(item);
}

export type {
  AgentInputItem,
  Session,
  SessionHistoryRewriteAwareSession,
  SessionHistoryTransactionAwareSession
} from "@openai/agents";
