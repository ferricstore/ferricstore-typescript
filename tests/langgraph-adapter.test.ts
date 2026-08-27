import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { Annotation, END, START, StateGraph, type Checkpoint, type CheckpointMetadata } from "@langchain/langgraph";
import { describe, expect, it } from "vitest";

import { FerricStoreSaver, FerricStoreStore } from "../src/langgraph.js";
import type { CommandArgument } from "../src/internal.js";
import { MemoryCommandClient } from "./agent-persistence-test-client.js";

describe("FerricStoreSaver", () => {
  it("persists checkpoints, parent chains, pending writes, listing, and deletion", async () => {
    const client = new MemoryCommandClient();
    const saver = new FerricStoreSaver(client);
    const baseConfig = { configurable: { checkpoint_ns: "agent", thread_id: "thread-1" } };
    const first = checkpoint("001", 1);
    const second = checkpoint("002", 2);
    const metadataOne = metadata(-1, "input", "acme");
    const metadataTwo = metadata(0, "loop", "acme");

    const firstConfig = await saver.put(baseConfig, first, metadataOne, {});
    await saver.putWrites(firstConfig, [["messages", { text: "first" }]], "task-b");
    await saver.putWrites(firstConfig, [["messages", { text: "ignored retry" }]], "task-b");
    await saver.putWrites(firstConfig, [["__error__", "initial"]], "task-a");
    await saver.putWrites(firstConfig, [["__error__", "replacement"]], "task-a");
    const secondConfig = await saver.put(firstConfig, second, metadataTwo, {});

    const exact = await saver.getTuple(firstConfig);
    expect(exact?.checkpoint).toEqual(first);
    expect(exact?.pendingWrites).toEqual([
      ["task-a", "__error__", "replacement"],
      ["task-b", "messages", { text: "first" }]
    ]);
    const latest = await saver.getTuple(baseConfig);
    expect(latest?.checkpoint).toEqual(second);
    expect(latest?.parentConfig).toEqual(firstConfig);

    const filtered = await collect(saver.list({}, { filter: { tenant: "acme" }, limit: 1 }));
    expect(filtered.map((entry) => entry.checkpoint.id)).toEqual(["002"]);
    const before = await collect(saver.list(baseConfig, { before: secondConfig }));
    expect(before.map((entry) => entry.checkpoint.id)).toEqual(["001"]);

    await saver.deleteThread("thread-1");
    expect(await saver.getTuple(baseConfig)).toBeUndefined();
    expect(await collect(saver.list({}))).toEqual([]);
  });

  it("runs through the real LangGraph.js compile and invoke contract", async () => {
    const client = new MemoryCommandClient();
    const saver = new FerricStoreSaver(client);
    const State = Annotation.Root({
      count: Annotation<number>({ reducer: (left, right) => left + right, default: () => 0 })
    });
    const graph = new StateGraph(State)
      .addNode("increment", () => ({ count: 1 }))
      .addEdge(START, "increment")
      .addEdge("increment", END)
      .compile({ checkpointer: saver });

    const config = { configurable: { thread_id: "compiled-graph" } };
    expect(await graph.invoke({ count: 2 }, config)).toEqual({ count: 3 });
    const persisted = await saver.getTuple(config);
    expect(persisted?.checkpoint.channel_values.count).toBe(3);
    expect((await collect(saver.list(config))).length).toBeGreaterThan(0);
  });

  it("applies a checkpoint namespace filter during global listing", async () => {
    const saver = new FerricStoreSaver(new MemoryCommandClient());
    await saver.put(
      { configurable: { checkpoint_ns: "wanted", thread_id: "thread-1" } },
      checkpoint("001", 1),
      metadata(0, "loop", "acme"),
      {}
    );
    await saver.put(
      { configurable: { checkpoint_ns: "other", thread_id: "thread-2" } },
      checkpoint("002", 2),
      metadata(0, "loop", "acme"),
      {}
    );

    const matches = await collect(saver.list({ configurable: { checkpoint_ns: "wanted" } }));
    expect(matches.map((entry) => {
      const namespace: unknown = entry.config.configurable?.checkpoint_ns;
      return typeof namespace === "string" ? namespace : undefined;
    })).toEqual(["wanted"]);
  });

  it("keeps a newer checkpoint when an expired writer finishes late", async () => {
    const storage = new MemoryCommandClient();
    const config = { configurable: { checkpoint_ns: "race", thread_id: "thread-race" } };
    await new FerricStoreSaver(storage).put(config, checkpoint("001", 1), metadata(0, "loop", "acme"), {});
    let commitStarted = false;
    const losingClient = {
      async command(...args: CommandArgument[]): Promise<unknown> {
        const command = typeof args[0] === "string" ? args[0].toUpperCase() : "";
        const key = typeof args[1] === "string" ? args[1] : "";
        if (command === "EXTEND" && commitStarted) return 0;
        if ((command === "SET" || command === "CAS") && key.includes(":checkpoint:") && !commitStarted) {
          commitStarted = true;
          await delay(60);
        }
        return await storage.command(...args);
      }
    };
    const options = { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 };
    const stale = new FerricStoreSaver(losingClient, options);
    const current = new FerricStoreSaver(storage, options);
    const staleWrite = stale.put(config, checkpoint("002", 2), metadata(1, "loop", "acme"), {})
      .then(() => undefined, (error: unknown) => error);

    while (!commitStarted) await delay(1);
    await delay(40);
    const exact = await current.put(config, checkpoint("002", 9), metadata(2, "loop", "acme"), {});

    expect(await staleWrite).toBeInstanceOf(Error);
    expect((await current.getTuple(exact))?.checkpoint.channel_values.count).toBe(9);
  });

  it("reads legacy hash checkpoints, migrates on write, and fences them on deletion", async () => {
    const client = new MemoryCommandClient();
    const saver = new FerricStoreSaver(client, { keyPrefix: "migration:checkpoint" });
    const threadId = "legacy-thread";
    const checkpointNs = "legacy-ns";
    const legacy = checkpoint("001", 1);
    const threadKey = legacyThreadKey("migration:checkpoint", threadId, checkpointNs);
    const record = {
      checkpoint: legacy,
      checkpointId: legacy.id,
      checkpointNs,
      formatVersion: 1,
      metadata: metadata(0, "loop", "acme"),
      threadId
    };
    const [type, data] = await saver.serde.dumpsTyped(record);
    await client.command(
      "HSET",
      threadKey,
      `checkpoint:${Buffer.from(legacy.id).toString("base64url")}`,
      typedSnapshot(type, data)
    );
    const exact = { configurable: { checkpoint_id: legacy.id, checkpoint_ns: checkpointNs, thread_id: threadId } };

    expect((await saver.getTuple(exact))?.checkpoint).toEqual(legacy);
    await saver.put(
      { configurable: { checkpoint_ns: checkpointNs, thread_id: threadId } },
      checkpoint("002", 2),
      metadata(1, "loop", "acme"),
      {}
    );
    expect((await saver.getTuple(exact))?.checkpoint).toEqual(legacy);
    await saver.deleteThread(threadId);
    expect(await saver.getTuple(exact)).toBeUndefined();
  });

  it("makes an in-flight pre-deletion checkpoint invisible and reports the lost epoch", async () => {
    const storage = new MemoryCommandClient();
    const config = { configurable: { checkpoint_ns: "delete-race", thread_id: "delete-race" } };
    const current = new FerricStoreSaver(storage, { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 });
    await current.put(config, checkpoint("001", 1), metadata(0, "loop", "acme"), {});
    let commitStarted = false;
    const losingClient = {
      async command(...args: CommandArgument[]): Promise<unknown> {
        const command = typeof args[0] === "string" ? args[0].toUpperCase() : "";
        const key = typeof args[1] === "string" ? args[1] : "";
        if (command === "EXTEND" && commitStarted) return 0;
        if (command === "SET" && key.includes(":checkpoint:") && !commitStarted) {
          commitStarted = true;
          await delay(60);
        }
        return await storage.command(...args);
      }
    };
    const stale = new FerricStoreSaver(losingClient, { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 });
    const staleWrite = stale.put(config, checkpoint("002", 2), metadata(1, "loop", "acme"), {})
      .then(() => undefined, (error: unknown) => error);

    while (!commitStarted) await delay(1);
    await delay(40);
    await current.deleteThread("delete-race");

    expect(await staleWrite).toBeInstanceOf(Error);
    expect(await current.getTuple(config)).toBeUndefined();
    expect(await collect(current.list({}))).toEqual([]);
  });
});

describe("FerricStoreStore", () => {
  it("implements ordered long-term memory, filters, namespaces, batching, and deletion", async () => {
    const client = new MemoryCommandClient();
    const store = new FerricStoreStore(client, { scanCount: 2 });
    await store.put(["users", "alice"], "profile", { active: true, score: 8, tags: ["one"] });
    await store.put(["users", "bob"], "profile", { active: false, score: 3, tags: ["two"] });
    await store.put(["users", "alice", "notes"], "n1", { active: true, score: 10 });

    expect((await store.get(["users", "alice"], "profile"))?.value.score).toBe(8);
    const matches = await store.search(["users"], {
      filter: { active: true, score: { $gte: 8 } },
      limit: 10
    });
    expect(matches.map((entry) => entry.key)).toEqual(["profile", "n1"]);
    expect((await store.search(["users"], { filter: { score: { $in: [3, 99] } } }))
      .map((entry) => entry.namespace)).toEqual([["users", "bob"]]);
    expect(await store.listNamespaces({ maxDepth: 2, prefix: ["users"] })).toEqual([
      ["users", "alice"],
      ["users", "bob"]
    ]);

    const batch = await store.batch([
      { key: "batch", namespace: ["users", "alice"], value: { active: true } },
      { key: "batch", namespace: ["users", "alice"] }
    ]);
    expect(batch[1]).toBeNull();
    expect((await store.get(["users", "alice"], "batch"))?.value).toEqual({ active: true });

    await store.delete(["users", "alice"], "profile");
    expect(await store.get(["users", "alice"], "profile")).toBeNull();
    await expect(store.search(["users"], { query: "semantic request" })).rejects.toThrow(/not configured/u);
    await expect(store.put(["users"], "invalid", { missing: undefined })).rejects.toThrow(/undefined/u);
  });

  it("keeps a newer store item when an expired writer finishes late", async () => {
    const storage = new MemoryCommandClient();
    const seed = new FerricStoreStore(storage);
    await seed.put(["users", "race"], "profile", { value: "base" });
    let commitStarted = false;
    const losingClient = {
      async command(...args: CommandArgument[]): Promise<unknown> {
        const command = typeof args[0] === "string" ? args[0].toUpperCase() : "";
        const key = typeof args[1] === "string" ? args[1] : "";
        if (command === "EXTEND" && commitStarted) return 0;
        if (command === "CAS" && key.endsWith(":atomic-item") && !commitStarted) {
          commitStarted = true;
          await delay(60);
        }
        return await storage.command(...args);
      }
    };
    const options = { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 };
    const stale = new FerricStoreStore(losingClient, options);
    const current = new FerricStoreStore(storage, options);
    const staleWrite = stale.put(["users", "race"], "profile", { value: "stale" })
      .then(() => undefined, (error: unknown) => error);

    while (!commitStarted) await delay(1);
    await delay(40);
    await current.put(["users", "race"], "profile", { value: "current" });

    expect(await staleWrite).toBeInstanceOf(Error);
    expect((await current.get(["users", "race"], "profile"))?.value).toEqual({ value: "current" });
  });

  it("reads and atomically migrates legacy hash store items", async () => {
    const client = new MemoryCommandClient();
    const keyPrefix = "migration:store";
    const namespace = ["users", "legacy"];
    const key = "profile";
    const createdAt = "2026-01-01T00:00:00.000Z";
    await client.command(
      "HSET",
      legacyNamespaceKey(keyPrefix, namespace),
      `item:${Buffer.from(key).toString("base64url")}`,
      Buffer.from(JSON.stringify({
        createdAt,
        formatVersion: 1,
        key,
        namespace,
        updatedAt: createdAt,
        value: { source: "legacy" }
      }))
    );
    const store = new FerricStoreStore(client, { keyPrefix });

    expect((await store.get(namespace, key))?.value).toEqual({ source: "legacy" });
    await store.put(namespace, key, { source: "atomic" });
    const migrated = await store.get(namespace, key);
    expect(migrated?.createdAt.toISOString()).toBe(createdAt);
    expect(migrated?.value).toEqual({ source: "atomic" });
    await store.delete(namespace, key);
    expect(await store.get(namespace, key)).toBeNull();
  });
});

function checkpoint(id: string, count: number): Checkpoint {
  return {
    channel_values: { count },
    channel_versions: { count },
    id,
    ts: new Date(2026, 0, count).toISOString(),
    v: 4,
    versions_seen: {}
  };
}

function metadata(
  step: number,
  source: CheckpointMetadata["source"],
  tenant: string
): CheckpointMetadata & { tenant: string } {
  return { parents: {}, source, step, tenant };
}

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const value of values) result.push(value);
  return result;
}

function legacyThreadKey(keyPrefix: string, threadId: string, checkpointNs: string): string {
  const digest = createHash("sha256").update(lengthPrefixed([threadId, checkpointNs])).digest("hex");
  return `${keyPrefix}:{lg:${digest}}:thread`;
}

function legacyNamespaceKey(keyPrefix: string, namespace: readonly string[]): string {
  const digest = createHash("sha256").update(lengthPrefixed(namespace)).digest("hex");
  return `${keyPrefix}:{lgs:${digest}}:namespace`;
}

function lengthPrefixed(values: readonly string[]): Buffer {
  return Buffer.concat(values.flatMap((value) => {
    const bytes = Buffer.from(value);
    const length = Buffer.allocUnsafe(8);
    length.writeBigUInt64BE(BigInt(bytes.length));
    return [length, bytes];
  }));
}

function typedSnapshot(type: string, data: string | Uint8Array): Buffer {
  const typeBytes = Buffer.from(type);
  const header = Buffer.allocUnsafe(2);
  header.writeUInt16BE(typeBytes.length);
  return Buffer.concat([header, typeBytes, Buffer.from(data)]);
}
