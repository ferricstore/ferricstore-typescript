import { Annotation, END, START, StateGraph, type Checkpoint, type CheckpointMetadata } from "@langchain/langgraph";
import { describe, expect, it } from "vitest";

import { FerricStoreSaver, FerricStoreStore } from "../src/langgraph.js";
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
