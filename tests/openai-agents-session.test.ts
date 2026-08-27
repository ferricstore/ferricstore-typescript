import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { describe, expect, it } from "vitest";
import type { AgentInputItem } from "@openai/agents";

import { encodeSnapshot, legacySnapshotDigest } from "../src/agent-persistence/snapshot.js";
import { FerricStoreSession } from "../src/openai-agents.js";
import type { CommandArgument } from "../src/internal.js";
import { MemoryCommandClient } from "./agent-persistence-test-client.js";

const item = (value: string): AgentInputItem => ({ role: "user", content: value });

describe("FerricStoreSession", () => {
  it("persists complete Session history and supports compaction operations", async () => {
    const client = new MemoryCommandClient();
    const session = new FerricStoreSession(client, {
      initialItems: [item("initial")],
      sessionId: "conversation-1"
    });

    expect(await session.getSessionId()).toBe("conversation-1");
    expect(await session.getItems(-1)).toEqual([]);
    await session.addItems([item("one"), item("two")]);
    expect(await session.getItems(2)).toEqual([item("one"), item("two")]);

    const reopened = new FerricStoreSession(client, { sessionId: "conversation-1" });
    expect(await reopened.getItems()).toEqual([item("initial"), item("one"), item("two")]);
    expect(await reopened.popItem()).toEqual(item("two"));
    await reopened.replaceHistoryWithCompaction([item("summary"), item("one")]);
    expect(await reopened.getItems()).toEqual([item("summary"), item("one")]);

    await reopened.clearSession();
    expect(await reopened.getItems()).toEqual([]);
  });

  it("applies history rewrites and atomic idempotent transactions", async () => {
    const client = new MemoryCommandClient();
    const session = new FerricStoreSession(client, { sessionId: "conversation-2" });
    const original: Extract<AgentInputItem, { type: "function_call" }> = {
      arguments: "{}",
      callId: "call-1",
      name: "lookup",
      type: "function_call"
    };
    const replacement: Extract<AgentInputItem, { type: "function_call" }> = {
      ...original,
      name: "safe_lookup"
    };
    await session.addItems([original, original]);
    await session.applyHistoryMutations({
      mutations: [{ callId: "call-1", replacement, type: "replace_function_call" }]
    });
    expect(await session.getItems()).toEqual([replacement]);

    const append = {
      operationId: "turn-1",
      transaction: { items: [item("done")], type: "append_items" as const }
    };
    await session.applyHistoryTransaction(append);
    await session.applyHistoryTransaction(append);
    expect(await session.getItems()).toEqual([replacement, item("done")]);
    await expect(session.applyHistoryTransaction({
      operationId: "turn-1",
      transaction: { items: [item("different")], type: "append_items" }
    })).rejects.toThrow(/different transaction/u);

    await session.applyHistoryTransaction({
      operationId: "rewrite-1",
      transaction: {
        expectedSuffix: [item("done")],
        replacement: [item("finished")],
        type: "replace_suffix"
      }
    });
    expect(await session.getItems()).toEqual([replacement, item("finished")]);
    await expect(session.applyHistoryTransaction({
      operationId: "rewrite-2",
      transaction: {
        expectedSuffix: [item("missing")],
        replacement: [],
        type: "replace_suffix"
      }
    })).rejects.toThrow(/suffix no longer matches/u);
  });

  it("prevents lost updates across adapters and rejects unsupported snapshots before mutation", async () => {
    const client = new MemoryCommandClient();
    const first = new FerricStoreSession(client, { sessionId: "concurrent" });
    const second = new FerricStoreSession(client, { sessionId: "concurrent" });
    await Promise.all([first.addItems([item("one")]), second.addItems([item("two")])]);
    expect(await first.getItems()).toEqual([item("one"), item("two")]);

    const invalid = { role: "user" } as Record<string, unknown>;
    Object.defineProperty(invalid, "content", { enumerable: true, get: () => "unsafe" });
    await expect(first.addItems([invalid as AgentInputItem])).rejects.toThrow(/unsupported property/u);
    expect(await first.getItems()).toEqual([item("one"), item("two")]);

    await first.applyHistoryTransaction({
      operationId: "__proto__",
      transaction: { items: [item("safe")], type: "append_items" }
    });
    await first.applyHistoryTransaction({
      operationId: "__proto__",
      transaction: { items: [item("safe")], type: "append_items" }
    });
    expect(await first.getItems()).toEqual([item("one"), item("two"), item("safe")]);
  });

  it("accepts legacy locale-ordered transaction receipts during migration", async () => {
    const client = new MemoryCommandClient();
    const sessionId = "legacy-receipt";
    const legacyItem = {
      content: "legacy",
      role: "user",
      type: "message",
      z: 1,
      "ä": 2
    } as unknown as AgentInputItem;
    const transaction = { items: [legacyItem], type: "append_items" as const };
    const sessionDigest = createHash("sha256").update(sessionId, "utf8").digest("hex");
    await client.command(
      "HSET",
      `openai:agents:session:{oais:${sessionDigest}}:session`,
      "state",
      encodeSnapshot({
        formatVersion: 1,
        items: [legacyItem],
        operations: { turn: legacySnapshotDigest(transaction, "sv") },
        sessionId
      })
    );

    const session = new FerricStoreSession(client, { legacyReceiptLocales: ["sv"], sessionId });
    await expect(session.applyHistoryTransaction({ operationId: "turn", transaction })).resolves.toBeUndefined();
    expect(await session.getItems()).toEqual([legacyItem]);
  });

  it("does not let an expired session writer overwrite a newer CAS state", async () => {
    const storage = new MemoryCommandClient();
    const seed = new FerricStoreSession(storage, { sessionId: "session-race" });
    await seed.addItems([item("base")]);
    let commitStarted = false;
    const losingClient = {
      async command(...args: CommandArgument[]): Promise<unknown> {
        const command = typeof args[0] === "string" ? args[0].toUpperCase() : "";
        if (command === "EXTEND" && commitStarted) return 0;
        if (command === "CAS" && !commitStarted) {
          commitStarted = true;
          await delay(60);
        }
        return await storage.command(...args);
      }
    };
    const lockOptions = { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 };
    const stale = new FerricStoreSession(losingClient, { ...lockOptions, sessionId: "session-race" });
    const current = new FerricStoreSession(storage, { ...lockOptions, sessionId: "session-race" });
    const staleWrite = stale.addItems([item("stale")]).then(
      () => undefined,
      (error: unknown) => error
    );

    while (!commitStarted) await delay(1);
    await delay(40);
    await current.addItems([item("current")]);

    expect(await staleWrite).toBeInstanceOf(Error);
    expect(await current.getItems()).toEqual([item("base"), item("current")]);
  });
});
