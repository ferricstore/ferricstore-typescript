import { setTimeout as delay } from "node:timers/promises";

import { describe, expect, it } from "vitest";

import { withMutationLocks } from "../src/agent-persistence/durability.js";
import type { CommandArgument } from "../src/internal.js";
import { MemoryCommandClient } from "./agent-persistence-test-client.js";

describe("agent persistence mutation locks", () => {
  it("renews ownership during long mutations and serializes contenders", async () => {
    const client = new MemoryCommandClient();
    const events: string[] = [];
    const first = withMutationLocks(client, ["lock"], async () => {
      events.push("first:start");
      await delay(45);
      events.push("first:end");
    }, { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 });
    await delay(1);
    const second = withMutationLocks(client, ["lock"], async () => {
      events.push("second:start");
      events.push("second:end");
    }, { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 });

    await Promise.all([first, second]);
    expect(events).toEqual(["first:start", "first:end", "second:start", "second:end"]);
    expect(client.calls.some(([command]) => command === "EXTEND")).toBe(true);
  });

  it("rejects non-additive publications from mutation leases", async () => {
    const client = new MemoryCommandClient();
    await expect(withMutationLocks(client, ["lock"], async (lease) => {
      await lease.publish("HSET", "state", "value", Buffer.from("unsafe"));
    }, { lockRetryMs: 2, lockTtlMs: 30 })).rejects.toThrow(/add-only/u);
    await expect(withMutationLocks(client, ["lock"], async (lease) => {
      await lease.publish("ZADD", "index", 1, "member");
    }, { lockRetryMs: 2, lockTtlMs: 30 })).rejects.toThrow(/zero-score/u);
  });

  it("rejects an in-flight stale CAS after another owner commits", async () => {
    const storage = new MemoryCommandClient();
    await storage.command("SET", "state", Buffer.from("base"));
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
    const options = { lockRetryMs: 2, lockTtlMs: 30, lockWaitMs: 500 };
    const first = withMutationLocks(losingClient, ["lock"], async (lease) => {
      const committed = await lease.compareAndSet("state", Buffer.from("base"), Buffer.from("stale"));
      if (!committed) throw new Error("stale CAS was rejected");
    }, options).then(() => undefined, (error: unknown) => error);

    while (!commitStarted) await delay(1);
    await delay(40);
    await withMutationLocks(storage, ["lock"], async (lease) => {
      expect(await lease.compareAndSet("state", Buffer.from("base"), Buffer.from("current"))).toBe(true);
    }, options);

    expect(await first).toBeInstanceOf(Error);
    expect(await storage.command("GET", "state")).toEqual(Buffer.from("current"));
  });
});
