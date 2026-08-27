import { setTimeout as delay } from "node:timers/promises";

import { describe, expect, it } from "vitest";

import { withMutationLocks } from "../src/agent-persistence/durability.js";
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
});
