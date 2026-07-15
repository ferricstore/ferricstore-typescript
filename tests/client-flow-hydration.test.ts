import { describe, expect, it } from "vitest";

import { ClaimHydrationError, FerricStoreClient } from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient legacy claim hydration", () => {
  it("stops scheduling after failure and waits for active siblings", async () => {
    const failure = new Error("legacy hydration failed");
    const compact = Array.from({ length: 5 }, (_, index) => [
      `order-${index}`,
      "tenant-a",
      Buffer.from(`lease-${index}`),
      index + 1
    ]);
    const started: string[] = [];
    let releaseSecond: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    let secondFinished = false;
    const secondGate = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const secondStarted = new Promise<void>((resolve) => {
      markSecondStarted = resolve;
    });
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") return compact;
        if (args[0] !== "FLOW.GET" || typeof args[1] !== "string") {
          throw new Error("unexpected command");
        }
        started.push(args[1]);
        if (args[1] === "order-0") throw failure;
        if (args[1] === "order-1") {
          markSecondStarted?.();
          await secondGate;
          secondFinished = true;
        }
        return new Map<unknown, unknown>([
          ["id", args[1]],
          ["type", "order"],
          ["state", "running"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 1],
          ["version", 2]
        ]);
      }
    };
    const client = new FerricStoreClient(executor, {
      legacyClaimHydrationConcurrency: 2
    });
    const operation = client.claimDue("order", {
      states: ["created", "charged"],
      worker: "worker-1"
    });
    let settled = false;
    void operation.then(
      () => { settled = true; },
      () => { settled = true; }
    );

    await secondStarted;
    try {
      await new Promise((resolve) => setImmediate(resolve));
      expect(started).toEqual(["order-0", "order-1"]);
      expect(settled).toBe(false);
    } finally {
      releaseSecond?.();
    }
    let caught: unknown;
    try {
      await operation;
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ClaimHydrationError);
    expect(caught).toMatchObject({
      claimed: compact.map((item) => ({
        fencingToken: item[3],
        id: item[0],
        leaseToken: item[2],
        partitionKey: item[1]
      })),
      failedIndex: 0,
      hydratedItems: [{
        index: 1,
        record: {
          fencingToken: 1,
          id: "order-1",
          leaseToken: Buffer.from("lease"),
          state: "running",
          type: "order",
          version: 2
        }
      }]
    });
    expect((caught as Error).cause).toBe(failure);
    expect(secondFinished).toBe(true);
  });

  it("rejects invalid concurrency", () => {
    expect(() => new FerricStoreClient(new FakeExecutor(), {
      legacyClaimHydrationConcurrency: 0
    })).toThrow(/legacyClaimHydrationConcurrency.*positive safe integer/i);
  });

  it("uses full-record claims when compact callers request payloads or named values", async () => {
    const claimed = new Map<unknown, unknown>([
      ["id", "order-1"],
      ["type", "order"],
      ["state", "running"],
      ["partition_key", "tenant-a"],
      ["lease_token", Buffer.from("lease")],
      ["fencing_token", 7],
      ["version", 2],
      ["payload", Buffer.from("payload")],
      ["values", new Map([["profile", Buffer.from("profile")]])]
    ]);
    const executor = new FakeExecutor([[claimed], [claimed]]);
    const client = new FerricStoreClient(executor);

    const [job] = await client.claimDue("order", {
      jobOnly: true,
      payload: true,
      values: ["profile"],
      worker: "worker-1"
    });
    const [reclaimed] = await client.reclaim("order", {
      jobOnly: true,
      payload: false,
      values: ["profile"],
      worker: "worker-1"
    });

    expect(job).toMatchObject({ id: "order-1", payload: Buffer.from("payload") });
    expect(reclaimed).toMatchObject({ id: "order-1", values: { profile: Buffer.from("profile") } });
    expect(executor.calls[0]).not.toContain("RETURN");
    expect(executor.calls[0]).toContain("PAYLOAD");
    expect(executor.calls[1]).not.toContain("RETURN");
    expect(executor.calls[1]).toContain("VALUE");
  });
});
