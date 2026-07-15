import { describe, expect, it } from "vitest";
import { FerricStoreClient, QueueClient, fail, retry } from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { sleep, type CommandArgument } from "../src/internal.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Queue batching and asynchronous completion", () => {
  it("keeps synchronous completion batches at one in flight when a server over-returns claims", async () => {
    let activeCompletions = 0;
    let completionCalls = 0;
    let maxActiveCompletions = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          return Array.from({ length: 5 }, (_, index) => [
            `email-${index + 1}`,
            "tenant-a",
            Buffer.from(`lease-${index + 1}`),
            index + 1
          ]);
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          completionCalls += 1;
          activeCompletions += 1;
          maxActiveCompletions = Math.max(maxActiveCompletions, activeCompletions);
          await sleep(10);
          activeCompletions -= 1;
          return Buffer.from("OK");
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 0,
      leaseRenewal: false,
      worker: "worker-1"
    }).runBatchOnce(() => undefined);

    expect(result.completed).toBe(5);
    expect(completionCalls).toBe(5);
    expect(maxActiveCompletions).toBe(1);
  });

  it.each([0, 1])(
    "attempts every over-returned completion chunk after an earlier failure at async depth %i",
    async (completeAsyncDepth) => {
      const failure = new Error("first completion failed");
      const attempted: string[] = [];
      const executor: CommandExecutor = {
        async executeCommand(...args: CommandArgument[]): Promise<unknown> {
          if (args[0] === "FLOW.CLAIM_DUE") {
            return Array.from({ length: 3 }, (_, index) => [
              `email-${index + 1}`,
              "tenant-a",
              Buffer.from(`lease-${index + 1}`),
              index + 1
            ]);
          }
          if (args[0] === "FLOW.COMPLETE_MANY") {
            const itemsIndex = args.indexOf("ITEMS");
            const rawId = args[itemsIndex + 1];
            const id = Buffer.isBuffer(rawId) ? rawId.toString("utf8") : rawId;
            if (typeof id !== "string") throw new Error("completion did not contain an id");
            attempted.push(id);
            if (id === "email-1") throw failure;
          }
          return Buffer.from("OK");
        }
      };
      const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

      await expect(queue.worker({
        batchSize: 1,
        claimPayload: false,
        completeAsyncDepth,
        leaseRenewal: false,
        worker: "worker-1"
      }).runBatchOnce(() => undefined)).rejects.toMatchObject({
        cause: failure,
        completed: 2
      });

      expect(attempted).toEqual(["email-1", "email-2", "email-3"]);
    }
  );

  it("normalizes non-finite completion depth without unbounded completion requests", async () => {
    let activeCompletions = 0;
    let maxActiveCompletions = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          return Array.from({ length: 5 }, (_, index) => [
            `email-${index + 1}`,
            "tenant-a",
            Buffer.from(`lease-${index + 1}`),
            index + 1
          ]);
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          activeCompletions += 1;
          maxActiveCompletions = Math.max(maxActiveCompletions, activeCompletions);
          await sleep(10);
          activeCompletions -= 1;
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: Number.POSITIVE_INFINITY,
      leaseRenewal: false,
      worker: "worker-1"
    });

    const result = await worker.runBatchOnce(() => undefined);
    const flushed = await worker.flush();

    expect(result.completed + flushed).toBe(5);
    expect(maxActiveCompletions).toBe(1);
  });

  it("refills asynchronous completion capacity when any slot settles", async () => {
    const completionIds: string[] = [];
    const releases = new Map<string, () => void>();
    let claimed = false;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          if (claimed) return [];
          claimed = true;
          return Array.from({ length: 3 }, (_unused, index) => [
            `email-${index + 1}`,
            "tenant-a",
            Buffer.from(`lease-${index + 1}`),
            index + 1
          ]);
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          const itemsIndex = args.indexOf("ITEMS");
          const rawId = args[itemsIndex + 1];
          const id = Buffer.isBuffer(rawId) ? rawId.toString("utf8") : rawId;
          if (typeof id !== "string") throw new Error("completion did not contain an id");
          completionIds.push(id);
          if (id !== "email-3") {
            await new Promise<void>((resolve) => releases.set(id, resolve));
          }
          return Buffer.from("OK");
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const operation = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      leaseRenewal: false,
      worker: "worker-1"
    }).runBatchOnce(() => undefined);

    while (completionIds.length < 2) await new Promise((resolve) => setImmediate(resolve));
    releases.get("email-1")?.();
    for (let attempt = 0; attempt < 20 && completionIds.length < 3; attempt += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    const refilledBeforeSlowSibling = completionIds.length === 3;
    releases.get("email-2")?.();

    await expect(operation).resolves.toMatchObject({ claimed: 3 });
    expect(refilledBeforeSlowSibling).toBe(true);
    expect(completionIds).toEqual(["email-1", "email-2", "email-3"]);
  });

  it("runs queue batch handlers with one handler call and one completion batch", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const seen: string[][] = [];

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      worker: "worker-1"
    }).runBatchOnce((jobs) => {
      seen.push(jobs.map((job) => job.id));
    });

    expect(seen).toEqual([["email-1", "email-2"]]);
    expect(result).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });
    expect(executor.calls).toHaveLength(2);
    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE_MANY");
  });

  it("applies one batched retry outcome for every job from a batch handler", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      leaseRenewal: false,
      worker: "worker-1"
    }).runBatchOnce(() => retry({ error: "try later", runAtMs: 500 }));

    expect(result).toEqual({ claimed: 2, completed: 0, failed: 0, retried: 2 });
    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.RETRY_MANY"
    ]);
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "ERROR", Buffer.from("try later"),
      "RUN_AT", 500,
      "INDEPENDENT", "true",
      "RETURN", "OK_ON_SUCCESS",
      "ITEMS",
      Buffer.from("email-1"), Buffer.from("lease-1"), 1,
      Buffer.from("email-2"), Buffer.from("lease-2"), 2
    ]));
  });

  it("applies batch exception policy with one independent terminal request", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      exceptionPolicy: "fail",
      leaseRenewal: false,
      worker: "worker-1"
    }).runBatchOnce(() => {
      throw new Error("batch failed");
    });

    expect(result).toEqual({ claimed: 2, completed: 0, failed: 2, retried: 0 });
    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.FAIL_MANY"
    ]);
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "INDEPENDENT", "true",
      "RETURN", "OK_ON_SUCCESS"
    ]));
  });

  it("surfaces a per-item error after submitting the whole batch outcome", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      [
        [Buffer.from("error"), Buffer.from("ERR stale flow lease")],
        [Buffer.from("ok"), Buffer.from("OK")]
      ]
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(queue.worker({
      batchSize: 2,
      claimPayload: false,
      leaseRenewal: false,
      worker: "worker-1"
    }).runBatchOnce(() => fail({ error: "failed" }))).rejects.toThrow("stale flow lease");

    expect(executor.calls.filter((call) => call[0] === "FLOW.FAIL_MANY")).toHaveLength(1);
    expect(executor.calls.some((call) => call[0] === "FLOW.FAIL")).toBe(false);
  });

  it("rejects non-OK per-item retry and fail batch responses", async () => {
    for (const outcome of [retry({ error: "later" }), fail({ error: "failed" })]) {
      const executor = new FakeExecutor([
        [
          ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
          ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
        ],
        [false, null]
      ]);
      const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

      await expect(queue.worker({
        batchSize: 2,
        claimPayload: false,
        leaseRenewal: false,
        worker: "worker-1"
      }).runBatchOnce(() => outcome)).rejects.toThrow("unexpected per-item result");
    }
  });

  it("drains async queue completions with explicit flush", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 2,
      completed: 0
    });
    await expect(worker.flush()).resolves.toBe(2);
  });

  it("scans a blocking async-completion drain linearly", async () => {
    const queue = new QueueClient(new FerricStoreClient(new FakeExecutor())).queue("email");
    const worker = queue.worker({ worker: "worker-1" });
    const internals = worker as unknown as {
      pendingCompletions: {
        drain(block: boolean): Promise<number>;
        pending: {
          readonly done: boolean;
          failed: boolean;
          promise: Promise<number>;
          value: number;
        }[];
      };
    };
    const count = 32;
    let doneReads = 0;
    const releases: (() => void)[] = [];
    for (let index = 0; index < count; index += 1) {
      let done = false;
      let resolve: ((value: number) => void) | undefined;
      const promise = new Promise<number>((next) => {
        resolve = next;
      });
      releases.push(() => {
        done = true;
        resolve?.(1);
      });
      internals.pendingCompletions.pending.push({
        get done(): boolean {
          doneReads += 1;
          return done;
        },
        failed: false,
        promise,
        value: 1
      });
    }

    const draining = internals.pendingCompletions.drain(true);
    await new Promise((resolve) => setImmediate(resolve));
    for (const release of releases) {
      release();
      await new Promise((resolve) => setImmediate(resolve));
    }

    await expect(draining).resolves.toBe(count);
    expect(doneReads).toBeLessThanOrEqual(count * 2);
  });

  it("drains pending async completions before claiming more jobs", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK"),
      Buffer.from("OK"),
      []
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 2,
      completed: 0
    });
    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 0,
      completed: 2
    });

    expect(executor.calls[0]?.[0]).toBe("FLOW.CLAIM_DUE");
    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE_MANY");
    expect(executor.calls[2]?.[0]).toBe("FLOW.COMPLETE_MANY");
    expect(executor.calls[3]?.[0]).toBe("FLOW.CLAIM_DUE");
  });

});
