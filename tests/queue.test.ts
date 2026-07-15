import { getEventListeners } from "node:events";
import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  JsonCodec,
  QueueClient,
  complete,
  fail,
  retry
} from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { sleep, type Command, type CommandArgument } from "../src/internal.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Queue", () => {
  it("rejects an invalid runtime worker exception policy before claiming", () => {
    const executor = new FakeExecutor([[flow("email-1", 1)]]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    expect(() => queue.worker({
      exceptionPolicy: "ignore" as never,
      worker: "worker-1"
    })).toThrow("exceptionPolicy must be 'retry', 'fail', or 'raise'");
    expect(executor.calls).toEqual([]);
  });

  it("removes abort listeners after worker sleeps complete", async () => {
    const controller = new AbortController();

    await Promise.all(Array.from({ length: 20 }, async () => await sleep(1, controller.signal)));

    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("enqueues a durable queue item through FLOW.CREATE", async () => {
    const executor = new FakeExecutor();
    const queue = new QueueClient(new FerricStoreClient(executor, { codec: new JsonCodec() })).queue("email");

    await queue.enqueue("email-1", {
      idempotent: true,
      nowMs: 100,
      payload: { to: "user@example.com" }
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.CREATE",
      "email-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      100,
      "PAYLOAD",
      Buffer.from('{"to":"user@example.com"}'),
      "RUN_AT",
      100,
      "IDEMPOTENT",
      "true"
    ]);
  });

  it("can retry and fail jobs from handler outcomes", async () => {
    const executor = new FakeExecutor([
      [
        flow("email-1", 1),
        flow("email-2", 2)
      ],
      Buffer.from("OK"),
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({ batchSize: 2, worker: "worker-1" }).runOnce((job) => {
      if (job.id === "email-1") {
        return retry({ error: "rate limited" });
      }
      return fail({ error: "bad address" });
    });

    expect(result).toEqual({ claimed: 2, completed: 0, failed: 1, retried: 1 });
    expect(executor.calls.at(1)?.[0]).toBe("FLOW.RETRY");
    expect(executor.calls.at(2)?.[0]).toBe("FLOW.FAIL");
  });

  it("does not claim a partition when claim credit is zero", async () => {
    const executor = new FakeExecutor([[]]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({ batchSize: 10, worker: "worker-1" });

    await expect(worker.runBatchOnceForPartitionKeys(
      () => undefined,
      ["tenant-a"],
      { claimCredit: 0 }
    )).resolves.toEqual({ claimed: 0, completed: 0, failed: 0, retried: 0 });
    expect(executor.calls).toEqual([]);
  });

  it.each([
    ["omits", undefined, false],
    ["includes", true, true]
  ])("%s handler stack traces in persisted errors when configured", async (_name, includeErrorStack, hasStack) => {
    const executor = new FakeExecutor([[flow("email-1", 1)], Buffer.from("OK")]);
    const queue = new QueueClient(
      new FerricStoreClient(executor, { codec: new JsonCodec() })
    ).queue("email");
    const handlerError = new Error("handler failed");
    handlerError.stack = "SENSITIVE_STACK_PATH";

    await queue.worker({
      includeErrorStack,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => {
      throw handlerError;
    });

    const retryCall = executor.calls.find((call) => call[0] === "FLOW.RETRY");
    const errorIndex = retryCall?.indexOf("ERROR") ?? -1;
    const errorPayload = JSON.parse((retryCall?.[errorIndex + 1] as Buffer).toString("utf8")) as Record<string, unknown>;
    expect(errorPayload).toMatchObject({ message: "handler failed", name: "Error" });
    expect(Object.hasOwn(errorPayload, "stack")).toBe(hasStack);
  });

  it("persists thrown handler errors with the default raw codec", async () => {
    const executor = new FakeExecutor([[flow("email-raw-error", 1)], Buffer.from("OK")]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => {
      throw new Error("raw handler failed");
    });

    expect(result).toEqual({ claimed: 1, completed: 0, failed: 0, retried: 1 });
    const retryCall = executor.calls.find((call) => call[0] === "FLOW.RETRY");
    const errorIndex = retryCall?.indexOf("ERROR") ?? -1;
    expect(JSON.parse((retryCall?.[errorIndex + 1] as Buffer).toString("utf8"))).toEqual({
      message: "raw handler failed",
      name: "Error"
    });
  });

  it("uses workers as handler concurrency and limits per-job claim credit", async () => {
    const executor = new FakeExecutor([
      [flow("email-1", 1), flow("email-2", 2)],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    let active = 0;
    let maxActive = 0;

    await queue.worker({
      batchSize: 10,
      leaseRenewal: false,
      worker: "worker-1",
      workers: 2
    }).runOnce(async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active--;
    });

    const limitIndex = executor.calls[0]?.indexOf("LIMIT") ?? -1;
    expect(executor.calls[0]?.[limitIndex + 1]).toBe(2);
    expect(maxActive).toBe(2);
  });

  it("caps worker claims and terminal batches to the core Flow limit", async () => {
    const claimLimits: number[] = [];
    const completionBatchSizes: number[] = [];
    let claimed = false;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          const limitIndex = args.indexOf("LIMIT");
          claimLimits.push(args[limitIndex + 1] as number);
          if (claimed) return [];
          claimed = true;
          return Array.from({ length: 1_001 }, (_, index) => flow(`email-${index}`, index + 1));
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          const itemsIndex = args.indexOf("ITEMS");
          completionBatchSizes.push((args.length - itemsIndex - 1) / 3);
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 5_000,
      concurrency: 5_000,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => undefined);

    expect(result).toEqual({ claimed: 1_001, completed: 1_001, failed: 0, retried: 0 });
    expect(claimLimits).toEqual([1_000]);
    expect(completionBatchSizes).toEqual([1_000, 1]);
  });

  it("uses the client Flow many limit for worker claims and completion batches", async () => {
    const claimLimits: number[] = [];
    const completionBatchSizes: number[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          const limitIndex = args.indexOf("LIMIT");
          claimLimits.push(args[limitIndex + 1] as number);
          return [flow("email-1", 1), flow("email-2", 2), flow("email-3", 3)];
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          const itemsIndex = args.indexOf("ITEMS");
          completionBatchSizes.push((args.length - itemsIndex - 1) / 3);
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor, {
      flowManyBatchLimit: 2
    })).queue("email");

    await expect(queue.worker({
      batchSize: 10,
      concurrency: 10,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => undefined)).resolves.toEqual({
      claimed: 3,
      completed: 3,
      failed: 0,
      retried: 0
    });

    expect(claimLimits).toEqual([2]);
    expect(completionBatchSizes).toEqual([2, 1]);
  });

  it("continuously refills available handler slots without waiting for slow siblings", async () => {
    const controller = new AbortController();
    const fastGate = deferred();
    const slowGate = deferred();
    const refillGate = deferred();
    const initialStarted = deferred();
    const refillStarted = deferred();
    let initialStartedCount = 0;
    let refillStartedCount = 0;
    let claimCalls = 0;
    let secondClaimLimit: number | undefined;
    const completionBatchSizes: number[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.COMPLETE_MANY") {
          const itemsIndex = args.indexOf("ITEMS");
          completionBatchSizes.push((args.length - itemsIndex - 1) / 3);
          return Buffer.from("OK");
        }
        if (args[0] !== "FLOW.CLAIM_DUE") return Buffer.from("OK");
        claimCalls += 1;
        const limitIndex = args.indexOf("LIMIT");
        const limit = args[limitIndex + 1];
        if (claimCalls === 1) {
          return Array.from({ length: 10 }, (_, index) => flow(`email-${index + 1}`, index + 1));
        }
        if (claimCalls === 2) {
          secondClaimLimit = typeof limit === "number" ? limit : undefined;
          return Array.from({ length: 5 }, (_, index) => flow(`email-${index + 11}`, index + 11));
        }
        return [];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    let active = 0;
    let maxActive = 0;

    const task = queue.worker({
      batchSize: 10,
      concurrency: 10,
      leaseRenewal: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      const index = Number(job.id.slice("email-".length));
      active += 1;
      maxActive = Math.max(maxActive, active);
      try {
        if (index <= 10) {
          initialStartedCount += 1;
          if (initialStartedCount === 10) initialStarted.resolve();
          await (index <= 5 ? fastGate.promise : slowGate.promise);
          return;
        }
        refillStartedCount += 1;
        if (refillStartedCount === 5) refillStarted.resolve();
        await refillGate.promise;
      } finally {
        active -= 1;
      }
    });

    await initialStarted.promise;
    fastGate.resolve();
    const refilledBeforeSlowJobsFinished = await resolvesWithin(refillStarted.promise, 1_000);
    const activeAfterRefill = active;
    controller.abort();
    slowGate.resolve();
    refillGate.resolve();
    await task;

    expect(refilledBeforeSlowJobsFinished).toBe(true);
    expect(secondClaimLimit).toBe(5);
    expect(activeAfterRefill).toBe(10);
    expect(maxActive).toBe(10);
    expect(completionBatchSizes[0]).toBe(5);
  });

  it("fuses an acknowledged completion batch with its continuous refill claim", async () => {
    const controller = new AbortController();
    const replacementStarted = deferred();
    const replacementGate = deferred();
    const pipelineCalls: Command[][] = [];
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          return claimCalls === 1 ? [flow("email-1", 1)] : [];
        }
        return Buffer.from("OK");
      },
      async executeFusedPipeline(commands: readonly Command[]): Promise<unknown[]> {
        pipelineCalls.push(commands.map((command) => [...command]));
        return [Buffer.from("OK"), [flow("email-2", 2)]];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const task = queue.worker({
      batchSize: 1,
      concurrency: 1,
      leaseRenewal: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "email-2") {
        replacementStarted.resolve();
        await replacementGate.promise;
      }
    });

    await replacementStarted.promise;
    controller.abort();
    replacementGate.resolve();
    await task;

    expect(pipelineCalls).toHaveLength(1);
    expect(pipelineCalls[0]?.map((command) => command[0])).toEqual([
      "FLOW.COMPLETE_MANY",
      "FLOW.CLAIM_DUE"
    ]);
  });

  it("drains replacement leases returned after shutdown begins", async () => {
    const controller = new AbortController();
    const pipelineStarted = deferred();
    const releasePipeline = deferred();
    const handled: string[] = [];
    const directCalls: CommandArgument[][] = [];
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        directCalls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          return claimCalls === 1 ? [flow("email-1", 1)] : [];
        }
        return Buffer.from("OK");
      },
      async executeFusedPipeline(): Promise<unknown[]> {
        pipelineStarted.resolve();
        await releasePipeline.promise;
        return [Buffer.from("OK"), [flow("email-2", 2)]];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const task = queue.worker({
      batchSize: 1,
      concurrency: 1,
      leaseRenewal: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run((job) => {
      handled.push(job.id);
    });

    await pipelineStarted.promise;
    controller.abort();
    releasePipeline.resolve();
    await task;

    expect(handled).toEqual(["email-1", "email-2"]);
    expect(directCalls.some((call) => call[0] === "FLOW.COMPLETE_MANY" && call.includes("email-2"))).toBe(true);
  });

  it("drains replacement leases before surfacing a fused completion error", async () => {
    const handled: string[] = [];
    const directCalls: CommandArgument[][] = [];
    const pipelineCalls: Command[][] = [];
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        directCalls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          return claimCalls === 1 ? [flow("email-1", 1)] : [];
        }
        return Buffer.from("OK");
      },
      async executeFusedPipeline(commands: readonly Command[]): Promise<unknown[]> {
        pipelineCalls.push(commands.map((command) => [...command]));
        return [
          [[Buffer.from("error"), Buffer.from("ERR stale flow lease")]],
          [flow("email-2", 2)]
        ];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(queue.worker({
      batchSize: 1,
      concurrency: 1,
      leaseRenewal: false,
      worker: "worker-1"
    }).run(async (job) => {
      handled.push(job.id);
    })).rejects.toThrow("stale flow lease");

    expect(handled).toEqual(["email-1", "email-2"]);
    expect(pipelineCalls).toHaveLength(1);
    expect(directCalls.some((call) => call[0] === "FLOW.COMPLETE_MANY" && call.includes("email-2"))).toBe(true);
  });

  it("allows continuous completion-and-claim fusion to be disabled", async () => {
    const controller = new AbortController();
    const replacementStarted = deferred();
    const replacementGate = deferred();
    const pipelineCalls: Command[][] = [];
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          if (claimCalls === 1) return [flow("email-1", 1)];
          if (claimCalls === 2) return [flow("email-2", 2)];
          return [];
        }
        return Buffer.from("OK");
      },
      async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
        pipelineCalls.push(commands.map((command) => [...command]));
        return [Buffer.from("OK"), []];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const task = queue.worker({
      batchSize: 1,
      concurrency: 1,
      fuseCompleteClaim: false,
      leaseRenewal: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "email-2") {
        replacementStarted.resolve();
        await replacementGate.promise;
      }
    });

    await replacementStarted.promise;
    controller.abort();
    replacementGate.resolve();
    await task;

    expect(pipelineCalls).toHaveLength(0);
  });

  it("does not refill a slot until the terminal write is acknowledged", async () => {
    const controller = new AbortController();
    const slowGate = deferred();
    const completionGate = deferred();
    const completionStarted = deferred();
    const replacementStarted = deferred();
    let claimCalls = 0;
    let completionCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          if (claimCalls === 1) return [flow("email-1", 1), flow("email-2", 2)];
          if (claimCalls === 2) return [flow("email-3", 3)];
          return [];
        }
        if (args[0] === "FLOW.COMPLETE") {
          completionCalls += 1;
          if (completionCalls === 1) {
            completionStarted.resolve();
            await completionGate.promise;
          }
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const task = queue.worker({
      batchSize: 2,
      concurrency: 2,
      leaseRenewal: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "email-1") return complete({ result: "done" });
      if (job.id === "email-2") {
        await slowGate.promise;
        return complete({ result: "done" });
      }
      replacementStarted.resolve();
      await slowGate.promise;
      return complete({ result: "done" });
    });

    await completionStarted.promise;
    await sleep(20);
    const claimsBeforeAck = claimCalls;
    completionGate.resolve();
    const refilledAfterAck = await resolvesWithin(replacementStarted.promise, 1_000);
    controller.abort();
    slowGate.resolve();
    await task;

    expect(claimsBeforeAck).toBe(1);
    expect(refilledAfterAck).toBe(true);
    expect(claimCalls).toBe(2);
  });

  it("supports wave refill scheduling as an explicit opt-out", async () => {
    const controller = new AbortController();
    const slowGate = deferred();
    const slowStarted = deferred();
    const replacementStarted = deferred();
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] !== "FLOW.CLAIM_DUE") return Buffer.from("OK");
        claimCalls += 1;
        if (claimCalls === 1) return [flow("email-1", 1), flow("email-2", 2)];
        if (claimCalls === 2) return [flow("email-3", 3)];
        return [];
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const task = queue.worker({
      batchSize: 2,
      concurrency: 2,
      leaseRenewal: false,
      refillStrategy: "wave",
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "email-1") return complete({ result: "done" });
      if (job.id === "email-2") {
        slowStarted.resolve();
        await slowGate.promise;
        return complete({ result: "done" });
      }
      replacementStarted.resolve();
      controller.abort();
      return complete({ result: "done" });
    });

    await slowStarted.promise;
    await sleep(20);
    expect(claimCalls).toBe(1);
    slowGate.resolve();
    await replacementStarted.promise;
    await task;

    expect(claimCalls).toBe(2);
  });

  it("stops an idle wave worker cleanly when its signal is aborted", async () => {
    const controller = new AbortController();
    const claimFinished = deferred();
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimFinished.resolve();
          return [];
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const task = queue.worker({
      idleSleepMs: 10_000,
      refillStrategy: "wave",
      signal: controller.signal,
      worker: "worker-1"
    }).run(() => undefined);

    await claimFinished.promise;
    await new Promise((resolve) => setImmediate(resolve));
    controller.abort();

    await expect(task).resolves.toBeUndefined();
  });

  it("bounds server blocking while an abortable worker is idle", async () => {
    const controller = new AbortController();
    let claimArgs: CommandArgument[] | undefined;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimArgs = args;
          controller.abort();
          return [];
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await queue.worker({
      abortPollMs: 25,
      blockMs: 0,
      claimPayload: false,
      signal: controller.signal,
      worker: "worker-1"
    }).run(() => undefined);

    const blockIndex = claimArgs?.indexOf("BLOCK") ?? -1;
    expect(blockIndex).toBeGreaterThanOrEqual(0);
    expect(claimArgs?.[blockIndex + 1]).toBe(25);
  });

  it("stops refilling on a raised handler error and drains active siblings", async () => {
    const siblingGate = deferred();
    const siblingStarted = deferred();
    let claimCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          claimCalls += 1;
          return claimCalls === 1 ? [flow("email-1", 1), flow("email-2", 2)] : [];
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const task = queue.worker({
      concurrency: 2,
      exceptionPolicy: "raise",
      leaseRenewal: false,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "email-1") throw new Error("handler failed");
      siblingStarted.resolve();
      await siblingGate.promise;
    });
    let settled = false;
    void task.then(
      () => { settled = true; },
      () => { settled = true; }
    );

    await siblingStarted.promise;
    await new Promise((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    siblingGate.resolve();

    await expect(task).rejects.toThrow("handler failed");
    expect(claimCalls).toBe(1);
  });

  it("falls back to one handler for non-finite concurrency", async () => {
    const executor = new FakeExecutor([[flow("email-1", 1)], Buffer.from("OK")]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    let handled = 0;

    await queue.worker({
      concurrency: Number.NaN,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => {
      handled++;
    });

    const limitIndex = executor.calls[0]?.indexOf("LIMIT") ?? -1;
    expect(executor.calls[0]?.[limitIndex + 1]).toBe(1);
    expect(handled).toBe(1);
  });

  it("waits for sibling handlers and flushes their completions before raising", async () => {
    const executor = new FakeExecutor([
      [flow("email-1", 1), flow("email-2", 2)],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    let releaseSecond: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const secondStarted = new Promise<void>((resolve) => {
      markSecondStarted = resolve;
    });
    const secondGate = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const task = queue.worker({
      batchSize: 2,
      concurrency: 2,
      exceptionPolicy: "raise",
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(async (job) => {
      if (job.id === "email-1") throw new Error("first handler failed");
      markSecondStarted?.();
      await secondGate;
    });
    let settled = false;
    void task.then(
      () => { settled = true; },
      () => { settled = true; }
    );

    await secondStarted;
    await new Promise((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    releaseSecond?.();
    await expect(task).rejects.toThrow("first handler failed");
    expect(executor.calls.some((call) => call[0] === "FLOW.COMPLETE_MANY")).toBe(true);
  });

  it("renews leases while queue handlers are still running", async () => {
    const calls: CommandArgument[][] = [];
    const record = flow("email-1", 1);
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return [record];
        if (args[0] === "FLOW.EXTEND_LEASE") return leaseRenewalResponse(args);
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await queue.worker({
      batchSize: 1,
      leaseMs: 20,
      leaseRenewIntervalMs: 5,
      worker: "worker-1"
    }).runOnce(async () => {
      await new Promise((resolve) => setTimeout(resolve, 25));
    });

    const names = calls.map((call) => call[0]);
    expect(names).toContain("FLOW.EXTEND_LEASE");
    expect(names.indexOf("FLOW.EXTEND_LEASE")).toBeLessThan(names.indexOf("FLOW.COMPLETE_MANY"));
  });

  it("does not apply a handler outcome after lease renewal is lost", async () => {
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return [flow("email-1", 1)];
        if (args[0] === "FLOW.EXTEND_LEASE") throw new Error("renewal rejected");
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(
      queue.worker({
        batchSize: 1,
        leaseMs: 20,
        leaseRenewIntervalMs: 5,
        worker: "worker-1"
      }).runOnce(async () => {
        await new Promise((resolve) => setTimeout(resolve, 25));
        return complete();
      })
    ).rejects.toThrow("lease renewal failed");

    expect(calls.some((call) => call[0] === "FLOW.EXTEND_LEASE")).toBe(true);
    const terminalCommands = new Set(["FLOW.COMPLETE", "FLOW.COMPLETE_MANY", "FLOW.RETRY", "FLOW.FAIL"]);
    expect(calls.some((call) => typeof call[0] === "string" && terminalCommands.has(call[0]))).toBe(false);
  });

  it("completes healthy jobs before surfacing another job's renewal failure", async () => {
    const calls: CommandArgument[][] = [];
    let renewalCount = 0;
    let markRenewalsStarted: (() => void) | undefined;
    const renewalsStarted = new Promise<void>((resolve) => {
      markRenewalsStarted = resolve;
    });
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return [flow("email-1", 1), flow("email-2", 2)];
        if (args[0] === "FLOW.EXTEND_LEASE") {
          renewalCount += 1;
          if (renewalCount === 2) markRenewalsStarted?.();
          if (args[1] === "email-1") throw new Error("renewal rejected");
          return leaseRenewalResponse(args);
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(queue.worker({
      batchSize: 2,
      concurrency: 2,
      leaseMs: 20,
      leaseRenewIntervalMs: 1,
      worker: "worker-1"
    }).runOnce(async () => {
      await renewalsStarted;
    })).rejects.toThrow("lease renewal failed");

    const completion = calls.find((call) => call[0] === "FLOW.COMPLETE_MANY");
    const completionTokens = completion?.map((item) => Buffer.isBuffer(item) ? item.toString("utf8") : item);
    expect(completionTokens).toContain("email-2");
    expect(completionTokens).not.toContain("email-1");
  });

  it("waits for an in-flight lease renewal before applying a terminal outcome", async () => {
    const calls: CommandArgument[][] = [];
    let releaseRenewal: (() => void) | undefined;
    let markRenewalStarted: (() => void) | undefined;
    const renewalStarted = new Promise<void>((resolve) => {
      markRenewalStarted = resolve;
    });
    const renewalGate = new Promise<void>((resolve) => {
      releaseRenewal = resolve;
    });
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return [flow("email-1", 1)];
        if (args[0] === "FLOW.EXTEND_LEASE") {
          markRenewalStarted?.();
          await renewalGate;
          return leaseRenewalResponse(args);
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const task = queue.worker({
      batchSize: 1,
      leaseMs: 20,
      leaseRenewIntervalMs: 5,
      worker: "worker-1"
    }).runOnce(async () => {
      await renewalStarted;
      return complete({ result: "done" });
    });

    await renewalStarted;
    await new Promise((resolve) => setImmediate(resolve));
    const terminalStartedBeforeRenewalFinished = calls.some((call) => call[0] === "FLOW.COMPLETE");
    releaseRenewal?.();
    await task;

    expect(terminalStartedBeforeRenewalFinished).toBe(false);
    expect(calls.map((call) => call[0])).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.EXTEND_LEASE",
      "FLOW.COMPLETE"
    ]);
  });

  it("stops every batch-job guard before one terminal write", async () => {
    const calls: CommandArgument[][] = [];
    let releaseRenewals: (() => void) | undefined;
    let markRenewalsStarted: (() => void) | undefined;
    let renewalCount = 0;
    const renewalsStarted = new Promise<void>((resolve) => {
      markRenewalsStarted = resolve;
    });
    const renewalGate = new Promise<void>((resolve) => {
      releaseRenewals = resolve;
    });
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return [flow("email-1", 1), flow("email-2", 2)];
        if (args[0] === "FLOW.EXTEND_LEASE") {
          renewalCount += 1;
          if (renewalCount === 2) markRenewalsStarted?.();
          await renewalGate;
          return leaseRenewalResponse(args);
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const task = queue.worker({
      batchSize: 2,
      leaseMs: 20,
      leaseRenewIntervalMs: 5,
      worker: "worker-1"
    }).runBatchOnce(async () => {
      await renewalsStarted;
      return complete({ result: "done" });
    });

    await renewalsStarted;
    await new Promise((resolve) => setImmediate(resolve));
    expect(calls.some((call) => call[0] === "FLOW.COMPLETE_MANY")).toBe(false);
    releaseRenewals?.();
    await task;

    expect(calls.filter((call) => call[0] === "FLOW.COMPLETE_MANY")).toHaveLength(1);
    expect(calls.some((call) => call[0] === "FLOW.COMPLETE")).toBe(false);
  });

  it("passes named-value drop and override mutations through queue outcomes", async () => {
    const executor = new FakeExecutor([[flow("email-1", 1)], Buffer.from("OK")]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await queue.worker({ leaseRenewal: false, worker: "worker-1" }).runOnce(() =>
      complete({
        attributesDelete: ["temporary"],
        attributesMerge: { phase: "completed" },
        dropValues: ["secret"],
        overrideValues: ["fresh"],
        result: "done"
      })
    );

    expect(executor.calls[1]).toContain("DROP_VALUE");
    expect(executor.calls[1]).toContain("secret");
    expect(executor.calls[1]).toContain("OVERRIDE_VALUE");
    expect(executor.calls[1]).toContain("fresh");
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "ATTRIBUTE_MERGE", "phase", "completed",
      "ATTRIBUTE_DELETE", "temporary"
    ]));
  });

  it("uses claimDrainBatches to process another claim without pre-leasing it", async () => {
    const executor = new FakeExecutor([
      [flow("email-1", 1)],
      Buffer.from("OK"),
      []
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 1,
      claimDrainBatches: 2,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => undefined);

    expect(result).toMatchObject({ claimed: 1, completed: 1 });
    expect(executor.calls.filter((call) => call[0] === "FLOW.CLAIM_DUE")).toHaveLength(2);
  });

  it("uses compact claims and batched completion when payload is not requested", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", lease1, 1],
        ["email-2", "tenant-a", lease2, 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      worker: "worker-1"
    }).runOnce(() => undefined);

    expect(result).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("JOBS_COMPACT");
    expect(executor.calls[0]).toContain("NOPAYLOAD");
    expect(executor.calls[1]).toEqual([
      "FLOW.COMPLETE_MANY",
      "tenant-a",
      "NOW",
      expect.any(Number),
      "INDEPENDENT",
      "true",
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      Buffer.from("email-1"),
      lease1,
      1,
      Buffer.from("email-2"),
      lease2,
      2
    ]);
  });

  it("uses full claims when named values are requested without payloads", async () => {
    const executor = new FakeExecutor([
      [flow("email-1", 1)],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await queue.worker({
      batchSize: 1,
      claimPayload: false,
      claimValues: ["profile"],
      worker: "worker-1"
    }).runOnce(() => undefined);

    expect(executor.calls[0]).not.toContain("RETURN");
    expect(executor.calls[0]).toContain("NOPAYLOAD");
    expect(executor.calls[0]).toContain("VALUE");
  });

  it("rejects a batched completion when any per-item terminal write fails", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      [
        [Buffer.from("error"), Buffer.from("ERR stale flow lease")],
        Buffer.from("ok")
      ]
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(queue.worker({
      batchSize: 2,
      claimPayload: false,
      leaseRenewal: false,
      worker: "worker-1"
    }).runOnce(() => undefined)).rejects.toThrow("stale flow lease");
  });

  it("expands throughput profile to compact claims and async completion defaults", async () => {
    const executor = new FakeExecutor([
      [["email-1", "tenant-a", Buffer.from("lease-1"), 1]],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({ profile: "throughput", worker: "worker-1" });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 1,
      completed: 0
    });
    await expect(worker.flush()).resolves.toBe(1);

    expect(executor.calls[0]).toContain("LIMIT");
    expect(executor.calls[0]).toContain(500);
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("JOBS_COMPACT");
    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE_MANY");
  });

  it("lets explicit worker options override throughput profile defaults", async () => {
    const executor = new FakeExecutor([
      [["email-1", "tenant-a", Buffer.from("lease-1"), 1]],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await queue.worker({
      batchSize: 2,
      claimPayload: false,
      completeAsyncDepth: 0,
      profile: "throughput",
      worker: "worker-1"
    }).runBatchOnce(() => undefined);

    expect(executor.calls[0]).toContain("LIMIT");
    expect(executor.calls[0]).toContain(2);
  });

});

function flow(id: string, fencingToken: number): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", id],
    ["type", "email"],
    ["state", "queued"],
    ["partition_key", "tenant-a"],
    ["lease_token", Buffer.from(`lease-${fencingToken}`)],
    ["fencing_token", fencingToken]
  ]);
}

function leaseRenewalResponse(args: readonly CommandArgument[]): Buffer | Map<unknown, unknown> {
  if (args.includes("RETURN")) return Buffer.from("OK");
  const id = args[1];
  const fencingIndex = args.indexOf("FENCING");
  const fencingToken = args[fencingIndex + 1];
  if (typeof id !== "string" || typeof fencingToken !== "number") {
    throw new TypeError("expected a fenced FLOW.EXTEND_LEASE command");
  }
  return flow(id, fencingToken);
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function resolvesWithin(promise: Promise<void>, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    void promise.then(() => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}
