import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec, WorkflowClient, complete, transition } from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";
import {
  nextWorkerIdleSleepMs,
  runContinuousWorkerPool,
  workerIdleSleepMs,
  workerMaxIdleSleepMs
} from "../src/worker-internal.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Workflow", () => {
  it("fails fast when a worker has no effective states", async () => {
    const executor = new FakeExecutor();
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    const worker = workflow.worker();
    const controller = new AbortController();
    controller.abort();

    await expect(worker.runOnce()).rejects.toThrow("Workflow worker requires at least one state");
    await expect(workflow.worker({ signal: controller.signal }).run()).rejects.toThrow(
      "Workflow worker requires at least one state"
    );
    expect(executor.calls).toEqual([]);
  });

  it("resolves states at execution time and claims each configured state once", async () => {
    const executor = new FakeExecutor([[]]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    const worker = workflow.worker({ states: ["charged", "charged"] });
    workflow.state("charged", () => complete());

    await expect(worker.runOnce()).resolves.toMatchObject({ claimCalls: 1 });
    expect(executor.calls).toHaveLength(1);
  });

  it("keeps transition helper discriminants authoritative for runtime JavaScript options", () => {
    const outcome = transition("charged", {
      kind: "retry",
      toState: "diverted"
    } as never);

    expect(outcome.kind).toBe("transition");
    expect(outcome.toState).toBe("charged");
  });

  it("rejects an invalid runtime worker exception policy before claiming", () => {
    const executor = new FakeExecutor();
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("created", () => complete());

    expect(() => workflow.worker({
      exceptionPolicy: "ignore" as never,
      states: ["created"],
      worker: "worker-1"
    })).toThrow("exceptionPolicy must be 'retry', 'fail', or 'raise'");
    expect(executor.calls).toEqual([]);
  });

  it("claims a state and applies an explicit transition outcome", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "created"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 11],
          ["payload", Buffer.from('{"amount":42}')]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({
      initialState: "created",
      type: "order",
      worker: "worker-1"
    });

    workflow.state("created", (ctx) => {
      expect(ctx.payload).toEqual({ amount: 42 });
      return transition("charged", {
        attributesDelete: ["temporary"],
        attributesMerge: { phases: ["created", "charged"] },
        values: { receipt: { ok: true } }
      });
    });

    const result = await workflow.worker({ batchSize: 1 }).runOnce();

    expect(result).toEqual({ applied: 1, claimCalls: 1, claimed: 1, emptyClaims: 0 });
    expect(executor.calls[1]).toEqual([
      "FLOW.TRANSITION",
      "order-1",
      "created",
      "charged",
      "LEASE_TOKEN",
      Buffer.from("lease"),
      "FENCING",
      11,
      "NOW",
      expect.any(Number),
      "PARTITION",
      "tenant-a",
      "RUN_AT",
      expect.any(Number),
      "VALUE",
      "receipt",
      Buffer.from('{"ok":true}'),
      "ATTRIBUTE_MERGE",
      "phases",
      ["created", "charged"],
      "ATTRIBUTE_DELETE",
      "temporary"
    ]);
  });

  it("treats plain handler return values as complete results", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-2"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 12]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ initialState: "created", type: "order" });

    workflow.state("charged", () => ({ done: true }));

    await workflow.worker({ batchSize: 1, states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toContain("FLOW.COMPLETE");
    expect(executor.calls[1]).toContain("RESULT");
    expect(executor.calls[1]).toContainEqual(Buffer.from('{"done":true}'));
  });

  it("does not interpret domain objects with outcome-like kinds as worker control flow", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-domain-kind"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 13]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ initialState: "created", type: "order" });

    workflow.state("charged", () => ({ kind: "retry", orderId: "order-domain-kind" }));

    await workflow.worker({ batchSize: 1, states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE");
    expect(executor.calls[1]).toContain("RESULT");
    expect(executor.calls[1]).toContainEqual(
      Buffer.from('{"kind":"retry","orderId":"order-domain-kind"}')
    );
  });

  it.each([
    ["omits", undefined, false],
    ["includes", true, true]
  ])("%s handler stack traces in workflow errors when configured", async (_name, includeErrorStack, hasStack) => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-error"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 12]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ type: "order" });
    const handlerError = new Error("handler failed");
    handlerError.stack = "SENSITIVE_STACK_PATH";
    workflow.state("charged", () => {
      throw handlerError;
    });

    await workflow.worker({
      includeErrorStack,
      leaseRenewal: false,
      states: ["charged"],
      worker: "worker-1"
    }).runOnce();

    const retryCall = executor.calls.find((call) => call[0] === "FLOW.RETRY");
    const errorIndex = retryCall?.indexOf("ERROR") ?? -1;
    const errorPayload = JSON.parse((retryCall?.[errorIndex + 1] as Buffer).toString("utf8")) as Record<string, unknown>;
    expect(errorPayload).toMatchObject({ message: "handler failed", name: "Error" });
    expect(Object.hasOwn(errorPayload, "stack")).toBe(hasStack);
  });

  it("persists thrown workflow errors with the default raw codec", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-raw-error"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 12]
        ])
      ],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("charged", () => {
      throw new Error("raw workflow failed");
    });

    await workflow.worker({
      leaseRenewal: false,
      states: ["charged"],
      worker: "worker-1"
    }).runOnce();

    const retryCall = executor.calls.find((call) => call[0] === "FLOW.RETRY");
    const errorIndex = retryCall?.indexOf("ERROR") ?? -1;
    expect(JSON.parse((retryCall?.[errorIndex + 1] as Buffer).toString("utf8"))).toEqual({
      message: "raw workflow failed",
      name: "Error"
    });
  });

  it("allows handlers to return complete explicitly", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-3"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 13]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ type: "order" });

    workflow.state("charged", () => complete({ result: { ok: true }, ttlMs: 60_000 }));

    await workflow.worker({ states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toContain("TTL");
    expect(executor.calls[1]).toContain(60_000);
  });

  it("uses workflow concurrency as claim credit and handler concurrency", async () => {
    const jobs = [1, 2, 3].map(
      (index) =>
        new Map<unknown, unknown>([
          ["id", `order-${index}`],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from(`lease-${index}`)],
          ["fencing_token", index]
        ])
    );
    const executor = new FakeExecutor([jobs]);
    const flow = new FerricStoreClient(executor);
    const workflow = new WorkflowClient(flow).workflow({ type: "order" });
    let active = 0;
    let maxActive = 0;

    workflow.state("charged", async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active--;
      return complete();
    });

    await workflow.worker({
      batchSize: 10,
      concurrency: 3,
      leaseRenewal: false,
      states: ["charged"],
      worker: "worker-1"
    }).runOnce();

    const limitIndex = executor.calls[0]?.indexOf("LIMIT") ?? -1;
    expect(executor.calls[0]?.[limitIndex + 1]).toBe(3);
    expect(maxActive).toBe(3);
  });

  it("stops renewal guards for over-returned jobs left unprocessed after a handler failure", async () => {
    const jobs = [workflowFlow("order-first", 1), workflowFlow("order-second", 2)];
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return jobs;
        if (args[0] === "FLOW.EXTEND_LEASE") {
          if (args.includes("RETURN")) return Buffer.from("OK");
          const record = jobs.find((job) => job.get("id") === args[1]);
          if (record == null) throw new TypeError("expected a known workflow flow id");
          return record;
        }
        return Buffer.from("OK");
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("charged", async () => {
      await new Promise((resolve) => setTimeout(resolve, 8));
      throw new Error("handler failed");
    });

    await expect(workflow.worker({
      batchSize: 2,
      concurrency: 1,
      exceptionPolicy: "raise",
      leaseMs: 20,
      leaseRenewIntervalMs: 1,
      states: ["charged"],
      worker: "worker-1"
    }).runOnce()).rejects.toThrow("handler failed");

    const renewalCount = (): number => calls.filter((call) =>
      call[0] === "FLOW.EXTEND_LEASE" && call[1] === "order-second"
    ).length;
    const stoppedAt = renewalCount();
    expect(stoppedAt).toBeGreaterThan(0);
    await new Promise((resolve) => setTimeout(resolve, 8));
    expect(renewalCount()).toBe(stoppedAt);
  });

  it("bounds handlers when a server returns more jobs than requested", async () => {
    const controller = new AbortController();
    let active = 0;
    let handled = 0;
    let maxActive = 0;
    let firstClaim = true;

    await runContinuousWorkerPool({
      claim: async () => {
        if (firstClaim) {
          firstClaim = false;
          return [1, 2, 3];
        }
        return [];
      },
      concurrency: 2,
      handle: async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        active -= 1;
        handled += 1;
        if (handled === 3) controller.abort();
      },
      idleSleepMs: 0,
      maxClaimSize: 2,
      maxIdleSleepMs: 1,
      signal: controller.signal
    });

    expect(handled).toBe(3);
    expect(maxActive).toBe(2);
  });

  it("continuously refills workflow slots without waiting for slow siblings", async () => {
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
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] !== "FLOW.CLAIM_DUE") return Buffer.from("OK");
        claimCalls += 1;
        const limitIndex = args.indexOf("LIMIT");
        const limit = args[limitIndex + 1];
        if (claimCalls === 1) {
          return Array.from({ length: 10 }, (_, index) => workflowFlow(`order-${index + 1}`, index + 1));
        }
        if (claimCalls === 2) {
          secondClaimLimit = typeof limit === "number" ? limit : undefined;
          return Array.from({ length: 5 }, (_, index) => workflowFlow(`order-${index + 11}`, index + 11));
        }
        return [];
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    let active = 0;
    let maxActive = 0;

    workflow.state("charged", async (ctx) => {
      const index = Number(ctx.id.slice("order-".length));
      active += 1;
      maxActive = Math.max(maxActive, active);
      try {
        if (index <= 10) {
          initialStartedCount += 1;
          if (initialStartedCount === 10) initialStarted.resolve();
          await (index <= 5 ? fastGate.promise : slowGate.promise);
        } else {
          refillStartedCount += 1;
          if (refillStartedCount === 5) refillStarted.resolve();
          await refillGate.promise;
        }
        return complete();
      } finally {
        active -= 1;
      }
    });

    const task = workflow.worker({
      batchSize: 10,
      concurrency: 10,
      leaseRenewal: false,
      signal: controller.signal,
      states: ["charged"],
      worker: "worker-1"
    }).run();

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
  });

  it("round-robins continuous refill claims across workflow states", async () => {
    const controller = new AbortController();
    const claimedStates: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] !== "FLOW.CLAIM_DUE") return Buffer.from("OK");
        const stateIndex = args.indexOf("STATE");
        const state = args[stateIndex + 1];
        if (typeof state !== "string") throw new Error("expected workflow state");
        claimedStates.push(state);
        if (claimedStates.length === 1) return [workflowFlow("order-a", 1, "a")];
        if (claimedStates.length === 2) return [workflowFlow("order-b", 2, "b")];
        return [];
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("a", () => complete());
    workflow.state("b", () => {
      controller.abort();
      return complete();
    });

    await workflow.worker({
      concurrency: 1,
      leaseRenewal: false,
      signal: controller.signal,
      states: ["a", "b"],
      worker: "worker-1"
    }).run();

    expect(claimedStates).toEqual(["a", "b"]);
  });

  it("does not multiply runOnce blocking latency across workflow states", async () => {
    const executor = new FakeExecutor([[], []]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("a", () => complete());
    workflow.state("b", () => complete());

    await workflow.worker({
      blockMs: 250,
      states: ["a", "b"],
      worker: "worker-1"
    }).runOnce();

    expect(executor.calls).toHaveLength(2);
    expect(executor.calls.every((call) => !call.includes("BLOCK"))).toBe(true);

    const singleState = new FakeExecutor([[]]);
    const singleWorkflow = new WorkflowClient(new FerricStoreClient(singleState)).workflow({ type: "order" });
    singleWorkflow.state("a", () => complete());
    await singleWorkflow.worker({ blockMs: 250, states: ["a"], worker: "worker-1" }).runOnce();
    expect(singleState.calls[0]).toEqual(expect.arrayContaining(["BLOCK", 250]));
  });

  it("fills capacity immediately from the next workflow state after a partial claim", async () => {
    const controller = new AbortController();
    const bothStarted = deferred();
    const started: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] !== "FLOW.CLAIM_DUE") return Buffer.from("OK");
        const stateIndex = args.indexOf("STATE");
        const state = args[stateIndex + 1];
        if (state === "a") return [workflowFlow("order-a", 1, "a")];
        if (state === "b") return [workflowFlow("order-b", 2, "b")];
        return [];
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    const handler = (id: string) => {
      started.push(id);
      if (started.length === 2) {
        controller.abort();
        bothStarted.resolve();
      }
      return complete();
    };
    workflow.state("a", (ctx) => handler(ctx.id));
    workflow.state("b", (ctx) => handler(ctx.id));

    const task = workflow.worker({
      concurrency: 2,
      idleSleepMs: 10_000,
      leaseRenewal: false,
      signal: controller.signal,
      states: ["a", "b"],
      worker: "worker-1"
    }).run();

    expect(await resolvesWithin(bothStarted.promise, 1_000)).toBe(true);
    await task;
    expect(started.sort()).toEqual(["order-a", "order-b"]);
  });

  it("finishes partial multi-state claims before surfacing a later claim failure", async () => {
    const handled: string[] = [];
    const terminalWrites: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          const stateIndex = args.indexOf("STATE");
          const state = args[stateIndex + 1];
          if (state === "a") return [workflowFlow("order-a", 1, "a")];
          if (state === "b") throw new Error("state b claim failed");
          return [];
        }
        if (typeof args[0] === "string") terminalWrites.push(args[0]);
        return Buffer.from("OK");
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("a", (ctx) => {
      handled.push(ctx.id);
      return complete();
    });
    workflow.state("b", () => complete());

    await expect(workflow.worker({
      concurrency: 2,
      idleSleepMs: 0,
      leaseRenewal: false,
      states: ["a", "b"],
      worker: "worker-1"
    }).run()).rejects.toThrow("state b claim failed");

    expect(handled).toEqual(["order-a"]);
    expect(terminalWrites).toContain("FLOW.COMPLETE");
  });

  it("applies the throughput profile to workflow claim hydration", async () => {
    const executor = new FakeExecutor([[]]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("charged", () => complete());

    await workflow.worker({
      profile: "throughput",
      states: ["charged"],
      worker: "worker-1"
    }).runOnce();

    expect(executor.calls[0]).toContain("NOPAYLOAD");
    expect(executor.calls[0]).not.toContain("PAYLOAD");
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("JOBS_COMPACT_STATE");
  });

  it("does not use compact workflow claims when handlers request record data", async () => {
    const executor = new FakeExecutor([[workflowFlow("order-1", 1)], Buffer.from("OK")]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("charged", () => complete(), {
      claimPayload: true,
      claimRecord: false,
      claimValues: ["profile"]
    });

    await workflow.worker({ states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[0]).not.toContain("RETURN");
    expect(executor.calls[0]).toContain("PAYLOAD");
    expect(executor.calls[0]).toContain("VALUE");
  });

  it("normalizes invalid worker idle timing without creating a hot poll loop", () => {
    expect(workerIdleSleepMs({ idleSleepMs: Number.NaN })).toBe(250);
    expect(workerIdleSleepMs({ idleSleepMs: -1 })).toBe(250);
    expect(workerMaxIdleSleepMs({ maxIdleSleepMs: Number.NaN }, 250)).toBe(5_000);
    expect(workerMaxIdleSleepMs({ maxIdleSleepMs: 10 }, 250)).toBe(250);
  });

  it("advances zero idle delay into positive backoff unless zero is the configured cap", () => {
    expect(nextWorkerIdleSleepMs(0, 5_000)).toBe(1);
    expect(nextWorkerIdleSleepMs(1, 5_000)).toBe(2);
    expect(nextWorkerIdleSleepMs(4_000, 5_000)).toBe(5_000);
    expect(nextWorkerIdleSleepMs(0, 0)).toBe(0);
  });

  it("installs FIFO state policy and rejects priority transitions into FIFO states", async () => {
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      [
        new Map<unknown, unknown>([
          ["id", "order-4"],
          ["type", "order"],
          ["state", "created"],
          ["partition_key", "tenant-a"],
          ["lease_token", lease],
          ["fencing_token", 14]
        ])
      ]
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({
      initialState: "created",
      type: "order"
    });

    workflow
      .state("created", () => transition("ready", { priority: 1 }))
      .state("ready", () => complete({ result: "done" }), { mode: "fifo" });

    await workflow.installPolicy();
    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "ready",
      "MODE",
      "FIFO"
    ]);

    await expect(
      workflow.worker({ batchSize: 1, states: ["created"], worker: "worker-1" }).runOnce()
    ).rejects.toThrow("priority is not supported for fifo state");
    expect(executor.calls).toHaveLength(2);
  });

  it("installs policies for prototype-shaped workflow state names", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("__proto__", () => complete(), { mode: "fifo" });

    await workflow.installPolicy();

    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "__proto__",
      "MODE",
      "FIFO"
    ]);
  });
});

function workflowFlow(id: string, fencingToken: number, state = "charged"): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", id],
    ["type", "order"],
    ["state", state],
    ["partition_key", "tenant-a"],
    ["lease_token", Buffer.from(`lease-${fencingToken}`)],
    ["fencing_token", fencingToken]
  ]);
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
