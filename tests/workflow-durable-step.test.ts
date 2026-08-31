import { describe, expect, it, vi } from "vitest";
import {
  FerricStoreClient,
  ConnectionClosedError,
  FlowWrongStateError,
  JsonCodec,
  WorkflowClient,
  complete,
  type CommandArgument,
  type CommandExecutor
} from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

const STEP_NAME = "charge-customer:v1";
const STEP_VALUE_NAME =
  "__ferricstore_step__:sha256:ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7";

describe("WorkflowContext durable mutations", () => {
  it("runs a durable step, returns an applied outcome, and releases the refreshed claim", async () => {
    const firstLease = Buffer.from("lease-1");
    const secondLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", firstLease, 7, "created")]),
      flowRecord("order-1", firstLease, 7, "created"),
      compactClaim("order-1", secondLease, 8),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    const run = vi.fn(async () => ({ receipt: "rcpt-1" }));
    workflow.state("created", async (ctx) => {
      const applied = await ctx.step({
        name: STEP_NAME,
        run,
        toState: "charged"
      });
      expect(applied).toMatchObject({
        job: { fencingToken: 8, runState: "charged" },
        kind: "already_applied",
        result: { receipt: "rcpt-1" }
      });
      return applied;
    });

    await expect(worker(workflow).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

    expect(run).toHaveBeenCalledOnce();
    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.EXTEND_LEASE",
      "FLOW.STEP_CONTINUE",
      "FLOW.TRANSITION"
    ]);
    expect(executor.calls[2]).toEqual(expect.arrayContaining([
      "VALUE", STEP_VALUE_NAME, Buffer.from('{"receipt":"rcpt-1"}')
    ]));
    expect(executor.calls[3]).toEqual(expect.arrayContaining([
      "FLOW.TRANSITION", "order-1", "running", "charged",
      "LEASE_TOKEN", secondLease, "FENCING", 8
    ]));
  });

  it("advances ergonomically and never auto-completes with the stale claim", async () => {
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", Buffer.from("lease-1"), 7, "created")]),
      compactClaim("order-1", Buffer.from("lease-2"), 8),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => await ctx.advance("charged"));

    await worker(workflow).runOnce();

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.STEP_CONTINUE",
      "FLOW.TRANSITION"
    ]);
    expect(executor.calls.some((call) => call[0] === "FLOW.COMPLETE")).toBe(false);
    expect(executor.calls[2]).toEqual(expect.arrayContaining([
      "LEASE_TOKEN", Buffer.from("lease-2"), "FENCING", 8
    ]));
  });

  it("chains multiple durable steps through each refreshed lease", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const lease3 = Buffer.from("lease-3");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease1, 7, "created")]),
      flowRecord("order-1", lease1, 7, "created"),
      compactClaim("order-1", lease2, 8),
      flowRecord("order-1", lease2, 8, "charged"),
      compactClaim("order-1", lease3, 9),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => {
      await ctx.step({ name: "reserve:v1", run: () => "reserved", toState: "charged" });
      return await ctx.step({ name: "capture:v1", run: () => "captured", toState: "captured" });
    });

    await worker(workflow).runOnce();

    const preflights = executor.calls.filter((call) => call[0] === "FLOW.EXTEND_LEASE");
    const commits = executor.calls.filter((call) => call[0] === "FLOW.STEP_CONTINUE");
    expect(preflights[0]).toEqual(expect.arrayContaining(["order-1", lease1, "FENCING", 7]));
    expect(commits[0]?.slice(0, 5)).toEqual(["FLOW.STEP_CONTINUE", "order-1", lease1, "created", "charged"]);
    expect(preflights[1]).toEqual(expect.arrayContaining(["order-1", lease2, "FENCING", 8]));
    expect(commits[1]?.slice(0, 5)).toEqual(["FLOW.STEP_CONTINUE", "order-1", lease2, "charged", "captured"]);
    expect(executor.calls.at(-1)).toEqual(expect.arrayContaining([
      "FLOW.TRANSITION", "order-1", "running", "captured",
      "LEASE_TOKEN", lease3, "FENCING", 9
    ]));
  });

  it("replays a committed context step and completes with its stored result", async () => {
    const lease = Buffer.from("lease-b");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease, 9, "charged")]),
      flowRecord("order-1", lease, 9, "charged", {
        [STEP_VALUE_NAME]: { ref: "receipt-ref" }
      }),
      [Buffer.from('{"receipt":"rcpt-1"}')],
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    const run = vi.fn(() => ({ receipt: "duplicate" }));
    workflow.state("charged", async (ctx) => await ctx.step({
      name: STEP_NAME,
      run,
      toState: "charged"
    }));

    await workflow.worker({
      batchSize: 1,
      leaseRenewal: false,
      profile: "throughput",
      states: ["charged"],
      worker: "worker-1"
    }).runOnce();

    expect(run).not.toHaveBeenCalled();
    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.VALUE.MGET", "FLOW.COMPLETE"
    ]);
    expect(executor.calls.some((call) => call[0] === "FLOW.STEP_CONTINUE")).toBe(false);
    expect(executor.calls[3]).toEqual(expect.arrayContaining([
      "FLOW.COMPLETE", "order-1", lease, "FENCING", 9,
      "PARTITION", "tenant-a", "RESULT", Buffer.from('{"receipt":"rcpt-1"}')
    ]));
  });

  it("applies an explicit outcome after advance with the refreshed claim", async () => {
    const lease2 = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", Buffer.from("lease-1"), 7, "created")]),
      compactClaim("order-1", lease2, 8),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => {
      await ctx.advance("charged");
      return complete({ result: "done" });
    });

    await worker(workflow).runOnce();

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.STEP_CONTINUE", "FLOW.COMPLETE"
    ]);
    expect(executor.calls[2]?.slice(0, 5)).toEqual([
      "FLOW.COMPLETE", "order-1", lease2, "FENCING", 8
    ]);
  });

  it("never infers a missing run state from a full worker claim", async () => {
    const malformedClaim = flowRecord(
      "order-1", Buffer.from("lease-1"), 7, "created"
    );
    malformedClaim.delete("run_state");
    const executor = new FakeExecutor([
      [malformedClaim],
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    const run = vi.fn(() => "charged");
    workflow.state("created", async (ctx) => await ctx.step({
      name: STEP_NAME,
      run,
      toState: "charged"
    }));

    await workflow.worker({
      batchSize: 1,
      leaseRenewal: false,
      states: ["created"],
      worker: "worker-1"
    }).runOnce();

    expect(run).not.toHaveBeenCalled();
    expect(commandNames(executor)).toEqual(["FLOW.CLAIM_DUE", "FLOW.RETRY"]);
  });

  it("keeps renewal active through the closure and hands it to the refreshed lease", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const calls: CommandArgument[][] = [];
    let claimed = false;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") {
          if (claimed) return [];
          claimed = true;
          return compactClaims([claim("order-1", lease1, 7, "created")]);
        }
        if (args[0] === "FLOW.EXTEND_LEASE") {
          if (args.includes("RETURN")) return Buffer.from("OK");
          return flowRecord("order-1", lease1, 7, "created");
        }
        if (args[0] === "FLOW.STEP_CONTINUE") return compactClaim("order-1", lease2, 8);
        return Buffer.from("OK");
      }
    };
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => {
      const applied = await ctx.step({
        name: STEP_NAME,
        run: async () => await delay(8).then(() => "charged"),
        toState: "charged"
      });
      await delay(8);
      return applied;
    });

    await workflow.worker({
      batchSize: 1,
      leaseMs: 20,
      leaseRenewIntervalMs: 1,
      profile: "throughput",
      states: ["created"],
      worker: "worker-1"
    }).runOnce();

    const commitIndex = calls.findIndex((call) => call[0] === "FLOW.STEP_CONTINUE");
    const releaseIndex = calls.findIndex((call) => call[0] === "FLOW.TRANSITION");
    const renewalTokens = calls.flatMap((call, index) =>
      call[0] === "FLOW.EXTEND_LEASE" && call.includes("RETURN")
        ? [{ index, token: call[2] }]
        : []
    );
    expect(renewalTokens.some(({ index, token }) => index < commitIndex && Buffer.from(token as Buffer).equals(lease1))).toBe(true);
    expect(renewalTokens.some(({ index, token }) => index > commitIndex && index < releaseIndex && Buffer.from(token as Buffer).equals(lease2))).toBe(true);
    expect(renewalTokens.some(({ index, token }) => index > commitIndex && Buffer.from(token as Buffer).equals(lease1))).toBe(false);
  });

  it("uses the normal exception policy when the closure fails before commit", async () => {
    const lease = Buffer.from("lease-1");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease, 7, "created")]),
      flowRecord("order-1", lease, 7, "created"),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => await ctx.step({
      name: STEP_NAME,
      run: () => { throw new Error("provider failed"); },
      toState: "charged"
    }));

    await worker(workflow).runOnce();

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.RETRY"
    ]);
    expect(executor.calls[2]?.slice(0, 5)).toEqual([
      "FLOW.RETRY", "order-1", lease, "FENCING", 7
    ]);
  });

  it("does not issue a fallback stale write when the atomic commit response fails", async () => {
    const lease = Buffer.from("lease-1");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease, 7, "created")]),
      flowRecord("order-1", lease, 7, "created"),
      new Error("connection closed after write")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => await ctx.step({
      name: STEP_NAME,
      run: () => "charged",
      toState: "charged"
    }));

    await expect(worker(workflow).runOnce()).rejects.toThrow("connection closed after write");

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"
    ]);
  });

  it("applies the exception policy after a definite continuation rejection", async () => {
    const lease = Buffer.from("lease-1");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease, 7, "created")]),
      new FlowWrongStateError("FLOW wrong state"),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => await ctx.advance("charged"));

    await expect(worker(workflow).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.STEP_CONTINUE", "FLOW.RETRY"
    ]);
    expect(executor.calls[2]?.slice(0, 5)).toEqual([
      "FLOW.RETRY", "order-1", lease, "FENCING", 7
    ]);
  });

  it("drains unrelated jobs before surfacing a committed response-loss error", async () => {
    const calls: CommandArgument[][] = [];
    let continuationCommitted = false;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") {
          return compactClaims([
            claim("durable", Buffer.from("lease-d1"), 7, "created"),
            claim("ordinary", Buffer.from("lease-o1"), 9, "created")
          ]);
        }
        if (args[0] === "FLOW.STEP_CONTINUE") {
          continuationCommitted = true;
          throw new ConnectionClosedError("possibly_sent");
        }
        return Buffer.from("OK");
      }
    };
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => ctx.id === "durable"
      ? await ctx.advance("charged")
      : complete({ result: "ordinary-done" }));

    await expect(workflow.worker({
      batchSize: 2,
      concurrency: 1,
      leaseRenewal: false,
      profile: "throughput",
      states: ["created"],
      worker: "worker-1"
    }).runOnce()).rejects.toThrow(ConnectionClosedError);

    expect(continuationCommitted).toBe(true);
    expect(calls.map((call) => call[0])).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.STEP_CONTINUE", "FLOW.COMPLETE"
    ]);
    expect(calls.some((call) => call[0] === "FLOW.RETRY" || call[0] === "FLOW.FAIL")).toBe(false);
    expect(calls.find((call) => call[0] === "FLOW.COMPLETE")?.slice(0, 5)).toEqual([
      "FLOW.COMPLETE", "ordinary", Buffer.from("lease-o1"), "FENCING", 9
    ]);
  });

  it("applies post-step handler errors with the refreshed claim", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      compactClaims([claim("order-1", lease1, 7, "created")]),
      compactClaim("order-1", lease2, 8),
      Buffer.from("OK")
    ]);
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => {
      await ctx.advance("charged");
      throw new Error("handler failed after advance");
    });

    await worker(workflow).runOnce();

    expect(commandNames(executor)).toEqual([
      "FLOW.CLAIM_DUE", "FLOW.STEP_CONTINUE", "FLOW.RETRY"
    ]);
    expect(executor.calls[2]?.slice(0, 5)).toEqual([
      "FLOW.RETRY", "order-1", lease2, "FENCING", 8
    ]);
  });

  it("handles durable and ordinary jobs in the same claimed batch", async () => {
    const calls: CommandArgument[][] = [];
    const claims = compactClaims([
      claim("durable", Buffer.from("lease-d1"), 7, "created"),
      claim("ordinary", Buffer.from("lease-o1"), 9, "created")
    ]);
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.CLAIM_DUE") return claims;
        if (args[0] === "FLOW.STEP_CONTINUE") {
          return compactClaim("durable", Buffer.from("lease-d2"), 8);
        }
        return Buffer.from("OK");
      }
    };
    const workflow = workflowFor(executor);
    workflow.state("created", async (ctx) => ctx.id === "durable"
      ? await ctx.advance("charged")
      : complete({ result: "ordinary-done" }));

    await workflow.worker({
      batchSize: 2,
      concurrency: 2,
      leaseRenewal: false,
      profile: "throughput",
      states: ["created"],
      worker: "worker-1"
    }).runOnce();

    expect(calls.filter((call) => call[0] === "FLOW.STEP_CONTINUE")).toHaveLength(1);
    expect(calls.filter((call) => call[0] === "FLOW.TRANSITION")).toHaveLength(1);
    expect(calls.filter((call) => call[0] === "FLOW.COMPLETE")).toHaveLength(1);
    const release = calls.find((call) => call[0] === "FLOW.TRANSITION");
    expect(release).toEqual(expect.arrayContaining([
      "durable", "LEASE_TOKEN", Buffer.from("lease-d2"), "FENCING", 8
    ]));
  });
});

function workflowFor(executor: CommandExecutor): ReturnType<WorkflowClient["workflow"]> {
  return new WorkflowClient(new FerricStoreClient(executor, { codec: new JsonCodec() }))
    .workflow({ type: "order" });
}

function worker(workflow: ReturnType<WorkflowClient["workflow"]>) {
  return workflow.worker({
    batchSize: 1,
    leaseRenewal: false,
    profile: "throughput",
    states: ["created"],
    worker: "worker-1"
  });
}

function claim(id: string, leaseToken: Buffer, fencingToken: number, runState: string) {
  return [Buffer.from(id), Buffer.from("tenant-a"), leaseToken, fencingToken, Buffer.from(runState)];
}

function compactClaims(items: unknown[]): unknown[] {
  return items;
}

function compactClaim(id: string, leaseToken: Buffer, fencingToken: number): unknown[] {
  return [Buffer.from(id), Buffer.from("tenant-a"), leaseToken, fencingToken];
}

function flowRecord(
  id: string,
  leaseToken: Buffer,
  fencingToken: number,
  runState: string,
  valueRefs: Record<string, unknown> = {}
): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", id],
    ["type", "order"],
    ["state", "running"],
    ["run_state", runState],
    ["partition_key", "tenant-a"],
    ["lease_token", leaseToken],
    ["fencing_token", fencingToken],
    ["value_refs", new Map(Object.entries(valueRefs))],
    ["version", fencingToken]
  ]);
}

function commandNames(executor: FakeExecutor): unknown[] {
  return executor.calls.map((call) => call[0]);
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
