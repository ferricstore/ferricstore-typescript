import { describe, expect, it, vi } from "vitest";
import {
  ConnectionClosedError,
  FerricStoreClient,
  JsonCodec,
  WorkflowClient,
  complete,
  type ClaimedItem,
  type CommandExecutor
} from "../../src/index.js";
import {
  createAndClaim,
  deletePrefixedKeys,
  eventually,
  integrationClient,
  integrationExecutor,
  suffix
} from "./live-support.js";

const STEP_NAME = "charge-customer:v1";
const STEP_VALUE_NAME =
  "__ferricstore_step__:sha256:ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7";

describe("durable step integration", () => {
  it("recovers a stopped WorkflowWorker without duplicating an idempotent effect", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-worker-recovery-${runId}`;
    const id = `ts-sdk:worker-recovery:${runId}`;
    const partitionKey = `${id}:partition`;
    const workflow = new WorkflowClient(client).workflow({ type });
    const effects = new Map<string, { receipt: string }>();
    const idempotencyKey = `${id}:${STEP_NAME}`;
    let attempts = 0;
    let providerCalls = 0;
    let staleWorkerA: ClaimedItem | undefined;
    let workerBLease: Buffer | undefined;
    let workerBFencing: bigint | undefined;
    const charge = (): { receipt: string } => {
      providerCalls += 1;
      const previous = effects.get(idempotencyKey);
      if (previous != null) return previous;
      const receipt = { receipt: `receipt-${runId}` };
      effects.set(idempotencyKey, receipt);
      return receipt;
    };

    workflow.state("created", async (ctx) => {
      attempts += 1;
      if (attempts === 1) {
        staleWorkerA = claimSnapshot(ctx);
        return await ctx.step({
          leaseMs: 25,
          name: STEP_NAME,
          run: () => {
            charge();
            throw new Error("worker A stopped before committing");
          },
          toState: "charged"
        });
      }
      workerBLease = ctx.leaseToken;
      workerBFencing = BigInt(ctx.fencingToken);
      return await ctx.step({
        leaseMs: 2_000,
        name: STEP_NAME,
        run: charge,
        toState: "charged"
      });
    }, { exceptionPolicy: "raise" });

    try {
      const now = Date.now();
      await workflow.start(id, { nowMs: now, partitionKey, runAtMs: now, state: "created" });
      await expect(workflow.worker({
        leaseMs: 25,
        leaseRenewal: false,
        partitionKey,
        profile: "throughput",
        states: ["created"],
        worker: "worker-a"
      }).runOnce()).rejects.toThrow("worker A stopped before committing");

      expect((await client.get(id, { partitionKey }))?.runState).toBe("created");
      await delay(80);
      await expect(workflow.worker({
        leaseMs: 2_000,
        leaseRenewal: false,
        partitionKey,
        profile: "throughput",
        reclaimExpired: true,
        reclaimRatio: 1,
        states: ["created"],
        worker: "worker-b"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      expect(staleWorkerA).toBeDefined();
      expect(workerBLease).toBeDefined();
      expect(workerBLease).not.toEqual(staleWorkerA?.leaseToken);
      expect(workerBFencing).toBeGreaterThan(BigInt(staleWorkerA?.fencingToken ?? 0));
      expect(providerCalls).toBe(2);
      expect(effects.size).toBe(1);
      expect((await client.get(id, { partitionKey }))?.state).toBe("charged");
      if (staleWorkerA == null) throw new Error("worker A claim was not captured");
      await expect(client.advance(staleWorkerA, { toState: "stale-write" }))
        .rejects.toThrow(/stale|lease|fencing|state/iu);
    } finally {
      await deletePrefixedKeys(client, id).catch(() => undefined);
      await client.close();
    }
  });

  it("releases a waiting workflow and resumes it on any available worker", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-worker-wait-${runId}`;
    const id = `ts-sdk:worker-wait:${runId}`;
    const partitionKey = `${id}:partition`;
    const workflow = new WorkflowClient(client).workflow({ type });
    const prepare = vi.fn(() => ({ prepared: true }));
    let waitingLease: Buffer | undefined;
    let waitingFence: bigint | undefined;
    let resumedLease: Buffer | undefined;
    let resumedFence: bigint | undefined;

    workflow.state("created", async (ctx) => {
      const applied = await ctx.step({
        name: STEP_NAME,
        run: prepare,
        toState: "waiting"
      });
      waitingLease = Buffer.from(applied.job.leaseToken);
      waitingFence = BigInt(applied.job.fencingToken);
      return applied;
    });
    workflow.state("ready", async (ctx) => {
      resumedLease = ctx.leaseToken;
      resumedFence = BigInt(ctx.fencingToken);
      expect(await ctx.value(STEP_VALUE_NAME)).toEqual({ prepared: true });
      return complete({ result: { resumed: true } });
    });

    try {
      const now = Date.now();
      await workflow.start(id, { nowMs: now, partitionKey, runAtMs: now, state: "created" });
      await expect(workflow.worker({
        leaseRenewal: false,
        partitionKey,
        profile: "throughput",
        states: ["created"],
        worker: "worker-a"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      const waiting = await client.get(id, { partitionKey });
      expect(waiting).toMatchObject({ state: "waiting" });
      expect(waiting?.leaseToken.byteLength).toBe(0);
      await client.signal(id, {
        ifState: "waiting",
        partitionKey,
        signal: "approved",
        transitionTo: "ready"
      });
      await expect(workflow.worker({
        leaseRenewal: false,
        partitionKey,
        states: ["ready"],
        worker: "worker-b"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      expect(waitingLease).toBeDefined();
      expect(resumedLease).toBeDefined();
      expect(resumedLease).not.toEqual(waitingLease);
      expect(resumedFence).toBeGreaterThan(waitingFence ?? 0n);
      expect(prepare).toHaveBeenCalledOnce();
      await expect(eventually(
        async () => (await client.get(id, { partitionKey }))?.state,
        (state) => state === "completed",
        "resumed waiting workflow did not complete"
      )).resolves.toBe("completed");
    } finally {
      await deletePrefixedKeys(client, id).catch(() => undefined);
      await client.close();
    }
  });

  it("reclaims live server state after a committed response is deliberately lost", async () => {
    const base = await integrationExecutor();
    let discardContinuationResponse = true;
    const lossy: CommandExecutor = {
      async close(): Promise<void> {
        await base.close();
      },
      async executeCommand(...args): Promise<unknown> {
        const response = await base.executeCommand(...args);
        if (args[0] === "FLOW.STEP_CONTINUE" && discardContinuationResponse) {
          discardContinuationResponse = false;
          throw new ConnectionClosedError("possibly_sent");
        }
        return response;
      }
    };
    const client = new FerricStoreClient(lossy, { codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-worker-loss-${runId}`;
    const id = `ts-sdk:worker-loss:${runId}`;
    const partitionKey = `${id}:partition`;
    const workflow = new WorkflowClient(client).workflow({ type });
    let staleWorkerA: ClaimedItem | undefined;
    let workerBLease: Buffer | undefined;
    let workerBFencing: bigint | undefined;

    workflow.state("created", async (ctx) => {
      staleWorkerA = claimSnapshot(ctx);
      return await ctx.advance("charged", { leaseMs: 25 });
    }, { exceptionPolicy: "raise" });
    workflow.state("charged", async (ctx) => {
      workerBLease = ctx.leaseToken;
      workerBFencing = BigInt(ctx.fencingToken);
      if (staleWorkerA == null) throw new Error("worker A claim was not captured");
      await expect(client.advance(staleWorkerA, { toState: "stale-write" }))
        .rejects.toThrow(/stale|lease|fencing|state/iu);
      return complete({ result: { recovered: true } });
    });

    try {
      const now = Date.now();
      await workflow.start(id, { nowMs: now, partitionKey, runAtMs: now, state: "created" });
      await expect(workflow.worker({
        leaseMs: 25,
        leaseRenewal: false,
        partitionKey,
        profile: "throughput",
        states: ["created"],
        worker: "worker-a"
      }).runOnce()).rejects.toThrow(ConnectionClosedError);
      expect(await client.get(id, { partitionKey })).toMatchObject({
        runState: "charged",
        state: "running"
      });

      await delay(80);
      await expect(workflow.worker({
        leaseMs: 2_000,
        leaseRenewal: false,
        partitionKey,
        profile: "throughput",
        reclaimExpired: true,
        reclaimRatio: 1,
        states: ["charged"],
        worker: "worker-b"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      expect(workerBLease).toBeDefined();
      expect(workerBLease).not.toEqual(staleWorkerA?.leaseToken);
      expect(workerBFencing).toBeGreaterThan(BigInt(staleWorkerA?.fencingToken ?? 0));
      expect((await client.get(id, { partitionKey }))?.state).toBe("completed");
    } finally {
      await deletePrefixedKeys(client, id).catch(() => undefined);
      await client.close();
    }
  });

  it("hands a context step from worker A to worker B without a stale worker write", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-worker-step-${runId}`;
    const id = `ts-sdk:worker-step:${runId}`;
    const partitionKey = `ts-sdk:worker-step:partition:${runId}`;
    const now = Date.now();
    const workflow = new WorkflowClient(client).workflow({ type });
    const run = vi.fn(async () => ({ receipt: `receipt-${runId}` }));
    let workerAInitialLease: Buffer | undefined;
    let workerARefreshedLease: Buffer | undefined;
    let workerAFencing: bigint | undefined;
    let workerBLease: Buffer | undefined;
    let workerBFencing: bigint | undefined;

    workflow.state("created", async (ctx) => {
      workerAInitialLease = ctx.leaseToken;
      const applied = await ctx.step({
        name: STEP_NAME,
        run,
        toState: "charged"
      });
      workerARefreshedLease = Buffer.from(applied.job.leaseToken);
      workerAFencing = BigInt(applied.job.fencingToken);
      return applied;
    });
    workflow.state("charged", async (ctx) => {
      workerBLease = ctx.leaseToken;
      workerBFencing = BigInt(ctx.fencingToken);
      expect(await ctx.value(STEP_VALUE_NAME)).toEqual({ receipt: `receipt-${runId}` });
      return complete({ result: { done: true } });
    });

    try {
      await workflow.start(id, {
        nowMs: now,
        partitionKey,
        runAtMs: now,
        state: "created"
      });
      await expect(workflow.worker({
        leaseMs: 2_000,
        nowMs: now + 1,
        partitionKey,
        profile: "throughput",
        states: ["created"],
        worker: "worker-a"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      expect(run).toHaveBeenCalledOnce();
      expect(workerAInitialLease).toBeDefined();
      expect(workerARefreshedLease).toBeDefined();
      expect(workerARefreshedLease).not.toEqual(workerAInitialLease);
      expect((await client.get(id, { partitionKey }))?.state).toBe("charged");

      await expect(workflow.worker({
        leaseMs: 2_000,
        nowMs: now + 10_000,
        partitionKey,
        states: ["charged"],
        worker: "worker-b"
      }).runOnce()).resolves.toMatchObject({ applied: 1, claimed: 1 });

      expect(workerBLease).toBeDefined();
      expect(workerBLease).not.toEqual(workerARefreshedLease);
      expect(workerBFencing).toBeGreaterThan(workerAFencing ?? 0n);
      expect(run).toHaveBeenCalledOnce();
      expect((await client.get(id, {
        partitionKey,
        values: [STEP_VALUE_NAME]
      }))?.values?.[STEP_VALUE_NAME]).toEqual({ receipt: `receipt-${runId}` });
    } finally {
      await deletePrefixedKeys(client, id).catch(() => undefined);
      await client.close();
    }
  });

  it("journals, replays, renews, and rejects the stale pre-step claim", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-durable-step-${runId}`;
    const created = await createAndClaim(client, type, runId, "durable-step", {
      leaseMs: 10,
      nowMs: 1_000,
      state: "created"
    });
    const run = vi.fn(async () => ({ receipt: `receipt-${runId}` }));

    try {
      const stepped = await client.step(created.job, {
        leaseMs: 10,
        name: STEP_NAME,
        nowMs: 1_001,
        run,
        toState: "charged"
      });

      expect(run).toHaveBeenCalledOnce();
      expect(stepped.result).toEqual({ receipt: `receipt-${runId}` });
      expect(stepped.job.runState).toBe("charged");
      expect(stepped.job.leaseToken).not.toEqual(created.job.leaseToken);
      expect(BigInt(stepped.job.fencingToken)).toBeGreaterThan(BigInt(created.job.fencingToken));

      await expect(client.advance(created.job, { nowMs: 1_002, toState: "invalid-stale-write" }))
        .rejects.toThrow(/stale|lease|fencing/iu);

      const reclaimed = await client.reclaim(type, {
        jobOnly: true,
        leaseMs: 1_000,
        limit: 1,
        nowMs: 2_000,
        partitionKey: created.partitionKey,
        worker: "worker-b"
      });
      const workerBJob = reclaimed[0];
      if (workerBJob == null || !("leaseToken" in workerBJob)) {
        throw new Error("worker B did not acquire a compact claim");
      }
      expect(workerBJob.runState).toBe("charged");
      expect(workerBJob.leaseToken).not.toEqual(stepped.job.leaseToken);
      expect(BigInt(workerBJob.fencingToken)).toBeGreaterThan(BigInt(stepped.job.fencingToken));
      await expect(client.advance(stepped.job, {
        nowMs: 2_001,
        toState: "stale-worker-a-write"
      })).rejects.toThrow(/stale|lease|fencing/iu);

      const replay = await client.step(workerBJob, {
        leaseMs: 1_000,
        name: STEP_NAME,
        nowMs: 2_002,
        run: () => { throw new Error("committed closure ran twice"); },
        toState: "charged"
      });
      expect(replay.result).toEqual(stepped.result);

      const stored = await client.get(created.id, {
        partitionKey: created.partitionKey,
        values: [STEP_VALUE_NAME]
      });
      expect(stored?.values?.[STEP_VALUE_NAME]).toEqual(stepped.result);

      const finished = await client.advance(replay.job, { nowMs: 2_003, toState: "finished" });
      expect(finished.runState).toBe("finished");
      expect(BigInt(finished.fencingToken)).toBeGreaterThan(BigInt(replay.job.fencingToken));
      await client.complete(created.id, {
        fencingToken: finished.fencingToken,
        leaseToken: finished.leaseToken,
        partitionKey: created.partitionKey,
        result: { done: true }
      });
    } finally {
      await deletePrefixedKeys(client, created.id).catch(() => undefined);
      await client.close();
    }
  });

  it("retries an uncommitted closure after takeover without duplicating an idempotent effect", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-durable-retry-${runId}`;
    const created = await createAndClaim(client, type, runId, "durable-retry", {
      leaseMs: 10,
      nowMs: 3_000,
      state: "created"
    });
    const effects = new Map<string, { receipt: string }>();
    const idempotencyKey = `${created.id}:${STEP_NAME}`;
    let providerCalls = 0;
    const charge = (): { receipt: string } => {
      providerCalls += 1;
      const existing = effects.get(idempotencyKey);
      if (existing != null) return existing;
      const createdEffect = { receipt: `receipt-${runId}` };
      effects.set(idempotencyKey, createdEffect);
      return createdEffect;
    };

    try {
      await expect(client.step(created.job, {
        leaseMs: 10,
        name: STEP_NAME,
        nowMs: 3_001,
        run: () => {
          charge();
          throw new Error("worker A stopped before committing");
        },
        toState: "charged"
      })).rejects.toThrow("worker A stopped");

      const reclaimed = await client.reclaim(type, {
        jobOnly: true,
        leaseMs: 1_000,
        limit: 1,
        nowMs: 4_000,
        partitionKey: created.partitionKey,
        worker: "worker-b"
      });
      const workerBJob = reclaimed[0];
      if (workerBJob == null || !("leaseToken" in workerBJob)) {
        throw new Error("worker B did not acquire the expired claim");
      }
      expect(workerBJob.leaseToken).not.toEqual(created.job.leaseToken);
      expect(BigInt(workerBJob.fencingToken)).toBeGreaterThan(BigInt(created.job.fencingToken));

      const stepped = await client.step(workerBJob, {
        leaseMs: 1_000,
        name: STEP_NAME,
        nowMs: 4_001,
        run: charge,
        toState: "charged"
      });

      expect(stepped.result).toEqual({ receipt: `receipt-${runId}` });
      expect(stepped.job.runState).toBe("charged");
      expect(providerCalls).toBe(2);
      expect(effects.size).toBe(1);
      await expect(client.advance(created.job, {
        nowMs: 4_002,
        toState: "stale-worker-a-write"
      })).rejects.toThrow(/stale|lease|fencing/iu);
      await client.complete(created.id, {
        fencingToken: stepped.job.fencingToken,
        leaseToken: stepped.job.leaseToken,
        nowMs: 4_003,
        partitionKey: created.partitionKey,
        result: { done: true }
      });
    } finally {
      await deletePrefixedKeys(client, created.id).catch(() => undefined);
      await client.close();
    }
  });
});

function claimSnapshot(ctx: {
  readonly fencingToken: number | bigint;
  readonly id: string;
  readonly leaseToken: Buffer;
  readonly logicalState: string;
  readonly partitionKey?: string;
  readonly state: string;
  readonly type: string;
}): ClaimedItem {
  return {
    fencingToken: ctx.fencingToken,
    id: ctx.id,
    leaseToken: ctx.leaseToken,
    partitionKey: ctx.partitionKey,
    runState: ctx.logicalState,
    state: ctx.state,
    type: ctx.type
  };
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
