import { expect, it } from "vitest";
import {
  FerricStoreClient,
  JsonCodec,
  QueueClient,
  RawCodec,
  WorkflowClient,
  complete,
  transition
} from "../../src/index.js";
import {
  deletePrefixedKeys,
  field,
  isReadonlyArray,
  suffix,
  text,
  url
} from "./live-support.js";

export function registerGovernanceWorkflowIntegrationTests(): void {
  it("covers fused Flow, schedule, query, and governance helpers", async () => {
    const flow = await FerricStoreClient.fromUrl(url(), { codec: new JsonCodec() });
    const runId = suffix();
    const now = Date.now();
    const type = `ts-sdk-admin-${runId}`;
    const partitionKey = `ts-sdk:admin:${runId}`;

    try {
      await flow.installPolicy(type, { indexedAttributes: ["tenant"] });
      await flow.create(`ts-sdk:admin-attribute:${runId}`, {
        attributes: { tenant: "acme" },
        nowMs: now,
        partitionKey,
        state: "attribute-test",
        type
      });
      await expect(flow.stats(type, {
        attributes: { tenant: "acme" },
        consistentProjection: true,
        partitionKey,
        state: "attribute-test"
      })).resolves.toBeTypeOf("object");
      await expect(flow.attributes(type, {
        consistentProjection: true, partitionKey, state: "attribute-test"
      })).resolves.toBeInstanceOf(Array);
      await expect(flow.attributeValues(type, "tenant", {
        consistentProjection: true, partitionKey, state: "attribute-test"
      })).resolves.toBeInstanceOf(Array);

      const id = `ts-sdk:admin-flow:${runId}`;
      const started = await flow.startAndClaim(id, {
        attributes: { tenant: "acme" },
        initialState: "step-a",
        nowMs: now + 1,
        partitionKey,
        payload: { step: "a" },
        type,
        worker: "ts-sdk-admin-worker"
      });
      const continued = await flow.stepContinue(id, {
        attributesMerge: { processor: "payments-v2" },
        fencingToken: started.fencingToken,
        fromState: "step-a",
        leaseToken: started.leaseToken,
        nowMs: now + 2,
        partitionKey,
        payload: { step: "b" },
        returnJob: true,
        toState: "step-b",
        worker: "ts-sdk-admin-worker"
      });

      const effectKey = "charge";
      await expect(flow.effectReserve(id, effectKey, "payment", {
        fencingToken: continued.fencingToken,
        idempotencyKey: `ts-sdk:effect:${runId}`,
        leaseToken: continued.leaseToken,
        nowMs: now + 3,
        operationDigest: "sha256:test",
        partitionKey
      })).resolves.toBeTypeOf("object");
      await expect(flow.effectConfirm(id, effectKey, {
        externalId: `payment:${runId}`,
        fencingToken: continued.fencingToken,
        latencyMs: 2,
        leaseToken: continued.leaseToken,
        nowMs: now + 4,
        partitionKey
      })).resolves.toBeTypeOf("object");
      await expect(flow.effectGet(id, effectKey, { partitionKey })).resolves.toBeTypeOf("object");
      await expect(flow.governanceLedger(id, { limit: 10, partitionKey })).resolves.toBeInstanceOf(Array);

      await flow.complete(id, {
        fencingToken: continued.fencingToken,
        leaseToken: continued.leaseToken,
        nowMs: now + 5,
        partitionKey,
        result: { done: true }
      });
      await expect(flow.history(id, {
        consistentProjection: true,
        fromVersion: 1,
        includeCold: true,
        partitionKey,
        payloadMaxBytes: 64_000,
        values: true
      })).resolves.toBeInstanceOf(Array);

      await expect(flow.runStepsMany([`ts-sdk:run-steps:${runId}`], {
        nowMs: now + 6,
        partitionKey,
        result: { done: true },
        states: ["queued", "done"],
        type,
        worker: "ts-sdk-admin-worker"
      })).resolves.toBeDefined();

      const scheduleId = `ts-sdk:schedule:${runId}`;
      await expect(flow.scheduleCreate(scheduleId, {
        atMs: now + 60_000,
        kind: "one_shot",
        nowMs: now + 7,
        overwrite: true,
        target: {
          id: `ts-sdk:scheduled-flow:${runId}`,
          partition_key: partitionKey,
          state: "scheduled",
          type
        }
      })).resolves.toBeTypeOf("object");
      await expect(flow.scheduleGet(scheduleId)).resolves.toBeTypeOf("object");
      await expect(flow.schedulePause(scheduleId, { nowMs: now + 8 })).resolves.toBeTypeOf("object");
      await expect(flow.scheduleResume(scheduleId, { nowMs: now + 9 })).resolves.toBeTypeOf("object");
      await expect(flow.scheduleList({ count: 10 })).resolves.toBeInstanceOf(Array);
      await expect(flow.scheduleFireDue({ limit: 1, nowMs: now + 10 })).resolves.toBeTypeOf("object");
      await expect(flow.scheduleDelete(scheduleId, { nowMs: now + 11 })).resolves.toBeTypeOf("object");

      const approvalId = `ts-sdk:approval:${runId}`;
      await expect(flow.approvalRequest(approvalId, {
        assignees: ["ops"],
        flowId: id,
        nowMs: now + 12,
        requestedBy: "integration",
        scope: `approval:${runId}`
      })).resolves.toBeTypeOf("object");
      await expect(flow.approvalApprove(approvalId, {
        approver: "ops", nowMs: now + 13
      })).resolves.toBeTypeOf("object");

      const circuitScope = `circuit:${runId}`;
      await expect(flow.circuitOpen(circuitScope, {
        nowMs: now + 14, openMs: 1_000
      })).resolves.toBeTypeOf("object");
      await expect(flow.circuitClose(circuitScope, {
        nowMs: now + 15
      })).resolves.toBeTypeOf("object");

      const budgetScope = `budget:${runId}`;
      const reservationId = `reservation:${runId}`;
      await expect(flow.budgetReserve(budgetScope, 5, {
        limit: 100, nowMs: now + 16, reservationId, windowMs: 60_000
      })).resolves.toBeTypeOf("object");
      await expect(flow.budgetCommit(budgetScope, reservationId, 4, {
        nowMs: now + 17, usage: { tokens: 4 }
      })).resolves.toBeTypeOf("object");

      const limitScope = `limit:${runId}`;
      await expect(flow.limitLease(limitScope, {
        amount: 5, limit: 10, nowMs: now + 18, shardId: 0, ttlMs: 30_000
      })).resolves.toBeTypeOf("object");
      const spentLimit = await flow.limitSpend(limitScope, {
        amount: 2, nowMs: now + 19, shardId: 0
      });
      const spentReservationIds = field(spentLimit, "reservation_ids");
      if (!isReadonlyArray(spentReservationIds) || spentReservationIds.length !== 2) {
        throw new Error("FLOW.LIMIT.SPEND did not return one reservation id per credit");
      }
      await expect(flow.limitRelease(limitScope, {
        amount: 1,
        reservationIds: [text(spentReservationIds[0])],
        shardId: 0
      })).resolves.toBeTypeOf("object");
      await expect(flow.governanceOverview({ limit: 10 })).resolves.toBeTypeOf("object");
    } finally {
      await flow.close();
    }
  }, 20_000);

  it("covers queue and workflow wrappers against the live server", async () => {
    const flow = await FerricStoreClient.fromUrl(url(), { codec: new JsonCodec() });
    const runId = suffix();
    const now = Date.now();

    try {
      const queueType = `ts-sdk-queue-${runId}`;
      const queuePartition = `ts-sdk:queue:${runId}:partition`;
      const queue = new QueueClient(flow).queue({ type: queueType, worker: "ts-sdk-queue-worker" });
      await queue.enqueue(`ts-sdk:queue:${runId}`, {
        idempotent: true,
        nowMs: now,
        partitionKey: queuePartition,
        payload: { step: "queued" },
        runAtMs: now
      });
      const queueResult = await queue.worker({ batchSize: 1, nowMs: now + 1, partitionKey: queuePartition, worker: "ts-sdk-queue-worker" }).runOnce((job) => {
        expect(job.payload).toEqual({ step: "queued" });
        return { ok: true };
      });
      expect(queueResult).toEqual({ claimed: 1, completed: 1, failed: 0, retried: 0 });

      const queueManyPartition = `ts-sdk:queue-many:${runId}:partition`;
      await queue.enqueueMany([
        { id: `ts-sdk:queue-many:${runId}:a`, payload: { n: 1 } },
        { id: `ts-sdk:queue-many:${runId}:b`, payload: { n: 2 } }
      ], {
        nowMs: now,
        partitionKey: queueManyPartition,
        runAtMs: now
      });
      const batchResult = await queue.worker({
        batchSize: 2,
        nowMs: now + 2,
        partitionKey: queueManyPartition,
        worker: "ts-sdk-queue-batch-worker"
      }).runBatchOnce((jobs) => {
        expect(jobs).toHaveLength(2);
      });
      expect(batchResult).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });

      const queuePartitionA = `ts-sdk:queue-partition:${runId}:a`;
      const queuePartitionB = `ts-sdk:queue-partition:${runId}:b`;
      await queue.enqueueMany([
        { id: `ts-sdk:queue-partition:${runId}:a`, partitionKey: queuePartitionA },
        { id: `ts-sdk:queue-partition:${runId}:b`, partitionKey: queuePartitionB }
      ], {
        nowMs: now,
        runAtMs: now
      });
      const partitionBatchResult = await queue.worker({
        batchSize: 2,
        nowMs: now + 3,
        worker: "ts-sdk-queue-partition-worker"
      }).runBatchOnceForPartitionKeys((jobs) => {
        expect(jobs.map((job) => job.partitionKey).sort()).toEqual([queuePartitionA, queuePartitionB].sort());
      }, [queuePartitionA, queuePartitionB], { claimCredit: 2 });
      expect(partitionBatchResult).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });

      const queueAsyncPartition = `ts-sdk:queue-async:${runId}:partition`;
      await queue.enqueue(`ts-sdk:queue-async:${runId}`, {
        nowMs: now,
        partitionKey: queueAsyncPartition,
        runAtMs: now
      });
      const asyncWorker = queue.worker({
        batchSize: 1,
        completeAsyncDepth: 1,
        nowMs: now + 4,
        partitionKey: queueAsyncPartition,
        worker: "ts-sdk-queue-async-worker"
      });
      await expect(asyncWorker.runOnce(() => undefined)).resolves.toMatchObject({ claimed: 1, completed: 0 });
      await expect(asyncWorker.flush()).resolves.toBe(1);

      const workflowType = `ts-sdk-workflow-${runId}`;
      const workflow = new WorkflowClient(flow).workflow({
        initialState: "received",
        type: workflowType,
        worker: "ts-sdk-workflow-worker"
      });
      const workflowId = `ts-sdk:workflow:${runId}`;
      const workflowPartition = `${workflowId}:partition`;
      const customerValue = await flow.valuePut(
        { id: "customer-1" },
        { partitionKey: workflowPartition, ttlMs: 60_000 }
      );
      const nullableValue = await flow.valuePut(
        null,
        { partitionKey: workflowPartition, ttlMs: 60_000 }
      );
      const customerRefValue = field(customerValue, "ref");
      const nullableRefValue = field(nullableValue, "ref");
      if (customerRefValue == null || nullableRefValue == null) {
        throw new Error("FLOW.VALUE.PUT did not return workflow value refs");
      }
      const customerRef = text(customerRefValue);
      const nullableRef = text(nullableRefValue);
      workflow
        .state("received", async (ctx) => {
          await expect(ctx.value("missing", "fallback")).resolves.toBe("fallback");
          await expect(ctx.valueMany(["missing"])).resolves.toHaveProperty("missing", undefined);
          await expect(ctx.valueMany(["customer", "nullable"])).resolves.toEqual({
            customer: { id: "customer-1" },
            nullable: null
          });
          await expect(ctx.flow.putValue("handler", { stored: true })).resolves.toBeDefined();
          await expect(ctx.flow.value("missing", "fallback")).resolves.toBe("fallback");
          await expect(ctx.flow.values(["missing"])).resolves.toHaveProperty("missing", undefined);
          return transition("validated", {
            payload: { validated: true },
            runAtMs: now + 1,
            values: { receivedMarker: { ok: true } }
          });
        })
        .state("validated", (ctx) => {
          expect(ctx.values.receivedMarker).toEqual({ ok: true });
          return complete({
            result: { id: ctx.id, done: true },
            values: { completedMarker: { ok: true } }
          });
        }, { claimValues: ["receivedMarker"] });
      expect(workflow.stateNames()).toEqual(["received", "validated"]);
      expect(workflow.stateRegistration("received")).toMatchObject({ name: "received" });

      const workflowManyPartition = `ts-sdk:workflow-many:${runId}:partition`;
      const workflowManyCustomerValue = await flow.valuePut(
        { id: "customer-1" },
        { partitionKey: workflowManyPartition, ttlMs: 60_000 }
      );
      const workflowManyCustomerRefValue = field(workflowManyCustomerValue, "ref");
      if (workflowManyCustomerRefValue == null) {
        throw new Error("FLOW.VALUE.PUT did not return workflow-many value ref");
      }
      const workflowManyCustomerRef = text(workflowManyCustomerRefValue);
      const workflowManyIds = [`ts-sdk:workflow-many:${runId}:a`, `ts-sdk:workflow-many:${runId}:b`];
      await workflow.startMany(workflowManyIds.map((id, index) => ({
        id,
        values: { itemMarker: { index } }
      })), {
        nowMs: now,
        partitionKey: workflowManyPartition,
        runAtMs: now,
        valueRefs: { customer: workflowManyCustomerRef },
        values: { sharedMarker: { ok: true } }
      });
      for (const [index, id] of workflowManyIds.entries()) {
        await expect(workflow.get(id, {
          partitionKey: workflowManyPartition,
          values: ["itemMarker", "sharedMarker", "customer"]
        })).resolves.toMatchObject({
          state: "received",
          values: {
            customer: { id: "customer-1" },
            itemMarker: { index },
            sharedMarker: { ok: true }
          }
        });
      }
      await workflow.start(workflowId, {
        idempotent: true,
        nowMs: now,
        partitionKey: workflowPartition,
        payload: { order: runId },
        runAtMs: now,
        valueRefs: { customer: customerRef, nullable: nullableRef }
      });
      await expect(workflow.worker({ batchSize: 1, nowMs: now + 1, partitionKey: workflowPartition, states: ["received"], worker: "ts-sdk-workflow-worker" }).runOnce()).resolves.toMatchObject({
        claimed: 1,
        applied: 1
      });
      await expect(workflow.worker({ batchSize: 1, nowMs: now + 2, partitionKey: workflowPartition, states: ["validated"], worker: "ts-sdk-workflow-worker" }).runOnce()).resolves.toMatchObject({
        claimed: 1,
        applied: 1
      });
      await expect(workflow.get(workflowId, {
        partitionKey: workflowPartition,
        values: ["completedMarker"]
      })).resolves.toMatchObject({
        state: "completed",
        values: { completedMarker: { ok: true } }
      });
      await expect(workflow.history(workflowId, { count: 5, partitionKey: workflowPartition })).resolves.toBeDefined();
    } finally {
      await flow.close();
    }
  }, 20_000);

  it("auto-batches concurrent safe API calls over the native protocol", async () => {
    const flow = await FerricStoreClient.fromUrl(url(), {
      autoBatch: true,
      codec: new RawCodec()
    });
    const runId = suffix();
    const prefix = `ts-sdk:autobatch:${runId}`;
    const kvA = `${prefix}:a`;
    const kvB = `${prefix}:b`;
    const hashKey = `${prefix}:hash`;
    const setKey = `${prefix}:set`;
    const zsetKey = `${prefix}:zset`;
    const type = `ts-sdk-autobatch-${runId}`;
    const flowA = `${prefix}:flow:a`;
    const flowB = `${prefix}:flow:b`;
    const now = Date.now();

    try {
      await Promise.all([
        flow.kv.set(kvA, Buffer.from("1")),
        flow.kv.set(kvB, Buffer.from("2")),
        flow.hash.hset(hashKey, { field: Buffer.from("value") }),
        flow.sets.sadd(setKey, Buffer.from("member")),
        flow.zset.zadd(zsetKey, [{ member: Buffer.from("member"), score: 1 }])
      ]);

      await expect(flow.kv.get(kvA)).resolves.toEqual(Buffer.from("1"));
      await expect(flow.kv.get(kvB)).resolves.toEqual(Buffer.from("2"));
      await expect(flow.hash.hget(hashKey, "field")).resolves.toEqual(Buffer.from("value"));
      await expect(flow.sets.sismember(setKey, Buffer.from("member"))).resolves.toBe(true);
      expect(Number(await flow.zset.zscore(zsetKey, Buffer.from("member")))).toBe(1);

      await Promise.all([
        flow.create(flowA, {
          nowMs: now,
          partitionKey: flowA,
          runAtMs: now,
          state: "queued",
          type
        }),
        flow.create(flowB, {
          nowMs: now,
          partitionKey: flowB,
          runAtMs: now,
          state: "queued",
          type
        })
      ]);

      const jobs = await flow.claimJobs(type, {
        leaseMs: 30_000,
        limit: 2,
        nowMs: now + 1,
        partitionKeys: [flowA, flowB],
        state: "queued",
        worker: `ts-sdk-autobatch-${runId}`
      });
      expect(jobs).toHaveLength(2);

      await Promise.all(jobs.map((job) => flow.complete(job.id, {
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        nowMs: now + 2,
        partitionKey: job.partitionKey
      })));

      const records = await Promise.all([
        flow.get(flowA, { partitionKey: flowA }),
        flow.get(flowB, { partitionKey: flowB })
      ]);
      expect(records.map((record) => record?.state).sort()).toEqual(["completed", "completed"]);
    } finally {
      await deletePrefixedKeys(flow, prefix);
      await flow.close();
    }
  });
}
