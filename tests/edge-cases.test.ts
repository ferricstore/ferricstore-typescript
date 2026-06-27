import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec, WorkflowClient, fail } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient edge cases", () => {
  it("rejects mutually exclusive claim options before sending a command", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(
      client.claimDue("order", {
        state: "created",
        states: ["charged"],
        worker: "worker-1"
      })
    ).rejects.toThrow("state and states are mutually exclusive");

    await expect(
      client.claimDue("order", {
        partitionKey: "tenant-a",
        partitionKeys: ["tenant-b"],
        state: "created",
        worker: "worker-1"
      })
    ).rejects.toThrow("partitionKey and partitionKeys are mutually exclusive");

    expect(executor.calls).toEqual([]);
  });

  it("rejects empty claim lists before sending a command", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.claimDue("order", { states: [], worker: "worker-1" })).rejects.toThrow(
      "states must be non-empty"
    );
    await expect(
      client.claimDue("order", { partitionKeys: [], state: "created", worker: "worker-1" })
    ).rejects.toThrow("partitionKeys must be non-empty");

    expect(executor.calls).toEqual([]);
  });

  it("rejects reclaim for non-running states", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(
      client.reclaim("order", {
        state: "created" as "running",
        worker: "worker-1"
      })
    ).rejects.toThrow("FLOW.RECLAIM only supports running state");

    expect(executor.calls).toEqual([]);
  });

  it("rejects batch partition mismatches before sending a command", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(
      client.createMany(
        "tenant-a",
        [{ id: "order-1", partitionKey: "tenant-b", payload: "payload" }],
        { type: "order" }
      )
    ).rejects.toThrow("createMany item partitionKey does not match batch partitionKey");

    await expect(
      client.completeMany("tenant-a", [
        {
          fencingToken: 1,
          id: "order-1",
          leaseToken: Buffer.from("lease"),
          partitionKey: "tenant-b",
          state: "created",
          type: "order"
        }
      ])
    ).rejects.toThrow("FLOW.COMPLETE_MANY item partitionKey does not match batch partitionKey");

    expect(executor.calls).toEqual([]);
  });

  it("auto-partitions enqueueMany items with no explicit partition", async () => {
    const executor = new FakeExecutor([Buffer.from("OK"), Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await client.enqueueMany(
      [
        { id: "order-1", payload: "one" },
        { id: "order-2", payload: "two" }
      ],
      { nowMs: 100, type: "order" }
    );

    expect(executor.calls.length).toBeGreaterThan(0);
    for (const call of executor.calls) {
      expect(call[0]).toBe("FLOW.CREATE_MANY");
      expect(String(call[1])).toMatch(/^__flow_auto__:[0-9]+$/u);
      expect(call).toContain("INDEPENDENT");
      expect(call).toContain("true");
    }
  });

  it("builds extended createMany items with named values and value refs", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.createMany(
      "tenant-a",
      [
        {
          id: "order-1",
          payload: { id: 1 },
          valueRefs: { invoice: "ref-1" },
          values: { customer: { id: "c1" } }
        }
      ],
      {
        nowMs: 100,
        type: "order",
        values: { tenant: "tenant-a" }
      }
    );

    expect(executor.calls[0]).toEqual([
      "FLOW.CREATE_MANY",
      "tenant-a",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "RUN_AT",
      100,
      "ITEMS_EXT",
      1,
      "order-1",
      "-",
      Buffer.from('{"id":1}'),
      2,
      "tenant",
      Buffer.from('"tenant-a"'),
      "customer",
      Buffer.from('{"id":"c1"}'),
      1,
      "invoice",
      "ref-1"
    ]);
  });

  it("decodes compact job-only claim responses", async () => {
    const executor = new FakeExecutor([[["order-1", "tenant-a", Buffer.from("lease"), 7, "created"]]]);
    const client = new FerricStoreClient(executor);

    const jobs = await client.claimJobs("order", {
      includeState: true,
      state: "created",
      worker: "worker-1"
    });

    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("JOBS_COMPACT_STATE");
    expect(jobs).toEqual([
      {
        fencingToken: 7,
        id: "order-1",
        leaseToken: Buffer.from("lease"),
        partitionKey: "tenant-a",
        runState: "created",
        state: "running",
        type: ""
      }
    ]);
  });

  it("does not send FLOW.VALUE.MGET for an empty ref list", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.valueMGet([])).resolves.toEqual([]);
    expect(executor.calls).toEqual([]);
  });

  it("decodes FLOW.VALUE.MGET payloads through the configured codec", async () => {
    const executor = new FakeExecutor([[Buffer.from('{"ok":true}'), null]]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.valueMGet(["ref-1", "ref-2"], { maxBytes: 1024 })).resolves.toEqual([
      { ok: true },
      null
    ]);
    expect(executor.calls[0]).toEqual(["FLOW.VALUE.MGET", "ref-1", "ref-2", "MAX_BYTES", 1024]);
  });

  it("backs off and retries overloaded producer writes", async () => {
    const overloaded = new Error("BUSY FerricStore overloaded: retry_after_ms=0 reason=rss_pressure");
    overloaded.name = "ResponseError";
    const executor = new FakeExecutor([overloaded, Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, {
      backpressure: {
        baseDelayMs: 0,
        jitterPct: 0,
        maxDelayMs: 0,
        maxRetries: 1
      }
    });

    await expect(client.enqueue("order-1", { nowMs: 100, type: "order" })).resolves.toEqual(
      Buffer.from("OK")
    );
    expect(executor.calls).toHaveLength(2);
  });
});

describe("Workflow edge cases", () => {
  it("hydrates named value refs lazily through ctx.value", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "inspect"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 9],
          ["value_refs", new Map<unknown, unknown>([["customer", new Map([["ref", "ref-customer"]])]])]
        ])
      ],
      [Buffer.from('{"id":"c1"}')],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor, { codec: new JsonCodec() })).workflow({
      type: "order"
    });

    workflow.state("inspect", async (ctx) => {
      const customer = await ctx.value("customer");
      return fail({ error: customer });
    });

    await workflow.worker({ states: ["inspect"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toEqual(["FLOW.VALUE.MGET", "ref-customer"]);
    expect(executor.calls[2]).toContain("FLOW.FAIL");
    expect(executor.calls[2]).toContainEqual(Buffer.from('{"id":"c1"}'));
  });

  it("applies workflow handler exceptions according to fail policy", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "boom"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 9]
        ])
      ],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor, { codec: new JsonCodec() })).workflow({
      type: "order"
    });

    workflow.state(
      "boom",
      () => {
        throw new Error("bad input");
      },
      { exceptionPolicy: "fail" }
    );

    await workflow.worker({ states: ["boom"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toContain("FLOW.FAIL");
    expect(executor.calls[1]).toContain("ERROR");
    const errorIndex = executor.calls[1]?.indexOf("ERROR") ?? -1;
    const errorPayload = executor.calls[1]?.[errorIndex + 1];
    expect(String(errorPayload)).toContain("bad input");
  });
});
