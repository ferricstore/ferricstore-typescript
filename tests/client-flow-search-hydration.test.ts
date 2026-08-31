import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec } from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient Flow search and claim hydration", () => {
  it("builds FLOW.QUERY with attributes and state metadata", async () => {
    const executor = new FakeExecutor([
      {
        version: "ferric.flow.query.result/v1",
        records: [new Map<unknown, unknown>([
          ["id", "flow-1"],
          ["type", "order"],
          ["state", "queued"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.alloc(0)],
          ["fencing_token", 0],
          ["version", 1]
        ])],
        page: { has_more: false, cursor: null },
        quality: {
          exactness: "projected_exact",
          freshness: "projection_watermark",
          coverage: "complete",
          pagination: "live_seek"
        },
        usage: {
          range_seeks: 1,
          range_pages: 1,
          scanned_entries: 1,
          scanned_bytes: 1,
          hydrated_records: 1,
          residual_checks: 0,
          duplicate_entries: 0,
          result_records: 1,
          response_bytes: 1,
          memory_high_water_bytes: 1,
          wall_time_us: 1
        }
      }
    ]);
    const client = new FerricStoreClient(executor);

    const records = await client.search("order", {
      attributes: { tenant: "acme" },
      count: 10,
      partitionKey: "tenant-a",
      state: "queued",
      stateMeta: { version: 1 },
      terminalOnly: false
    });

    expect(records[0]).toMatchObject({ id: "flow-1", partitionKey: "tenant-a" });
    expect(executor.calls[0]).toEqual([
      "FLOW.QUERY",
      "FQL1",
      "FROM runs WHERE partition_key = @partition_key AND type = @type " +
        "AND state = @state AND attribute['tenant'] = @attribute_0 " +
        "AND state_meta['queued']['version'] = @state_meta_0 " +
        "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS",
      "attribute_0",
      "acme",
      "partition_key",
      "tenant-a",
      "state",
      "queued",
      "state_meta_0",
      1,
      "type",
      "order"
    ]);

    await expect(client.search("order", {
      partitionKey: "tenant-a",
      stateMeta: { version: 1 }
    })).rejects.toThrow(
      "stateMeta filters require state"
    );
  });

  it("decodes claimed flow records from wire maps", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "created"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 7],
          ["version", 2],
          ["payload", Buffer.from('{"amount":42}')]
        ])
      ]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    const records = await client.claimDue("order", {
      leaseMs: 30_000,
      payload: true,
      state: "created",
      worker: "worker-1"
    });

    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      fencingToken: 7,
      id: "order-1",
      partitionKey: "tenant-a",
      payload: { amount: 42 },
      state: "created",
      type: "order",
      version: 2
    });
  });

  it("encodes chainable multi-state compact claims with the count-preserving STATES form", async () => {
    const executor = new FakeExecutor([[[
      Buffer.from("order-1"),
      Buffer.from("tenant-a"),
      Buffer.from("lease-1"),
      7,
      Buffer.from("charged")
    ]]]);
    const client = new FerricStoreClient(executor);

    const jobs = await client.claimJobs("order", {
      states: ["created", "charged"],
      worker: "worker-1"
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.CLAIM_DUE",
      "order",
      "STATES",
      2,
      "created",
      "charged",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      100,
      "RETURN",
      "JOBS_COMPACT_STATE"
    ]);
    expect(jobs[0]?.runState).toBe("charged");
  });

  it("requests full multi-state records in one claim response", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "running"],
          ["run_state", "created"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 7],
          ["version", 2],
          ["payload", Buffer.from("full-payload")]
        ])
      ]
    ]);
    const client = new FerricStoreClient(executor);

    const records = await client.claimDue("order", {
      payload: true,
      states: ["created", "charged"],
      worker: "worker-1"
    });

    expect(records[0]).toMatchObject({
      id: "order-1",
      payload: Buffer.from("full-payload"),
      state: "running"
    });
    expect(executor.calls).toHaveLength(1);
    expect(executor.calls[0]).toEqual([
      "FLOW.CLAIM_DUE",
      "order",
      "STATE",
      "created",
      "STATE",
      "charged",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      1,
      "PAYLOAD"
    ]);
  });

  it("hydrates legacy compact tuples returned for full multi-state claims", async () => {
    const id = Buffer.from("order-1");
    const partition = Buffer.from("tenant-a");
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      [[id, partition, lease, 7, Buffer.from("created"), {}]],
      new Map<unknown, unknown>([
        ["id", id],
        ["type", "order"],
        ["state", "running"],
        ["partition_key", partition],
        ["lease_token", lease],
        ["fencing_token", 7],
        ["version", 2]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    const jobs = await client.claimDue("order", {
      payload: false,
      states: ["created", "charged"],
      valueMaxBytes: 42,
      values: ["profile"],
      worker: "worker-1"
    });

    expect(jobs[0]).toMatchObject({
      fencingToken: 7,
      id: "order-1",
      partitionKey: "tenant-a",
      state: "running"
    });
    expect(executor.calls[1]).toEqual([
      "FLOW.GET",
      "order-1",
      "PARTITION",
      "tenant-a",
      "FULL",
      "false",
      "NOPAYLOAD",
      "VALUE",
      "profile",
      "VALUE_MAX_BYTES",
      42
    ]);
  });

  it("hydrates legacy compact tuples whose ids use Uint8Array", async () => {
    const id = new Uint8Array(Buffer.from("order-typed-array"));
    const partition = new Uint8Array(Buffer.from("tenant-a"));
    const lease = new Uint8Array(Buffer.from("lease"));
    const executor = new FakeExecutor([
      [[id, partition, lease, 7]],
      new Map<unknown, unknown>([
        ["id", id],
        ["type", "order"],
        ["state", "running"],
        ["partition_key", partition],
        ["lease_token", lease],
        ["fencing_token", 7],
        ["version", 2]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    const jobs = await client.claimDue("order", {
      states: ["created", "charged"],
      worker: "worker-1"
    });

    expect(jobs[0]).toMatchObject({
      id: "order-typed-array",
      partitionKey: "tenant-a",
      state: "running"
    });
    expect(executor.calls).toHaveLength(2);
  });

  it("bounds legacy compact-claim hydration concurrency and preserves order", async () => {
    const compact = Array.from({ length: 8 }, (_, index) => [
      `order-${index}`,
      "tenant-a",
      Buffer.from(`lease-${index}`),
      index + 1,
      Buffer.from("created"),
      {}
    ]);
    let active = 0;
    let maxActive = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") return compact;
        if (args[0] !== "FLOW.GET" || typeof args[1] !== "string") {
          throw new Error("unexpected command");
        }
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        active -= 1;
        const index = Number(args[1].split("-")[1]);
        return new Map<unknown, unknown>([
          ["id", args[1]],
          ["type", "order"],
          ["state", "running"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from(`lease-${index}`)],
          ["fencing_token", index + 1],
          ["version", 2]
        ]);
      }
    };
    const client = new FerricStoreClient(executor, {
      legacyClaimHydrationConcurrency: 3
    });

    const jobs = await client.claimDue("order", {
      states: ["created", "charged"],
      worker: "worker-1"
    });

    expect(maxActive).toBe(3);
    expect(jobs.map((job) => job.id)).toEqual(compact.map((item) => item[0]));
  });

});
