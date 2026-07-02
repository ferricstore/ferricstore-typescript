import { describe, expect, it } from "vitest";
import { FerricStoreClient, FlowAlreadyExistsError, JsonCodec, OverloadedError, StaleLeaseError } from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient", () => {
  it("auto-batches concurrent safe commands when enabled", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("SET", "auto-batch:a", "1"),
      client.command("SET", "auto-batch:b", "2")
    ]);

    expect(executor.pipelineCalls).toEqual([
      [
        ["SET", "auto-batch:a", "1"],
        ["SET", "auto-batch:b", "2"]
      ]
    ]);
  });

  it("does not auto-batch blocking claim commands", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("FLOW.CLAIM_DUE", "email", "WORKER", "worker-1"),
      client.command("SET", "auto-batch:claim-safe", "1")
    ]);

    expect(executor.calls[0]).toEqual(["FLOW.CLAIM_DUE", "email", "WORKER", "worker-1"]);
    expect(executor.pipelineCalls).toEqual([[["SET", "auto-batch:claim-safe", "1"]]]);
  });

  it("rejects only the failed promise for auto-batched item errors", async () => {
    const itemError = new Error("ERR item failed");
    const executor: CommandExecutor = {
      async executeCommand() {
        return Buffer.from("OK");
      },
      async executePipeline() {
        return [Buffer.from("OK"), itemError];
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const [first, second] = await Promise.allSettled([
      client.command("SET", "auto-batch:ok", "1"),
      client.command("SET", "auto-batch:error", "2")
    ]);

    expect(first).toMatchObject({ status: "fulfilled", value: Buffer.from("OK") });
    expect(second).toMatchObject({ status: "rejected", reason: itemError });
  });

  it("builds FLOW.CREATE with explicit state-machine data", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.create("order-1", {
      correlationId: "cart-1",
      idempotent: true,
      nowMs: 100,
      partitionKey: "tenant-a",
      payload: { amount: 42 },
      priority: 3,
      runAtMs: 150,
      state: "created",
      type: "order",
      values: { customer: { id: "c1" } }
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.CREATE",
      "order-1",
      "TYPE",
      "order",
      "STATE",
      "created",
      "NOW",
      100,
      "PARTITION",
      "tenant-a",
      "PAYLOAD",
      Buffer.from('{"amount":42}'),
      "CORRELATION_ID",
      "cart-1",
      "RUN_AT",
      150,
      "PRIORITY",
      3,
      "IDEMPOTENT",
      "true",
      "VALUE",
      "customer",
      Buffer.from('{"id":"c1"}')
    ]);
  });

  it("builds flow mutation commands with state metadata", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const lease = Buffer.from("lease");
    const claimed = [{ id: "flow-1", leaseToken: lease, fencingToken: 7, partitionKey: "tenant-a", type: "order", state: "queued" }];
    const fenced = [{ id: "flow-1", leaseToken: lease, fencingToken: 7, partitionKey: "tenant-a" }];

    const expectStateMeta = (value: string): void => {
      const call = executor.calls.at(-1);
      expect(call).toBeDefined();
      const index = call?.indexOf("STATE_META") ?? -1;
      expect(call?.slice(index, index + 3)).toEqual(["STATE_META", "version", value]);
    };

    await client.create("flow-1", { nowMs: 100, stateMeta: { version: "1" }, type: "order" });
    expectStateMeta("1");

    await client.transition("flow-1", {
      fencingToken: 7,
      fromState: "queued",
      leaseToken: lease,
      nowMs: 101,
      stateMeta: { version: "2" },
      toState: "charged"
    });
    expectStateMeta("2");

    await client.complete("flow-1", {
      fencingToken: 7,
      leaseToken: lease,
      stateMeta: { version: "3" }
    });
    expectStateMeta("3");

    await client.retry("flow-1", {
      fencingToken: 7,
      leaseToken: lease,
      stateMeta: { version: "4" }
    });
    expectStateMeta("4");

    await client.fail("flow-1", {
      fencingToken: 7,
      leaseToken: lease,
      stateMeta: { version: "5" }
    });
    expectStateMeta("5");

    await client.cancel("flow-1", {
      fencingToken: 7,
      leaseToken: lease,
      stateMeta: { version: "6" }
    });
    expectStateMeta("6");

    await client.completeMany("tenant-a", claimed, { stateMeta: { version: "7" } });
    expectStateMeta("7");

    await client.transitionMany("tenant-a", {
      fromState: "queued",
      items: fenced,
      stateMeta: { version: "8" },
      toState: "charged"
    });
    expectStateMeta("8");

    await client.retryMany("tenant-a", claimed, { stateMeta: { version: "9" } });
    expectStateMeta("9");

    await client.failMany("tenant-a", claimed, { stateMeta: { version: "10" } });
    expectStateMeta("10");

    await client.cancelMany("tenant-a", fenced, { stateMeta: { version: "11" } });
    expectStateMeta("11");
  });

  it("builds cancelMany without lease tokens", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const lease = Buffer.from("lease");

    await client.cancelMany("tenant-a", [
      { id: "flow-1", leaseToken: lease, fencingToken: 7, partitionKey: "tenant-a" }
    ], { nowMs: 100 });
    expect(executor.calls[0]).toEqual([
      "FLOW.CANCEL_MANY",
      "tenant-a",
      "NOW",
      100,
      "ITEMS",
      "flow-1",
      7
    ]);

    await client.cancelMany(undefined, [
      { id: "flow-2", leaseToken: lease, fencingToken: 8, partitionKey: "tenant-b" }
    ], { nowMs: 101 });
    expect(executor.calls[1]).toEqual([
      "FLOW.CANCEL_MANY",
      "MIXED",
      "NOW",
      101,
      "ITEMS",
      "flow-2",
      "tenant-b",
      8
    ]);
  });

  it("builds createMany with shared state metadata", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.createMany("tenant-a", [
      { id: "flow-1", payload: "one" },
      { id: "flow-2", payload: "two" }
    ], {
      nowMs: 100,
      state: "queued",
      stateMeta: { owner: "risk", version: "1" },
      type: "order"
    });

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
      "STATE_META",
      "owner",
      "risk",
      "STATE_META",
      "version",
      "1",
      "ITEMS",
      "flow-1",
      Buffer.from("one"),
      "flow-2",
      Buffer.from("two")
    ]);
  });

  it("reuses identical createMany item state metadata and rejects mixed state metadata", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.createMany("tenant-a", [
      { id: "flow-1", stateMeta: { version: "1" } },
      { id: "flow-2", stateMeta: { version: "1" } }
    ], {
      nowMs: 100,
      type: "order"
    });

    const call = executor.calls[0];
    const index = call?.indexOf("STATE_META") ?? -1;
    expect(call?.slice(index, index + 3)).toEqual(["STATE_META", "version", "1"]);

    await expect(client.createMany("tenant-a", [
      { id: "flow-3", stateMeta: { version: "1" } },
      { id: "flow-4", stateMeta: { version: "2" } }
    ], {
      nowMs: 100,
      type: "order"
    })).rejects.toThrow("shared stateMeta");
  });

  it("builds policy indexing and decodes state metadata fields", async () => {
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      new Map<unknown, unknown>([
        ["id", "flow-1"],
        ["type", "order"],
        ["state", "completed"],
        ["partition_key", "tenant-a"],
        ["version", 3],
        ["state_meta", new Map<unknown, unknown>([
          ["accept", new Map<unknown, unknown>([["version", "1"]])],
          ["completed", new Map<unknown, unknown>([["version", "3"]])]
        ])],
        ["indexed_state_meta", "version"]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    await client.installPolicy("order", { indexedStateMeta: "version" });
    expect(executor.calls[0]).toEqual(["FLOW.POLICY.SET", "order", "INDEXED_STATE_META", "version"]);

    await expect(client.get("flow-1", { partitionKey: "tenant-a" })).resolves.toMatchObject({
      indexedStateMeta: "version",
      stateMeta: {
        accept: { version: "1" },
        completed: { version: "3" }
      }
    });
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

  it("maps common FerricStore server errors", async () => {
    const alreadyExists = new Error("ERR flow already exists");
    alreadyExists.name = "ResponseError";
    const staleLease = new Error("ERR stale flow lease");
    staleLease.name = "ResponseError";
    const overloaded = new Error("BUSY FerricStore overloaded: retry_after_ms=25 reason=rss_pressure");
    overloaded.name = "ResponseError";
    const executor = new FakeExecutor([alreadyExists, staleLease, overloaded]);
    const client = new FerricStoreClient(executor);

    await expect(client.command("FLOW.CREATE", "f1")).rejects.toBeInstanceOf(FlowAlreadyExistsError);
    await expect(client.command("FLOW.COMPLETE", "f1")).rejects.toBeInstanceOf(StaleLeaseError);
    await expect(client.command("FLOW.CREATE", "f2")).rejects.toMatchObject({
      constructor: OverloadedError,
      reason: "rss_pressure",
      retryAfterMs: 25
    });
  });

  it("builds native admin and cluster helper commands", async () => {
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      Buffer.from("OK"),
      Buffer.from("OK"),
      Buffer.from("OK"),
      Buffer.from("OK"),
      Buffer.from("OK"),
      ["leader", "node-a"],
      "shards: 1",
      "hot: 1",
      Buffer.from("OK"),
      ["status", "ok"]
    ]);
    const client = new FerricStoreClient(executor);

    await client.clusterJoin("node-b", { replace: true });
    await client.clusterLeave();
    await client.clusterFailover(0, "node-b");
    await client.clusterPromote("node-b");
    await client.clusterDemote("node-a");
    await client.ferricstoreConfig("GET", "*");
    await client.clusterRole();
    await client.ferricstoreMetrics();
    await client.ferricstoreHotness();
    await client.ferricstoreBlobgc("RUN");
    await client.ferricstoreDoctor("CHECK");

    expect(executor.calls).toEqual([
      ["CLUSTER.JOIN", "node-b", "REPLACE"],
      ["CLUSTER.LEAVE"],
      ["CLUSTER.FAILOVER", 0, "node-b"],
      ["CLUSTER.PROMOTE", "node-b"],
      ["CLUSTER.DEMOTE", "node-a"],
      ["FERRICSTORE.CONFIG", "GET", "*"],
      ["CLUSTER.ROLE"],
      ["FERRICSTORE.METRICS"],
      ["FERRICSTORE.HOTNESS"],
      ["FERRICSTORE.BLOBGC", "RUN"],
      ["FERRICSTORE.DOCTOR", "CHECK"]
    ]);
  });

  it("builds server helper commands", async () => {
    const executor = new FakeExecutor([
      Buffer.from("PONG"),
      "server info",
      Buffer.from("OK"),
      ["GET", "SET"],
      2,
      Buffer.from("OK"),
      "client info",
      3,
      "user-1"
    ]);
    const client = new FerricStoreClient(executor);

    await client.ping();
    await client.serverInfo("server");
    await client.configSet("maxmemory", "0");
    await client.commandList();
    await client.clientId();
    await client.clientTracking("ON", { bcast: true, prefixes: ["user:"], redirect: 7 });
    await client.clientInfo();
    await client.publish("events", "hello");
    await client.aclWhoami();

    expect(executor.calls).toEqual([
      ["PING"],
      ["INFO", "server"],
      ["CONFIG", "SET", "maxmemory", "0"],
      ["COMMAND", "LIST"],
      ["CLIENT", "ID"],
      ["CLIENT", "TRACKING", "ON", "REDIRECT", 7, "PREFIX", "user:", "BCAST"],
      ["CLIENT", "INFO"],
      ["PUBLISH", "events", "hello"],
      ["ACL", "WHOAMI"]
    ]);
  });
});
