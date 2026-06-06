import { describe, expect, it } from "vitest";
import { FlowClient, FlowAlreadyExistsError, JsonCodec, OverloadedError, StaleLeaseError } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FlowClient", () => {
  it("builds FLOW.CREATE with explicit state-machine data", async () => {
    const executor = new FakeExecutor();
    const client = new FlowClient(executor, { codec: new JsonCodec() });

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

  it("decodes claimed flow records from RESP maps", async () => {
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
    const client = new FlowClient(executor, { codec: new JsonCodec() });

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
    const client = new FlowClient(executor);

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
    const client = new FlowClient(executor);

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
    const client = new FlowClient(executor);

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
