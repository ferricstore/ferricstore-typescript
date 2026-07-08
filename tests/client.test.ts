import { describe, expect, it } from "vitest";
import { FerricStoreClient, FlowAlreadyExistsError, JsonCodec, OverloadedError, RoutingTopology, StaleLeaseError } from "../src/index.js";
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

  it("delegates topology helpers through client wrappers", async () => {
    const executor = new FakeExecutor() as FakeExecutor & {
      refreshTopology: () => Promise<RoutingTopology>;
      route: (key: string) => Promise<{
        endpoint: { host: string; nativePort: number; node: string };
        endpointKey: string;
        key: string;
        laneId: number;
        leaderNode: string;
        shard: number;
      }>;
    };
    const topology = RoutingTopology.empty();
    executor.refreshTopology = async () => topology;
    executor.route = async (key: string) => ({
      endpoint: { host: "127.0.0.1", nativePort: 6388, node: "node@local" },
      endpointKey: "127.0.0.1:6388",
      key,
      laneId: 1,
      leaderNode: "node@local",
      shard: 0
    });
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await expect(client.refreshTopology()).resolves.toBe(topology);
    await expect(client.route("tenant-key")).resolves.toMatchObject({ key: "tenant-key", shard: 0 });
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

  it("builds state-scoped Flow mode policies without making FIFO the default", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.installPolicy("order", {
      states: {
        audit: { maxRetries: 2 },
        queued: {
          mode: "fifo",
          retry: { maxRetries: 1, exhaustedTo: "failed" }
        },
        ready: { mode: "parallel" }
      }
    });
    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "audit",
      "MAX_RETRIES",
      2,
      "STATE",
      "queued",
      "MODE",
      "FIFO",
      "MAX_RETRIES",
      1,
      "EXHAUSTED_TO",
      "failed",
      "STATE",
      "ready",
      "MODE",
      "PARALLEL"
    ]);

    await client.installPolicy("order", {
      mode: "fifo",
      retry: { maxRetries: 1, exhaustedTo: "failed" },
      state: "queued"
    });
    expect(executor.calls[1]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "queued",
      "MODE",
      "FIFO",
      "MAX_RETRIES",
      1,
      "EXHAUSTED_TO",
      "failed"
    ]);

    await client.installPolicy("order", { mode: "parallel", state: "ready" });
    expect(executor.calls[2]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "ready",
      "MODE",
      "PARALLEL"
    ]);

    await client.installPolicy("order", { state: "audit" });
    expect(executor.calls[3]).toEqual(["FLOW.POLICY.SET", "order", "STATE", "audit"]);
  });

  it("builds FIFO-compatible partitioned create, transition, and partition-list claims", async () => {
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      Buffer.from("OK"),
      [["order-1", "tenant-a", lease, 17, "queued"]]
    ]);
    const client = new FerricStoreClient(executor);

    await client.create("order-1", {
      nowMs: 100,
      partitionKey: "tenant-a",
      state: "queued",
      type: "order"
    });
    expect(executor.calls[0]).toEqual([
      "FLOW.CREATE",
      "order-1",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "PARTITION",
      "tenant-a",
      "RUN_AT",
      100
    ]);

    await client.transition("order-1", {
      fencingToken: 17,
      fromState: "running",
      leaseToken: lease,
      nowMs: 110,
      partitionKey: "tenant-a",
      toState: "ready"
    });
    expect(executor.calls[1]).toEqual([
      "FLOW.TRANSITION",
      "order-1",
      "running",
      "ready",
      "LEASE_TOKEN",
      lease,
      "FENCING",
      17,
      "NOW",
      110,
      "PARTITION",
      "tenant-a",
      "RUN_AT",
      110
    ]);

    const jobs = await client.claimJobs("order", {
      includeState: true,
      partitionKeys: ["tenant-a", "tenant-b"],
      state: "queued",
      worker: "worker-1"
    });
    expect(executor.calls[2]).toEqual([
      "FLOW.CLAIM_DUE",
      "order",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      100,
      "PARTITIONS",
      2,
      "tenant-a",
      "tenant-b",
      "RETURN",
      "JOBS_COMPACT_STATE"
    ]);
    expect(jobs).toEqual([
      {
        fencingToken: 17,
        id: "order-1",
        leaseToken: lease,
        partitionKey: "tenant-a",
        runState: "queued",
        state: "running",
        type: ""
      }
    ]);
  });

  it("builds FLOW.SEARCH with attributes and state metadata", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "flow-1"],
          ["type", "order"],
          ["state", "queued"],
          ["partition_key", "tenant-a"],
          ["version", 1]
        ])
      ]
    ]);
    const client = new FerricStoreClient(executor);

    const records = await client.search("order", {
      attributes: { tenant: "acme" },
      consistentProjection: true,
      count: 10,
      state: "queued",
      stateMeta: { version: 1 },
      terminalOnly: true
    });

    expect(records[0]).toMatchObject({ id: "flow-1", partitionKey: "tenant-a" });
    expect(executor.calls[0]).toEqual([
      "FLOW.SEARCH",
      "order",
      "COUNT",
      10,
      "STATE",
      "queued",
      "TERMINAL_ONLY",
      "true",
      "CONSISTENT_PROJECTION",
      "true",
      "ATTRIBUTE",
      "tenant",
      "acme",
      "STATE_META",
      "queued",
      { version: 1 }
    ]);

    await expect(client.search("order", { stateMeta: { version: 1 } })).rejects.toThrow(
      "search stateMeta filters require state"
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

  it("builds control-plane helper commands and normalizes responses", async () => {
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([["sdk", true]]),
      Buffer.from("OK"),
      new Map<unknown, unknown>([["prefix", Buffer.from("tenant:")]]),
      [Buffer.from("tenant:")],
      Buffer.from("OK"),
      Buffer.from("OK"),
      new Map<unknown, unknown>([["keys", 10]]),
      new Map<unknown, unknown>([["keys", 1]]),
      new Map<unknown, unknown>([["nodes", 1]]),
      new Map<unknown, unknown>([["keys", 1]]),
      [[Buffer.from("id"), Buffer.from("flow-1")]],
      [[Buffer.from("event"), Buffer.from("created")]]
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.capabilities()).resolves.toEqual({ sdk: true });
    await expect(client.ensureNamespace("tenant:", { durability: "hot" })).resolves.toBe("OK");
    await expect(client.getNamespace("tenant:")).resolves.toEqual({ prefix: "tenant:" });
    await expect(client.listNamespaces()).resolves.toEqual(["tenant:"]);
    await expect(client.deleteNamespace("tenant:")).resolves.toBe("OK");
    await expect(client.setQuota("tenant:", { keys: 10 })).resolves.toBe("OK");
    await expect(client.getQuota("tenant:")).resolves.toEqual({ keys: 10 });
    await expect(client.quotaUsage("tenant:")).resolves.toEqual({ keys: 1 });
    await expect(client.clusterInfo()).resolves.toEqual({ nodes: 1 });
    await expect(client.namespaceUsage("tenant:")).resolves.toEqual({ keys: 1 });
    await expect(client.flowQuery({ type: "order" })).resolves.toEqual([["id", "flow-1"]]);
    await expect(client.flowHistory("flow-1", { partition: "tenant:" })).resolves.toEqual([["event", "created"]]);

    expect(executor.calls).toEqual([
      ["FERRICSTORE.CAPABILITIES"],
      ["FERRICSTORE.NAMESPACE", "ENSURE", "tenant:", "DURABILITY", "hot"],
      ["FERRICSTORE.NAMESPACE", "GET", "tenant:"],
      ["FERRICSTORE.NAMESPACE", "LIST"],
      ["FERRICSTORE.NAMESPACE", "DELETE", "tenant:"],
      ["FERRICSTORE.QUOTA", "SET", "tenant:", "KEYS", 10],
      ["FERRICSTORE.QUOTA", "GET", "tenant:"],
      ["FERRICSTORE.QUOTA", "USAGE", "tenant:"],
      ["FERRICSTORE.TELEMETRY", "CLUSTER_INFO"],
      ["FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", "tenant:"],
      ["FERRICSTORE.TELEMETRY", "FLOW_QUERY", "TYPE", "order"],
      ["FERRICSTORE.TELEMETRY", "FLOW_HISTORY", "flow-1", "PARTITION", "tenant:"]
    ]);
  });

  it("builds invocation helper commands and carries request context", async () => {
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([["name", Buffer.from("send-email")]]),
      new Map<unknown, unknown>([["name", Buffer.from("send-email")]]),
      [new Map<unknown, unknown>([["name", Buffer.from("send-email")]])],
      new Map<unknown, unknown>([["invocation_id", Buffer.from("inv-1")]]),
      new Map<unknown, unknown>([["id", Buffer.from("inv-1")]]),
      [new Map<unknown, unknown>([["scope", Buffer.from("tenant:acme")]])]
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.invocationDefinitionPut({
      acl: { scopeRequired: true },
      name: "send-email"
    })).resolves.toEqual({ name: "send-email" });
    await expect(client.invocationDefinitionGet("send-email")).resolves.toEqual({ name: "send-email" });
    await expect(client.invocationDefinitionList()).resolves.toEqual([{ name: "send-email" }]);
    await expect(client.invocationCreate("send-email", { tenant: "acme" }, {
      context: { subject: "user-1" },
      idempotencyKey: "idem-1",
      requestContext: {
        scopes: ["invocation:create:*"],
        subject: "proxy",
        tenant: "acme"
      }
    })).resolves.toEqual({ invocation_id: "inv-1" });
    await expect(client.invocationGet("inv-1")).resolves.toEqual({ id: "inv-1" });
    await expect(client.invocationPartitionList("send-email", { scope: "tenant:acme" })).resolves.toEqual([
      { scope: "tenant:acme" }
    ]);

    const definitionArg = executor.calls[0]?.[1];
    expect(typeof definitionArg).toBe("string");
    expect(JSON.parse(definitionArg as string)).toEqual({
      acl: { scopeRequired: true },
      name: "send-email"
    });
    expect(executor.calls[3]?.slice(0, 2)).toEqual(["INVOCATION.CREATE", "send-email"]);
    const createArg = executor.calls[3]?.[2];
    expect(typeof createArg).toBe("string");
    expect(JSON.parse(createArg as string)).toEqual({
      attrs: { tenant: "acme" },
      context: { subject: "user-1" },
      idempotency_key: "idem-1"
    });
    expect(executor.calls[3]?.slice(3)).toEqual([
      "REQUEST_CONTEXT",
      {
        scopes: ["invocation:create:*"],
        subject: "proxy",
        tenant: "acme"
      }
    ]);
    expect(executor.calls[5]).toEqual([
      "INVOCATION.PARTITION.LIST",
      "send-email",
      "SCOPE",
      "tenant:acme"
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
