import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  FlowAlreadyExistsError,
  OverloadedError,
  RerouteError,
  StaleLeaseError,
  classifyServerError
} from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient administration and response validation", () => {
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

  it("rejects unsafe string-backed overload retry metadata", () => {
    const safe = classifyServerError("BUSY overloaded", {
      code: "busy",
      retry_after_ms: Buffer.from(String(Number.MAX_SAFE_INTEGER))
    });
    const unsafe = classifyServerError("BUSY overloaded", {
      code: "busy",
      retry_after_ms: Buffer.from("9007199254740992")
    });
    const overflowing = classifyServerError("BUSY overloaded", {
      code: "busy",
      retry_after_ms: Buffer.from("9".repeat(400))
    });

    expect(safe).toMatchObject({ retryAfterMs: Number.MAX_SAFE_INTEGER });
    expect(unsafe).toMatchObject({ retryAfterMs: undefined });
    expect(overflowing).toMatchObject({ retryAfterMs: undefined });
  });

  it("preserves native reroute status and structured codes as typed errors", () => {
    const byStatus = classifyServerError(
      "try another node",
      { code: "reroute", retryable: true, safe_to_retry: true },
      undefined,
      5
    );
    const byCode = classifyServerError("try another node", { code: Buffer.from("reroute") });

    expect(byStatus).toBeInstanceOf(RerouteError);
    expect(byStatus).toMatchObject({ code: "reroute" });
    expect(byCode).toBeInstanceOf(RerouteError);
  });

  it("does not classify BUSYGROUP or unrelated existence conflicts as Flow overload errors", () => {
    const busyGroup = classifyServerError("BUSYGROUP Consumer Group name already exists");
    const existingItem = classifyServerError("ERR item already exists");

    expect(busyGroup.constructor).toBe(FerricStoreError);
    expect(existingItem.constructor).toBe(FerricStoreError);
  });

  it("decodes the flat key/value response returned by FERRICSTORE.KEY_INFO", async () => {
    const executor = new FakeExecutor([[
      "type",
      "string",
      "value_size",
      "3",
      "ttl_ms",
      "-1",
      "hot_cache_status",
      "hot",
      "last_write_shard",
      "2"
    ]]);
    const client = new FerricStoreClient(executor);

    await expect(client.keyInfo("key")).resolves.toEqual({
      hotCacheStatus: "hot",
      lastWriteShard: 2,
      raw: {
        hot_cache_status: "hot",
        last_write_shard: "2",
        ttl_ms: "-1",
        type: "string",
        value_size: "3"
      },
      ttlMs: -1,
      type: "string",
      valueSize: 3
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
    await client.clientInfo();
    await client.publish("events", "hello");
    await client.aclWhoami();

    expect(executor.calls).toEqual([
      ["PING"],
      ["INFO", "server"],
      ["CONFIG", "SET", "maxmemory", "0"],
      ["COMMAND", "LIST"],
      ["CLIENT", "ID"],
      ["CLIENT", "INFO"],
      ["PUBLISH", "events", "hello"],
      ["ACL", "WHOAMI"]
    ]);
  });

  it("rejects unsupported native CLIENT tracking helpers without dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.clientTracking("ON", { optin: true })).rejects.toThrow(
      /CLIENT TRACKING.*not supported.*native protocol/i
    );
    await expect(client.clientCaching("NO")).rejects.toThrow(
      /CLIENT CACHING.*not supported.*native protocol/i
    );
    expect(executor.calls).toEqual([]);
  });

  it("fails closed on malformed success and key-value responses", async () => {
    const executor = new FakeExecutor([
      Buffer.from("NO"),
      [Buffer.from("status")]
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.configSet("maxmemory", "0")).rejects.toThrow(
      "invalid OK response"
    );
    await expect(client.clusterHealth()).rejects.toThrow(
      "invalid key-value response"
    );
  });

  it("preserves exact integers in textual diagnostic responses", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      Buffer.from([
        "safe: 9007199254740991",
        "large: 9007199254740993",
        "negative_large: -9007199254740993",
        "shard_0:",
        "  memory_bytes: 18446744073709551615"
      ].join("\r\n"))
    ]));

    await expect(client.clusterStats()).resolves.toEqual({
      large: 9_007_199_254_740_993n,
      negative_large: -9_007_199_254_740_993n,
      safe: 9_007_199_254_740_991,
      shard_0: { memory_bytes: 18_446_744_073_709_551_615n }
    });
  });

  it("fails closed on malformed textual server responses", async () => {
    const client = new FerricStoreClient(new FakeExecutor(
      Array.from({ length: 6 }, () => ({ unexpected: true }))
    ));

    await expect(client.serverInfo()).rejects.toThrow("INFO returned an invalid text response");
    await expect(client.clientGetName()).rejects.toThrow("CLIENT GETNAME returned an invalid text response");
    await expect(client.clientList()).rejects.toThrow("CLIENT LIST returned an invalid text response");
    await expect(client.lolwut()).rejects.toThrow("LOLWUT returned an invalid text response");
    await expect(client.aclWhoami()).rejects.toThrow("ACL WHOAMI returned an invalid text response");
    await expect(client.ferricstoreMetrics()).rejects.toThrow("FERRICSTORE.METRICS returned an invalid text response");
  });

  it("accepts a Uint8Array OK response", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      new Uint8Array(Buffer.from("OK"))
    ]));

    await expect(client.configSet("maxmemory", "0")).resolves.toBe(true);
  });

  it("returns the Prometheus metrics scrape as text", async () => {
    const scrape = "# HELP ferricstore_connected_clients Connected clients\nferricstore_connected_clients 1\n";
    const client = new FerricStoreClient(new FakeExecutor([Buffer.from(scrape)]));

    await expect(client.ferricstoreMetrics()).resolves.toBe(scrape);
  });

  it("decodes CAS results without JavaScript truthiness", async () => {
    const executor = new FakeExecutor([
      Buffer.from("0"),
      "false",
      0,
      0n,
      false,
      null,
      Buffer.from("1"),
      "true",
      1,
      1n,
      true
    ]);
    const client = new FerricStoreClient(executor);

    for (let index = 0; index < 6; index += 1) {
      await expect(client.cas(`false-${index}`, "old", "new")).resolves.toBe(false);
    }
    for (let index = 0; index < 5; index += 1) {
      await expect(client.cas(`true-${index}`, "old", "new")).resolves.toBe(true);
    }
  });

  it("rejects malformed CAS results", async () => {
    const executor = new FakeExecutor([Buffer.from("OK"), 2]);
    const client = new FerricStoreClient(executor);

    await expect(client.cas("malformed-text", "old", "new")).rejects.toThrow("boolean response");
    await expect(client.cas("malformed-number", "old", "new")).rejects.toThrow("boolean response");
  });
});
