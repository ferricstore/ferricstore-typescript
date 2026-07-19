import { describe, expect, it } from "vitest";
import {
  ClaimHydrationError,
  FerricStoreClient,
  FlowBatchError,
  JsonCodec,
  ReconnectingExecutor,
  StaleLeaseError
} from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import { FakeExecutor, fakeFlowPolicySnapshot } from "./fake-executor.js";

describe("FerricStoreClient Flow and administration", () => {
  it("rejects TTLs on named Flow values before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.valuePut({ stored: true }, {
      name: "handler",
      ownerFlowId: "flow-1",
      partitionKey: "tenant-a",
      ttlMs: 60_000
    })).rejects.toThrow(/named Flow values cannot have a TTL/u);
    expect(executor.calls).toHaveLength(0);
  });

  it("builds FLOW.CREATE with explicit state-machine data", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.create("order-1", {
      attributes: { region: "us-east", tenant: "tenant-a" },
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
      "ATTRIBUTE",
      "region",
      "us-east",
      "ATTRIBUTE",
      "tenant",
      "tenant-a",
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

  it("keeps bigint fencing tokens exact through public mutation APIs", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);
    const fencingToken = BigInt(Number.MAX_SAFE_INTEGER) + 1n;

    await client.complete("flow-1", {
      fencingToken,
      leaseToken: Buffer.from("lease"),
      nowMs: 100
    });

    expect(executor.calls[0]).toContain(fencingToken);
  });

  it("requests OK-only successful retry and fail batches when configured", async () => {
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([Buffer.from("OK"), Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);
    const items = [{
      fencingToken: 7,
      id: "flow-1",
      leaseToken: lease,
      partitionKey: "tenant-a",
      state: "running",
      type: "order"
    }];

    await expect(client.retryMany("tenant-a", items, {
      returnOkOnSuccess: true,
      runAtMs: 500
    })).resolves.toEqual(Buffer.from("OK"));
    await expect(client.failMany("tenant-a", items, {
      returnOkOnSuccess: true
    })).resolves.toEqual(Buffer.from("OK"));

    for (const call of executor.calls) {
      expect(call).toEqual(expect.arrayContaining(["RETURN", "OK_ON_SUCCESS"]));
    }
  });

  it("builds compact attribute claims and Flow attribute mutations", async () => {
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      [[Buffer.from("flow-1"), Buffer.from("tenant-a"), lease, 7, "queued", { phase: "charge" }]],
      Buffer.from("OK"),
      Buffer.from("OK")
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.claimJobs("order", {
      includeAttributes: true,
      includeState: true,
      state: "queued",
      worker: "worker-1"
    })).resolves.toEqual([expect.objectContaining({
      attributes: { phase: "charge" },
      id: "flow-1",
      runState: "queued"
    })]);
    expect(executor.calls[0]).toContain("JOBS_COMPACT_STATE_ATTRS");

    await client.transition("flow-1", {
      attributesDelete: ["obsolete"],
      attributesMerge: { phases: ["charge", "settle"] },
      fencingToken: 7,
      fromState: "queued",
      leaseToken: lease,
      nowMs: 100,
      toState: "charged"
    });
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "ATTRIBUTE_MERGE", "phases", ["charge", "settle"],
      "ATTRIBUTE_DELETE", "obsolete"
    ]));

    await client.cancel("flow-1", {
      attributesDelete: ["temporary"],
      attributesMerge: { phase: "cancelled" },
      fencingToken: 7,
      leaseToken: lease,
      nowMs: 101,
      values: { audit: "cancelled" }
    });
    expect(executor.calls[2]).toEqual(expect.arrayContaining([
      "VALUE", "audit", Buffer.from("cancelled"),
      "ATTRIBUTE_MERGE", "phase", "cancelled",
      "ATTRIBUTE_DELETE", "temporary"
    ]));
  });

  it("completes jobs and claims replacements in one ordered pipeline", async () => {
    const lease = Buffer.from("lease-1");
    const nextLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      [["flow-2", "tenant-a", nextLease, 8]]
    ]);
    const client = new FerricStoreClient(executor);

    const result = await client.completeJobsAndClaimJobs(
      [{ id: "flow-1", leaseToken: lease, fencingToken: 7, partitionKey: "tenant-a", type: "order", state: "queued" }],
      "order",
      {
        jobOnly: true,
        partitionKey: "tenant-a",
        state: "queued",
        worker: "worker-1"
      }
    );

    expect(result).toMatchObject({ fused: true, completion: Buffer.from("OK") });
    expect(result.completionError).toBeUndefined();
    expect(result.claimed).toEqual([
      expect.objectContaining({ id: "flow-2", fencingToken: 8, partitionKey: "tenant-a" })
    ]);
    expect(executor.pipelineCalls).toHaveLength(1);
    expect(executor.pipelineCalls[0]?.map((command) => command[0])).toEqual([
      "FLOW.COMPLETE_MANY",
      "FLOW.CLAIM_DUE"
    ]);
  });

  it("does not report logical pipeline fallbacks as fused and preserves partial outcomes", async () => {
    const calls: string[] = [];
    const claimFailure = new Error("claim unavailable");
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(typeof args[0] === "string" ? args[0] : "");
        if (args[0] === "FLOW.COMPLETE_MANY") return Buffer.from("OK");
        throw claimFailure;
      },
      async executePipeline(): Promise<unknown[]> {
        throw new Error("logical pipeline must not be used for fusion");
      }
    };
    const client = new FerricStoreClient(executor);

    const result = await client.completeJobsAndClaimJobs(
      [{
        id: "flow-1",
        leaseToken: Buffer.from("lease-1"),
        fencingToken: 7,
        partitionKey: "tenant-a",
        type: "order",
        state: "queued"
      }],
      "order",
      { jobOnly: true, partitionKey: "tenant-a", state: "queued", worker: "worker-1" }
    );

    expect(result).toMatchObject({
      claimed: [],
      claimError: claimFailure,
      completion: Buffer.from("OK"),
      fused: false
    });
    expect(calls).toEqual(["FLOW.COMPLETE_MANY", "FLOW.CLAIM_DUE"]);
  });

  it("returns replacement claims alongside a completion item error", async () => {
    const nextLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      [[Buffer.from("error"), Buffer.from("ERR stale flow lease")]],
      [["flow-2", "tenant-a", nextLease, 8]]
    ]);
    const client = new FerricStoreClient(executor);

    const result = await client.completeJobsAndClaimJobs(
      [{ id: "flow-1", leaseToken: Buffer.from("lease-1"), fencingToken: 7, partitionKey: "tenant-a", type: "order", state: "queued" }],
      "order",
      { jobOnly: true, partitionKey: "tenant-a", state: "queued", worker: "worker-1" }
    );

    expect(result.completionError).toBeInstanceOf(Error);
    expect(result.claimed).toEqual([
      expect.objectContaining({ id: "flow-2", fencingToken: 8, partitionKey: "tenant-a" })
    ]);
  });

  it("preserves compact replacement leases when fused hydration fails", async () => {
    const replacementLease = Buffer.from("lease-2");
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "FLOW.GET") throw new Error("hydration unavailable");
        return Buffer.from("OK");
      },
      async executeFusedPipeline(): Promise<unknown[]> {
        return [
          Buffer.from("OK"),
          [["flow-2", "tenant-a", replacementLease, 8, "created", {}]]
        ];
      }
    };
    const client = new FerricStoreClient(executor);

    const result = await client.completeJobsAndClaimJobs(
      [{
        fencingToken: 7,
        id: "flow-1",
        leaseToken: Buffer.from("lease-1"),
        partitionKey: "tenant-a",
        state: "created",
        type: "order"
      }],
      "order",
      {
        partitionKey: "tenant-a",
        states: ["created", "charged"],
        worker: "worker-1"
      }
    );

    expect(result.claimError).toBeInstanceOf(ClaimHydrationError);
    expect(result.claimed).toEqual([
      expect.objectContaining({
        fencingToken: 8,
        id: "flow-2",
        leaseToken: replacementLease,
        partitionKey: "tenant-a"
      })
    ]);
    expect((result.claimError as ClaimHydrationError).claimed).toEqual(result.claimed);
  });

  it("uses ordered separate requests when complete and claim routes differ", async () => {
    const executor = new FakeExecutor([
      [[Buffer.from("error"), Buffer.from("ERR stale flow lease")]],
      [["flow-2", "tenant-b", Buffer.from("lease-2"), 8]]
    ]);
    const client = new FerricStoreClient(executor);

    const result = await client.completeJobsAndClaimJobs(
      [{ id: "flow-1", leaseToken: Buffer.from("lease-1"), fencingToken: 7, partitionKey: "tenant-a", type: "order", state: "queued" }],
      "order",
      { jobOnly: true, partitionKey: "tenant-b", state: "queued", worker: "worker-1" }
    );

    expect(result).toMatchObject({ claimed: [], fused: false });
    expect(result.completionError).toBeInstanceOf(Error);
    expect(executor.pipelineCalls).toHaveLength(0);
    expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.COMPLETE_MANY"]);
  });

  it("keeps completion ahead of claim when a reconnecting executor has no native pipeline", async () => {
    let releaseCompletion: (() => void) | undefined;
    let markCompletionStarted: (() => void) | undefined;
    const completionGate = new Promise<void>((resolve) => { releaseCompletion = resolve; });
    const completionStarted = new Promise<void>((resolve) => { markCompletionStarted = resolve; });
    const order: string[] = [];
    const executor = new ReconnectingExecutor(async () => ({
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "FLOW.COMPLETE_MANY") {
          order.push("complete:start");
          markCompletionStarted?.();
          await completionGate;
          order.push("complete:end");
          return Buffer.from("OK");
        }
        order.push("claim");
        return [];
      }
    }));
    const client = new FerricStoreClient(executor);

    const operation = client.completeJobsAndClaimJobs(
      [{ id: "flow-1", leaseToken: Buffer.from("lease-1"), fencingToken: 7, partitionKey: "tenant-a", type: "order", state: "queued" }],
      "order",
      { jobOnly: true, partitionKey: "tenant-a", state: "queued", worker: "worker-1" }
    );

    await completionStarted;
    await new Promise((resolve) => setImmediate(resolve));
    expect(order).toEqual(["complete:start"]);
    releaseCompletion?.();
    await operation;
    expect(order).toEqual(["complete:start", "complete:end", "claim"]);
    await client.close();
  });

  it("maps collected pipeline item errors to typed SDK errors", async () => {
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(): Promise<unknown[]> {
        return [new Error("ERR stale flow lease")];
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    await expect(client.command("FLOW.COMPLETE", "flow-1")).rejects.toBeInstanceOf(StaleLeaseError);
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
      { attributes: { region: "us-east" }, id: "flow-1", payload: "one" },
      { attributes: { region: "us-east" }, id: "flow-2", payload: "two" }
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
      "ATTRIBUTE",
      "region",
      "us-east",
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

  it("builds max-active limits for create, createMany, and type policy commands", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.create("flow-active", {
      maxActiveMs: 30_000,
      nowMs: 100,
      type: "order"
    });
    await client.createMany("tenant-a", [{ id: "flow-infinite" }], {
      maxActiveMs: "infinity",
      nowMs: 100,
      type: "order"
    });
    await client.installPolicy("order", {
      maxActiveMs: 60_000,
      mode: "fifo",
      state: "queued"
    });

    const createMaxActiveIndex = executor.calls[0]?.indexOf("MAX_ACTIVE_MS") ?? -1;
    const createManyMaxActiveIndex = executor.calls[1]?.indexOf("MAX_ACTIVE_MS") ?? -1;
    expect(executor.calls[0]?.slice(createMaxActiveIndex, createMaxActiveIndex + 2)).toEqual([
      "MAX_ACTIVE_MS",
      30_000
    ]);
    expect(executor.calls[1]?.slice(createManyMaxActiveIndex, createManyMaxActiveIndex + 2)).toEqual([
      "MAX_ACTIVE_MS",
      "infinity"
    ]);
    expect(executor.calls[2]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "MAX_ACTIVE_MS",
      60_000,
      "STATE",
      "queued",
      "MODE",
      "FIFO"
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

  it("rejects partial item-level createMany state metadata before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.createMany("tenant-a", [
      { id: "flow-1", stateMeta: { version: "1" } },
      { id: "flow-2" }
    ], {
      nowMs: 100,
      type: "order"
    })).rejects.toThrow("shared stateMeta");

    expect(executor.calls).toHaveLength(0);
  });

  it("rejects partial item-level createMany attributes before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.createMany("tenant-a", [
      { attributes: { region: "us-east" }, id: "flow-1" },
      { id: "flow-2" }
    ], {
      nowMs: 100,
      type: "order"
    })).rejects.toThrow("shared attributes");

    expect(executor.calls).toHaveLength(0);
  });

  it("reports confirmed partial progress when a later independent many chunk fails", async () => {
    const failure = new Error("second chunk failed");
    let createCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "FLOW.CREATE_MANY") {
          createCalls += 1;
          if (createCalls === 2) throw failure;
        }
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 1 });

    let caught: unknown;
    try {
      await client.createMany("tenant-a", [{ id: "flow-1" }, { id: "flow-2" }], {
        independent: true,
        nowMs: 100,
        type: "order"
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(FlowBatchError);
    expect(caught).toMatchObject({
      cause: failure,
      completed: 1,
      completedItems: [{ index: 0, value: Buffer.from("OK") }],
      operation: "createMany"
    });
  });

  it("reports prior independent progress when a later chunk fails local codec encoding", async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, {
      codec: new JsonCodec(),
      flowManyBatchLimit: 1
    });

    await expect(client.createMany("tenant-a", [
      { id: "flow-1", payload: { valid: true } },
      { id: "flow-2", payload: circular }
    ], {
      independent: true,
      nowMs: 100,
      type: "order"
    })).rejects.toMatchObject({
      completed: 1,
      completedItems: [{ index: 0, value: Buffer.from("OK") }],
      name: "FlowBatchError",
      operation: "createMany"
    });
    expect(executor.calls).toHaveLength(1);
  });

  it("encodes child named values and references in FLOW.SPAWN_CHILDREN", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.spawnChildren("parent-1", [{
      id: "child-1",
      payload: "child-payload",
      type: "child-type",
      values: { child: "child-value" },
      valueRefs: { shared: "ref-shared" }
    }], {
      groupId: "children",
      nowMs: 100,
      values: { parent: "parent-value" }
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.SPAWN_CHILDREN",
      "parent-1",
      "GROUP",
      "children",
      "WAIT",
      "all",
      "NOW",
      100,
      "VALUE",
      "parent",
      Buffer.from("parent-value"),
      "ITEMS_EXT",
      1,
      "child-1",
      "-",
      "child-type",
      Buffer.from("child-payload"),
      1,
      "child",
      Buffer.from("child-value"),
      1,
      "shared",
      "ref-shared"
    ]);
  });

  it("builds policy indexing and decodes state metadata fields", async () => {
    const executor = new FakeExecutor([
      fakeFlowPolicySnapshot("order"),
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
        ["indexed_state_meta", "version"],
        ["max_active_ms", 120_000]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    await client.installPolicy("order", {
      indexedAttributes: ["tenant", "region"],
      indexedStateMeta: "version"
    });
    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "INDEXED_ATTRIBUTES",
      '["tenant","region"]',
      "INDEXED_STATE_META",
      "version"
    ]);

    await expect(client.get("flow-1", { partitionKey: "tenant-a" })).resolves.toMatchObject({
      indexedStateMeta: "version",
      maxActiveMs: 120_000,
      stateMeta: {
        accept: { version: "1" },
        completed: { version: "3" }
      }
    });
  });

  it("preserves an absent Flow max-active lifetime and rejects invalid present values", async () => {
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([
        ["id", "flow-without-limit"],
        ["type", "order"],
        ["state", "queued"]
      ]),
      new Map<unknown, unknown>([
        ["id", "flow-invalid-limit"],
        ["type", "order"],
        ["state", "queued"],
        ["max_active_ms", 0]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    const record = await client.get("flow-without-limit");
    expect(record?.maxActiveMs).toBeUndefined();
    expect(Object.hasOwn(record ?? {}, "maxActiveMs")).toBe(false);
    await expect(client.get("flow-invalid-limit")).rejects.toThrow(
      "FLOW record max_active_ms returned an unexpected response"
    );
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
        type: "order"
      }
    ]);
  });

  it("exposes fused start, continuation, and deterministic step-chain commands", async () => {
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([
        ["id", "flow-1"], ["type", "order"], ["state", "running"],
        ["run_state", "created"], ["partition_key", "tenant-a"],
        ["lease_token", lease], ["fencing_token", 7], ["version", 1]
      ]),
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), lease, 8],
      Buffer.from("OK")
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.startAndClaim("flow-1", {
      attributes: { phases: ["created", "charged"] },
      initialState: "created",
      leaseMs: 30_000,
      nowMs: 100,
      partitionKey: "tenant-a",
      type: "order",
      worker: "worker-1"
    })).resolves.toMatchObject({ id: "flow-1", runState: "created" });

    await expect(client.stepContinue("flow-1", {
      attributesDelete: ["temporary"],
      fencingToken: 7,
      fromState: "created",
      leaseMs: 30_000,
      leaseToken: lease,
      nowMs: 101,
      partitionKey: "tenant-a",
      returnJob: true,
      toState: "charged",
      type: "order",
      worker: "worker-1"
    })).resolves.toMatchObject({ fencingToken: 8, id: "flow-1", type: "order" });

    await expect(client.runStepsMany([
      "flow-2",
      { id: "flow-3", partitionKey: "tenant-b" }
    ], {
      nowMs: 102,
      partitionKey: "tenant-a",
      states: ["created", "charged"],
      type: "order",
      worker: "worker-1"
    })).resolves.toEqual(Buffer.from("OK"));

    expect(executor.calls[0]).toEqual(expect.arrayContaining([
      "FLOW.START_AND_CLAIM", "flow-1", "TYPE", "order", "INITIAL_STATE", "created"
    ]));
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "FLOW.STEP_CONTINUE", "flow-1", lease, "created", "charged", "RETURN", "JOBS_COMPACT"
    ]));
    expect(executor.calls[2]).toEqual(expect.arrayContaining([
      "FLOW.RUN_STEPS_MANY", "TYPE", "order", "STATES", ["created", "charged"],
      "ITEMS", [
        { id: "flow-2", partition_key: "tenant-a" },
        { id: "flow-3", partition_key: "tenant-b" }
      ]
    ]));

    await expect(client.runStepsMany(["flow-4"], {
      states: ["created"], steps: 1, type: "order", worker: "worker-1"
    })).rejects.toThrow("exactly one of states or steps");
  });

  it("exposes Flow statistics, attribute queries, and schedule administration", async () => {
    const calls: unknown[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.STATS") return { count: 3 };
        if (args[0] === "FLOW.ATTRIBUTES" || args[0] === "FLOW.ATTRIBUTE_VALUES" || args[0] === "FLOW.SCHEDULE.LIST") return [];
        if (args[0] === "FLOW.SCHEDULE.GET") return null;
        return { id: "schedule-1", status: "active" };
      }
    };
    const client = new FerricStoreClient(executor);

    await expect(client.stats("order", {
      attributes: { tenant: "tenant-a" },
      consistentProjection: true,
      count: 10,
      partitionKey: "tenant-a",
      state: "queued"
    })).resolves.toMatchObject({ count: 3 });
    await expect(client.countByState("order", "queued")).resolves.toBe(3);
    await expect(client.attributes("order", {
      consistentProjection: true, count: 10, partitionKey: "tenant-a", state: "queued"
    })).resolves.toEqual([]);
    await expect(client.attributeValues("order", "tenant", {
      consistentProjection: true, count: 5, partitionKey: "tenant-a", state: "queued"
    })).resolves.toEqual([]);
    await client.scheduleCreate("schedule-1", {
      cron: "*/5 * * * *",
      kind: "cron",
      target: { type: "order" },
      timezone: "UTC"
    });
    await expect(client.scheduleGet("schedule-1")).resolves.toBeNull();
    await client.scheduleFire("schedule-1", { nowMs: 100 });
    await client.schedulePause("schedule-1", { nowMs: 101 });
    await client.scheduleResume("schedule-1", { nowMs: 102 });
    await client.scheduleDelete("schedule-1", { nowMs: 103 });
    await client.scheduleFireDue({ blockMs: 50, limit: 10, nowMs: 104, worker: "scheduler-1" });
    await expect(client.scheduleList({ kind: "cron", count: 10 })).resolves.toEqual([]);

    expect(calls[4]).toEqual([
      "FLOW.SCHEDULE.CREATE", "schedule-1", "KIND", "cron", "CRON", "*/5 * * * *",
      "TIMEZONE", "UTC", "TARGET", { type: "order" }
    ]);
    expect(calls[0]).toEqual([
      "FLOW.STATS", "order", "STATE", "queued", "COUNT", 10, "PARTITION", "tenant-a",
      "ATTRIBUTE", "tenant", "tenant-a", "CONSISTENT_PROJECTION", "true"
    ]);
    expect(calls[2]).toEqual([
      "FLOW.ATTRIBUTES", "order", "STATE", "queued", "PARTITION", "tenant-a", "COUNT", 10,
      "CONSISTENT_PROJECTION", "true"
    ]);
    expect(calls[10]).toEqual([
      "FLOW.SCHEDULE.FIRE_DUE", "NOW", 104, "WORKER", "scheduler-1", "BLOCK", 50, "LIMIT", 10
    ]);
  });

  it("supports every server-side Flow history filter", async () => {
    const executor = new FakeExecutor([[]]);
    const client = new FerricStoreClient(executor);

    await expect(client.history("flow-1", {
      consistentProjection: true,
      count: 25,
      event: "transitioned",
      fromEvent: "100-1",
      fromMs: 100,
      fromVersion: 2,
      includeCold: true,
      partitionKey: "tenant-a",
      payloadMaxBytes: 64_000,
      rev: true,
      toEvent: "200-1",
      toMs: 200,
      toVersion: 8,
      values: true,
      worker: "worker-1"
    })).resolves.toEqual([]);

    expect(executor.calls[0]).toEqual([
      "FLOW.HISTORY", "flow-1", "PARTITION", "tenant-a", "COUNT", 25,
      "FROM_EVENT", "100-1", "TO_EVENT", "200-1", "FROM_MS", 100, "TO_MS", 200,
      "FROM_VERSION", 2, "TO_VERSION", 8, "REV", "true", "EVENT", "transitioned",
      "WORKER", "worker-1", "INCLUDE_COLD", "true", "CONSISTENT_PROJECTION", "true",
      "VALUES", "true", "PAYLOAD_MAX_BYTES", 64_000
    ]);
  });

  it("exposes effect and governance administration commands", async () => {
    const calls: unknown[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        const command = typeof args[0] === "string" ? args[0] : "";
        if (command.endsWith(".LIST") || command === "FLOW.GOVERNANCE.LEDGER") return [];
        if (command.endsWith(".GET")) return null;
        return { scope: "tenant-a", status: "ok" };
      }
    };
    const client = new FerricStoreClient(executor);

    await client.effectReserve("flow-1", "charge", "payment", { operationDigest: "sha256:1" });
    await client.effectConfirm("flow-1", "charge", { externalId: "pay-1" });
    await client.effectFail("flow-1", "charge", { error: "declined" });
    await client.effectCompensate("flow-1", "charge", { reason: "rollback" });
    await expect(client.effectGet("flow-1", "charge")).resolves.toBeNull();
    await client.approvalRequest("approval-1", { flowId: "flow-1", scope: "payments" });
    await client.approvalApprove("approval-1", { approver: "alice" });
    await client.approvalReject("approval-2", { approver: "bob", reason: "policy" });
    await expect(client.approvalGet("approval-1")).resolves.toBeNull();
    await expect(client.approvalList({ status: "pending" })).resolves.toEqual([]);
    await client.governanceOverview({ scope: "payments" });
    await client.circuitOpen("payments", { failureThreshold: 5, openMs: 1_000 });
    await client.circuitClose("payments");
    await expect(client.circuitGet("payments")).resolves.toBeNull();
    await client.budgetReserve("payments", 10, { limit: 100, reservationId: "reservation-1" });
    await client.budgetCommit("payments", "reservation-1", 8, { usage: { tokens: 8 } });
    await client.budgetRelease("payments", "reservation-1");
    await expect(client.budgetGet("payments")).resolves.toBeNull();
    await expect(client.budgetList({ scope: "payments" })).resolves.toEqual([]);
    await client.limitLease("payments", { amount: 10, shardId: 1, ttlMs: 5_000 });
    await client.limitSpend("payments", { amount: 2, shardId: 1 });
    await client.limitRelease("payments", {
      amount: 1,
      reservationIds: ["lease:1"],
      shardId: 1
    });
    await expect(client.limitGet("payments")).resolves.toBeNull();
    await expect(client.limitList({ scope: "payments" })).resolves.toEqual([]);
    await expect(client.governanceLedger("flow-1", {
      fromMs: 100, limit: 10, partitionKey: "tenant-a", rev: true, toMs: 200
    })).resolves.toEqual([]);

    expect(calls.map((call) => call[0])).toEqual([
      "FLOW.EFFECT.RESERVE", "FLOW.EFFECT.CONFIRM", "FLOW.EFFECT.FAIL", "FLOW.EFFECT.COMPENSATE", "FLOW.EFFECT.GET",
      "FLOW.APPROVAL.REQUEST", "FLOW.APPROVAL.APPROVE", "FLOW.APPROVAL.REJECT", "FLOW.APPROVAL.GET", "FLOW.APPROVAL.LIST",
      "FLOW.GOVERNANCE.OVERVIEW", "FLOW.CIRCUIT.OPEN", "FLOW.CIRCUIT.CLOSE", "FLOW.CIRCUIT.GET",
      "FLOW.BUDGET.RESERVE", "FLOW.BUDGET.COMMIT", "FLOW.BUDGET.RELEASE", "FLOW.BUDGET.GET", "FLOW.BUDGET.LIST",
      "FLOW.LIMIT.LEASE", "FLOW.LIMIT.SPEND", "FLOW.LIMIT.RELEASE", "FLOW.LIMIT.GET", "FLOW.LIMIT.LIST",
      "FLOW.GOVERNANCE.LEDGER"
    ]);
    expect(calls[4]).toEqual([
      "FLOW.EFFECT.GET", "flow-1", "EFFECT_KEY", "charge"
    ]);
    expect(calls[21]).toEqual([
      "FLOW.LIMIT.RELEASE", "payments", "SHARD_ID", 1, "AMOUNT", 1,
      "RESERVATION_IDS", 1, "lease:1"
    ]);
    expect(calls.at(-1)).toEqual([
      "FLOW.GOVERNANCE.LEDGER", "flow-1", "PARTITION", "tenant-a", "LIMIT", 10,
      "FROM_MS", 100, "TO_MS", 200, "REV", "true"
    ]);
  });

  it("routes governance ledger reads by Flow id when no partition is explicit", async () => {
    const executor = new FakeExecutor([[]]);
    const client = new FerricStoreClient(executor);

    await expect(client.governanceLedger("flow-auto")).resolves.toEqual([]);

    expect(executor.calls[0]).toEqual(["FLOW.GOVERNANCE.LEDGER", "flow-auto"]);
  });

  it("rejects unidentified or inconsistent limit releases before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.limitRelease("payments", {
      reservationIds: [],
      shardId: 1
    })).rejects.toThrow(/one unique non-empty id per credit/i);
    await expect(client.limitRelease("payments", {
      reservationIds: ["lease:1", "lease:1"],
      shardId: 1
    })).rejects.toThrow(/one unique non-empty id per credit/i);
    await expect(client.limitRelease("payments", {
      amount: 2,
      reservationIds: ["lease:1"],
      shardId: 1
    })).rejects.toThrow(/amount must match/i);

    expect(executor.calls).toEqual([]);
  });
});
