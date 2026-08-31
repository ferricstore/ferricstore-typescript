import { Buffer } from "node:buffer";
import { performance } from "node:perf_hooks";
import { describe, expect, it, vi } from "vitest";
import type { CommandExecutor } from "../src/adapters.js";
import { FerricStoreClient, FlowBatchError, groupAutoPartitionItems } from "../src/client.js";
import { JsonCodec, RawCodec, WorkflowClient, fail } from "../src/index.js";
import { autoPartitionKeyForId, expandManyResponse, type CommandArgument } from "../src/internal.js";
import { claimedItemFromResp, flowRecordFromResp } from "../src/types.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient edge cases", () => {
  it("decodes string-backed named Flow values through the configured codec", () => {
    const record = flowRecordFromResp(
      new Map<unknown, unknown>([
        ["id", "flow-1"],
        ["type", "order"],
        ["state", "running"],
        ["partition_key", "tenant-a"],
        ["fencing_token", 1],
        ["version", 1],
        ["values", new Map<unknown, unknown>([["marker", '{"ok":true}']])]
      ]),
      new JsonCodec()
    );

    expect(record.values).toEqual({ marker: { ok: true } });
  });

  it("preserves opaque binary Flow values, attributes, and state metadata", () => {
    const opaque = Buffer.from([0xff, 0x00, 0x80]);
    const record = flowRecordFromResp(
      new Map<unknown, unknown>([
        ["id", "flow-1"],
        ["type", "order"],
        ["state", "running"],
        ["values", new Map([[Buffer.from("blob"), opaque]])],
        ["attributes", new Map([[Buffer.from("marker"), opaque]])],
        ["state_meta", new Map([[Buffer.from("checkpoint"), opaque]])]
      ]),
      new RawCodec()
    );
    const compactClaim = claimedItemFromResp([
      Buffer.from("flow-1"),
      null,
      Buffer.from("lease"),
      1,
      "running",
      new Map([[Buffer.from("marker"), opaque]])
    ]);

    expect(record.values).toEqual({ blob: opaque });
    expect(record.attributes).toEqual({ marker: opaque });
    expect(record.stateMeta).toEqual({ checkpoint: opaque });
    expect(compactClaim.attributes).toEqual({ marker: opaque });
  });

  it("decodes Uint8Array-backed Flow text fields as UTF-8", () => {
    const encoded = (value: string): Uint8Array => new Uint8Array(Buffer.from(value));

    expect(flowRecordFromResp({
      id: encoded("flow-1"),
      partition_key: encoded("tenant-a"),
      state: encoded("running"),
      type: encoded("order")
    })).toMatchObject({
      id: "flow-1",
      partitionKey: "tenant-a",
      state: "running",
      type: "order"
    });
  });

  it("reads Flow records whose map keys use Uint8Array", () => {
    const encoded = (value: string): Uint8Array => new Uint8Array(Buffer.from(value));
    const record = flowRecordFromResp(new Map<unknown, unknown>([
      [encoded("id"), encoded("flow-1")],
      [encoded("state"), encoded("running")],
      [encoded("type"), encoded("order")]
    ]));

    expect(record).toMatchObject({
      id: "flow-1",
      state: "running",
      type: "order"
    });
  });

  it("decodes Uint8Array-backed reference metadata as UTF-8", () => {
    const ref = new Uint8Array(Buffer.from("ref-customer"));
    const record = flowRecordFromResp({
      id: "flow-1",
      state: "running",
      type: "order",
      value_refs: new Map([
        ["customer", new Map([["ref", ref]])]
      ])
    });

    expect(record.valueRefs).toEqual({
      customer: { ref: "ref-customer" }
    });
  });

  it("preserves unsafe 64-bit integers in exact fencing-token fields", () => {
    const fencingToken = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    expect(flowRecordFromResp(new Map<unknown, unknown>([
      ["id", "flow-1"],
      ["type", "order"],
      ["state", "running"],
      ["partition_key", "tenant-a"],
      ["fencing_token", fencingToken],
      ["version", 1]
    ]))).toMatchObject({ fencingToken });

    expect(claimedItemFromResp([
      Buffer.from("flow-1"),
      Buffer.from("tenant-a"),
      Buffer.from("lease"),
      fencingToken
    ])).toMatchObject({ fencingToken });
  });

  it("rejects unsafe integer replies from number-typed client helpers", async () => {
    const client = new FerricStoreClient(new FakeExecutor([9_007_199_254_740_993n]));

    await expect(client.slowlogLen()).rejects.toThrow("integer exceeds the JavaScript safe range");
  });

  it("rejects malformed integer replies instead of returning NaN or fractions", async () => {
    const client = new FerricStoreClient(new FakeExecutor([Buffer.from("not-an-integer"), 1.5, "NaN"]));

    await expect(client.slowlogLen()).rejects.toThrow("not an integer");
    await expect(client.kv.exists("key")).rejects.toThrow("not an integer");
    await expect(client.bloom.add("filter", "item")).rejects.toThrow("not an integer");
  });

  it("rejects malformed Flow records and compact claims", () => {
    expect(() => flowRecordFromResp("not-a-record")).toThrow("FLOW record returned an unexpected response");
    expect(() => flowRecordFromResp({ id: "flow-1", type: "order" })).toThrow("missing required state");
    expect(() => claimedItemFromResp([Buffer.from("flow-1")])).toThrow("compact claim returned an unexpected response");
    expect(() => claimedItemFromResp({
      id: "flow-1",
      type: "order",
      state: "running",
      run_state: "queued",
      lease_token: Buffer.from("lease"),
      fencing_token: "not-an-integer"
    })).toThrow("not an integer");
  });

  it("rejects malformed optional typed Flow record fields", () => {
    const base = { id: "flow-1", state: "running", type: "order" };

    expect(() => flowRecordFromResp({ ...base, partition_key: { invalid: true } })).toThrow(
      "FLOW record partition_key returned an unexpected response"
    );
    expect(() => flowRecordFromResp({ ...base, values: Buffer.from("not-a-map") })).toThrow(
      "FLOW record values returned an unexpected response"
    );
    expect(() => flowRecordFromResp({ ...base, attributes: ["not-a-map"] })).toThrow(
      "FLOW record attributes returned an unexpected response"
    );
  });

  it("does not stringify legacy compact-claim attributes as run state", () => {
    const attributes = { tenant: Buffer.from("acme") };

    const claimed = claimedItemFromResp([
      Buffer.from("flow-1"),
      null,
      Buffer.from("lease"),
      1,
      attributes
    ]);

    expect(claimed).toMatchObject({
      attributes: { tenant: Buffer.from("acme") },
      runState: undefined
    });
    expect(claimed).not.toHaveProperty("type");
  });

  it("rejects malformed specialized helper response arity and status", async () => {
    const rateClient = new FerricStoreClient(new FakeExecutor([["allowed"]]));
    const fetchClient = new FerricStoreClient(new FakeExecutor([["unexpected", Buffer.alloc(0)]]));
    const keyInfoClient = new FerricStoreClient(new FakeExecutor([["type", "string", "value_size"]]));

    await expect(rateClient.rateLimitAdd("rate", { max: 1, windowMs: 1 })).rejects.toThrow(
      "RATELIMIT.ADD returned an unexpected response"
    );
    await expect(fetchClient.fetchOrCompute("cache", { ttlMs: 1 })).rejects.toThrow(
      "FETCH_OR_COMPUTE returned an unexpected response"
    );
    await expect(keyInfoClient.keyInfo("key")).rejects.toThrow(
      "FERRICSTORE.KEY_INFO returned an unexpected response"
    );
  });

  it("rejects the removed tokenless compute response", async () => {
    const client = new FerricStoreClient(new FakeExecutor([["compute", Buffer.alloc(0)]]));

    await expect(client.fetchOrCompute("cache", { ttlMs: 1 })).rejects.toThrow(
      "FETCH_OR_COMPUTE returned an unexpected response"
    );
  });

  it("round-trips mandatory fetch-or-compute ownership tokens", async () => {
    const token = Buffer.from("fence");
    const executor = new FakeExecutor([
      ["compute", Buffer.from("hint"), token],
      Buffer.from("OK")
    ]);
    const client = new FerricStoreClient(executor);

    const miss = await client.fetchOrCompute("cache", { ttlMs: 5_000 });
    expect(miss).toMatchObject({
      computeHint: Buffer.from("hint"),
      computeMode: "fenced",
      computeToken: token,
      hit: false,
      shouldCompute: true,
      status: "compute"
    });
    if (!miss.shouldCompute) throw new Error("expected a fetch-or-compute lease");

    await expect(client.fetchOrComputeResult("cache", "value", {
      computeToken: miss.computeToken,
      ttlMs: 5_000
    })).resolves.toBe(true);
    await expect(client.fetchOrComputeError("cache", "failed", {
      computeToken: miss.computeToken
    })).resolves.toBe(true);
    expect(executor.calls.slice(1)).toEqual([
      ["FETCH_OR_COMPUTE_RESULT", "cache", token, Buffer.from("value"), 5_000],
      ["FETCH_OR_COMPUTE_ERROR", "cache", token, "failed"]
    ]);
  });

  it("rejects an ambiguous fetch-or-compute publication before dispatch", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    // @ts-expect-error An ownership token is required.
    await expect(client.fetchOrComputeResult("cache", "value", {
      ttlMs: 5_000
    })).rejects.toThrow(/computeToken/u);
    // @ts-expect-error An ownership token is required.
    await expect(client.fetchOrComputeError("cache", "failed")).rejects.toThrow(/computeToken/u);
    expect(executor.calls).toEqual([]);
  });

  it("rejects malformed management list responses instead of returning empty lists", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      { malformed: true },
      { malformed: true },
      { malformed: true },
      { malformed: true }
    ]));

    await expect(client.flowQuery()).rejects.toThrow("invalid array response");
    await expect(client.flowHistory("flow-1")).rejects.toThrow("invalid array response");
    await expect(client.invocationDefinitionList()).rejects.toThrow("invalid array response");
    await expect(client.invocationPartitionList("definition")).rejects.toThrow("invalid array response");
  });

  it("rejects wrong-length per-item batch responses while preserving scalar batch success", () => {
    expect(() => expandManyResponse(["only-one"], 2)).toThrow("response length");
    expect(expandManyResponse("OK", 2)).toEqual(["OK", "OK"]);
  });

  it("rejects malformed array-returning store and Flow responses", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      Buffer.from("not-an-array"),
      Buffer.from("not-an-array"),
      Buffer.from("not-an-array"),
      Buffer.from("not-an-array")
    ]));

    await expect(client.kv.mget(["key"])).rejects.toThrow("array response");
    await expect(client.cms.query("cms", "item")).rejects.toThrow("array response");
    await expect(client.claimDue("order", { state: "created", worker: "worker-1" })).rejects.toThrow(
      "FLOW.CLAIM_DUE returned an invalid response"
    );
    await expect(client.search("order", {
      attributes: { tenant: "acme" },
      partitionKey: "tenant-a"
    })).rejects.toThrow("FLOW.QUERY result must be a map");
  });

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

  it("rejects invalid state mode policy helpers before sending a command", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.installPolicy("order", { mode: "fifo" })).rejects.toThrow(
      "policy mode requires state"
    );
    await expect(
      client.installPolicy("order", { mode: "priority" as "fifo", state: "queued" })
    ).rejects.toThrow("policy mode must be 'fifo' or 'parallel'");

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
      const partition = call[1];
      if (typeof partition !== "string") {
        throw new TypeError("expected auto partition key to be a string");
      }
      expect(partition).toMatch(/^__flow_auto__:[0-9]+$/u);
      expect(call).toContain("INDEPENDENT");
      expect(call).toContain("true");
    }
  });

  it("rejects sparse enqueueMany inputs before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const items = new Array<{ id: string }>(2);
    items[1] = { id: "order-2" };

    await expect(client.enqueueMany(items, { type: "order" })).rejects.toThrow(
      "enqueueMany items must be dense"
    );
    expect(executor.calls).toEqual([]);
  });

  it("keeps unpartitioned independent=false enqueueMany in one AUTO request", async () => {
    const ids: string[] = [];
    const buckets = new Set<string>();
    for (let index = 0; ids.length < 2; index += 1) {
      const id = `atomic-order-${index}`;
      const bucket = autoPartitionKeyForId(id);
      if (buckets.has(bucket)) continue;
      buckets.add(bucket);
      ids.push(id);
    }
    const executor = new FakeExecutor([[Buffer.from("one"), Buffer.from("two")]]);
    const client = new FerricStoreClient(executor);

    await expect(client.enqueueMany(
      ids.map((id) => ({ id })),
      { independent: false, nowMs: 100, type: "order" }
    )).resolves.toEqual([Buffer.from("one"), Buffer.from("two")]);

    expect(executor.calls).toHaveLength(1);
    expect(executor.calls[0]?.slice(0, 2)).toEqual(["FLOW.CREATE_MANY", "AUTO"]);
    expect(executor.calls[0]).toContain("INDEPENDENT");
    expect(executor.calls[0]).toContain("false");
  });

  it("dispatches auto-partition buckets with bounded concurrency and preserves input order", async () => {
    const ids: string[] = [];
    const buckets = new Set<string>();
    for (let index = 0; ids.length < 6; index += 1) {
      const id = `order-${index}`;
      const bucket = autoPartitionKeyForId(id);
      if (buckets.has(bucket)) continue;
      buckets.add(bucket);
      ids.push(id);
    }
    let active = 0;
    let maxActive = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise<void>((resolve) => setTimeout(resolve, 5));
        active -= 1;
        const itemsIndex = args.indexOf("ITEMS");
        const id = args[itemsIndex + 1];
        if (typeof id !== "string") throw new TypeError("expected string item id");
        return [Buffer.from(`created:${id}`)];
      }
    };
    const client = new FerricStoreClient(executor);

    const results = await client.enqueueMany(
      ids.map((id) => ({ id })),
      { autoPartitionConcurrency: 2, nowMs: 100, type: "order" }
    );

    expect(maxActive).toBe(2);
    expect(results).toEqual(ids.map((id) => Buffer.from(`created:${id}`)));
  });

  it("groups large same-bucket enqueueMany inputs without quadratic copying", async () => {
    const count = 50_000;
    const items = Array.from({ length: count }, () => ({ id: "same-bucket" }));
    const startedAt = performance.now();

    const groups = groupAutoPartitionItems(items);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.items).toHaveLength(count);
    expect(performance.now() - startedAt).toBeLessThan(500);
  }, 10_000);

  it("chunks one auto-partition bucket without reordering its results", async () => {
    const ids: string[] = [];
    const targetBucket = autoPartitionKeyForId("same-bucket-0");
    for (let index = 0; ids.length < 5; index += 1) {
      const id = `same-bucket-${index}`;
      if (autoPartitionKeyForId(id) === targetBucket) ids.push(id);
    }
    const batchSizes: number[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        const itemsIndex = args.indexOf("ITEMS");
        const idsInBatch: string[] = [];
        for (let index = itemsIndex + 1; index < args.length; index += 2) {
          const id = args[index];
          if (typeof id !== "string") throw new TypeError("expected string item id");
          idsInBatch.push(id);
        }
        batchSizes.push(idsInBatch.length);
        return idsInBatch.map((id) => Buffer.from(`created:${id}`));
      }
    };
    const client = new FerricStoreClient(executor);

    const results = await client.enqueueMany(
      ids.map((id) => ({ id })),
      { autoPartitionBatchSize: 2, autoPartitionConcurrency: 4, nowMs: 100, type: "order" }
    );

    expect(batchSizes).toEqual([2, 2, 1]);
    expect(results).toEqual(ids.map((id) => Buffer.from(`created:${id}`)));
  });

  it("stops chunking other auto-partition buckets after the first observed failure", async () => {
    const failingId = "failing-order";
    const failingBucket = autoPartitionKeyForId(failingId);
    const siblingIds: string[] = [];
    let siblingBucket: string | undefined;
    for (let index = 0; siblingIds.length < 3; index += 1) {
      const id = `sibling-order-${index}`;
      const bucket = autoPartitionKeyForId(id);
      if (bucket === failingBucket) continue;
      siblingBucket ??= bucket;
      if (bucket === siblingBucket) siblingIds.push(id);
    }

    const failure = new Error("first bucket failed");
    const started: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        const itemsIndex = args.indexOf("ITEMS");
        const id = args[itemsIndex + 1];
        if (typeof id !== "string") throw new TypeError("expected string item id");
        started.push(id);
        if (id === failingId) throw failure;
        await new Promise((resolve) => setImmediate(resolve));
        return [Buffer.from(`created:${id}`)];
      }
    };
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 1 });

    let caught: unknown;
    try {
      await client.enqueueMany([
        { id: failingId },
        ...siblingIds.map((id) => ({ id }))
      ], {
        autoPartitionBatchSize: 1,
        autoPartitionConcurrency: 2,
        nowMs: 100,
        type: "order"
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(FlowBatchError);
    expect(caught).toMatchObject({
      cause: failure,
      completedItems: [{ index: 1, value: Buffer.from(`created:${siblingIds[0]}`) }],
      operation: "enqueueMany"
    });
    expect(started).toEqual([failingId, siblingIds[0]]);
  });

  it("caps an oversized auto-partition batch setting at the Flow many limit", async () => {
    const batchSizes: number[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        const itemsIndex = args.indexOf("ITEMS");
        const count = (args.length - itemsIndex - 1) / 2;
        batchSizes.push(count);
        return Array.from({ length: count }, () => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor);
    const items = Array.from({ length: 1_001 }, () => ({ id: "same-bucket" }));

    await expect(client.enqueueMany(items, {
      autoPartitionBatchSize: 10_000,
      nowMs: 100,
      type: "order"
    })).resolves.toHaveLength(1_001);
    expect(batchSizes).toEqual([1_000, 1]);
  });

  it("chunks explicit and mixed-partition enqueueMany calls at the Flow many limit", async () => {
    const runCase = async (mixed: boolean): Promise<number[]> => {
      const batchSizes: number[] = [];
      const executor: CommandExecutor = {
        async executeCommand(...args: CommandArgument[]): Promise<unknown> {
          const itemsIndex = args.indexOf("ITEMS");
          const width = mixed ? 3 : 2;
          const count = (args.length - itemsIndex - 1) / width;
          batchSizes.push(count);
          return Array.from({ length: count }, (_, index) => Buffer.from(`created-${index}`));
        }
      };
      const client = new FerricStoreClient(executor);
      const items = Array.from({ length: 1_001 }, (_, index) => ({
        id: `order-${index}`,
        ...(mixed ? { partitionKey: `tenant-${index % 2}` } : {})
      }));

      await expect(client.enqueueMany(items, {
        ...(mixed ? {} : { partitionKey: "tenant-a" }),
        nowMs: 100,
        type: "order"
      })).resolves.toHaveLength(1_001);
      return batchSizes;
    };

    await expect(runCase(false)).resolves.toEqual([1_000, 1]);
    await expect(runCase(true)).resolves.toEqual([1_000, 1]);
  });

  it("rejects oversized low-level createMany before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.createMany(
      "tenant-a",
      Array.from({ length: 1_001 }, (_, index) => ({ id: `order-${index}` })),
      { independent: false, type: "order" }
    )).rejects.toThrow(/createMany.*1,000/i);
    await expect(client.enqueueMany(
      Array.from({ length: 1_001 }, (_, index) => ({ id: `queued-${index}` })),
      { independent: false, partitionKey: "tenant-a", type: "order" }
    )).rejects.toThrow(/independent.*1,000/i);
    await expect(client.enqueueMany(
      Array.from({ length: 1_001 }, (_, index) => ({ id: `auto-${index}` })),
      { independent: false, type: "order" }
    )).rejects.toThrow(/independent.*1,000/i);
    expect(executor.calls).toEqual([]);
  });

  it("applies a configured Flow many limit to every mutation without weakening semantics", async () => {
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 2 });
    const createItems = Array.from({ length: 3 }, (_, index) => ({ id: `job-${index}` }));
    const claimedItems = Array.from({ length: 3 }, (_, index) => ({
      fencingToken: index + 1,
      id: `job-${index}`,
      leaseToken: Buffer.from(`lease-${index}`),
      partitionKey: "tenant-a",
      state: "running",
      type: "order"
    }));

    await expect(client.createMany("tenant-a", createItems, {
      independent: true,
      nowMs: 100,
      type: "order"
    })).resolves.toHaveLength(3);
    await expect(client.completeMany("tenant-a", claimedItems, {
      independent: true,
      nowMs: 100
    })).resolves.toHaveLength(3);
    await expect(client.transitionMany("tenant-a", {
      fromState: "running",
      independent: true,
      items: claimedItems,
      nowMs: 100,
      toState: "done"
    })).resolves.toHaveLength(3);
    await expect(client.retryMany("tenant-a", claimedItems, {
      independent: true,
      nowMs: 100
    })).resolves.toHaveLength(3);
    await expect(client.failMany("tenant-a", claimedItems, {
      independent: true,
      nowMs: 100
    })).resolves.toHaveLength(3);
    await expect(client.cancelMany("tenant-a", claimedItems, {
      independent: true,
      nowMs: 100
    })).resolves.toHaveLength(3);

    for (const command of [
      "FLOW.CREATE_MANY",
      "FLOW.COMPLETE_MANY",
      "FLOW.TRANSITION_MANY",
      "FLOW.RETRY_MANY",
      "FLOW.FAIL_MANY",
      "FLOW.CANCEL_MANY"
    ]) {
      const commandCalls = calls.filter((call) => call[0] === command);
      expect(commandCalls).toHaveLength(2);
      expect(commandCalls.map((call) => {
        const itemsIndex = call.indexOf("ITEMS");
        return call.slice(itemsIndex + 1).filter(
          (value): value is string => typeof value === "string" && value.startsWith("job-")
        );
      })).toEqual([["job-0", "job-1"], ["job-2"]]);
    }

    const rejectedExecutor = new FakeExecutor();
    const atomicClient = new FerricStoreClient(rejectedExecutor, { flowManyBatchLimit: 2 });
    await expect(atomicClient.createMany("tenant-a", createItems, {
      independent: false,
      type: "order"
    })).rejects.toThrow(/createMany.*at most 2/i);
    await expect(atomicClient.completeMany("tenant-a", claimedItems, {
      independent: false
    })).rejects.toThrow(/completeMany.*at most 2/i);
    await expect(atomicClient.transitionMany("tenant-a", {
      fromState: "running",
      independent: false,
      items: claimedItems,
      toState: "done"
    })).rejects.toThrow(/transitionMany.*at most 2/i);
    await expect(atomicClient.retryMany("tenant-a", claimedItems, {
      independent: false
    })).rejects.toThrow(/retryMany.*at most 2/i);
    await expect(atomicClient.failMany("tenant-a", claimedItems, {
      independent: false
    })).rejects.toThrow(/failMany.*at most 2/i);
    await expect(atomicClient.cancelMany("tenant-a", claimedItems, {
      independent: false
    })).rejects.toThrow(/cancelMany.*at most 2/i);
    expect(rejectedExecutor.calls).toEqual([]);
  });

  it("rejects invalid Flow many limit configuration", () => {
    expect(() => new FerricStoreClient(new FakeExecutor(), {
      flowManyBatchLimit: 0
    })).toThrow(/flowManyBatchLimit.*positive safe integer/i);
    expect(() => new FerricStoreClient(new FakeExecutor(), {
      flowManyBatchLimit: Number.POSITIVE_INFINITY
    })).toThrow(/flowManyBatchLimit.*positive safe integer/i);
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
        type: "order"
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
    const executor = new FakeExecutor([['{"ok":true}', null]]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.valueMGet(["ref-1", "ref-2"], { maxBytes: 1024 })).resolves.toEqual([
      { ok: true },
      null
    ]);
    expect(executor.calls[0]).toEqual(["FLOW.VALUE.MGET", "ref-1", "ref-2", "MAX_BYTES", 1024]);
  });

  it("rejects malformed FLOW.VALUE.MGET response shapes", async () => {
    const executor = new FakeExecutor([Buffer.from("not-an-array"), [Buffer.from("one")]]);
    const client = new FerricStoreClient(executor);

    await expect(client.valueMGet(["ref-1"])).rejects.toThrow("invalid response");
    await expect(client.valueMGet(["ref-1", "ref-2"])).rejects.toThrow("response length");
  });

  it("backs off and retries overloaded producer writes", async () => {
    const overloaded = Object.assign(
      new Error("BUSY FerricStore overloaded: retry_after_ms=0 reason=rss_pressure"),
      { retryable: true, safe_to_retry: true }
    );
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

  it("caps server-directed producer retry delays after jitter", async () => {
    vi.useFakeTimers();
    const random = vi.spyOn(Math, "random").mockReturnValue(1);
    const overloaded = Object.assign(
      new Error("BUSY FerricStore overloaded: retry_after_ms=10000 reason=rss_pressure"),
      { retryable: true, safe_to_retry: true }
    );
    overloaded.name = "ResponseError";
    const executor = new FakeExecutor([overloaded, Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, {
      backpressure: {
        baseDelayMs: 1,
        jitterPct: 100,
        maxDelayMs: 10,
        maxRetries: 1
      }
    });
    let settled = false;
    const operation = client.enqueue("order-1", { nowMs: 100, type: "order" }).finally(() => {
      settled = true;
    });

    try {
      await vi.advanceTimersByTimeAsync(10);
      const settledWithinConfiguredMaximum = settled;
      await vi.runAllTimersAsync();
      await expect(operation).resolves.toEqual(Buffer.from("OK"));
      expect(settledWithinConfiguredMaximum).toBe(true);
    } finally {
      random.mockRestore();
      vi.useRealTimers();
    }
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

  it("hydrates valueMany refs in one request and preserves stored JSON null", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-values"],
          ["type", "order"],
          ["state", "inspect"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 10],
          ["values", new Map<unknown, unknown>([["inline", Buffer.from('{"source":"inline"}')]])],
          ["value_refs", new Map<unknown, unknown>([
            ["customer", new Map([["ref", "ref-customer"]])],
            ["nullable", new Map([["ref", "ref-null"]])],
            ["missing", new Map([["ref", "ref-missing"]])]
          ])]
        ])
      ],
      [Buffer.from('{"id":"c1"}'), Buffer.from("null"), null],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(
      new FerricStoreClient(executor, { codec: new JsonCodec() })
    ).workflow({ type: "order" });

    workflow.state("inspect", async (ctx) => {
      await expect(
        ctx.valueMany(["inline", "customer", "nullable", "missing"])
      ).resolves.toEqual({
        customer: { id: "c1" },
        inline: { source: "inline" },
        missing: undefined,
        nullable: null
      });
      return fail({ error: "done" });
    });

    await workflow.worker({ states: ["inspect"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toEqual([
      "FLOW.VALUE.MGET",
      "ref-customer",
      "ref-null",
      "ref-missing"
    ]);
    expect(executor.calls.filter((call) => call[0] === "FLOW.VALUE.MGET")).toHaveLength(1);
  });

  it("does not cache a missing named value as a caller-specific default", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-missing"],
          ["type", "order"],
          ["state", "inspect"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 11],
          ["value_refs", new Map<unknown, unknown>([["missing", "ref-missing"]])]
        ])
      ],
      [null],
      [null],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({
      type: "order",
      valueConfig: { localCache: true }
    });
    const observed: unknown[] = [];

    workflow.state("inspect", async (ctx) => {
      observed.push(await ctx.value("missing", "first"));
      observed.push(await ctx.value("missing", "second"));
      return fail({ error: "done" });
    });
    await workflow.worker({ states: ["inspect"], worker: "worker-1" }).runOnce();

    expect(observed).toEqual(["first", "second"]);
    expect(executor.calls.filter((call) => call[0] === "FLOW.VALUE.MGET")).toHaveLength(1);
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
    const errorText =
      Buffer.isBuffer(errorPayload)
        ? errorPayload.toString("utf8")
        : typeof errorPayload === "string"
          ? errorPayload
          : JSON.stringify(errorPayload);
    expect(errorText).toContain("bad input");
  });
});
