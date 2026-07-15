import { describe, expect, it, vi } from "vitest";

import {
  FerricStoreClient,
  QueueClient,
  RoutingTopology,
  TopologyNativeAdapterPool,
  WorkflowClient,
  type Command,
  type CommandArgument,
  type NativeAdapter
} from "../src/index.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import { FakeExecutor } from "./fake-executor.js";

describe("sixth ten-pass review regressions", () => {
  it("rejects missing positional GEO response items", async () => {
    const response = new Array<unknown>(2);
    response[0] = ["1", "2"];
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.geo.geoposMany("places", ["first", "second"])).rejects.toThrow(
      "GEOPOS response item 1 is missing"
    );
  });

  it("rejects sparse raw module information responses", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("Capacity");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.bloom.info("filter")).rejects.toThrow(
      "BF.INFO response item 1 is missing"
    );
  });

  it("rejects sparse command arguments before custom pipeline dispatch", async () => {
    const executePipeline = vi.fn(async () => []);
    const client = new FerricStoreClient({
      executeCommand: async () => null,
      executePipeline
    });
    const sparse = new Array<unknown>(2);
    sparse[0] = "GET";

    await expect(client.pipeline([sparse as Command])).rejects.toThrow(
      "pipeline command arguments must be dense"
    );
    expect(executePipeline).not.toHaveBeenCalled();
  });

  it("snapshots excluded command arguments while an auto-batch flush is pending", async () => {
    let markBatchStarted: (() => void) | undefined;
    let releaseBatch: (() => void) | undefined;
    const batchStarted = new Promise<void>((resolve) => { markBatchStarted = resolve; });
    const batchGate = new Promise<void>((resolve) => { releaseBatch = resolve; });
    const directCalls: CommandArgument[][] = [];
    const client = new FerricStoreClient({
      async executeCommand(...args) {
        directCalls.push(args);
        return args[1];
      },
      async executePipeline(commands) {
        markBatchStarted?.();
        await batchGate;
        return commands.map(() => Buffer.from("OK"));
      }
    }, { autoBatch: { enabled: true, maxCommands: 1 } });

    const batched = client.command("SET", "key", "value");
    await batchStarted;
    const args: CommandArgument[] = ["PING", "original"];
    const direct = client.commandArgs(args);
    args[1] = "mutated";
    releaseBatch?.();

    await expect(batched).resolves.toEqual(Buffer.from("OK"));
    await expect(direct).resolves.toBe("original");
    expect(directCalls).toEqual([["PING", "original"]]);
    await client.close();
  });

  it("ignores an in-flight heartbeat failure after heartbeat shutdown", async () => {
    vi.useFakeTimers();
    try {
      let rejectSend: ((error: Error) => void) | undefined;
      const failures: unknown[] = [];
      const heartbeat = new NativeHeartbeat(
        10,
        () => new Promise<void>((_resolve, reject) => { rejectSend = reject; }),
        (error) => { failures.push(error); }
      );

      heartbeat.start();
      await vi.advanceTimersByTimeAsync(10);
      heartbeat.stop();
      rejectSend?.(new Error("late heartbeat failure"));
      await vi.advanceTimersByTimeAsync(0);

      expect(failures).toEqual([]);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    ["numerically lower opaque-epoch", 1, 1],
    ["same-epoch changed-route", 2, 2]
  ])("installs a %s topology", async (_kind, epoch, laneId) => {
    const Pool = TopologyNativeAdapterPool as unknown as new (
      urls: readonly string[]
    ) => TopologyNativeAdapterPool;
    const pool = new Pool(["ferric://seed.local:6388"]);
    const current = RoutingTopology.build(topologyPayload(2, 1));
    (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = current;
    const internals = pool as unknown as {
      adapterForSeedUrl(url: string): Promise<NativeAdapter>;
    };
    internals.adapterForSeedUrl = async () => ({
      executeCommand: async () => topologyPayload(epoch, laneId)
    }) as unknown as NativeAdapter;

    try {
      const refreshed = await pool.refreshTopology();
      expect(refreshed).not.toBe(current);
      expect(pool.topology).toBe(refreshed);
      expect(refreshed.routeEpoch).toBe(epoch);
      expect(pool.route("tenant-key").laneId).toBe(laneId);
    } finally {
      await pool.close();
    }
  });

  it("keeps the installed topology object for an identical same-epoch refresh", async () => {
    const Pool = TopologyNativeAdapterPool as unknown as new (
      urls: readonly string[]
    ) => TopologyNativeAdapterPool;
    const pool = new Pool(["ferric://seed.local:6388"]);
    const current = RoutingTopology.build(topologyPayload(2, 1));
    (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = current;
    const internals = pool as unknown as {
      adapterForSeedUrl(url: string): Promise<NativeAdapter>;
    };
    internals.adapterForSeedUrl = async () => ({
      executeCommand: async () => topologyPayload(2, 1)
    }) as unknown as NativeAdapter;

    try {
      await expect(pool.refreshTopology()).resolves.toBe(current);
      expect(pool.topology).toBe(current);
    } finally {
      await pool.close();
    }
  });

  it("rejects sparse Flow claim states before dispatch", async () => {
    const executor = new FakeExecutor([[]]);
    const client = new FerricStoreClient(executor);
    const states = new Array<string>(2);
    states[0] = "queued";

    await expect(client.claimDue("email", {
      jobOnly: true,
      states,
      worker: "worker-1"
    })).rejects.toThrow("states must be a dense array of strings");
    expect(executor.calls).toEqual([]);
  });

  it("snapshots queue worker scalar and list configuration", async () => {
    const executor = new FakeExecutor([[]]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const partitionKeys = ["tenant-original"];
    const claimValues = ["profile-original"];
    const options = {
      claimValues,
      leaseRenewal: false,
      partitionKeys,
      worker: "worker-original"
    };
    const worker = queue.worker(options);
    options.worker = "worker-mutated";
    partitionKeys[0] = "tenant-mutated";
    claimValues[0] = "profile-mutated";

    await worker.runOnce(() => undefined);

    expect(executor.calls[0]).toContain("worker-original");
    expect(executor.calls[0]).toContain("tenant-original");
    expect(executor.calls[0]).toContain("profile-original");
    expect(executor.calls[0]).not.toContain("worker-mutated");
  });

  it("snapshots workflow worker state configuration", async () => {
    const executor = new FakeExecutor([[]]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("queued", () => undefined);
    const states = ["queued"];
    const options = { leaseRenewal: false, states, worker: "worker-original" };
    const worker = workflow.worker(options);
    options.worker = "worker-mutated";
    states[0] = "unregistered";

    await expect(worker.runOnce()).resolves.toMatchObject({ claimed: 0 });
    expect(executor.calls[0]).toContain("worker-original");
  });

  it("rejects sparse Flow named-value mutation lists before dispatch", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);
    const dropValues = new Array<string>(2);
    dropValues[0] = "profile";

    await expect(client.signal("flow-1", {
      dropValues,
      signal: "refresh"
    })).rejects.toThrow("dropValues must be a dense array of strings");
    expect(executor.calls).toEqual([]);
  });

  it("rejects a blocking-list response with a missing value slot", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("jobs");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.lists.blpop(["jobs"], 0)).rejects.toThrow(
      "invalid blocking list pop response"
    );
  });
});

function topologyPayload(routeEpoch: number, laneId: number): unknown {
  return {
    ranges: [{
      endpoint: { host: "seed.local", native_port: 6388, node: "seed@local" },
      first_slot: 0,
      lane_id: laneId,
      last_slot: 1023,
      shard: 0
    }],
    route_epoch: routeEpoch,
    shard_count: 1
  };
}
