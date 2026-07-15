import { describe, expect, it, vi } from "vitest";

import { FerricStoreClient, QueueClient, type Command } from "../src/index.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import { encodeValue } from "../src/protocol.js";
import { TopologyPipelineExecutor } from "../src/topology-pipeline.js";
import { FakeExecutor } from "./fake-executor.js";

describe("fifth ten-pass review regressions", () => {
  it("rejects missing positional HMGET response items", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("first");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.hash.hmget("hash", ["first", "second"])).rejects.toThrow(
      "HMGET response item 1 is missing"
    );
  });

  it("rejects sorted-set score tuples with a missing member slot", async () => {
    const pair = new Array<unknown>(2);
    pair[1] = "1.5";
    const client = new FerricStoreClient(new FakeExecutor([[pair]]));

    await expect(client.zset.zrange("rank", 0, -1, { withScores: true })).rejects.toThrow(
      "invalid sorted-set member/score response"
    );
  });

  it("rejects sparse command arguments before individual pipeline fallback dispatch", async () => {
    const executeCommand = vi.fn(async () => null);
    const client = new FerricStoreClient({ executeCommand });
    const sparse = new Array<unknown>(2);
    sparse[0] = "GET";
    const command = sparse as Command;

    await expect(client.pipeline([command])).rejects.toThrow(
      "pipeline command arguments must be dense"
    );
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("rejects unsupported object containers instead of encoding silent empty maps", () => {
    expect(() => encodeValue(new Map([["key", "value"]]))).toThrow(
      "unsupported native protocol value type: object"
    );
  });

  it("does not leak an earlier native heartbeat interval across repeated starts", async () => {
    vi.useFakeTimers();
    try {
      let sends = 0;
      const heartbeat = new NativeHeartbeat(
        10,
        async () => { sends += 1; },
        () => undefined
      );

      heartbeat.start();
      heartbeat.start();
      heartbeat.stop();
      await vi.advanceTimersByTimeAsync(20);

      expect(sends).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    ["short", [Buffer.from("first")]],
    ["sparse", Object.assign(new Array<unknown>(2), { 0: Buffer.from("first") })]
  ])("rejects %s topology pipeline group responses", async (_kind, responses) => {
    const route = {
      endpoint: { host: "node.local", nativePort: 6388, node: "node@local" },
      endpointKey: "node.local:6388",
      laneId: 1,
      leaderNode: "node@local",
      shard: 0
    };
    const executor = new TopologyPipelineExecutor({
      concurrency: 1,
      controlPipeline: async () => { throw new Error("unexpected control pipeline"); },
      executeCommandArgs: async () => { throw new Error("unexpected standalone command"); },
      executePipelineOnRoute: async () => responses,
      routeData: () => ({ route })
    });

    await expect(executor.execute([["GET", "one"], ["GET", "two"]], {})).rejects.toThrow(
      "topology pipeline response did not match command group"
    );
  });

  it("rejects sparse Flow named-value selections before dispatch", async () => {
    const executor = new FakeExecutor([null]);
    const client = new FerricStoreClient(executor);
    const values = new Array<string>(2);
    values[0] = "profile";

    await expect(client.get("flow-1", { values })).rejects.toThrow(
      "Flow value names must be dense"
    );
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse queue partition claims before dispatch", async () => {
    const executor = new FakeExecutor([[]]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const partitionKeys = new Array<string>(2);
    partitionKeys[0] = "tenant-a";

    await expect(queue.worker({ leaseRenewal: false }).runBatchOnceForPartitionKeys(
      () => undefined,
      partitionKeys
    )).rejects.toThrow("partitionKeys must be a dense array of strings");
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse Flow signal state guards before dispatch", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);
    const ifState = new Array<string>(2);
    ifState[0] = "queued";

    await expect(client.signal("flow-1", { signal: "approve", ifState })).rejects.toThrow(
      "ifState must be a dense array of strings"
    );
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse variable-length KV collection responses", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("first");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.sets.smembers("members")).rejects.toThrow(
      "SMEMBERS response item 1 is missing"
    );
  });
});
