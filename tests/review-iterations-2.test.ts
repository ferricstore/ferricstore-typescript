import { describe, expect, it } from "vitest";

import { attachPipelineItemRejectionFlags } from "../src/adapters.js";
import {
  FerricStoreClient,
  QueueClient,
  type ClaimedItem,
  type LeaseMutationOptions
} from "../src/index.js";
import { executeNativePipeline, type NativePipelineHost } from "../src/native-pipeline-execution.js";
import {
  COMPACT_PIPELINE_RESPONSE,
  decodeResponse,
  OPCODES,
  STATUS_OK
} from "../src/protocol.js";
import { TopologyPipelineExecutor } from "../src/topology-pipeline.js";
import { BackpressureSocket, directNativeAdapter } from "./adapter-test-support.js";
import { FakeExecutor } from "./fake-executor.js";

describe("second ten-pass review regressions", () => {
  const leaseMutationOptions: LeaseMutationOptions = {
    fencingToken: 1,
    leaseToken: Buffer.from("lease")
  };
  void leaseMutationOptions;

  it("keeps TYPE restricted to top-level SCAN", async () => {
    const executor = new FakeExecutor([["0", []]]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.scan(0, { type: "hash" })).resolves.toEqual(["0", []]);
    await expect(
      client.hash.hscan("hash", 0, { type: "hash" } as never)
    ).rejects.toThrow("HSCAN does not support the TYPE option");
    await expect(
      client.sets.sscan("set", 0, { type: "set" } as never)
    ).rejects.toThrow("SSCAN does not support the TYPE option");
    await expect(
      client.zset.zscan("zset", 0, { type: "zset" } as never)
    ).rejects.toThrow("ZSCAN does not support the TYPE option");

    expect(executor.calls).toEqual([["SCAN", 0, "TYPE", "hash"]]);

    const assertCollectionScansRejectTypeAtCompileTime = (): void => {
      // @ts-expect-error TYPE is supported only by top-level SCAN.
      void client.hash.hscan("hash", 0, { type: "hash" });
      // @ts-expect-error TYPE is supported only by top-level SCAN.
      void client.sets.sscan("set", 0, { type: "set" });
      // @ts-expect-error TYPE is supported only by top-level SCAN.
      void client.zset.zscan("zset", 0, { type: "zset" });
    };
    void assertCollectionScansRejectTypeAtCompileTime;
  });

  it("rejects mixed sorted-set response shapes", async () => {
    const executor = new FakeExecutor([[
      [Buffer.from("member-a"), Buffer.from("1")],
      Buffer.from("member-b")
    ]]);
    const client = new FerricStoreClient(executor);

    await expect(
      client.zset.zrange("rank", 0, -1, { withScores: true })
    ).rejects.toThrow("invalid sorted-set member/score response");
  });

  it("preserves non-Error rejections across native pipeline chunks", async () => {
    const rejection = { code: "custom-rejection" };
    let call = 0;
    const execute = async (): Promise<unknown> => {
      call += 1;
      if (call === 1) {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- custom JS executors may reject with any value
        return await Promise.reject(rejection);
      }
      return Buffer.from("PONG");
    };
    const host: NativePipelineHost = {
      executeCommandArgs: execute,
      executeCommandOnLane: execute,
      executeProtocolCommand: async () => {
        throw new Error("PING must use the individual control-command fallback");
      }
    };

    await expect(
      executeNativePipeline(host, [["PING"], ["PING"]], undefined, {}, 1, 1_024)
    ).rejects.toBe(rejection);
    expect(call).toBe(2);
  });

  it("cancels socket-backpressured writes when native retirement begins", async () => {
    const socket = new BackpressureSocket();
    const adapter = directNativeAdapter(socket, 1024 * 1024, 1_000);
    const blocking = adapter.executeCommand("BLPOP", "queue", 0);
    const queued = adapter.executeCommand("SET", "key", "value");
    void blocking.catch(() => undefined);
    let queuedSettled = false;
    void queued.catch(() => { queuedSettled = true; });

    try {
      expect(socket.writes).toHaveLength(1);
      const retirement = adapter.retire();
      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(queuedSettled).toBe(true);
      await expect(queued).rejects.toMatchObject({ requestDisposition: "unsent" });
      await expect(blocking).rejects.toMatchObject({ requestDisposition: "possibly_sent" });
      await expect(retirement).resolves.toBeUndefined();

      socket.emit("drain");
      expect(socket.writes).toHaveLength(1);
    } finally {
      await adapter.close();
      await Promise.allSettled([blocking, queued]);
    }
  });

  it("preserves per-item rejections across topology pipeline groups", async () => {
    const rejection = { code: "routed-rejection" };
    const responses: unknown[] = [rejection];
    attachPipelineItemRejectionFlags(responses, [true]);
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

    await expect(executor.execute([["GET", "key"]], {})).rejects.toBe(rejection);
  });

  it("rejects sparse Flow many inputs before dispatching earlier chunks", async () => {
    const executor = new FakeExecutor([[["OK"], ["OK"]]]);
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 2 });
    const item = (id: string): ClaimedItem => ({
      fencingToken: 1,
      id,
      leaseToken: Buffer.from(`lease-${id}`),
      partitionKey: undefined,
      state: "running"
    });
    const items = new Array<ClaimedItem>(4);
    items[0] = item("one");
    items[1] = item("two");
    items[3] = item("four");

    await expect(
      client.completeMany(undefined, items, { independent: true, returnOkOnSuccess: true })
    ).rejects.toThrow("FLOW.COMPLETE_MANY items must be dense");
    expect(executor.calls).toEqual([]);
  });

  it("isolates queue batch bookkeeping from handler array mutations", async () => {
    const executor = new FakeExecutor([
      [
        ["one", "tenant", Buffer.from("lease-one"), 1],
        ["two", "tenant", Buffer.from("lease-two"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      leaseRenewal: false,
      worker: "worker"
    }).runBatchOnce((jobs) => {
      jobs.length = 0;
    });

    expect(result).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });
    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.CLAIM_DUE",
      "FLOW.COMPLETE_MANY"
    ]);
  });

  it("rejects impossible compact pipeline counts before allocating result slots", () => {
    const body = Buffer.alloc(7);
    body.writeUInt16BE(STATUS_OK, 0);
    body.writeUInt8(COMPACT_PIPELINE_RESPONSE, 2);
    body.writeUInt32BE(100_000, 3);

    expect(() => decodeResponse({
      body,
      bodyLength: body.byteLength,
      flags: 0,
      laneId: 0,
      opcode: OPCODES.pipeline,
      requestId: 1n
    }, OPCODES.pipeline)).toThrow("compact pipeline item count exceeds response bytes");
  });
});
