import { describe, expect, it } from "vitest";

import {
  FerricStoreClient,
  QueueClient,
  RoutingTopology,
  TopologyNativeAdapterPool,
  WorkflowClient,
  complete,
  isOutcome,
  type CommandExecutor
} from "../src/index.js";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import { NativeResponseHandler } from "../src/native-response-handler.js";
import {
  buildProtocolCommand,
  DEFAULT_MAX_FRAME_BYTES,
  encodeValue,
  MAX_I64,
  MAX_U64,
  OPCODES
} from "../src/protocol.js";
import { responseFrameFromBody } from "./adapter-test-support.js";
import { FakeExecutor } from "./fake-executor.js";

describe("third ten-pass review regressions", () => {
  it("normalizes and validates the fixed-width WAITAOF reply", async () => {
    const valid = new FakeExecutor([[Buffer.from("1"), 2n]]);
    const client = new FerricStoreClient(valid);

    await expect(client.kv.waitAof(1, 2, 100)).resolves.toEqual([1, 2]);

    const malformed = new FerricStoreClient(new FakeExecutor([[1]]));
    await expect(malformed.kv.waitAof(1, 2, 100)).rejects.toThrow(
      "WAITAOF returned 1 items; expected 2"
    );
  });

  it("does not coerce a null SISMEMBER reply into a negative membership result", async () => {
    const client = new FerricStoreClient(new FakeExecutor([null]));

    await expect(client.sets.sismember("set", "member")).rejects.toThrow(
      "SISMEMBER returned an invalid boolean response"
    );
  });

  it("rejects a missing auto-batch response item without losing valid siblings", async () => {
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        throw new Error("unexpected individual command");
      },
      async executePipeline(): Promise<unknown[]> {
        const results = new Array<unknown>(2);
        results[1] = Buffer.from("second");
        return results;
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 2, maxDelayMs: 0 }
    });

    const first = client.command("GET", "first");
    const second = client.command("GET", "second");

    await expect(first).rejects.toThrow("auto-batch response item 0 is missing");
    await expect(second).resolves.toEqual(Buffer.from("second"));
  });

  it("rejects out-of-range native bigints before frame allocation or compact writes", () => {
    const unsigned = MAX_I64 + 1n;
    const tooLarge = MAX_U64 + 1n;

    expect(() => encodeValue(unsigned)).not.toThrow();
    expect(() => encodeValue(tooLarge)).toThrow("signed or unsigned 64-bit");
    expect(() => buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "partition",
      "NOW",
      1,
      "ITEMS",
      "flow",
      Buffer.from("lease"),
      unsigned
    ])).toThrow("signed 64-bit");
  });

  it("releases partial native receive pages when the response handler stops", () => {
    const handler = new NativeResponseHandler({
      applyFlowControlLimits: () => undefined,
      beginDraining: () => undefined,
      chunkAssembler: new NativeChunkAssembler(1_000_000, 1_000, 1_000_000),
      destroy: () => undefined,
      failAll: () => undefined,
      heartbeat: new NativeHeartbeat(undefined, async () => undefined, () => undefined),
      maxChunkBytes: 1_000_000,
      maxChunkFrames: 1_000,
      maxFrameBytes: DEFAULT_MAX_FRAME_BYTES,
      maxResponseBytes: 1_000_000,
      pause: () => undefined,
      pending: new Map(),
      resume: () => undefined,
      takePending: () => undefined
    });
    const encoded = responseFrameFromBody(OPCODES.ping, 0, 1n, Buffer.alloc(128 * 1024));
    handler.onData(encoded.subarray(0, 32 * 1024));
    const receive = (handler as unknown as {
      receiveBuffer: {
        assembly?: { pages: Buffer[] };
        chunks: Buffer[];
      };
    }).receiveBuffer;

    expect((receive.assembly?.pages.length ?? 0) + receive.chunks.length).toBeGreaterThan(0);
    handler.stop();
    expect((receive.assembly?.pages.length ?? 0) + receive.chunks.length).toBe(0);
  });

  it("replans a routed command when topology changes during shard acquisition", async () => {
    const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
      urls: readonly string[],
      options: { endpointPolicy: "any" }
    ) => TopologyNativeAdapterPool;
    const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
    const topologyFor = (host: string, epoch: number) => RoutingTopology.build({
      ranges: [{
        endpoint: { host, native_port: 6388, node: `${host}@cluster` },
        first_slot: 0,
        lane_id: epoch,
        last_slot: 1023,
        shard: 0
      }],
      route_epoch: epoch
    });
    const oldTopology = topologyFor("old.local", 1);
    const newTopology = topologyFor("new.local", 2);
    const internals = pool as unknown as {
      adapterForEndpoint(endpoint: { host: string }): Promise<{
        executeCommandOnLane(args: readonly unknown[], laneId: number): Promise<unknown>;
      }>;
      topologyValue: RoutingTopology;
    };
    internals.topologyValue = oldTopology;
    let releaseOld: (() => void) | undefined;
    let markOldStarted: (() => void) | undefined;
    const oldGate = new Promise<void>((resolve) => { releaseOld = resolve; });
    const oldStarted = new Promise<void>((resolve) => { markOldStarted = resolve; });
    const dispatched: string[] = [];
    internals.adapterForEndpoint = async (endpoint) => {
      if (endpoint.host === "old.local") {
        markOldStarted?.();
        await oldGate;
      }
      return {
        async executeCommandOnLane(): Promise<unknown> {
          dispatched.push(endpoint.host);
          return endpoint.host;
        }
      };
    };

    const operation = pool.executeCommand("GET", "tenant-key");
    await oldStarted;
    internals.topologyValue = newTopology;
    releaseOld?.();

    try {
      await expect(operation).resolves.toBe("new.local");
      expect(dispatched).toEqual(["new.local"]);
    } finally {
      await pool.close();
    }
  });

  it("prevalidates direct createMany arrays and mixed partitions before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const sparse = new Array<{ id: string }>(2);
    sparse[0] = { id: "first" };

    await expect(client.createMany(undefined, sparse, {
      nowMs: 100,
      type: "order"
    })).rejects.toThrow("createMany items must be dense");
    await expect(client.createMany(undefined, [
      { id: "partitioned", partitionKey: "tenant-a" },
      { id: "missing" }
    ], {
      nowMs: 100,
      type: "order"
    })).rejects.toThrow("mixed createMany items require partitionKey");
    expect(executor.calls).toEqual([]);
  });

  it("isolates queue and workflow lease identity from handler mutations", async () => {
    const claimed = (id: string) => new Map<unknown, unknown>([
      ["id", id],
      ["type", "order"],
      ["state", "queued"],
      ["partition_key", "tenant-a"],
      ["lease_token", Buffer.from("original-lease")],
      ["fencing_token", 7]
    ]);
    const mutate = (job: {
      fencingToken: number | bigint;
      id: string;
      leaseToken: Buffer;
      partitionKey?: string;
    }): void => {
      job.id = "redirected";
      job.partitionKey = "other-tenant";
      job.fencingToken = 99;
      job.leaseToken.fill(0);
    };

    const queueExecutor = new FakeExecutor([[claimed("queue-1")], Buffer.from("OK")]);
    const queue = new QueueClient(new FerricStoreClient(queueExecutor)).queue("order");
    await queue.worker({ batchSize: 1, claimPayload: false, leaseRenewal: false }).runOnce((job) => {
      mutate(job);
    });
    const queueCompletion = queueExecutor.calls.find((call) => call[0] === "FLOW.COMPLETE_MANY");
    const queueItems = queueCompletion?.indexOf("ITEMS") ?? -1;
    expect(queueCompletion?.[1]).toBe("tenant-a");
    expect(queueCompletion?.slice(queueItems, queueItems + 4)).toEqual([
      "ITEMS",
      "queue-1",
      Buffer.from("original-lease"),
      7
    ]);

    const workflowExecutor = new FakeExecutor([[claimed("workflow-1")], Buffer.from("OK")]);
    const workflow = new WorkflowClient(new FerricStoreClient(workflowExecutor)).workflow({ type: "order" });
    workflow.state("queued", (ctx) => {
      mutate(ctx.job);
    }, { claimPayload: false });
    await workflow.worker({
      batchSize: 1,
      claimPayload: false,
      leaseRenewal: false,
      states: ["queued"]
    }).runOnce();
    const workflowCompletion = workflowExecutor.calls.find((call) => call[0] === "FLOW.COMPLETE");
    expect(workflowCompletion?.slice(0, 5)).toEqual([
      "FLOW.COMPLETE",
      "workflow-1",
      Buffer.from("original-lease"),
      "FENCING",
      7
    ]);
    const partition = workflowCompletion?.indexOf("PARTITION") ?? -1;
    expect(workflowCompletion?.slice(partition, partition + 2)).toEqual(["PARTITION", "tenant-a"]);
  });

  it("exposes the public outcome type guard from the package entrypoint", () => {
    expect(isOutcome(complete())).toBe(true);
    expect(isOutcome({ kind: "unknown" })).toBe(false);
  });

  it("rejects sparse runStepsMany inputs before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const items = new Array<string | { id: string }>(2);
    items[0] = "flow-1";

    await expect(client.runStepsMany(items, {
      nowMs: 100,
      states: ["queued"],
      type: "order",
      worker: "worker-1"
    })).rejects.toThrow("runStepsMany items must be dense");
    expect(executor.calls).toEqual([]);
  });
});
