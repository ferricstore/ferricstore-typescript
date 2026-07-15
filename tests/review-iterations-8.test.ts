import { describe, expect, it } from "vitest";

import { FerricStoreClient, QueueClient, WorkflowClient } from "../src/index.js";
import { ConnectionClosedError } from "../src/errors.js";
import type { Command, CommandArgument } from "../src/internal.js";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import { NativeResponseHandler } from "../src/native-response-handler.js";
import {
  DEFAULT_MAX_FRAME_BYTES,
  OPCODES,
  buildProtocolCommand,
  pipelineCommand
} from "../src/protocol.js";
import { isExplicitlySafeReroute } from "../src/topology-options.js";
import { responseFrame } from "./adapter-test-support.js";
import { FakeExecutor } from "./fake-executor.js";

function inheritedArray<T>(value: T): T[] {
  const values = new Array<T>(1);
  const prototype = Object.create(Array.prototype) as Record<number, T>;
  Object.defineProperty(prototype, 0, { configurable: true, value });
  Object.setPrototypeOf(values, prototype);
  return values;
}

describe("review iterations 8", () => {
  it("rejects inherited collection entries before KV dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const inheritedPair: [string, unknown] = ["key", "value"];
    const inheritedPairPrototype = Object.create(Array.prototype) as Record<number, unknown>;
    Object.defineProperties(inheritedPairPrototype, {
      0: { configurable: true, value: "key" },
      1: { configurable: true, value: "value" }
    });
    Reflect.deleteProperty(inheritedPair, "0");
    Reflect.deleteProperty(inheritedPair, "1");
    Object.setPrototypeOf(inheritedPair, inheritedPairPrototype);

    const operations = [
      async () => await client.kv.del(inheritedArray("key")),
      async () => await client.kv.mset([inheritedPair]),
      async () => await client.kv.mset(inheritedArray<[string, unknown]>(["key", "value"])),
      async () => await client.cms.incrBy("cms", inheritedArray<[unknown, number]>(["item", 1])),
      async () => await client.zset.zadd("zset", inheritedArray({ member: "item", score: 1 })),
      async () => await client.geo.geoadd("geo", inheritedArray({ latitude: 1, longitude: 2, member: "item" })),
      async () => await client.stream.xread(inheritedArray({ id: "0", key: "stream" }))
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow(/dense|own/u);
    }
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse arguments in compact native encoders", () => {
    const mget = new Array<CommandArgument>(2);
    mget[0] = "MGET";
    const get = new Array<CommandArgument>(2);
    get[0] = "GET";

    expect(() => buildProtocolCommand(mget)).toThrow(/dense/u);
    expect(() => pipelineCommand([get as Command])).toThrow(/dense/u);
  });

  it("does not drop unsupported MSET keys from auto-batch ordering", async () => {
    let markFirstStarted: (() => void) | undefined;
    let releaseFirst: (() => void) | undefined;
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    let secondStarted = false;
    const client = new FerricStoreClient({
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        if (commands[0]?.[1] === "first") {
          markFirstStarted?.();
          await firstGate;
        } else {
          secondStarted = true;
        }
        return [Buffer.from("OK")];
      }
    }, { autoBatch: { enabled: true, maxCommands: 1 } });

    const first = client.command("MSET", "first", "1", null, "shared-1");
    await firstStarted;
    const second = client.command("MSET", "second", "2", null, "shared-2");
    await new Promise((resolve) => setImmediate(resolve));

    expect(secondStarted).toBe(false);
    releaseFirst?.();
    await Promise.all([first, second]);
    expect(secondStarted).toBe(true);
  });

  it("retries only connection closures known to be unsent", () => {
    expect(isExplicitlySafeReroute(new ConnectionClosedError("unsent"))).toBe(true);
    expect(isExplicitlySafeReroute(new ConnectionClosedError("possibly_sent"))).toBe(false);
  });

  it("rejects inherited Flow bulk items before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const claimed = {
      fencingToken: 1,
      id: "job-1",
      leaseToken: Buffer.from("lease"),
      state: "queued",
      type: "order"
    };
    const operations = [
      async () => await client.createMany(undefined, inheritedArray({ id: "job-1", payload: "payload" }), {
        type: "order"
      }),
      async () => await client.completeMany(undefined, inheritedArray(claimed)),
      async () => await client.valueMGet(inheritedArray("ref-1")),
      async () => await client.spawnChildren("parent", inheritedArray({
        id: "child-1",
        payload: "payload",
        type: "order"
      }))
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow(/dense|own/u);
    }
    expect(executor.calls).toEqual([]);
  });

  it("rejects an empty child spawn locally", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.spawnChildren("parent", [])).rejects.toThrow("non-empty");
    expect(executor.calls).toEqual([]);
  });

  it("rejects unsupported request-zero management opcodes", () => {
    const failures: unknown[] = [];
    const events: unknown[] = [];
    const handler = new NativeResponseHandler({
      applyFlowControlLimits: () => undefined,
      beginDraining: () => undefined,
      chunkAssembler: new NativeChunkAssembler(1_000_000, 1_000, 1_000_000),
      destroy: () => undefined,
      failAll: (error) => { failures.push(error); },
      heartbeat: new NativeHeartbeat(undefined, async () => undefined, () => undefined),
      maxChunkBytes: 1_000_000,
      maxChunkFrames: 1_000,
      maxFrameBytes: DEFAULT_MAX_FRAME_BYTES,
      maxResponseBytes: 1_000_000,
      onEvent: (event) => { events.push(event); },
      pause: () => undefined,
      pending: new Map(),
      resume: () => undefined,
      takePending: () => undefined
    });

    handler.onData(responseFrame(OPCODES.get, 0, 0n, "unexpected"));

    expect(failures).toHaveLength(1);
    expect(String(failures[0])).toContain("unsupported management opcode");
    expect(events).toEqual([]);
  });

  it("keeps public worker configuration immutable after construction", () => {
    const client = new FerricStoreClient(new FakeExecutor());
    const queueWorker = new QueueClient(client).queue("email").worker({
      concurrency: 2,
      partitionKeys: ["tenant-a"],
      claimValues: ["details"]
    });
    const workflowWorker = new WorkflowClient(client).workflow({ type: "order" }).worker({
      states: ["created"]
    });
    const partitionKeys = queueWorker.options.partitionKeys;
    const claimValues = queueWorker.options.claimValues;
    const states = workflowWorker.options.states;
    if (partitionKeys == null || claimValues == null || states == null) {
      throw new Error("worker configuration snapshot is incomplete");
    }

    expect(() => { queueWorker.options.concurrency = 20; }).toThrow(TypeError);
    expect(() => { partitionKeys[0] = "tenant-b"; }).toThrow(TypeError);
    expect(() => { claimValues.push("secret"); }).toThrow(TypeError);
    expect(() => { states.push("cancelled"); }).toThrow(TypeError);
    expect(queueWorker.options).toMatchObject({
      concurrency: 2,
      partitionKeys: ["tenant-a"],
      claimValues: ["details"]
    });
    expect(workflowWorker.options.states).toEqual(["created"]);
  });

  it("rejects inherited fixed-position KV response slots", async () => {
    const response = new Array<unknown>(2);
    const prototype = Object.create(Array.prototype) as Record<number, unknown>;
    Object.defineProperties(prototype, {
      0: { configurable: true, value: 1 },
      1: { configurable: true, value: 2 }
    });
    Object.setPrototypeOf(response, prototype);
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.kv.waitAof(1, 1, 100)).rejects.toThrow(/missing|invalid/u);
  });

  it("rejects inherited nested stream response slots", async () => {
    const entry = new Array<unknown>(2);
    const prototype = Object.create(Array.prototype) as Record<number, unknown>;
    Object.defineProperties(prototype, {
      0: { configurable: true, value: "1-0" },
      1: { configurable: true, value: ["field", Buffer.from("value")] }
    });
    Object.setPrototypeOf(entry, prototype);
    const response = [["events", [entry]]];
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.stream.xread([{ id: "0", key: "events" }])).rejects.toThrow(
      /stream entry|missing|invalid/u
    );
  });
});
