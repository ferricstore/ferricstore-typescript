import { describe, expect, it } from "vitest";

import {
  NativeAdapter,
  FerricStoreClient,
  QueueClient,
  WorkflowClient,
  type CommandArgument,
  type CommandExecutor,
  type ExecutePipelineOptions
} from "../src/index.js";
import { flowRecordFromResp } from "../src/types.js";
import { FakeExecutor, fakeFlowPolicySnapshot } from "./fake-executor.js";
import { maybeAutoBatchExecutor } from "../src/auto-batch.js";
import {
  routingKeyArguments,
  splitFlowValueMGetArguments
} from "../src/command-grammar.js";
import {
  compactMsetPayload,
  compactPipelinePayload
} from "../src/protocol-compact-request.js";
import { buildProtocolCommand } from "../src/protocol.js";
import { planValueWithLimit } from "../src/protocol-value.js";
import { ReconnectingExecutor } from "../src/reconnecting-executor.js";
import { executeNativePipeline } from "../src/native-pipeline-execution.js";
import { compactFlowValueMGetPayload } from "../src/protocol-flow-compact-single.js";
import { compactFlowTransitionManyPayload } from "../src/protocol-flow-compact-transition.js";
import { TopologyPipelineExecutor } from "../src/topology-pipeline.js";
import { valueRefToString } from "../src/workflow-utilities.js";
import {
  RoutingTopology,
  TopologyNativeAdapterPool,
  type RoutingRoute
} from "../src/topology.js";

class DeferredExecutor implements CommandExecutor {
  readonly calls: CommandArgument[][] = [];
  private readonly pending: ((value: unknown) => void)[] = [];

  executeCommand(...args: CommandArgument[]): Promise<unknown> {
    this.calls.push([...args]);
    return new Promise((resolve) => this.pending.push(resolve));
  }

  respond(index: number, value: unknown): void {
    const resolve = this.pending[index];
    if (resolve == null) throw new Error(`request ${index} is not pending`);
    resolve(value);
  }
}

describe("review iterations 11", () => {
  it("keeps KV response cardinality aligned with the dispatched request", async () => {
    const executor = new DeferredExecutor();
    const client = new FerricStoreClient(executor);
    const keys = ["first", "second"];
    const fields = ["left", "right"];

    const mget = client.kv.mget(keys);
    keys.length = 1;
    executor.respond(0, [Buffer.from("one"), Buffer.from("two")]);
    await expect(mget).resolves.toEqual([Buffer.from("one"), Buffer.from("two")]);

    const hmget = client.hash.hmget("hash", fields);
    fields.length = 1;
    executor.respond(1, [Buffer.from("three"), Buffer.from("four")]);
    await expect(hmget).resolves.toEqual([Buffer.from("three"), Buffer.from("four")]);

    expect(executor.calls).toEqual([
      ["MGET", "first", "second"],
      ["HMGET", "hash", "left", "right"]
    ]);
  });

  it("keeps collection and module response cardinality aligned with dispatched inputs", async () => {
    const executor = new DeferredExecutor();
    const client = new FerricStoreClient(executor);
    const members = ["one", "two"];
    const elements = ["left", "right"];

    const positions = client.geo.geoposMany("places", members);
    members.length = 1;
    executor.respond(0, [["1", "2"], ["3", "4"]]);
    await expect(positions).resolves.toEqual([["1", "2"], ["3", "4"]]);

    const exists = client.bloom.mexistsMany("filter", elements);
    elements.length = 1;
    executor.respond(1, [1, 0]);
    await expect(exists).resolves.toEqual([1, 0]);
  });

  it("ignores an inherited COPY option", async () => {
    const executor = new FakeExecutor([1]);
    const client = new FerricStoreClient(executor);
    const options = Object.create({ replace: true }) as { replace?: boolean };

    await expect(client.kv.copy("source", "destination", options)).resolves.toBe(true);
    expect(executor.calls).toEqual([["COPY", "source", "destination"]]);
  });

  it("rejects inherited sorted-set LIMIT fields before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const limit = Object.create({ count: 2, offset: 1 }) as {
      count: number;
      offset: number;
    };

    await expect(client.zset.zrangebyscore("rank", 0, 10, { limit })).rejects.toThrow(
      "LIMIT requires own offset and count values"
    );
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse diagnostic key-value responses", async () => {
    const response = new Array<unknown>(2);
    response[0] = "status";
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.clusterHealth()).rejects.toThrow(
      "key-value response item 1 is missing"
    );
  });

  it("rejects sparse arrays nested in Flow response maps", () => {
    const metadata = new Array<unknown>(2);
    metadata[0] = "present";

    expect(() => flowRecordFromResp({
      id: "flow-1",
      state: "running",
      type: "order",
      value_refs: { customer: metadata }
    })).toThrow("response array item 1 is missing");
  });

  it("does not reinterpret non-decimal count strings for routing or value-MGET options", () => {
    expect(routingKeyArguments("SINTERCARD", [
      "SINTERCARD", "1e0", "key", "LIMIT", 0
    ])).toEqual({ handled: true, keys: [] });
    expect(splitFlowValueMGetArguments([
      "FLOW.VALUE.MGET", "ref", "MAX_BYTES", "1e3"
    ], 1)).toMatchObject({
      hasMaxBytes: false,
      refs: ["ref", "MAX_BYTES", "1e3"]
    });
  });

  it("does not turn malformed Flow integers into valid native values", () => {
    expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "order",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      "1e2",
      "RETURN",
      "JOBS_COMPACT"
    ])).toThrow("integer command argument must be an integer");
  });

  it("rejects impossible value plans before reading array items", () => {
    const values = new Array<unknown>(3);
    let reads = 0;
    for (let index = 0; index < values.length; index += 1) {
      Object.defineProperty(values, index, {
        configurable: true,
        enumerable: true,
        get: () => {
          reads += 1;
          return null;
        }
      });
    }

    expect(() => planValueWithLimit(values, 7)).toThrow("frame limit");
    expect(reads).toBe(0);
  });

  it("rejects impossible compact pipelines before proportional planning allocations", () => {
    const command: readonly CommandArgument[] = ["SET", "key", "value"];
    const target = new Array<readonly CommandArgument[]>(100_000);
    const commands = new Proxy(target, {
      get(source, property, receiver): unknown {
        return arrayIndex(property, source.length)
          ? command
          : Reflect.get(source, property, receiver) as unknown;
      },
      getOwnPropertyDescriptor(source, property) {
        return arrayIndex(property, source.length)
          ? { configurable: true, enumerable: true, value: command, writable: true }
          : Reflect.getOwnPropertyDescriptor(source, property);
      }
    });
    const nativeUint32Array = globalThis.Uint32Array;
    let planningAllocations = 0;
    const trackingUint32Array = new Proxy(nativeUint32Array, {
      construct(targetConstructor, args, newTarget): object {
        planningAllocations += 1;
        return Reflect.construct(targetConstructor, args, newTarget) as object;
      }
    });
    Object.defineProperty(globalThis, "Uint32Array", {
      configurable: true,
      value: trackingUint32Array,
      writable: true
    });

    try {
      expect(() => compactPipelinePayload(commands, 16)).toThrow("frame limit");
    } finally {
      Object.defineProperty(globalThis, "Uint32Array", {
        configurable: true,
        value: nativeUint32Array,
        writable: true
      });
    }
    expect(planningAllocations).toBe(0);
  });

  it("preserves compact fallback when an oversized shape is unsupported", () => {
    expect(compactPipelinePayload([
      ["SET", "key", "value"],
      ["GET", "key"]
    ], 1)).toBeUndefined();
    expect(compactMsetPayload(["key", {}], 1)).toBeUndefined();
  });

  it("snapshots reconnecting commands and pipeline options before initial connection", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const seenCommands: CommandArgument[][] = [];
    const seenPipelines: CommandArgument[][][] = [];
    const seenFused: CommandArgument[][][] = [];
    const seenOptions: unknown[] = [];
    const executor = new ReconnectingExecutor(async () => {
      await gate;
      return {
        async executeCommandArgs(args): Promise<unknown> {
          seenCommands.push([...args]);
          return Buffer.from("OK");
        },
        async executeCommand(...args): Promise<unknown> {
          seenCommands.push([...args]);
          return Buffer.from("OK");
        },
        async executePipeline(commands, options): Promise<unknown[]> {
          seenPipelines.push(commands.map((command) => [...command]));
          seenOptions.push(options);
          return commands.map(() => Buffer.from("OK"));
        },
        async executeFusedPipeline(commands, options): Promise<unknown[]> {
          seenFused.push(commands.map((command) => [...command]));
          seenOptions.push(options);
          return commands.map(() => Buffer.from("OK"));
        }
      };
    });
    const args: CommandArgument[] = ["SET", "key", "command-original"];
    const pipeline: CommandArgument[][] = [["SET", "key", "pipeline-original"]];
    const fused: CommandArgument[][] = [["SET", "key", "fused-original"]];
    const dependencies = [[0]];
    const pipelineOptions = { fallbackDependencies: dependencies, ordered: true };
    const fusedOptions = { ordered: true };

    const commandResult = executor.executeCommandArgs(args);
    const pipelineResult = executor.executePipeline(pipeline, pipelineOptions);
    const fusedResult = executor.executeFusedPipeline(fused, fusedOptions);
    const pipelineCommand = pipeline[0];
    const fusedCommand = fused[0];
    const dependency = dependencies[0];
    if (pipelineCommand == null || fusedCommand == null || dependency == null) {
      throw new Error("expected mutable pipeline fixtures");
    }
    args[2] = "command-mutated";
    pipelineCommand[2] = "pipeline-mutated";
    fusedCommand[2] = "fused-mutated";
    dependency[0] = 9;
    pipelineOptions.ordered = false;
    fusedOptions.ordered = false;
    release?.();

    await Promise.all([commandResult, pipelineResult, fusedResult]);
    expect(seenCommands).toEqual([["SET", "key", "command-original"]]);
    expect(seenPipelines).toEqual([[["SET", "key", "pipeline-original"]]]);
    expect(seenFused).toEqual([[["SET", "key", "fused-original"]]]);
    expect(seenOptions).toEqual([
      { fallbackDependencies: [[0]], ordered: true },
      { ordered: true }
    ]);
    await executor.close();
  });

  it("snapshots auto-batch fused pipelines before its ordering await", async () => {
    const seen: CommandArgument[][][] = [];
    const seenOrdered: (boolean | undefined)[] = [];
    const executor = maybeAutoBatchExecutor({
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        return commands.map(() => Buffer.from("OK"));
      },
      async executeFusedPipeline(commands, options): Promise<unknown[]> {
        seen.push(commands.map((command) => [...command]));
        seenOrdered.push(options?.ordered);
        return commands.map(() => Buffer.from("OK"));
      }
    }, true);
    const commands: CommandArgument[][] = [["SET", "key", "original"]];
    const options = { ordered: true };

    const pending = executor.executeFusedPipeline?.(commands, options);
    if (pending == null) throw new Error("fused pipeline is unavailable");
    const command = commands[0];
    if (command == null) throw new Error("expected a fused command fixture");
    command[2] = "mutated";
    options.ordered = false;
    await pending;

    expect(seen).toEqual([[["SET", "key", "original"]]]);
    expect(seenOrdered).toEqual([true]);
    await executor.close?.();
  });

  it("snapshots direct topology commands, pipelines, and options before adapter lookup", async () => {
    const Pool = TopologyNativeAdapterPool as unknown as new (
      urls: readonly string[],
      options: { endpointPolicy: "any" }
    ) => TopologyNativeAdapterPool;
    const pool = new Pool(["ferric://seed.local:6388"], { endpointPolicy: "any" });
    (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
      ranges: [{
        endpoint: { host: "leader.local", native_port: 6388, node: "leader@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      route_epoch: 1,
      shard_count: 1
    });
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const seenCommands: CommandArgument[][] = [];
    const seenPipelines: CommandArgument[][][] = [];
    const seenOptions: ExecutePipelineOptions[] = [];
    const adapter = {
      async executeCommandOnLane(args: readonly CommandArgument[]): Promise<unknown> {
        seenCommands.push([...args]);
        return Buffer.from("OK");
      },
      async executePipelineOnLane(
        commands: readonly (readonly CommandArgument[])[],
        _laneId: number,
        options: ExecutePipelineOptions
      ): Promise<unknown[]> {
        seenPipelines.push(commands.map((command) => [...command]));
        seenOptions.push(options);
        return commands.map(() => Buffer.from("OK"));
      },
      async executeFusedPipelineOnLane(
        commands: readonly (readonly CommandArgument[])[],
        _laneId: number,
        options: ExecutePipelineOptions
      ): Promise<unknown[]> {
        seenPipelines.push(commands.map((command) => [...command]));
        seenOptions.push(options);
        return commands.map(() => Buffer.from("OK"));
      }
    } as unknown as NativeAdapter;
    const internals = pool as unknown as {
      adapterForCurrentRoute(route: RoutingRoute): Promise<NativeAdapter>;
    };
    internals.adapterForCurrentRoute = async () => {
      await gate;
      return adapter;
    };
    const command: CommandArgument[] = ["GET", "command-original"];
    const pipeline: CommandArgument[][] = [["GET", "pipeline-original"]];
    const fused: CommandArgument[][] = [["GET", "fused-original"]];
    const dependencies = [[0]];
    const options = { fallbackDependencies: dependencies, ordered: true };

    const commandResult = pool.executeCommandArgs(command);
    const pipelineResult = pool.executePipeline(pipeline, options);
    const fusedResult = pool.executeFusedPipeline(fused, options);
    const pipelineCommand = pipeline[0];
    const fusedCommand = fused[0];
    const dependency = dependencies[0];
    if (pipelineCommand == null || fusedCommand == null || dependency == null) {
      throw new Error("expected mutable topology fixtures");
    }
    command[1] = "command-mutated";
    pipelineCommand[1] = "pipeline-mutated";
    fusedCommand[1] = "fused-mutated";
    dependency[0] = 8;
    options.ordered = false;
    release?.();

    try {
      await Promise.all([commandResult, pipelineResult, fusedResult]);
      expect(seenCommands).toEqual([["GET", "command-original"]]);
      expect(seenPipelines).toEqual([
        [["GET", "pipeline-original"]],
        [["GET", "fused-original"]]
      ]);
      expect(seenOptions).toEqual([
        { fallbackDependencies: [[0]], ordered: true },
        { fallbackDependencies: [[0]], ordered: true }
      ]);
    } finally {
      release?.();
      await pool.close();
    }
  });

  it("ignores forward split-pipeline dependencies instead of serializing shard work", async () => {
    const started: string[] = [];
    let releaseSecond: (() => void) | undefined;
    const secondGate = new Promise<void>((resolve) => { releaseSecond = resolve; });
    const route = (laneId: number): RoutingRoute => ({
      endpoint: { host: `lane-${laneId}.local`, nativePort: 6388, node: `lane-${laneId}` },
      endpointKey: `lane-${laneId}.local:6388`,
      laneId,
      leaderNode: `lane-${laneId}`,
      shard: laneId
    });
    const executor = new TopologyPipelineExecutor({
      concurrency: 2,
      async controlPipeline(): Promise<unknown[]> { return []; },
      async executeCommandArgs(): Promise<unknown> { return Buffer.from("OK"); },
      async executePipelineOnRoute(commands, selectedRoute): Promise<unknown[]> {
        const key = commands[0]?.[1];
        if (typeof key !== "string") throw new Error("expected a string routing key");
        started.push(key);
        if (selectedRoute.laneId === 2) await secondGate;
        return commands.map(() => Buffer.from("OK"));
      },
      routeData: (args) => ({ route: route(args[1] === "first" ? 1 : 2) })
    });

    const execution = executor.execute([
      ["GET", "first"],
      ["GET", "second"]
    ], { fallbackDependencies: [[1], []] });
    try {
      await new Promise((resolve) => setImmediate(resolve));
      expect(started).toEqual(["first", "second"]);
    } finally {
      releaseSecond?.();
    }
    await expect(execution).resolves.toEqual([Buffer.from("OK"), Buffer.from("OK")]);
  });

  it("snapshots native fallback commands and error options across sequential requests", async () => {
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const failure = new Error("second failed");
    const seen: CommandArgument[][] = [];
    const host = {
      async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
        seen.push([...args]);
        if (args[1] === "first") {
          await firstGate;
          return Buffer.from("first-value");
        }
        throw failure;
      },
      async executeCommandOnLane(): Promise<unknown> {
        throw new Error("unexpected lane dispatch");
      },
      async executeProtocolCommand(): Promise<unknown> {
        throw new Error("unexpected fused dispatch");
      }
    };
    const commands: CommandArgument[][] = [
      ["GET", "first"],
      ["GET", "second-original"]
    ];
    const options = { fallbackConcurrency: 1, throwOnItemError: false };

    const execution = executeNativePipeline(
      host,
      commands,
      undefined,
      options,
      0,
      1_024
    );
    const secondCommand = commands[1];
    if (secondCommand == null) throw new Error("expected a second native command fixture");
    secondCommand[1] = "second-mutated";
    options.throwOnItemError = true;
    releaseFirst?.();

    await expect(execution).resolves.toEqual([Buffer.from("first-value"), failure]);
    expect(seen).toEqual([
      ["GET", "first"],
      ["GET", "second-original"]
    ]);
  });

  it("reuses the admitted completion request when fused execution falls back", async () => {
    let releaseFused: (() => void) | undefined;
    const fusedGate = new Promise<void>((resolve) => { releaseFused = resolve; });
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push([...args]);
        return args[0] === "FLOW.CLAIM_DUE" ? [] : Buffer.from("OK");
      },
      async executeFusedPipeline(): Promise<undefined> {
        await fusedGate;
        return undefined;
      }
    };
    const client = new FerricStoreClient(executor);
    const jobs = [{
      fencingToken: 7,
      id: "flow-original",
      leaseToken: Buffer.from("lease-original"),
      partitionKey: "tenant-a",
      state: "running"
    }];

    const execution = client.completeJobsAndClaimJobs(jobs, "order", {
      jobOnly: true,
      partitionKey: "tenant-a",
      state: "queued",
      worker: "worker-1"
    });
    jobs[0] = {
      fencingToken: 9,
      id: "flow-mutated",
      leaseToken: Buffer.from("lease-mutated"),
      partitionKey: "tenant-b",
      state: "running"
    };
    releaseFused?.();

    await expect(execution).resolves.toMatchObject({ claimed: [], fused: false });
    expect(calls[0]).toEqual(expect.arrayContaining([
      "FLOW.COMPLETE_MANY",
      "tenant-a",
      "flow-original",
      Buffer.from("lease-original"),
      7
    ]));
    expect(calls.flat()).not.toContain("flow-mutated");
  });

  it("does not classify inherited Flow policy fields as state-policy options", async () => {
    const executor = new FakeExecutor([fakeFlowPolicySnapshot("order")]);
    const client = new FerricStoreClient(executor);
    const retryPolicy = inheritedObject({ mode: "fifo" }, { maxRetries: 3 });

    await client.installPolicy("order", {
      states: { queued: retryPolicy }
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "queued",
      "MAX_RETRIES",
      3
    ]);
  });

  it("rejects impossible compact Flow bodies before typed planning allocations", () => {
    const refs = virtualArray<CommandArgument>(100_000, () => "ref");
    const transitionItems = virtualArray<CommandArgument>(300_000, (index) => {
      if (index % 3 === 1) return 1;
      return index % 3 === 2 ? "-" : "flow";
    });
    const NativeUint32Array = globalThis.Uint32Array;
    const NativeBigInt64Array = globalThis.BigInt64Array;
    let planningAllocations = 0;
    const track = <T extends typeof Uint32Array | typeof BigInt64Array>(Constructor: T): T =>
      new Proxy(Constructor, {
        construct(target, args, newTarget): object {
          planningAllocations += 1;
          return Reflect.construct(target, args, newTarget) as object;
        }
      });
    Object.defineProperty(globalThis, "Uint32Array", {
      configurable: true,
      value: track(NativeUint32Array),
      writable: true
    });
    Object.defineProperty(globalThis, "BigInt64Array", {
      configurable: true,
      value: track(NativeBigInt64Array),
      writable: true
    });

    try {
      expect(() => compactFlowValueMGetPayload(refs, undefined, 16)).toThrow("frame limit");
      expect(() => compactFlowTransitionManyPayload(
        "tenant-a",
        "running",
        "done",
        transitionItems,
        false,
        { now_ms: 1 },
        16
      )).toThrow("frame limit");
    } finally {
      Object.defineProperty(globalThis, "Uint32Array", {
        configurable: true,
        value: NativeUint32Array,
        writable: true
      });
      Object.defineProperty(globalThis, "BigInt64Array", {
        configurable: true,
        value: NativeBigInt64Array,
        writable: true
      });
    }
    expect(planningAllocations).toBe(0);
  });

  it("captures every independent Flow chunk before the first request awaits", async () => {
    const executor = new DeferredExecutor();
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 1 });
    const items = [
      {
        fencingToken: 1,
        id: "flow-1",
        leaseToken: Buffer.from("lease-1"),
        state: "running"
      },
      {
        fencingToken: 2,
        id: "flow-2-original",
        leaseToken: Buffer.from("lease-2"),
        state: "running"
      }
    ];

    const execution = client.completeMany(undefined, items, {
      independent: true,
      nowMs: 1,
      returnOkOnSuccess: true
    });
    items[1] = {
      fencingToken: 9,
      id: "flow-2-mutated",
      leaseToken: Buffer.from("lease-mutated"),
      state: "running"
    };
    executor.respond(0, Buffer.from("OK"));
    while (executor.calls.length < 2) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    executor.respond(1, Buffer.from("OK"));

    await expect(execution).resolves.toEqual([Buffer.from("OK"), Buffer.from("OK")]);
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "flow-2-original",
      Buffer.from("lease-2"),
      2
    ]));
    expect(executor.calls[1]).not.toContain("flow-2-mutated");
  });

  it("ignores inherited Queue and Workflow configuration", () => {
    const client = new FerricStoreClient(new FakeExecutor());
    const queueOptions = inheritedObject({
      state: "prototype-state",
      worker: "prototype-worker"
    }, { type: "email" });
    const workflowOptions = inheritedObject({
      initialState: "prototype-state",
      valueConfig: { localCache: true, valueMaxBytes: 1 },
      worker: "prototype-worker"
    }, { type: "order" });

    const queue = new QueueClient(client).queue(queueOptions);
    const workflow = new WorkflowClient(client).workflow(workflowOptions);

    expect(queue.state).toBe("queued");
    expect(queue.defaultWorker).toMatch(/^email-/u);
    expect(workflow.initialState).toBe("queued");
    expect(workflow.valueConfig).toEqual({ localCache: false, valueMaxBytes: undefined });
    expect(workflow.defaultWorker).toMatch(/^order-/u);
    expect(() => new QueueClient(client).queue(
      inheritedObject({ type: "prototype-type" }, {})
    )).toThrow("Queue type must be an own non-empty string");
  });

  it("ignores inherited Flow value-reference metadata", () => {
    expect(valueRefToString(inheritedObject({ ref: "prototype-ref" }, {}))).toBeUndefined();
  });

  it("rejects inherited required fields in KV collection descriptors", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const zaddMember = inheritedObject({ member: "member", score: 1 }, {});
    const geoMember = inheritedObject({ latitude: 1, longitude: 2, member: "member" }, {});
    const stream = inheritedObject({ id: "0", key: "stream" }, {});

    await expect(client.zset.zadd("rank", [zaddMember])).rejects.toThrow(
      "ZADD members require own score and member fields"
    );
    await expect(client.geo.geoadd("places", [geoMember])).rejects.toThrow(
      "GEOADD members require own longitude, latitude, and member fields"
    );
    await expect(client.stream.xread([stream])).rejects.toThrow(
      "XREAD streams require own key and id fields"
    );
    expect(executor.calls).toEqual([]);
  });
});

function arrayIndex(property: string | symbol, length: number): boolean {
  if (typeof property !== "string" || !/^(?:0|[1-9]\d*)$/u.test(property)) return false;
  const index = Number(property);
  return index < length;
}

function virtualArray<T>(length: number, value: (index: number) => T): T[] {
  return new Proxy(new Array<T>(length), {
    get(source, property, receiver): unknown {
      return arrayIndex(property, source.length)
        ? value(Number(property))
        : Reflect.get(source, property, receiver) as unknown;
    },
    getOwnPropertyDescriptor(source, property) {
      return arrayIndex(property, source.length)
        ? { configurable: true, enumerable: true, value: value(Number(property)), writable: true }
        : Reflect.getOwnPropertyDescriptor(source, property);
    }
  });
}

function inheritedObject<TPrototype extends object, TOwn extends object>(
  prototype: TPrototype,
  own: TOwn
): TPrototype & TOwn {
  return Object.assign(Object.create(prototype) as TPrototype & TOwn, own);
}
