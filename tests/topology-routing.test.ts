import type { AddressInfo, Socket } from "node:net";
import { expect, test, vi } from "vitest";
import {
  NativeAdapter,
  pipelineItemRejectionFlags,
  type ExecutePipelineOptions
} from "../src/adapters.js";
import { FerricStoreError, RerouteError } from "../src/errors.js";
import type { Command, CommandArgument } from "../src/internal.js";
import {
  COMMAND_OPCODES,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  type ProtocolCommand
} from "../src/protocol.js";
import { RoutingTopology, TopologyNativeAdapterPool, type RoutingRoute } from "../src/topology.js";
import {
  type TestRequest,
  NO_RESPONSE,
  activeConnections,
  commandExecName,
  flowPartitionForSlot,
  keyForSlot,
  responseFrame,
  startCountingServer,
  twoShardTopology,
  validTopologyPayload,
  validTopologyRange,
  waitFor
} from "./adapter-test-support.js";

test("TopologyNativeAdapterPool routes keyed commands to learned shard leader lane", async () => {
  const leaderRequests: TestRequest[] = [];
  const leader = await startCountingServer((request) => {
    leaderRequests.push(request);
    return undefined;
  });
  const leaderAddress = leader.address() as AddressInfo;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: {
            host: "127.0.0.1",
            native_port: leaderAddress.port,
            node: "leader@local"
          },
          first_slot: 0,
          lane_id: 3,
          last_slot: 1023,
          shard: 0
        }
      ],
      route_epoch: 1,
      shard_count: 1
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    await pool.executeCommand("SET", "tenant-key", "value");

    const routedSet = leaderRequests.find((request) => request.opcode === OPCODES.set);
    expect(routedSet).toMatchObject({ laneId: 3, opcode: OPCODES.set });
    expect(pool.route("tenant-key")).toMatchObject({
      endpoint: { host: "127.0.0.1", nativePort: leaderAddress.port },
      leaderNode: "leader@local"
    });
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool coalesces concurrent learned-endpoint connections", async () => {
  const learned = await startCountingServer(() => Buffer.from("value"), { fragmentResponses: false });
  const learnedAddress = learned.address() as AddressInfo;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: learnedAddress.port, node: "learned@local" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    await Promise.all([pool.executeCommand("GET", "a"), pool.executeCommand("GET", "b")]);
    expect(await activeConnections(learned)).toBe(1);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool replaces a pending adapter that became unavailable", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[]
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"]);
  const unavailableRetire = vi.fn(async () => undefined);
  const unavailable = {
    close: vi.fn(async () => undefined),
    get isUnavailable(): boolean { return true; },
    retire: unavailableRetire
  } as unknown as NativeAdapter;
  const healthyClose = vi.fn(async () => undefined);
  const healthy = {
    close: healthyClose,
    get isUnavailable(): boolean { return false; },
    retire: vi.fn(async () => undefined)
  } as unknown as NativeAdapter;
  const fromUrl = vi.spyOn(NativeAdapter, "fromUrl").mockResolvedValue(healthy);
  const key = "ferric://seed.local:6388";
  const internals = pool as unknown as {
    adapterRegistry: {
      creations: Map<string, Promise<NativeAdapter>>;
      get(key: string, url: string, options: Record<string, never>): Promise<NativeAdapter>;
    };
  };
  internals.adapterRegistry.creations.set(key, Promise.resolve(unavailable));

  try {
    await expect(internals.adapterRegistry.get(key, key, {})).resolves.toBe(healthy);
    expect(fromUrl).toHaveBeenCalledOnce();
    expect(unavailableRetire).toHaveBeenCalledOnce();
  } finally {
    fromUrl.mockRestore();
    await pool.close();
  }
});

test("TopologyNativeAdapterPool evicts a closed cached adapter before refresh", async () => {
  let shardRequests = 0;
  let seedPort = 0;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    shardRequests += 1;
    return {
      ranges: [{
        endpoint: { host: "127.0.0.1", native_port: seedPort, node: "seed@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      route_epoch: shardRequests,
      shard_count: 1
    };
  }, { fragmentResponses: false });
  seedPort = (seed.address() as AddressInfo).port;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedPort}`]);
  const internals = pool as unknown as {
    adapterRegistry: { adapters: Map<string, NativeAdapter> };
  };

  try {
    const cached = [...internals.adapterRegistry.adapters.values()][0];
    if (cached == null) throw new Error("expected a cached seed adapter");
    await cached.close();

    await expect(pool.refreshTopology()).resolves.toMatchObject({ routeEpoch: 2 });
    expect(shardRequests).toBe(2);
    expect([...internals.adapterRegistry.adapters.values()][0]).not.toBe(cached);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool refreshes through the last successful endpoint first", async () => {
  let rejectedRefreshes = 0;
  const rejected = await startCountingServer((request) => {
    if (request.opcode === OPCODES.shards) {
      rejectedRefreshes += 1;
      return { ranges: [], shard_count: 0 };
    }
    return undefined;
  }, { fragmentResponses: false });
  const rejectedAddress = rejected.address() as AddressInfo;
  let healthyRefreshes = 0;
  let healthyPort = 0;
  const healthy = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    healthyRefreshes += 1;
    return {
      ranges: [{
        endpoint: { host: "127.0.0.1", native_port: healthyPort, node: "healthy@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      shard_count: 1
    };
  }, { fragmentResponses: false });
  healthyPort = (healthy.address() as AddressInfo).port;
  const pool = await TopologyNativeAdapterPool.fromUrls([
    `ferric://127.0.0.1:${rejectedAddress.port}`,
    `ferric://127.0.0.1:${healthyPort}`
  ]);

  try {
    expect(rejectedRefreshes).toBe(1);
    expect(healthyRefreshes).toBe(1);
    await pool.refreshTopology();
    expect(healthyRefreshes).toBe(2);
    expect(rejectedRefreshes).toBe(1);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool gracefully retires endpoints removed by a topology refresh", async () => {
  let oldRequest: TestRequest | undefined;
  let oldSocket: Socket | undefined;
  const oldLeader = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.get) {
      oldRequest = request;
      oldSocket = socket;
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const oldAddress = oldLeader.address() as AddressInfo;
  const newLeader = await startCountingServer((request) => {
    return request.opcode === OPCODES.get ? Buffer.from("new") : undefined;
  }, { fragmentResponses: false });
  const newAddress = newLeader.address() as AddressInfo;
  let leaderPort = oldAddress.port;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [{
        endpoint: { host: "127.0.0.1", native_port: leaderPort, node: `leader-${leaderPort}@local` },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      route_epoch: leaderPort,
      shard_count: 1
    };
  }, { fragmentResponses: false });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    const inFlight = pool.executeCommand("GET", "key");
    await waitFor(() => oldRequest != null);

    leaderPort = newAddress.port;
    await pool.refreshTopology();

    expect(await activeConnections(oldLeader)).toBe(1);
    await expect(pool.executeCommand("GET", "key")).resolves.toEqual(Buffer.from("new"));

    const request = oldRequest;
    if (request == null || oldSocket == null) throw new Error("old leader request was not captured");
    oldSocket.write(responseFrame(request.opcode, request.laneId, request.requestId, Buffer.from("old")));
    await expect(inFlight).resolves.toEqual(Buffer.from("old"));
    await waitFor(async () => (await activeConnections(oldLeader)) === 0);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool cannot reconnect after close", async () => {
  let seedPort = 0;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: seedPort, node: "seed@local" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  seedPort = (seed.address() as AddressInfo).port;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedPort}`]);

  await pool.close();

  await expect(pool.executeCommand("PING")).rejects.toThrow("adapter pool is closed");
});

test("TopologyNativeAdapterPool subscribes to topology events and refreshes automatically", async () => {
  let seedSocket: Socket | undefined;
  let shardRequests = 0;
  let seedPort = 0;
  const seed = await startCountingServer(
    (request, socket) => {
      seedSocket = socket;
      if (request.opcode !== OPCODES.shards) return undefined;
      shardRequests++;
      return {
        ranges: [
          {
            endpoint: { host: "127.0.0.1", native_port: seedPort, node: "seed@local" },
            first_slot: 0,
            lane_id: 1,
            last_slot: 1023,
            shard: 0
          }
        ],
        route_epoch: shardRequests,
        shard_count: 1
      };
    },
    { fragmentResponses: false }
  );
  seedPort = (seed.address() as AddressInfo).port;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedPort}`]);

  try {
    seedSocket?.write(
      responseFrame(COMMAND_OPCODES.EVENT, 0, 0n, {
        event: "TOPOLOGY_CHANGED",
        payload: { route_epoch: 2 }
      })
    );
    await waitFor(() => pool.topology.routeEpoch === 2);
    expect(pool.topology.routeEpoch).toBe(2);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool refreshes again when a topology event overlaps a refresh", async () => {
  let delayedRequest: TestRequest | undefined;
  let seedSocket: Socket | undefined;
  let shardRequests = 0;
  let seedPort = 0;
  const topology = (routeEpoch: number) => ({
    ranges: [{
      endpoint: { host: "127.0.0.1", native_port: seedPort, node: "seed@local" },
      first_slot: 0,
      lane_id: 1,
      last_slot: 1023,
      shard: 0
    }],
    route_epoch: routeEpoch,
    shard_count: 1
  });
  const seed = await startCountingServer((request, socket) => {
    seedSocket = socket;
    if (request.opcode !== OPCODES.shards) return undefined;
    shardRequests += 1;
    if (shardRequests === 2) {
      delayedRequest = request;
      return NO_RESPONSE;
    }
    return topology(shardRequests);
  }, { fragmentResponses: false });
  seedPort = (seed.address() as AddressInfo).port;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedPort}`]);

  try {
    seedSocket?.write(responseFrame(COMMAND_OPCODES.EVENT, 0, 0n, {
      event: "TOPOLOGY_CHANGED",
      payload: { route_epoch: 2 }
    }));
    await waitFor(() => delayedRequest != null);
    if (seedSocket == null || delayedRequest == null) throw new Error("expected a delayed SHARDS request");

    seedSocket.write(Buffer.concat([
      responseFrame(OPCODES.shards, delayedRequest.laneId, delayedRequest.requestId, topology(2)),
      responseFrame(COMMAND_OPCODES.EVENT, 0, 0n, {
        event: "TOPOLOGY_CHANGED",
        payload: { route_epoch: 3 }
      })
    ]));

    await waitFor(() => pool.topology.routeEpoch === 3);
    expect(shardRequests).toBe(3);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool keeps unpartitioned Flow queries on the control path", async () => {
  const leaderRequests: TestRequest[] = [];
  const leader = await startCountingServer((request) => {
    leaderRequests.push(request);
    return [];
  });
  const leaderAddress = leader.address() as AddressInfo;
  const seedRequests: TestRequest[] = [];
  const seed = await startCountingServer((request) => {
    seedRequests.push(request);
    if (request.opcode !== OPCODES.shards) return [];
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: leaderAddress.port, node: "leader@local" },
          first_slot: 0,
          lane_id: 4,
          last_slot: 1023,
          shard: 0
        }
      ],
      route_epoch: 1,
      shard_count: 1
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    trustedHosts: ["127.0.0.1"]
  });

  try {
    await pool.executeCommand("FLOW.SEARCH", "order");
    await pool.executeCommand("FLOW.SEARCH", "order", "PARTITION", "tenant-a");

    expect(seedRequests.some((request) => request.opcode === OPCODES.flowSearch)).toBe(true);
    expect(leaderRequests.some((request) => request.opcode === OPCODES.flowSearch && request.laneId === 4)).toBe(true);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool routes Flow commands by core storage tags", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
    ranges: [
      {
        endpoint: { host: "low.local", native_port: 6388, node: "low@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 511,
        shard: 0
      },
      {
        endpoint: { host: "high.local", native_port: 6388, node: "high@cluster" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      }
    ],
    shard_count: 2
  });
  const routeData = (args: readonly CommandArgument[]) => (
    pool as unknown as {
      routeData(command: readonly CommandArgument[]): { route: RoutingRoute } | undefined;
    }
  ).routeData(args);

  // These slots are derived from the core's Keys.tag/1 and
  // Keys.auto_partition_key/1 storage keys, not the public identifiers.
  expect(RoutingTopology.slotForKey("tenant-a")).toBe(11);
  expect(RoutingTopology.slotForKey("f:{f:gKcHr33HfuEij5EnGA85ZINeW-tMSrDYEvD-dZNXmzo}:s:flow")).toBe(914);
  expect(routeData(["FLOW.GET", "flow", "PARTITION", "tenant-a"])?.route).toMatchObject({
    shard: 1,
    slot: 914
  });
  expect(routeData([
    "FLOW.GET", "flow-0", "VALUE", "profile", "PARTITION", "tenant-a"
  ])?.route).toMatchObject({
    shard: 1,
    slot: 914
  });
  expect(routeData(["FLOW.SEARCH", "order", "PARTITION", "tenant-a"])?.route).toMatchObject({
    shard: 1,
    slot: 914
  });

  expect(RoutingTopology.slotForKey("flow-0")).toBe(107);
  expect(RoutingTopology.slotForKey("f:{fa:107}:s:flow-0")).toBe(903);
  expect(routeData(["FLOW.GET", "flow-0"])?.route).toMatchObject({ shard: 1, slot: 903 });

  expect(RoutingTopology.slotForKey("f:{f:50QN04TxIFb0hl8nnixAkyrjx6zsoaeYoBRevUmbkHI}:s:flow")).toBe(916);
  expect(routeData(["FLOW.GET", "flow", "PARTITION", "GLOBAL"])?.route.slot).toBe(916);
  expect(routeData([
    "FLOW.CLAIM_DUE", "order", "STATE", "queued", "PARTITION", "GLOBAL"
  ])?.route.slot).toBe(992);
  expect(routeData([
    "FLOW.CLAIM_DUE", "order", "STATE", "queued", "PARTITION", "AUTO"
  ])).toBeUndefined();
  expect(routeData([
    "FLOW.CLAIM_DUE", "order", "STATE", "queued", "PARTITION", "ANY"
  ])).toBeUndefined();

  // Schedule ids use Erlang phash2 on the server, which has no portable
  // JavaScript equivalent. They must stay on the control path.
  expect(routeData(["FLOW.SCHEDULE.GET", "schedule-1"])).toBeUndefined();
});

test("TopologyNativeAdapterPool parses the XREADGROUP STREAMS delimiter after group operands", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
    ranges: [
      {
        endpoint: { host: "low.local", native_port: 6388, node: "low@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 511,
        shard: 0
      },
      {
        endpoint: { host: "high.local", native_port: 6388, node: "high@cluster" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      }
    ],
    shard_count: 2
  });
  const routeData = (args: readonly CommandArgument[]) => (
    pool as unknown as {
      routeData(command: readonly CommandArgument[]): { route: RoutingRoute } | undefined;
    }
  ).routeData(args);
  const stream = "actual-stream-0";

  expect(RoutingTopology.slotForKey("STREAMS")).toBeLessThan(512);
  expect(RoutingTopology.slotForKey("consumer-1260")).toBe(RoutingTopology.slotForKey("STREAMS"));
  expect(RoutingTopology.slotForKey(stream)).toBeGreaterThanOrEqual(512);
  expect(routeData([
    "XREADGROUP", "GROUP", "STREAMS", "consumer-1260", "STREAMS", stream, ">"
  ])?.route).toMatchObject({ shard: 1, slot: RoutingTopology.slotForKey(stream) });
  expect(routeData([
    "XREADGROUP", "GROUP", "workers", "STREAMS", "STREAMS", stream, ">"
  ])?.route).toMatchObject({ shard: 1, slot: RoutingTopology.slotForKey(stream) });
});

test("TopologyNativeAdapterPool routes command_exec commands by real key positions only", async () => {
  const operationSlot = RoutingTopology.slotForKey("OR");
  const streamTokenSlot = RoutingTopology.slotForKey("STREAMS");
  const bitopTag = keyForSlot((slot) => (operationSlot < 512 ? slot >= 512 : slot < 512), "bitmap");
  const streamTag = keyForSlot((slot) => (streamTokenSlot < 512 ? slot >= 512 : slot < 512), "stream");
  const bitopSlot = RoutingTopology.slotForKey(bitopTag);
  const streamSlot = RoutingTopology.slotForKey(streamTag);
  const leftKey = keyForSlot((slot) => slot < 512, "rename-left");
  const rightKey = keyForSlot((slot) => slot >= 512, "rename-right");

  const lowRequests: TestRequest[] = [];
  const low = await startCountingServer((request) => {
    lowRequests.push(request);
    return "OK";
  });
  const lowAddress = low.address() as AddressInfo;
  const highRequests: TestRequest[] = [];
  const high = await startCountingServer((request) => {
    highRequests.push(request);
    return "OK";
  });
  const highAddress = high.address() as AddressInfo;
  const seedRequests: TestRequest[] = [];
  const seed = await startCountingServer((request) => {
    seedRequests.push(request);
    if (request.opcode !== OPCODES.shards) return "OK";
    return twoShardTopology(lowAddress.port, highAddress.port);
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    endpointPolicy: "any"
  });

  try {
    await pool.executeCommand("BITOP", "OR", `{${bitopTag}}:out`, `{${bitopTag}}:in`);
    await pool.executeCommand("XREAD", "STREAMS", `{${streamTag}}:events`, "0-0");
    await pool.executeCommand("RENAME", leftKey, rightKey);

    const bitopTarget = bitopSlot < 512 ? lowRequests : highRequests;
    const bitopWrong = bitopSlot < 512 ? highRequests : lowRequests;
    expect(bitopTarget.some((request) => commandExecName(request) === "BITOP")).toBe(true);
    expect(bitopWrong.some((request) => commandExecName(request) === "BITOP")).toBe(false);

    const streamTarget = streamSlot < 512 ? lowRequests : highRequests;
    const streamWrong = streamSlot < 512 ? highRequests : lowRequests;
    expect(streamTarget.some((request) => commandExecName(request) === "XREAD")).toBe(true);
    expect(streamWrong.some((request) => commandExecName(request) === "XREAD")).toBe(false);

    expect(seedRequests.filter((request) => commandExecName(request) === "RENAME")).toHaveLength(1);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool fans out decomposable cross-shard commands and routes Flow values by owner", async () => {
  const lowKey = keyForSlot((slot) => slot < 512, "multi-low");
  const highKey = keyForSlot((slot) => slot >= 512, "multi-high");
  const flowLowPartition = flowPartitionForSlot((slot) => slot < 512, "flow-low");
  const valueOnHighShard = Buffer.from(keyForSlot((slot) => slot >= 512, "value-high"));

  const lowRequests: TestRequest[] = [];
  const low = await startCountingServer((request) => {
    lowRequests.push(request);
    if (request.opcode === OPCODES.pipeline) return [Buffer.from("pipeline-low")];
    if (request.opcode === OPCODES.flowValueMGet) return [Buffer.from("value-low")];
    if (request.opcode === COMMAND_OPCODES.MGET) return [Buffer.from("mget-low")];
    if (commandExecName(request) === "EXISTS" || commandExecName(request) === "UNLINK") return 1;
    return "OK";
  });
  const lowAddress = low.address() as AddressInfo;
  const highRequests: TestRequest[] = [];
  const high = await startCountingServer((request) => {
    highRequests.push(request);
    if (request.opcode === OPCODES.pipeline) return [Buffer.from("pipeline-high")];
    if (request.opcode === OPCODES.flowValueMGet) return [Buffer.from("value-high")];
    if (request.opcode === COMMAND_OPCODES.MGET) return [Buffer.from("mget-high")];
    if (commandExecName(request) === "EXISTS" || commandExecName(request) === "UNLINK") return 1;
    return "OK";
  });
  const highAddress = high.address() as AddressInfo;
  const seedRequests: TestRequest[] = [];
  const seed = await startCountingServer((request) => {
    seedRequests.push(request);
    if (request.opcode !== OPCODES.shards) return "OK";
    return twoShardTopology(lowAddress.port, highAddress.port);
  });
  const seedAddress = seed.address() as AddressInfo;
  const pool = await TopologyNativeAdapterPool.fromUrls([`ferric://127.0.0.1:${seedAddress.port}`], {
    endpointPolicy: "any"
  });

  try {
    await expect(pool.executePipeline([
      ["GET", lowKey],
      ["GET", highKey]
    ])).resolves.toEqual([Buffer.from("pipeline-low"), Buffer.from("pipeline-high")]);
    await expect(pool.executeCommand("MGET", lowKey, highKey)).resolves.toEqual([
      Buffer.from("mget-low"),
      Buffer.from("mget-high")
    ]);
    await expect(pool.executeCommand("EXISTS", lowKey, highKey)).resolves.toBe(2);
    await expect(pool.executeCommand("UNLINK", lowKey, highKey)).resolves.toBe(2);
    await expect(pool.executeCommand("FLOW.VALUE.MGET", lowKey, highKey, "MAX_BYTES", 1_024)).resolves.toEqual([
      Buffer.from("value-low"),
      Buffer.from("value-high")
    ]);
    await pool.executeCommand(
      "FLOW.CLAIM_DUE",
      "order",
      "STATE",
      "PARTITION",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      1,
      "RETURN",
      "JOBS_COMPACT"
    ).catch(() => undefined);
    await pool.executeCommand(
      "FLOW.CLAIM_DUE",
      "order",
      "STATE",
      "queued",
      "PARTITION",
      flowLowPartition,
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      1,
      "RETURN",
      "JOBS_COMPACT"
    ).catch(() => undefined);
    await pool.executeCommand(
      "FLOW.SIGNAL",
      highKey,
      "SIGNAL",
      "PARTITION",
      "PARTITION",
      flowLowPartition,
      "NOW",
      100
    );
    await pool.executeCommand(
      "FLOW.VALUE.PUT",
      valueOnHighShard,
      "OWNER_FLOW_ID",
      lowKey,
      "NAME",
      "PARTITION",
      "PARTITION",
      flowLowPartition,
      "NOW",
      100
    );

    expect(seedRequests.some((request) => request.opcode === COMMAND_OPCODES.MGET)).toBe(false);
    expect(seedRequests.some((request) => request.opcode === OPCODES.pipeline)).toBe(false);
    expect(seedRequests.some((request) => commandExecName(request) === "EXISTS")).toBe(false);
    expect(seedRequests.some((request) => commandExecName(request) === "UNLINK")).toBe(false);
    expect(seedRequests.some((request) => request.opcode === OPCODES.flowValueMGet)).toBe(false);
    expect(seedRequests.some((request) => request.opcode === OPCODES.flowClaimDue)).toBe(true);
    expect(lowRequests.some((request) => request.opcode === COMMAND_OPCODES.MGET)).toBe(true);
    expect(highRequests.some((request) => request.opcode === COMMAND_OPCODES.MGET)).toBe(true);
    expect(lowRequests.some((request) => request.opcode === OPCODES.pipeline)).toBe(true);
    expect(highRequests.some((request) => request.opcode === OPCODES.pipeline)).toBe(true);
    expect(lowRequests.some((request) => commandExecName(request) === "EXISTS" || commandExecName(request) === "UNLINK")).toBe(true);
    expect(highRequests.some((request) => commandExecName(request) === "EXISTS" || commandExecName(request) === "UNLINK")).toBe(true);
    expect(lowRequests.some((request) => request.opcode === OPCODES.flowValueMGet)).toBe(true);
    expect(highRequests.some((request) => request.opcode === OPCODES.flowValueMGet)).toBe(true);
    expect(lowRequests.some((request) =>
      request.opcode === OPCODES.flowClaimDue && (request.flags & FLAG_CUSTOM_PAYLOAD) !== 0
    )).toBe(true);
    expect(highRequests.some((request) => request.opcode === OPCODES.flowClaimDue)).toBe(false);
    expect(lowRequests.some((request) => request.opcode === OPCODES.flowSignal)).toBe(true);
    expect(highRequests.some((request) => request.opcode === OPCODES.flowSignal)).toBe(false);
    expect(lowRequests.some((request) => request.opcode === OPCODES.flowValuePut)).toBe(true);
    expect(highRequests.some((request) => request.opcode === OPCODES.flowValuePut)).toBe(false);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool preserves ordered pipelines across routes", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const subject = pool as unknown as {
    executeCommand: (...args: CommandArgument[]) => Promise<unknown>;
    executeCommandArgs: (args: readonly CommandArgument[]) => Promise<unknown>;
    executePipelineOnRoute: (
      commands: readonly Command[],
      route: RoutingRoute,
      options: ExecutePipelineOptions
    ) => Promise<unknown[]>;
    routeData: (args: readonly CommandArgument[]) => {
      readonly command: ProtocolCommand;
      readonly route: RoutingRoute;
    } | undefined;
  };
  let markFirstStarted: (() => void) | undefined;
  let releaseFirst: (() => void) | undefined;
  const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
  const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const started: string[] = [];
  const execute = async (...args: CommandArgument[]): Promise<unknown> => {
    const key = typeof args[1] === "string" ? args[1] : "";
    started.push(key);
    if (key === "first") {
      markFirstStarted?.();
      await firstGate;
    }
    return Buffer.from(key);
  };
  subject.executeCommand = execute;
  subject.executeCommandArgs = async (args) => await execute(...args);
  subject.executePipelineOnRoute = async (commands) => await Promise.all(
    commands.map(async (command) => await execute(...command))
  );
  subject.routeData = (args) => {
    const key = typeof args[1] === "string" ? args[1] : "";
    const shard = key === "first" ? 0 : 1;
    return {
      command: { opcode: OPCODES.get, payload: { key } },
      route: {
        endpoint: { host: `node-${shard}.local`, nativePort: 6388, node: `node-${shard}@local` },
        endpointKey: `node-${shard}.local:6388`,
        laneId: shard + 1,
        leaderNode: `node-${shard}@local`,
        shard
      }
    };
  };

  await expect(pool.executeFusedPipeline([
    ["GET", "first"],
    ["GET", "second"]
  ], { ordered: true })).resolves.toBeUndefined();
  expect(started).toEqual([]);

  const operation = pool.executePipeline([
    ["GET", "first"],
    ["GET", "second"]
  ], { ordered: true });

  await firstStarted;
  await new Promise((resolve) => setImmediate(resolve));
  expect(started).toEqual(["first"]);
  releaseFirst?.();
  await expect(operation).resolves.toEqual([Buffer.from("first"), Buffer.from("second")]);
  expect(started).toEqual(["first", "second"]);
});

test("TopologyNativeAdapterPool preserves fallback dependencies across split route groups", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const lowKey = keyForSlot((slot) => slot < 512, "dependency-low");
  const highKey = keyForSlot((slot) => slot >= 512, "dependency-high");
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
    ranges: [
      validTopologyRange({
        endpoint: { host: "low.local", native_port: 6388, node: "low@local" },
        first_slot: 0,
        last_slot: 511,
        shard: 0
      }),
      validTopologyRange({
        endpoint: { host: "high.local", native_port: 6388, node: "high@local" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      })
    ],
    route_epoch: 1,
    shard_count: 2
  });
  const internals = pool as unknown as {
    executeCommand(...args: CommandArgument[]): Promise<unknown>;
    executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown>;
    executePipelineOnRoute(
      commands: readonly Command[],
      route: RoutingRoute,
      options: ExecutePipelineOptions
    ): Promise<unknown[]>;
  };
  let markScatterStarted: (() => void) | undefined;
  let releaseScatter: (() => void) | undefined;
  const scatterStarted = new Promise<void>((resolve) => { markScatterStarted = resolve; });
  const scatterGate = new Promise<void>((resolve) => { releaseScatter = resolve; });
  const started: string[] = [];
  internals.executeCommand = async (...args) => {
    if (typeof args[0] !== "string") throw new TypeError("expected string command name");
    started.push(args[0]);
    markScatterStarted?.();
    await scatterGate;
    return [Buffer.from("low"), Buffer.from("high")];
  };
  internals.executeCommandArgs = async (args) => await internals.executeCommand(...args);
  internals.executePipelineOnRoute = async (commands) => {
    started.push(...commands.map((command) => typeof command[0] === "string" ? command[0] : ""));
    return commands.map((command) =>
      command[0] === "GET" ? Buffer.from("independent") : Buffer.from("OK")
    );
  };

  const operation = pool.executePipeline([
    ["MGET", lowKey, highKey],
    ["SET", lowKey, "new"],
    ["GET", highKey]
  ], {
    fallbackDependencies: [[], [0], []],
    throwOnItemError: false
  });

  try {
    await scatterStarted;
    await new Promise((resolve) => setImmediate(resolve));
    expect(started).toContain("GET");
    expect(started).not.toContain("SET");

    releaseScatter?.();
    await expect(operation).resolves.toEqual([
      [Buffer.from("low"), Buffer.from("high")],
      Buffer.from("OK"),
      Buffer.from("independent")
    ]);
    expect(started).toContain("SET");
  } finally {
    releaseScatter?.();
    await pool.close();
  }
});

test("TopologyNativeAdapterPool preserves healthy split-pipeline outcomes when one route fails", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const lowKey = keyForSlot((slot) => slot < 512, "partial-low");
  const highKey = keyForSlot((slot) => slot >= 512, "partial-high");
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
    ranges: [
      validTopologyRange({
        endpoint: { host: "low.local", native_port: 6388, node: "low@local" },
        first_slot: 0,
        last_slot: 511,
        shard: 0
      }),
      validTopologyRange({
        endpoint: { host: "high.local", native_port: 6388, node: "high@local" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      })
    ],
    route_epoch: 1,
    shard_count: 2
  });
  const failure = { code: "route_unavailable" };
  const internals = pool as unknown as {
    executePipelineOnRoute(
      commands: readonly Command[],
      route: RoutingRoute,
      options: ExecutePipelineOptions
    ): Promise<unknown[]>;
  };
  let healthyCompleted = false;
  internals.executePipelineOnRoute = async (commands, route) => {
    if (route.shard === 0) {
      // Exercise defensive bookkeeping for arbitrary Promise rejection reasons.
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw failure;
    }
    healthyCompleted = true;
    return commands.map(() => Buffer.from("healthy"));
  };

  try {
    const results = await pool.executePipeline([
      ["GET", lowKey],
      ["GET", highKey]
    ], { throwOnItemError: false });

    expect(healthyCompleted).toBe(true);
    expect(results).toEqual([failure, Buffer.from("healthy")]);
    const rejected = pipelineItemRejectionFlags(results);
    expect(rejected?.[0]).toBe(true);
    expect(rejected?.[1]).not.toBe(true);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool retries one explicitly safe reroute on the refreshed route", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const poolState = pool as unknown as { topologyValue: RoutingTopology };
  poolState.topologyValue = RoutingTopology.build(validTopologyPayload({
    endpoint: { host: "old-leader.local", native_port: 6388, node: "old@local" }
  }));
  const endpoints: string[] = [];
  const reroute = new RerouteError("try another node", {
    raw: { code: "reroute", retryable: true, safe_to_retry: true }
  });
  const internals = pool as unknown as {
    adapterForEndpoint(endpoint: { host: string }): Promise<NativeAdapter>;
    refreshTopology(): Promise<RoutingTopology>;
  };
  internals.adapterForEndpoint = async (endpoint) => ({
    async executeCommandOnLane(): Promise<Buffer> {
      endpoints.push(endpoint.host);
      if (endpoint.host === "old-leader.local") throw reroute;
      return Buffer.from("value");
    }
  }) as unknown as NativeAdapter;
  const refresh = vi.fn(async () => {
    poolState.topologyValue = RoutingTopology.build(validTopologyPayload({
      endpoint: { host: "new-leader.local", native_port: 6388, node: "new@local" }
    }, { route_epoch: 2 }));
    return poolState.topologyValue;
  });
  internals.refreshTopology = refresh;

  try {
    await expect(pool.executeCommand("GET", "key")).resolves.toEqual(Buffer.from("value"));
    expect(endpoints).toEqual(["old-leader.local", "new-leader.local"]);
    expect(refresh).toHaveBeenCalledOnce();
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool retries one explicitly safe fused request on the refreshed route", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const poolState = pool as unknown as { topologyValue: RoutingTopology };
  poolState.topologyValue = RoutingTopology.build(validTopologyPayload({
    endpoint: { host: "old-leader.local", native_port: 6388, node: "old@local" }
  }));
  const endpoints: string[] = [];
  const reroute = new RerouteError("try another node", {
    raw: { code: "reroute", retryable: true, safe_to_retry: true }
  });
  const internals = pool as unknown as {
    adapterForEndpoint(endpoint: { host: string }): Promise<NativeAdapter>;
    refreshTopology(): Promise<RoutingTopology>;
  };
  internals.adapterForEndpoint = async (endpoint) => ({
    async executeFusedPipelineOnLane(): Promise<Buffer[]> {
      endpoints.push(endpoint.host);
      if (endpoint.host === "old-leader.local") throw reroute;
      return [Buffer.from("value")];
    }
  }) as unknown as NativeAdapter;
  const refresh = vi.fn(async () => {
    poolState.topologyValue = RoutingTopology.build(validTopologyPayload({
      endpoint: { host: "new-leader.local", native_port: 6388, node: "new@local" }
    }, { route_epoch: 2 }));
    return poolState.topologyValue;
  });
  internals.refreshTopology = refresh;

  try {
    await expect(pool.executeFusedPipeline([["GET", "key"]])).resolves.toEqual([Buffer.from("value")]);
    expect(endpoints).toEqual(["old-leader.local", "new-leader.local"]);
    expect(refresh).toHaveBeenCalledOnce();
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool refreshes only for typed or exact route failures", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build(
    validTopologyPayload({
      endpoint: { host: "leader.local", native_port: 6388, node: "leader@local" }
    })
  );
  let failure: Error = new RerouteError("try another node", {
    raw: { code: "reroute", retryable: true, safe_to_retry: true }
  });
  let attempts = 0;
  const refresh = vi.fn(async () => pool.topology);
  const internals = pool as unknown as {
    adapterForEndpoint(): Promise<NativeAdapter>;
    refreshTopology(): Promise<RoutingTopology>;
  };
  internals.adapterForEndpoint = async () => ({
    async executeCommandOnLane(): Promise<never> {
      attempts += 1;
      throw failure;
    }
  }) as unknown as NativeAdapter;
  internals.refreshTopology = refresh;

  try {
    await expect(pool.executeCommand("GET", "key")).rejects.toBe(failure);
    expect(refresh).toHaveBeenCalledOnce();
    expect(attempts).toBe(2);

    refresh.mockClear();
    failure = new RerouteError("unsafe reroute", {
      raw: { code: "reroute", retryable: true, safe_to_retry: false }
    });
    await expect(pool.executeCommand("GET", "key")).rejects.toBe(failure);
    expect(refresh).toHaveBeenCalledOnce();
    expect(attempts).toBe(3);

    refresh.mockClear();
    failure = new FerricStoreError("ERR route expression is invalid");
    await expect(pool.executeCommand("GET", "key")).rejects.toBe(failure);
    expect(refresh).not.toHaveBeenCalled();
    expect(attempts).toBe(4);
  } finally {
    await pool.close();
  }
});
