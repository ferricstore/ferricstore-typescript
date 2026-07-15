import type { AddressInfo } from "node:net";
import { expect, test } from "vitest";
import { NativeAdapter, type ExecutePipelineOptions } from "../src/adapters.js";
import type { Command } from "../src/internal.js";
import { OPCODES, buildProtocolCommand } from "../src/protocol.js";
import { RoutingTopology, TopologyNativeAdapterPool, type RoutingRoute } from "../src/topology.js";
import {
  type TestRequest,
  keyForSlot,
  startCountingServer,
  validTopologyPayload,
  validTopologyRange
} from "./adapter-test-support.js";

test("TopologyNativeAdapterPool waits for every split route before rejecting", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const lowKey = keyForSlot((slot) => slot < 512, "settle-low");
  const highKey = keyForSlot((slot) => slot >= 512, "settle-high");
  const failure = new Error("low route failed");
  let releaseHigh: (() => void) | undefined;
  let highFinished = false;
  const highGate = new Promise<void>((resolve) => {
    releaseHigh = resolve;
  });
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
    executePipelineOnRoute(
      commands: readonly Command[],
      route: RoutingRoute,
      options: ExecutePipelineOptions
    ): Promise<unknown[]>;
  };
  internals.executePipelineOnRoute = async (commands, route) => {
    if (route.shard === 0) throw failure;
    await highGate;
    highFinished = true;
    return commands.map(() => Buffer.from("high"));
  };
  let rejected = false;
  const execution = pool.executePipeline([
    ["GET", lowKey],
    ["GET", highKey]
  ]).catch((error: unknown) => {
    rejected = true;
    return error;
  });

  try {
    await new Promise((resolve) => setImmediate(resolve));
    expect(rejected).toBe(false);
    expect(highFinished).toBe(false);

    releaseHigh?.();
    await expect(execution).resolves.toBe(failure);
    expect(highFinished).toBe(true);
  } finally {
    releaseHigh?.();
    await pool.close();
  }
});

test("TopologyNativeAdapterPool waits for every cross-shard mutation before rejecting", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  const lowKey = keyForSlot((slot) => slot < 512, "mutation-low");
  const highKey = keyForSlot((slot) => slot >= 512, "mutation-high");
  const failure = new Error("low mutation failed");
  let releaseHigh: (() => void) | undefined;
  let highFinished = false;
  const highGate = new Promise<void>((resolve) => { releaseHigh = resolve; });
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
    executeOnRoute(command: Command, route: RoutingRoute): Promise<unknown>;
  };
  internals.executeOnRoute = async (_command, route) => {
    if (route.shard === 0) throw failure;
    await highGate;
    highFinished = true;
    return 1;
  };
  let rejected = false;
  const mutation = pool.executeCommand("DEL", lowKey, highKey).catch((error: unknown) => {
    rejected = true;
    throw error;
  });
  void mutation.catch(() => undefined);

  try {
    await new Promise((resolve) => setImmediate(resolve));
    expect(rejected).toBe(false);
    expect(highFinished).toBe(false);

    releaseHigh?.();
    await expect(mutation).rejects.toBe(failure);
    expect(highFinished).toBe(true);
  } finally {
    releaseHigh?.();
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects pinned commands before starting split routes", async () => {
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
  let routedCalls = 0;
  const internals = pool as unknown as {
    controlAdapter(): Promise<NativeAdapter>;
    executePipelineOnRoute(
      commands: readonly Command[],
      route: RoutingRoute,
      options: ExecutePipelineOptions
    ): Promise<unknown[]>;
  };
  internals.executePipelineOnRoute = async (commands) => {
    routedCalls += 1;
    return commands.map(() => Buffer.from("OK"));
  };
  internals.controlAdapter = async () => ({
    async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
      return commands.map((command) => buildProtocolCommand(command));
    }
  }) as unknown as NativeAdapter;

  try {
    await expect(pool.executePipeline([
      ["SET", "transaction:key", "value"],
      ["EXEC"]
    ])).rejects.toThrow(/EXEC.*pinned connection/i);
    expect(routedCalls).toBe(0);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects connection-local mutations before dispatch", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  let calls = 0;
  const internals = pool as unknown as {
    controlAdapter(): Promise<NativeAdapter>;
  };
  internals.controlAdapter = async () => ({
    async executeCommand(): Promise<unknown> {
      calls += 1;
      return Buffer.from("OK");
    },
    async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
      calls += commands.length;
      return commands.map(() => Buffer.from("OK"));
    }
  }) as unknown as NativeAdapter;

  try {
    await expect(pool.executeCommand("AUTH", "default", "secret")).rejects.toThrow(
      /AUTH.*stable single connection/i
    );
    await expect(pool.executePipeline([
      ["PING"],
      ["CLIENT", "SETNAME", "worker-1"]
    ])).rejects.toThrow(/CLIENT SETNAME.*stable single connection/i);
    expect(calls).toBe(0);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects untrusted learned endpoints by default", async () => {
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: { host: "other.local", native_port: 6388, node: "other@cluster" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const operation = TopologyNativeAdapterPool.fromUrls([
    `ferric://127.0.0.1:${seedAddress.port}`
  ]).then(async (pool) => {
    await pool.close();
    return pool;
  });

  await expect(operation).rejects.toThrow("no FerricStore topology endpoint reachable");
});

test("TopologyNativeAdapterPool retains its known-good topology when refresh learns an unsafe endpoint", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[]
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"]);
  const knownGood = RoutingTopology.build(validTopologyPayload({
    endpoint: { host: "seed.local", native_port: 6388, node: "seed@local" }
  }));
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = knownGood;
  const unsafeTopology = validTopologyPayload({
    endpoint: { host: "other.local", native_port: 6388, node: "other@cluster" }
  }, { route_epoch: 2 });
  const adapter = {
    async executeCommand(command: string): Promise<unknown> {
      if (command === "SHARDS") return unsafeTopology;
      return undefined;
    }
  } as NativeAdapter;
  const internals = pool as unknown as {
    adapterForEndpoint(endpoint: unknown): Promise<NativeAdapter>;
    adapterForSeedUrl(url: string): Promise<NativeAdapter>;
  };
  internals.adapterForSeedUrl = async () => adapter;
  internals.adapterForEndpoint = async () => adapter;

  try {
    await expect(pool.refreshTopology()).rejects.toThrow(
      "no FerricStore topology endpoint reachable"
    );
    expect(pool.topology).toBe(knownGood);
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool rejects learned seed host ports unless explicitly trusted", async () => {
  const learned = await startCountingServer(() => "OK");
  const learnedAddress = learned.address() as AddressInfo;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return "OK";
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: learnedAddress.port, node: "other-port@local" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const seedUrl = `ferric://127.0.0.1:${seedAddress.port}`;
  const strictOperation = TopologyNativeAdapterPool.fromUrls([seedUrl]).then(async (pool) => {
    await pool.close();
    return pool;
  });
  await expect(strictOperation).rejects.toThrow("no FerricStore topology endpoint reachable");

  const trustedPool = await TopologyNativeAdapterPool.fromUrls([seedUrl], { trustedHosts: ["127.0.0.1"] });
  try {
    await expect(trustedPool.executeCommand("GET", "tenant-key")).resolves.toBeDefined();
  } finally {
    await trustedPool.close();
  }
});

test("TopologyNativeAdapterPool validates learned endpoints before topology refresh connects", async () => {
  const learnedRequests: TestRequest[] = [];
  let learnedPort = 0;
  const learned = await startCountingServer((request) => {
    learnedRequests.push(request);
    if (request.opcode === OPCODES.shards) {
      return {
        ranges: [
          {
            endpoint: { host: "127.0.0.1", native_port: learnedPort, node: "learned@local" },
            first_slot: 0,
            lane_id: 1,
            last_slot: 1023,
            shard: 0
          }
        ]
      };
    }
    return undefined;
  });
  learnedPort = (learned.address() as AddressInfo).port;

  let shardRequests = 0;
  const seed = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    shardRequests++;
    if (shardRequests > 1) return "invalid topology";
    return {
      ranges: [
        {
          endpoint: { host: "127.0.0.1", native_port: learnedPort, node: "learned@local" },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  });
  const seedAddress = seed.address() as AddressInfo;
  const operation = TopologyNativeAdapterPool.fromUrls([
    `ferric://127.0.0.1:${seedAddress.port}`
  ]).then(async (pool) => {
    await pool.close();
    return pool;
  });

  await expect(operation).rejects.toThrow("no FerricStore topology endpoint reachable");
  expect(learnedRequests).toHaveLength(0);
});
