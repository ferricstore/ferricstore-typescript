import tls from "node:tls";
import type { AddressInfo } from "node:net";
import { expect, test, vi } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";
import {
  OPCODES
} from "../src/protocol.js";
import { RoutingTopology, TopologyNativeAdapterPool, type RoutingRoute } from "../src/topology.js";
import { TopologyScatterExecutor } from "../src/topology-scatter.js";
import {
  type TestRequest,
  keyForSlot,
  startCountingServer,
  validTopologyPayload,
  validTopologyRange,
  waitFor
} from "./adapter-test-support.js";

test("RoutingTopology builds 1024-slot hash-tag aware routes", () => {
  const topology = RoutingTopology.build({
    route_epoch: 7,
    shard_count: 2,
    ranges: [
      {
        endpoint: { host: "node-a.local", native_port: 6388, node: "a@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 511,
        shard: 0
      },
      {
        endpoint: { host: "node-b.local", native_port: 6389, node: "b@cluster" },
        first_slot: 512,
        lane_id: 2,
        last_slot: 1023,
        shard: 1
      }
    ]
  });

  const shard0Key = Array.from({ length: 10_000 }, (_, index) => `slot-a-${index}`).find(
    (key) => RoutingTopology.slotForKey(key) < 512
  );
  const shard1Key = Array.from({ length: 10_000 }, (_, index) => `slot-b-${index}`).find(
    (key) => RoutingTopology.slotForKey(key) >= 512
  );

  expect(shard0Key).toBeDefined();
  expect(shard1Key).toBeDefined();
  expect(topology.routeEpoch).toBe(7);
  expect(RoutingTopology.slotForKey("{tenant}:one")).toBe(RoutingTopology.slotForKey("{tenant}:two"));
  expect(topology.routeKey(shard0Key ?? "slot-a-0")).toMatchObject({ shard: 0, endpoint: { host: "node-a.local" } });
  expect(topology.routeKey(shard1Key ?? "slot-b-0")).toMatchObject({ laneId: 2, endpointKey: "node-b.local:6389" });
});

test("RoutingTopology hashes Buffer keys as raw bytes", () => {
  expect(RoutingTopology.slotForKey(Buffer.from([0x80]))).toBe(173);
  expect(RoutingTopology.slotForKey(Buffer.from([0x81]))).toBe(59);
});

test("RoutingTopology ignores inherited response fields", () => {
  const response: unknown = Object.create({
    ranges: [
      {
        endpoint: { host: "attacker.local", native_port: 6388, node: "attacker@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }
    ]
  });

  expect(() => RoutingTopology.build(response)).toThrow("invalid SHARDS");
});

test.each([
  ["overlapping slots", {
    ...validTopologyPayload(),
    ranges: [
      validTopologyRange({ first_slot: 0, last_slot: 700 }),
      validTopologyRange({ first_slot: 700, last_slot: 1023 })
    ]
  }],
  ["missing slots", {
    ...validTopologyPayload(),
    ranges: [
      validTopologyRange({ first_slot: 0, last_slot: 500 }),
      validTopologyRange({ first_slot: 502, last_slot: 1023 })
    ]
  }]
])("RoutingTopology rejects %s", (_name, payload) => {
  expect(() => RoutingTopology.build(payload)).toThrow("invalid SHARDS topology");
});

test.each([
  ["different endpoints", {
    host: "node-b.local",
    native_port: 6389,
    node: "b@cluster"
  }, 1],
  ["different lanes", {
    host: "node-a.local",
    native_port: 6388,
    node: "a@cluster"
  }, 2]
])("RoutingTopology rejects one shard routed through %s", (_name, endpoint, laneId) => {
  const payload = validTopologyPayload({}, {
    ranges: [
      validTopologyRange({ first_slot: 0, last_slot: 511 }),
      validTopologyRange({
        endpoint,
        first_slot: 512,
        lane_id: laneId,
        last_slot: 1023
      })
    ]
  });

  expect(() => RoutingTopology.build(payload)).toThrow("inconsistent route for shard 0");
});

test("RoutingTopology accepts multiple ranges with one consistent shard route", () => {
  const payload = validTopologyPayload({}, {
    ranges: [
      validTopologyRange({ first_slot: 0, last_slot: 511 }),
      validTopologyRange({ first_slot: 512, last_slot: 1023 })
    ]
  });

  expect(RoutingTopology.build(payload).shardCount).toBe(1);
});

test.each([
  ["a control lane", validTopologyPayload({ lane_id: 0 })],
  ["a negative shard", validTopologyPayload({ shard: -1 })],
  ["an out-of-range native port", validTopologyPayload({
    endpoint: { host: "node-a.local", native_port: 65_536, node: "a@cluster" }
  })],
  ["an out-of-range TLS port", validTopologyPayload({
    endpoint: {
      host: "node-a.local",
      native_port: 6388,
      native_tls_port: 0,
      node: "a@cluster"
    }
  })],
  ["an empty endpoint host", validTopologyPayload({
    endpoint: { host: " ", native_port: 6388, node: "a@cluster" }
  })],
  ["a negative route epoch", validTopologyPayload({}, { route_epoch: -1 })],
  ["a mismatched shard count", validTopologyPayload({}, { shard_count: 2 })],
  ["a non-1024 slot declaration", validTopologyPayload({}, { slots: 512 })]
])("RoutingTopology rejects %s", (_name, payload) => {
  expect(() => RoutingTopology.build(payload)).toThrow("invalid SHARDS");
});

test.each(["[", "]", "bad\\host", "a:b:c"])(
  "RoutingTopology rejects malformed endpoint host %s",
  (host) => {
    expect(() => RoutingTopology.build(validTopologyPayload({
      endpoint: { host, native_port: 6388, node: "a@cluster" }
    }))).toThrow("invalid SHARDS endpoint");
  }
);

test("TopologyNativeAdapterPool trusts a TLS seed by its advertised TLS port", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[]
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferrics://db.example:6389"]);
  const topology = RoutingTopology.build({
    ranges: [
      {
        endpoint: {
          host: "db.example",
          native_port: 6388,
          native_tls_port: 6389,
          node: "db@cluster"
        },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }
    ]
  });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = topology;

  expect(() => pool.route("tenant-key")).not.toThrow();
});

test("TopologyNativeAdapterPool treats a void endpoint validator result as allowed", () => {
  let validated = 0;
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointValidator: (endpoint: { host: string }) => void }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://db.example:6388"], {
    endpointValidator: () => {
      validated++;
    }
  });
  const topology = RoutingTopology.build({
    ranges: [
      {
        endpoint: { host: "db.example", native_port: 6388, node: "db@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }
    ]
  });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = topology;

  expect(() => pool.route("tenant-key")).not.toThrow();
  expect(validated).toBe(1);
});

test("TopologyNativeAdapterPool endpointPolicy none permits only exact seed endpoints", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "none"; trustedHosts?: readonly string[] }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], {
    endpointPolicy: "none",
    trustedHosts: ["seed.local"]
  });
  const setEndpoint = (nativePort: number) => {
    (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
      ranges: [
        {
          endpoint: { host: "seed.local", native_port: nativePort, node: `seed-${nativePort}@local` },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    });
  };

  setEndpoint(6388);
  expect(() => pool.route("tenant-key")).not.toThrow();

  setEndpoint(6389);
  expect(() => pool.route("tenant-key")).toThrow("unsafe learned endpoint");
});

test("TopologyNativeAdapterPool precomputes custom endpoint host allowlists", () => {
  let allowHostsReads = 0;
  const endpointPolicy = {
    get allowHosts(): readonly string[] {
      allowHostsReads += 1;
      return ["allowed.local"];
    }
  };
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: { readonly allowHosts: readonly string[] } }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy });
  (pool as unknown as { topologyValue: RoutingTopology }).topologyValue = RoutingTopology.build({
    ranges: [
      {
        endpoint: { host: "allowed.local", native_port: 6388, node: "allowed@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }
    ]
  });

  expect(() => pool.route("first-key")).not.toThrow();
  expect(() => pool.route("second-key")).not.toThrow();
  expect(allowHostsReads).toBe(1);
});

test("TopologyNativeAdapterPool rejects mixed secure and plaintext seed transports", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options?: { tlsOptions?: tls.ConnectionOptions }
  ) => TopologyNativeAdapterPool;

  expect(() => new PoolConstructor([
    "ferrics://user:secret@secure.example:6389",
    "ferric://plain.example:6388"
  ])).toThrow("cannot mix ferric:// and ferrics:// seed URLs");

  const plaintext = new PoolConstructor(["ferric://plain.example:6388"], {
    tlsOptions: { rejectUnauthorized: false }
  });
  expect((plaintext as unknown as { tls: boolean }).tls).toBe(false);
});

test("TopologyNativeAdapterPool derives seed credentials from one URL pair", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[]
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor([
    "ferric://alice@seed-a.example:6388",
    "ferric://bob:secret@seed-b.example:6388"
  ]);
  const options = (pool as unknown as {
    adapterOptions: { password?: string; username?: string };
  }).adapterOptions;

  expect(options).toMatchObject({ password: "secret", username: "bob" });
});

test("TopologyNativeAdapterPool keeps URL credentials scoped to their seed", async () => {
  const seedARequests: TestRequest[] = [];
  const seedBRequests: TestRequest[] = [];
  const learnedRequests: TestRequest[] = [];
  const seedA = await startCountingServer((request) => {
    seedARequests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const seedB = await startCountingServer((request) => {
    seedBRequests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const learned = await startCountingServer((request) => {
    learnedRequests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const seedAUrl = `ferric://alice:secret-a@127.0.0.1:${(seedA.address() as AddressInfo).port}`;
  const seedBUrl = `ferric://bob:secret-b@127.0.0.1:${(seedB.address() as AddressInfo).port}`;
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor([seedAUrl, seedBUrl], { endpointPolicy: "any" });
  const internals = pool as unknown as {
    adapterForEndpoint(endpoint: { host: string; nativePort: number; node: string }): Promise<NativeAdapter>;
    adapterForSeedUrl(url: string): Promise<NativeAdapter>;
  };
  const authPayload = (requests: readonly TestRequest[]): { password?: string; username?: string } => {
    const payload = requests.find((request) => request.opcode === OPCODES.auth)?.payload as {
      password?: unknown;
      username?: unknown;
    } | undefined;
    const decode = (value: unknown): string | undefined =>
      typeof value === "string"
        ? value
        : Buffer.isBuffer(value) || value instanceof Uint8Array
          ? Buffer.from(value).toString("utf8")
          : undefined;
    return { password: decode(payload?.password), username: decode(payload?.username) };
  };

  try {
    await internals.adapterForSeedUrl(seedAUrl);
    await internals.adapterForSeedUrl(seedBUrl);
    await internals.adapterForEndpoint({
      host: "127.0.0.1",
      nativePort: (learned.address() as AddressInfo).port,
      node: "learned@local"
    });

    expect(authPayload(seedARequests)).toEqual({
      password: "secret-a",
      username: "alice"
    });
    expect(authPayload(seedBRequests)).toEqual({
      password: "secret-b",
      username: "bob"
    });
    expect(authPayload(learnedRequests)).toEqual({
      password: "secret-a",
      username: "alice"
    });
  } finally {
    await pool.close();
  }
});

test("TopologyNativeAdapterPool uses matching seed credentials for a learned seed endpoint", async () => {
  const seedBRequests: TestRequest[] = [];
  const seedB = await startCountingServer((request) => {
    seedBRequests.push(request);
    return undefined;
  }, { fragmentResponses: false });
  const seedBAddress = seedB.address() as AddressInfo;
  const seedA = await startCountingServer((request) => {
    if (request.opcode !== OPCODES.shards) return undefined;
    return {
      ranges: [
        {
          endpoint: {
            host: "127.0.0.1",
            native_port: seedBAddress.port,
            node: "seed-b@local"
          },
          first_slot: 0,
          lane_id: 1,
          last_slot: 1023,
          shard: 0
        }
      ]
    };
  }, { fragmentResponses: false });
  const seedAAddress = seedA.address() as AddressInfo;
  const seedAUrl = `ferric://alice:secret-a@127.0.0.1:${seedAAddress.port}`;
  const seedBUrl = `ferric://bob:secret-b@127.0.0.1:${seedBAddress.port}`;
  const pool = await TopologyNativeAdapterPool.fromUrls([seedAUrl, seedBUrl], {
    warmConnections: true
  });

  try {
    const auth = seedBRequests.find((request) => request.opcode === OPCODES.auth)?.payload as {
      password?: unknown;
      username?: unknown;
    } | undefined;
    const decode = (value: unknown): string | undefined =>
      typeof value === "string"
        ? value
        : Buffer.isBuffer(value) || value instanceof Uint8Array
          ? Buffer.from(value).toString("utf8")
          : undefined;
    expect({ password: decode(auth?.password), username: decode(auth?.username) }).toEqual({
      password: "secret-b",
      username: "bob"
    });
  } finally {
    await pool.close();
  }
});

test("TopologyScatterExecutor bounds fan-out and refills freed slots", async () => {
  const keys = Array.from({ length: 5 }, (_, index) => `key:${index}`);
  const routes: RoutingRoute[] = keys.map((_key, shard) => ({
    endpoint: { host: `node-${shard}.local`, nativePort: 6388, node: `node-${shard}@local` },
    endpointKey: `node-${shard}.local:6388`,
    laneId: shard + 1,
    leaderNode: `node-${shard}@local`,
    shard
  }));
  const releases: (() => void)[] = [];
  const gates = keys.map(() => new Promise<void>((resolve) => releases.push(resolve)));
  const started: number[] = [];
  let active = 0;
  let maxActive = 0;
  const routeByKey = new Map(keys.map((key, index) => {
    const route = routes[index];
    if (route == null) throw new Error("missing test route");
    return [key, route] as const;
  }));
  const subject = new TopologyScatterExecutor({
    concurrency: 2,
    executeOnRoute: async (_args, route) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      started.push(route.shard);
      await gates[route.shard];
      active -= 1;
      return [Buffer.from(`value:${route.shard}`)];
    },
    route: (key) => {
      const route = routeByKey.get(key.toString());
      if (route == null) throw new Error("missing test route");
      return route;
    }
  });

  const operation = subject.execute(["MGET", ...keys]);
  await waitFor(() => started.length >= 2);
  expect(started).toEqual([0, 1]);
  expect(maxActive).toBe(2);

  releases[0]?.();
  await waitFor(() => started.length >= 3);
  expect(started).toEqual([0, 1, 2]);
  expect(active).toBe(2);
  expect(maxActive).toBe(2);

  for (const release of releases) release();
  await expect(operation).resolves.toEqual({
    handled: true,
    value: keys.map((_key, index) => Buffer.from(`value:${index}`))
  });
});

test("TopologyNativeAdapterPool rejects invalid topology concurrency", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { topologyConcurrency: number }
  ) => TopologyNativeAdapterPool;

  expect(() => new PoolConstructor(["ferric://seed.local:6388"], {
    topologyConcurrency: 0
  })).toThrow(/topologyConcurrency.*positive safe integer/i);
});

test("TopologyNativeAdapterPool rejects duplicate seed endpoints with conflicting credentials", () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[]
  ) => TopologyNativeAdapterPool;

  expect(() => new PoolConstructor([
    "ferric://alice:secret-a@seed.example:6388",
    "ferric://bob:secret-b@seed.example:6388"
  ])).toThrow(/duplicate seed endpoint.*conflicting credentials/i);
  expect(() => new PoolConstructor([
    "ferric://alice:secret@seed.example:6388",
    "ferric://alice:secret@seed.example:6388"
  ])).not.toThrow();
});

test("TopologyNativeAdapterPool joins concurrent close callers", async () => {
  const PoolConstructor = TopologyNativeAdapterPool as unknown as new (
    urls: readonly string[],
    options: { endpointPolicy: "any" }
  ) => TopologyNativeAdapterPool;
  const pool = new PoolConstructor(["ferric://seed.local:6388"], { endpointPolicy: "any" });
  let releaseClose: (() => void) | undefined;
  let markCloseStarted: (() => void) | undefined;
  const closeGate = new Promise<void>((resolve) => { releaseClose = resolve; });
  const closeStarted = new Promise<void>((resolve) => { markCloseStarted = resolve; });
  let closeCalls = 0;
  const adapter = {
    async close(): Promise<void> {
      closeCalls += 1;
      markCloseStarted?.();
      await closeGate;
    }
  };
  (pool as unknown as {
    adapterRegistry: { adapters: Map<string, typeof adapter> };
  }).adapterRegistry.adapters.set("fake", adapter);

  const first = pool.close();
  await closeStarted;
  let secondFinished = false;
  const second = pool.close().then(() => { secondFinished = true; });
  await new Promise((resolve) => setImmediate(resolve));

  expect(closeCalls).toBe(1);
  expect(secondFinished).toBe(false);
  releaseClose?.();
  await Promise.all([first, second]);
});

test("TopologyNativeAdapterPool has complete typed-command key routing metadata", () => {
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
      routeData(command: readonly CommandArgument[]): { route: { shard: number } } | undefined;
    }
  ).routeData(args);
  const lowKey = keyForSlot((slot) => slot < 512, "typed-low");
  const highKey = keyForSlot((slot) => slot >= 512, "typed-high");

  const firstKeyCommands: readonly CommandArgument[][] = [
    ["INCR", highKey],
    ["EXTEND", highKey, "token", 1_000],
    ["HINCRBYFLOAT", highKey, "field", 1],
    ["LPOS", highKey, "value"],
    ["SMISMEMBER", highKey, "value"],
    ["ZINCRBY", highKey, 1, "value"],
    ["XACK", highKey, "group", "0-0"],
    ["PFADD", highKey, "value"],
    ["GEOADD", highKey, 1, 2, "member"],
    ["BF.ADD", highKey, "value"],
    ["TOPK.COUNT", highKey, "value"],
    ["OBJECT", "ENCODING", highKey],
    ["MEMORY", "USAGE", highKey],
    ["XINFO", "STREAM", highKey],
    ["XGROUP", "CREATE", highKey, "group", "0-0"]
  ];
  for (const command of firstKeyCommands) {
    expect(routeData(command), command[0] as string).toMatchObject({ route: { shard: 1 } });
  }

  const sharedA = "{typed-shared}:a";
  const sharedB = "{typed-shared}:b";
  const sharedShard = RoutingTopology.slotForKey(sharedA) < 512 ? 0 : 1;
  const sameShardCommands: readonly CommandArgument[][] = [
    ["MSETNX", sharedA, "1", sharedB, "2"],
    ["COPY", sharedA, sharedB],
    ["LMOVE", sharedA, sharedB, "LEFT", "RIGHT"],
    ["BLPOP", sharedA, sharedB, 1],
    ["BLMPOP", 1, 2, sharedA, sharedB, "LEFT"],
    ["SDIFF", sharedA, sharedB],
    ["SINTERCARD", 2, sharedA, sharedB],
    ["PFMERGE", sharedA, sharedB],
    ["GEOSEARCHSTORE", sharedA, sharedB, "FROMMEMBER", "member", "BYRADIUS", 1, "m"],
    ["CMS.MERGE", sharedA, 1, sharedB],
    ["TDIGEST.MERGE", sharedA, 1, sharedB]
  ];
  for (const command of sameShardCommands) {
    expect(routeData(command), command[0] as string).toMatchObject({ route: { shard: sharedShard } });
  }

  const crossShardCommands: readonly CommandArgument[][] = [
    ["MSETNX", lowKey, "1", highKey, "2"],
    ["COPY", lowKey, highKey],
    ["LMOVE", lowKey, highKey, "LEFT", "RIGHT"],
    ["BLPOP", lowKey, highKey, 1],
    ["BLMPOP", 1, 2, lowKey, highKey, "LEFT"],
    ["SDIFF", lowKey, highKey],
    ["SINTERCARD", 2, lowKey, highKey],
    ["PFMERGE", lowKey, highKey],
    ["GEOSEARCHSTORE", lowKey, highKey, "FROMMEMBER", "member", "BYRADIUS", 1, "m"],
    ["CMS.MERGE", lowKey, 1, highKey],
    ["TDIGEST.MERGE", lowKey, 1, highKey]
  ];
  for (const command of crossShardCommands) {
    expect(routeData(command), command[0] as string).toBeUndefined();
  }
  // The pinned core rejects this legacy Redis command as unknown, so it must
  // remain on the control path rather than acquiring invented shard metadata.
  expect(routeData(["BRPOPLPUSH", sharedA, sharedB, 1])).toBeUndefined();

  const largeValue = Buffer.alloc(1024 * 1024);
  const allocate = vi.spyOn(Buffer, "allocUnsafe");
  try {
    expect(routeData(["MSET", lowKey, largeValue, highKey, largeValue])).toBeUndefined();
    expect(allocate.mock.calls.some(([size]) => size >= largeValue.byteLength)).toBe(false);
  } finally {
    allocate.mockRestore();
  }
});
