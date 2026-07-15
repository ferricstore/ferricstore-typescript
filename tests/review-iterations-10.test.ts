import { describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "node:net";

import {
  FerricStoreClient,
  FlowNotFoundError,
  JsonCodec,
  NativeAdapter,
  QueueClient,
  RawCodec,
  StaleLeaseError,
  TopologyNativeAdapterPool,
  WorkflowClient,
  WorkflowContext,
  classifyServerError,
  type ClaimedItem,
  type Command,
  type CommandArgument,
  type GetExOptions,
  type FerricStoreClientOptions,
  type ScanOptions,
  type SetOptions,
  type StateOptions,
  type WorkerConfig
} from "../src/index.js";
import { OPCODES, tryPipelineCommand } from "../src/protocol.js";
import { snapshotTopologyNativeAdapterOptions } from "../src/topology-options.js";
import { startCountingServer } from "./adapter-test-support.js";
import { FakeExecutor } from "./fake-executor.js";

describe("review iterations 10", () => {
  const inheritedStringArray = (value: string): string[] => {
    const values = new Array<string>(1);
    const prototype = Object.create(Array.prototype) as Record<number, string>;
    Object.defineProperty(prototype, 0, { configurable: true, value });
    Object.setPrototypeOf(values, prototype);
    return values;
  };

  it("ignores inherited options in direct native adapter bootstrap", async () => {
    const opcodes: number[] = [];
    const server = await startCountingServer((request) => {
      opcodes.push(request.opcode);
      return request.opcode === OPCODES.startup ? { auth_required: false } : undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;
    const options = Object.create({
      password: "prototype-secret",
      username: "prototype-user"
    }) as NonNullable<Parameters<typeof NativeAdapter.fromUrl>[1]>;

    const adapter = await NativeAdapter.fromUrl(
      `ferric://127.0.0.1:${address.port}`,
      options
    );
    try {
      expect(opcodes).toEqual([OPCODES.startup]);
    } finally {
      await adapter.close();
    }
  });

  it("ignores inherited top-level client options", () => {
    const options = Object.create({
      codec: new JsonCodec(),
      flowManyBatchLimit: 1
    }) as FerricStoreClientOptions;

    const client = new FerricStoreClient(new FakeExecutor(), options);

    expect(client.codec).toBeInstanceOf(RawCodec);
    expect(client.flowManyBatchLimit).toBe(1_000);
  });

  it("snapshots URL-factory client options before connecting", async () => {
    let releaseConnection: (() => void) | undefined;
    const connectionGate = new Promise<void>((resolve) => { releaseConnection = resolve; });
    const fromUrl = vi.spyOn(NativeAdapter, "fromUrl").mockImplementation(async () => {
      await connectionGate;
      return {
        async close(): Promise<void> { return undefined; },
        async executeCommand(): Promise<unknown> { return Buffer.from("OK"); }
      } as NativeAdapter;
    });
    const originalCodec = new JsonCodec();
    const options = {
      codec: originalCodec,
      nativeOptions: {},
      reconnect: false
    };

    try {
      const opening = FerricStoreClient.fromUrl("ferric://node.local:6388", options);
      options.codec = new RawCodec();
      releaseConnection?.();
      const client = await opening;

      expect(client.codec).toBe(originalCodec);
      await client.close();
    } finally {
      releaseConnection?.();
      fromUrl.mockRestore();
    }
  });

  it("rejects inherited ACL rules and credit reservation ids before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(
      client.aclSetUser("operator", inheritedStringArray("+@all"))
    ).rejects.toThrow(/dense|own/u);
    await expect(client.limitRelease("payments", {
      reservationIds: inheritedStringArray("reservation-1"),
      shardId: 1
    })).rejects.toThrow(/dense|own/u);

    expect(executor.calls).toEqual([]);
  });

  it("classifies structured server codes without relying on message text", () => {
    expect(classifyServerError("opaque failure", {
      code: Buffer.from("stale_lease")
    })).toBeInstanceOf(StaleLeaseError);
    expect(classifyServerError("opaque failure", {
      code: "flow_not_found"
    })).toBeInstanceOf(FlowNotFoundError);
  });

  it("ignores inherited KV command options", async () => {
    const executor = new FakeExecutor([
      Buffer.from("OK"),
      Buffer.from("value"),
      [Buffer.from("0"), []]
    ]);
    const client = new FerricStoreClient(executor);
    const setOptions = Object.create({ get: true, nx: true }) as SetOptions;
    const getexOptions = Object.create({ persist: true }) as GetExOptions;
    const scanOptions = Object.create({ count: 1, match: "attacker:*" }) as ScanOptions;

    await client.kv.set("key", "value", setOptions);
    await client.kv.getex("key", getexOptions);
    await client.kv.scan(0, scanOptions);

    expect(executor.calls).toEqual([
      ["SET", "key", Buffer.from("value")],
      ["GETEX", "key"],
      ["SCAN", 0]
    ]);
  });

  it("does not let inherited SET GET switch the response decoder", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const options = Object.create({ get: true }) as SetOptions;

    await expect(client.kv.set("key", { value: 1 }, options)).resolves.toEqual(Buffer.from("OK"));
    expect(executor.calls).toEqual([["SET", "key", Buffer.from('{"value":1}')]]);
  });

  it("snapshots SET GET once for matching request and response modes", async () => {
    const executor = new FakeExecutor([Buffer.from('{"old":1}')]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    let reads = 0;
    const options = {} as SetOptions;
    Object.defineProperty(options, "get", {
      enumerable: true,
      get: () => {
        reads += 1;
        return reads === 1;
      }
    });

    await expect(client.kv.set("key", { value: 1 }, options)).resolves.toEqual({ old: 1 });
    expect(reads).toBe(1);
    expect(executor.calls).toEqual([["SET", "key", Buffer.from('{"value":1}'), "GET"]]);
  });

  it("ignores inherited collection and module options", async () => {
    const executor = new FakeExecutor([
      [],
      [],
      [],
      Buffer.from("OK"),
      Buffer.from("OK"),
      Buffer.from("OK")
    ]);
    const client = new FerricStoreClient(executor);
    const rangeOptions = Object.create({ withScores: true }) as { withScores?: boolean };
    const readOptions = Object.create({ blockMs: 1, count: 1 }) as { blockMs?: number; count?: number };
    const listOptions = Object.create({ withCount: true }) as { withCount?: boolean };
    const mergeOptions = Object.create({ weights: [1, 2] }) as { weights?: number[] };
    const createOptions = Object.create({ compression: 200 }) as { compression?: number };
    const digestMergeOptions = Object.create({ override: true }) as { override?: boolean };

    await client.zset.zrange("rank", 0, -1, rangeOptions);
    await client.stream.xread(
      [{ key: "events", id: "0-0" }],
      readOptions
    );
    await client.topk.list("frequent", listOptions);
    await client.cms.merge("merged", ["left", "right"], mergeOptions);
    await client.tdigest.create("latency", createOptions);
    await client.tdigest.merge("latency", ["left"], digestMergeOptions);

    expect(executor.calls).toEqual([
      ["ZRANGE", "rank", 0, -1],
      ["XREAD", "STREAMS", "events", "0-0"],
      ["TOPK.LIST", "frequent"],
      ["CMS.MERGE", "merged", 2, "left", "right"],
      ["TDIGEST.CREATE", "latency"],
      ["TDIGEST.MERGE", "latency", 1, "left"]
    ]);
  });

  it("keeps LPOS scalar decoding when COUNT is inherited", async () => {
    const executor = new FakeExecutor([2]);
    const client = new FerricStoreClient(executor);
    const options = Object.create({ count: 1 }) as { count?: number };

    await expect(client.lists.lpos("items", "needle", options)).resolves.toBe(2);
    expect(executor.calls).toEqual([["LPOS", "items", Buffer.from("needle")]]);
  });

  it("snapshots compact pipeline arguments before unsafe allocation", () => {
    let keyReads = 0;
    const command = ["SET", "placeholder", "value"] as Command;
    Object.defineProperty(command, 1, {
      enumerable: true,
      get: () => {
        keyReads += 1;
        return keyReads === 1 ? "stable-key" : "x";
      }
    });

    const protocol = tryPipelineCommand([command]);
    expect(protocol?.flags).toBeDefined();
    expect(keyReads).toBe(1);
    const payload = protocol?.payload as Buffer;
    const keySize = payload.readUInt32BE(6);
    expect(payload.subarray(10, 10 + keySize).toString("utf8")).toBe("stable-key");
  });

  it("snapshots public pipelines before an executor await", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const seenCommands: CommandArgument[][][] = [];
    const seenOrdered: (boolean | undefined)[] = [];
    const client = new FerricStoreClient({
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands, options): Promise<unknown[]> {
        await gate;
        seenCommands.push(commands.map((command) => [...command]));
        seenOrdered.push(options?.ordered);
        return commands.map(() => Buffer.from("OK"));
      }
    });
    const commands: CommandArgument[][] = [["SET", "key", "original"]];
    const options = { ordered: true };

    const pending = client.pipeline(commands, options);
    const firstCommand = commands[0];
    if (firstCommand == null) throw new Error("test command is missing");
    firstCommand[2] = "mutated";
    options.ordered = false;
    release?.();
    await pending;

    expect(seenCommands).toEqual([[["SET", "key", "original"]]]);
    expect(seenOrdered).toEqual([true]);
  });

  it("snapshots only own worker options and reads arrays once", () => {
    const executor = new FakeExecutor();
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const partitionKeys = ["tenant-a"];
    let partitionReads = 0;
    const options = Object.create({
      claimValues: ["prototype-value"],
      profile: "throughput"
    }) as WorkerConfig;
    Object.defineProperty(options, "partitionKeys", {
      enumerable: true,
      get: () => {
        partitionReads += 1;
        return partitionKeys;
      }
    });

    const worker = queue.worker(options);
    partitionKeys[0] = "mutated";

    expect(partitionReads).toBe(1);
    expect(worker.options).toMatchObject({ partitionKeys: ["tenant-a"] });
    expect(worker.options.profile).toBeUndefined();
    expect(worker.options.batchSize).toBeUndefined();
    expect(worker.options.claimValues).toBeUndefined();
  });

  it("ignores inherited workflow state options", () => {
    const workflow = new WorkflowClient(new FerricStoreClient(new FakeExecutor()))
      .workflow({ type: "order" });
    const options = Object.create({
      claimPayload: false,
      claimValues: ["prototype-value"],
      leaseMs: 1
    }) as StateOptions;

    workflow.state("queued", () => undefined, options);

    expect(workflow.stateRegistration("queued")).toMatchObject({
      claimPayload: true,
      leaseMs: 30_000
    });
    expect(workflow.stateRegistration("queued")?.claimValues).toBeUndefined();
  });

  it("keeps claim hydration options aligned with the dispatched request", async () => {
    let releaseClaim: (() => void) | undefined;
    const claimGate = new Promise<void>((resolve) => { releaseClaim = resolve; });
    const calls: CommandArgument[][] = [];
    const client = new FerricStoreClient({
      async executeCommand(...args): Promise<unknown> {
        calls.push([...args]);
        if (args[0] === "FLOW.CLAIM_DUE") {
          await claimGate;
          return [[
            Buffer.from("flow-1"),
            Buffer.from("tenant-a"),
            Buffer.from("lease"),
            1
          ]];
        }
        return new Map<unknown, unknown>([
          ["id", "flow-1"],
          ["type", "order"],
          ["state", "running"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 1]
        ]);
      }
    });
    const values = ["original"];
    const options = { values, worker: "worker-1" };

    const pending = client.claimDue("order", options);
    values[0] = "mutated";
    releaseClaim?.();
    await pending;

    expect(calls[0]).toContain("original");
    expect(calls[1]).toContain("original");
    expect(calls[1]).not.toContain("mutated");
  });

  it("rejects sparse workflow value-name lists before fetching", async () => {
    const executor = new FakeExecutor();
    const workflow = new WorkflowClient(new FerricStoreClient(executor))
      .workflow({ type: "order" });
    const job: ClaimedItem = {
      fencingToken: 1,
      id: "flow-1",
      leaseToken: Buffer.from("lease"),
      state: "running",
      type: "order"
    };
    const context = new WorkflowContext(workflow, job, "running");
    const names = new Array<string>(1);

    await expect(context.valueMany(names)).rejects.toThrow(/names.*dense/u);
    expect(executor.calls).toEqual([]);
  });

  it("snapshots direct topology-pool TLS options for learned connections", async () => {
    const seenVerification: (boolean | undefined)[] = [];
    const fromUrl = vi.spyOn(NativeAdapter, "fromUrl").mockImplementation(async (_url, options) => {
      seenVerification.push(options?.tlsOptions?.rejectUnauthorized);
      const index = seenVerification.length;
      return {
        isUnavailable: false,
        async close(): Promise<void> { return undefined; },
        async executeCommand(command: string): Promise<unknown> {
          if (index === 1 && command === "SHARDS") {
            return {
              ranges: [{
                endpoint: {
                  host: "learned.local",
                  native_port: 6388,
                  native_tls_port: 6389,
                  node: "learned@local"
                },
                first_slot: 0,
                lane_id: 1,
                last_slot: 1023,
                shard: 0
              }]
            };
          }
          return Buffer.from("OK");
        },
        async executeCommandOnLane(): Promise<unknown> {
          return Buffer.from("value");
        }
      } as unknown as NativeAdapter;
    });
    const tlsOptions = { rejectUnauthorized: true };
    let pool: TopologyNativeAdapterPool | undefined;

    try {
      pool = await TopologyNativeAdapterPool.fromUrls(["ferrics://seed.local:6389"], {
        endpointPolicy: "any",
        tlsOptions
      });
      tlsOptions.rejectUnauthorized = false;

      await expect(pool.executeCommand("GET", "key")).resolves.toEqual(Buffer.from("value"));
      expect(seenVerification).toEqual([true, true]);
    } finally {
      await pool?.close();
      fromUrl.mockRestore();
    }
  });

  it("does not authorize learned hosts through an inherited endpoint policy", () => {
    const endpointPolicy = Object.create({
      allowHosts: ["learned.local"]
    }) as { allowHosts: readonly string[] };

    expect(() => snapshotTopologyNativeAdapterOptions({ endpointPolicy })).toThrow(
      /endpointPolicy\.allowHosts.*own/u
    );
  });

  it("does not complete a topology refresh after close wins during warming", async () => {
    let markLearnedStarted: (() => void) | undefined;
    let releaseLearned: (() => void) | undefined;
    const learnedStarted = new Promise<void>((resolve) => { markLearnedStarted = resolve; });
    const learnedGate = new Promise<void>((resolve) => { releaseLearned = resolve; });
    const fromUrl = vi.spyOn(NativeAdapter, "fromUrl").mockImplementation(async (url) => {
      if (url.includes("learned.local")) {
        markLearnedStarted?.();
        await learnedGate;
      }
      return {
        isUnavailable: false,
        async close(): Promise<void> { return undefined; },
        async executeCommand(command: string): Promise<unknown> {
          if (command === "SHARDS") {
            return {
              ranges: [{
                endpoint: { host: "learned.local", native_port: 6388, node: "learned@local" },
                first_slot: 0,
                lane_id: 1,
                last_slot: 1023,
                shard: 0
              }]
            };
          }
          return Buffer.from("OK");
        }
      } as unknown as NativeAdapter;
    });
    const Pool = TopologyNativeAdapterPool as unknown as new (
      urls: readonly string[],
      options: { endpointPolicy: "any"; warmConnections: boolean }
    ) => TopologyNativeAdapterPool;
    const pool = new Pool(["ferric://seed.local:6388"], {
      endpointPolicy: "any",
      warmConnections: true
    });

    try {
      const refreshing = pool.refreshTopology();
      await learnedStarted;
      const closing = pool.close();
      releaseLearned?.();

      await expect(refreshing).rejects.toThrow("closed");
      await closing;
    } finally {
      releaseLearned?.();
      await pool.close();
      fromUrl.mockRestore();
    }
  });

});
