import { describe, expect, it, vi } from "vitest";

import {
  ConnectionClosedError,
  FerricStoreClient,
  TopologyNativeAdapterPool,
  WorkflowClient
} from "../src/index.js";
import type { CommandArgument } from "../src/internal.js";
import { buildProtocolCommand } from "../src/protocol.js";
import type { Command } from "../src/internal.js";
import { mapSettledWithConcurrency } from "../src/topology-utilities.js";
import { RoutingTopology } from "../src/topology.js";
import { topologyForInstallation } from "../src/topology-installation.js";
import { FakeExecutor } from "./fake-executor.js";
import { BackpressureSocket, directNativeAdapter } from "./adapter-test-support.js";

describe("review iterations 9", () => {
  it("normalizes and validates scalar and COUNT LPOS replies", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      Buffer.from("2"),
      [Buffer.from("1"), Buffer.from("3")]
    ]));

    await expect(client.lists.lpos("list", "item")).resolves.toBe(2);
    await expect(client.lists.lpos("list", "item", { count: 2 })).resolves.toEqual([1, 3]);
  });

  it("normalizes GEOHASH and GEOPOS textual replies", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      [Buffer.from("sqc8b49rny0"), null],
      [[Buffer.from("13.361389"), Buffer.from("38.115556")], null]
    ]));

    await expect(client.geo.geohashMany("geo", ["palermo", "missing"])).resolves.toEqual([
      "sqc8b49rny0",
      null
    ]);
    await expect(client.geo.geoposMany("geo", ["palermo", "missing"])).resolves.toEqual([
      ["13.361389", "38.115556"],
      null
    ]);
  });

  it("rejects missing compact Flow partition options instead of encoding an empty key", () => {
    const args: CommandArgument[] = [
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      10,
      "PARTITIONS",
      2,
      "tenant-a",
      "tenant-b",
      "RETURN",
      "JOBS_COMPACT",
      "NOPAYLOAD"
    ];
    Reflect.deleteProperty(args, "13");

    expect(() => buildProtocolCommand(args)).toThrow(/PARTITIONS.*dense/u);
  });

  it("rejects missing compact Flow item fields instead of encoding empty data", () => {
    const args: CommandArgument[] = [
      "FLOW.CREATE_MANY",
      "MIXED",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1_000,
      "ITEMS",
      "flow-1",
      "tenant-a",
      Buffer.from("payload")
    ];
    Reflect.deleteProperty(args, "9");

    expect(() => buildProtocolCommand(args)).toThrow(/ITEMS.*dense/u);
  });

  it("rejects missing compact Flow option values instead of coercing them to zero", () => {
    const args: CommandArgument[] = [
      "FLOW.CREATE_MANY",
      "AUTO",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1_000,
      "ITEMS",
      "flow-1",
      Buffer.from("payload")
    ];
    Reflect.deleteProperty(args, "7");

    expect(() => buildProtocolCommand(args)).toThrow(/Flow options.*dense/u);
  });

  it("rejects missing Flow admin fields before building a native payload", () => {
    const args: CommandArgument[] = ["FLOW.SCHEDULE.GET", "schedule-1"];
    Reflect.deleteProperty(args, "1");

    expect(() => buildProtocolCommand(args)).toThrow(/Flow admin arguments.*dense/u);
  });

  it("snapshots runStepsMany states before an auto-batch delay", async () => {
    const pipelineCalls: Command[][] = [];
    const client = new FerricStoreClient({
      async executeCommand() {
        return Buffer.from("PONG");
      },
      async executePipeline(commands) {
        pipelineCalls.push(commands.map((command) => [...command]));
        return commands.map(() => Buffer.from("OK"));
      }
    }, { autoBatch: { enabled: true, maxDelayMs: 1_000 } });
    const states = ["created"];

    const request = client.runStepsMany(["flow-1"], {
      states,
      type: "order",
      worker: "worker-1"
    });
    states[0] = "mutated";
    await Promise.all([request, client.ping()]);

    expect(pipelineCalls[0]).toEqual([expect.arrayContaining(["STATES", ["created"]])]);
    await client.close();
  });

  it("snapshots explicit pipelines waiting behind an auto-batch", async () => {
    let markStarted: (() => void) | undefined;
    let release: (() => void) | undefined;
    const started = new Promise<void>((resolve) => { markStarted = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const calls: Command[][] = [];
    const orderedValues: (boolean | undefined)[] = [];
    const client = new FerricStoreClient({
      async executeCommand() {
        return Buffer.from("OK");
      },
      async executePipeline(commands, options) {
        calls.push(commands.map((command) => [...command]));
        orderedValues.push(options?.ordered);
        if (calls.length === 1) {
          markStarted?.();
          await gate;
        }
        return commands.map(() => Buffer.from("OK"));
      }
    }, { autoBatch: { enabled: true, maxCommands: 1 } });

    const batched = client.command("SET", "first", "value");
    await started;
    const commands: CommandArgument[][] = [["SET", "second", "original"]];
    const options = { ordered: true };
    const explicit = client.pipeline(commands, options);
    const firstCommand = commands[0];
    if (firstCommand == null) throw new Error("test pipeline command is missing");
    firstCommand[2] = "mutated";
    options.ordered = false;
    release?.();
    await Promise.all([batched, explicit]);

    expect(calls[1]).toEqual([["SET", "second", "original"]]);
    expect(orderedValues[1]).toBe(true);
    await client.close();
  });

  it("keeps nullish topology work items positional", async () => {
    const results = await mapSettledWithConcurrency(
      [null, undefined],
      2,
      async (value, index) => `${index}:${String(value)}`
    );

    expect(results).toEqual([
      { status: "fulfilled", value: "0:null" },
      { status: "fulfilled", value: "1:undefined" }
    ]);
  });

  it("treats the core route epoch as an opaque fingerprint during failover", () => {
    const topology = (host: string, routeEpoch: number) => RoutingTopology.build({
      ranges: [{
        endpoint: { host, native_port: 6_388, node: `${host}@cluster` },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1_023,
        shard: 0
      }],
      route_epoch: routeEpoch,
      shard_count: 1
    });
    const current = topology("old.local", 900);
    const sameFingerprintLeaderChange = topology("new.local", 900);
    const lowerNewFingerprint = topology("newer.local", 100);

    expect(topologyForInstallation(current, sameFingerprintLeaderChange)).toBe(sameFingerprintLeaderChange);
    expect(topologyForInstallation(current, lowerNewFingerprint)).toBe(lowerNewFingerprint);
  });

  it("keeps the local native frame cap when the server advertises a larger limit", async () => {
    const adapter = directNativeAdapter(new BackpressureSocket(), 1_024);
    const internals = adapter as unknown as {
      applyStartupLimits(value: unknown): void;
      capabilities: {
        activateAuthenticated(): void;
        requestFrameBytes: number;
      };
    };

    try {
      expect(internals.capabilities.requestFrameBytes).toBe(64 * 1_024);
      internals.applyStartupLimits({ limits: { max_frame_bytes: 32 * 1_024 * 1_024 } });
      expect(internals.capabilities.requestFrameBytes).toBe(64 * 1_024);
      internals.capabilities.activateAuthenticated();
      expect(internals.capabilities.requestFrameBytes).toBe(16 * 1_024 * 1_024);
      internals.applyStartupLimits({ limits: { max_frame_bytes: 1_024 } });
      expect(internals.capabilities.requestFrameBytes).toBe(1_024);
    } finally {
      await adapter.close();
    }
  });

  it("does not expose the workflow lease guard's mutable token", async () => {
    const leaseToken = Buffer.from("lease-token");
    const executor = new FakeExecutor([
      [new Map<unknown, unknown>([
        ["id", "order-1"],
        ["type", "order"],
        ["state", "created"],
        ["lease_token", leaseToken],
        ["fencing_token", 7]
      ])],
      Buffer.from("OK")
    ]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("created", (ctx) => {
      ctx.leaseToken.fill(0);
    });

    await workflow.worker({ leaseRenewal: false, states: ["created"] }).runOnce();

    expect(executor.calls[1]).toContainEqual(Buffer.from("lease-token"));
  });

  it("rejects inherited request-context authorization scopes", () => {
    const scopes = new Array<string>(1);
    const prototype = Object.assign(Object.create(Array.prototype) as string[], {
      0: "invocation:create:*"
    });
    Object.setPrototypeOf(scopes, prototype);

    expect(() => buildProtocolCommand([
      "INVOCATION.CREATE",
      "send-email",
      "{}",
      "REQUEST_CONTEXT",
      { scopes }
    ])).toThrow(/scopes.*own|scopes.*dense/iu);
  });

  it("snapshots reconnect seeds and transport security options", async () => {
    const creations: {
      password?: string;
      rejectUnauthorized?: boolean;
      urls: string[];
    }[] = [];
    let generation = 0;
    const fromUrls = vi.spyOn(TopologyNativeAdapterPool, "fromUrls").mockImplementation(
      async (urls, options) => {
        const currentGeneration = generation;
        generation += 1;
        creations.push({
          password: options?.password,
          rejectUnauthorized: options?.tlsOptions?.rejectUnauthorized,
          urls: [...urls]
        });
        return {
          close() { return undefined; },
          async executeCommand() {
            if (currentGeneration === 0) throw new ConnectionClosedError("unsent");
            return Buffer.from("PONG");
          }
        } as unknown as TopologyNativeAdapterPool;
      }
    );
    const urls = ["ferrics://seed.local:6389"];
    const nativeOptions = {
      password: "original-password",
      tlsOptions: { rejectUnauthorized: true }
    };
    let client: FerricStoreClient | undefined;

    try {
      client = await FerricStoreClient.fromUrls(urls, { nativeOptions });
      urls[0] = "ferrics://attacker.local:6389";
      nativeOptions.password = "mutated-password";
      nativeOptions.tlsOptions.rejectUnauthorized = false;

      await expect(client.ping()).resolves.toEqual(Buffer.from("PONG"));
      expect(creations).toEqual([
        {
          password: "original-password",
          rejectUnauthorized: true,
          urls: ["ferrics://seed.local:6389"]
        },
        {
          password: "original-password",
          rejectUnauthorized: true,
          urls: ["ferrics://seed.local:6389"]
        }
      ]);
    } finally {
      await client?.close();
      fromUrls.mockRestore();
    }
  });
});
