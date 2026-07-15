import { describe, expect, it } from "vitest";

import { FerricStoreClient, RoutingTopology, WorkflowClient } from "../src/index.js";
import type { CommandArgument } from "../src/internal.js";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import { NativeResponseHandler } from "../src/native-response-handler.js";
import {
  DEFAULT_MAX_FRAME_BYTES,
  OPCODES,
  decodeValue,
  encodeValue
} from "../src/protocol.js";
import { responseFrame } from "./adapter-test-support.js";
import { FakeExecutor } from "./fake-executor.js";

describe("seventh ten-pass review regressions", () => {
  it("rejects sparse OBJECT HELP responses", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("ENCODING key");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.kv.objectHelp()).rejects.toThrow(
      "OBJECT HELP response item 1 is missing"
    );
  });

  it("rejects sparse GEOSEARCH argument arrays before dispatch", async () => {
    const executor = new FakeExecutor([[]]);
    const client = new FerricStoreClient(executor);
    const args = new Array<CommandArgument>(6);
    args[0] = "FROMLONLAT";
    args[1] = 34.78;
    args[2] = 32.08;
    args[4] = 1;
    args[5] = "km";

    await expect(client.geo.geosearch("places", args)).rejects.toThrow(
      "GEOSEARCH arguments must be dense"
    );
    expect(executor.calls).toEqual([]);
  });

  it("preserves negative zero through native value encoding", () => {
    const decoded = decodeValue(encodeValue(-0)).value;

    expect(Object.is(decoded, -0)).toBe(true);
  });

  it("rejects sparse explicit-pipeline responses at the executor boundary", async () => {
    const client = new FerricStoreClient({
      async executeCommand(): Promise<unknown> {
        throw new Error("unexpected individual command");
      },
      async executePipeline(): Promise<unknown[]> {
        const results = new Array<unknown>(2);
        results[0] = Buffer.from("first");
        return results;
      }
    });

    await expect(client.pipeline([
      ["GET", "first"],
      ["GET", "second"]
    ])).rejects.toThrow("pipeline response item 1 is missing");
  });

  it("rejects management frames received on a data lane", () => {
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

    handler.onData(responseFrame(OPCODES.event, 1, 0n, { event: "FLOW_WAKE" }));

    expect(failures).toHaveLength(1);
    expect(String(failures[0])).toContain("management frame used non-control lane");
    expect(events).toEqual([]);
  });

  it("does not expose mutable topology endpoint ownership", () => {
    const topology = RoutingTopology.build({
      ranges: [{
        endpoint: { host: "node.local", native_port: 6388, node: "node@cluster" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      route_epoch: 1
    });
    const route = topology.routeKey("tenant-key");

    try {
      (route.endpoint as { host: string }).host = "attacker.local";
    } catch {
      // Frozen snapshots may throw in strict-mode callers.
    }

    expect(topology.routeKey("tenant-key").endpoint.host).toBe("node.local");
    expect("set" in topology.endpoints).toBe(false);
  });

  it("hydrates legacy compact FLOW.RECLAIM tuples in full-record mode", async () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("tenant-a");
    const lease = Buffer.from("lease");
    const executor = new FakeExecutor([
      [[id, partition, lease, 7]],
      new Map<unknown, unknown>([
        ["id", id],
        ["type", "order"],
        ["state", "running"],
        ["partition_key", partition],
        ["lease_token", lease],
        ["fencing_token", 7],
        ["version", 2]
      ])
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.reclaim("order", {
      worker: "worker-1"
    })).resolves.toEqual([
      expect.objectContaining({ id: "flow-1", partitionKey: "tenant-a", state: "running" })
    ]);
    expect(executor.calls[1]?.slice(0, 4)).toEqual([
      "FLOW.GET",
      "flow-1",
      "PARTITION",
      "tenant-a"
    ]);
  });

  it("snapshots and protects workflow state registration", async () => {
    const executor = new FakeExecutor([[]]);
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    const claimValues = ["profile-original"];
    const retryPolicy = { maxRetries: 2 };
    workflow.state("queued", () => undefined, {
      claimValues,
      leaseMs: 1234,
      retryPolicy
    });

    claimValues[0] = "profile-mutated";
    retryPolicy.maxRetries = 99;
    const registration = workflow.stateRegistration("queued");
    try {
      (registration as { leaseMs: number }).leaseMs = 9999;
      (registration?.claimValues as string[])[0] = "profile-exposed";
    } catch {
      // Immutable registrations throw in strict-mode callers.
    }

    await workflow.worker({ leaseRenewal: false, states: ["queued"] }).runOnce();

    const claim = executor.calls[0] ?? [];
    expect(claim[claim.indexOf("LEASE_MS") + 1]).toBe(1234);
    expect(claim).toContain("profile-original");
    expect(claim).not.toContain("profile-mutated");
    expect(workflow.stateRegistration("queued")?.retryPolicy?.maxRetries).toBe(2);
  });

  it("rejects sparse public array responses instead of materializing undefined", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("get");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.commandList()).rejects.toThrow(
      "server response item 1 is missing"
    );
  });
});
