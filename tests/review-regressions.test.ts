import { describe, expect, it, vi } from "vitest";

import { executeCommandArraysIndividually } from "../src/adapters.js";
import { autoBatchOrderingPlan } from "../src/auto-batch-ordering.js";
import { FerricStoreClient } from "../src/client.js";
import { RawCodec } from "../src/codecs.js";
import type { Command } from "../src/internal.js";
import { KeyValueStore } from "../src/store-key-value.js";
import type { ExecutePipelineOptions } from "../src/adapters.js";
import { flowRoutingData, routingKeyFromArgs } from "../src/topology-routing.js";
import { routingSlotForKey } from "../src/topology-utilities.js";
import { Workflow } from "../src/workflow.js";

describe("review regressions", () => {
  it("rejects sparse public pipelines before native dispatch", async () => {
    const executePipeline = vi.fn(async () => []);
    const executeCommand = vi.fn(async () => null);
    const client = new FerricStoreClient({ executeCommand, executePipeline });
    const commands = new Array<Command>(2);
    commands[1] = ["PING"];

    await expect(client.pipeline(commands)).rejects.toThrow("pipeline commands must be a dense array");
    expect(executePipeline).not.toHaveBeenCalled();
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("rejects sparse public pipelines before ordered fallback dispatch", async () => {
    const executeCommand = vi.fn(async () => null);
    const client = new FerricStoreClient({ executeCommand });
    const commands = new Array<Command>(2);
    commands[1] = ["PING"];

    await expect(client.pipeline(commands, { ordered: true })).rejects.toThrow(
      "pipeline commands must be a dense array"
    );
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("rejects sparse individual fallbacks before any command starts", async () => {
    const executeCommand = vi.fn(async () => null);
    const commands = new Array<Command>(2);
    commands[1] = ["PING"];

    await expect(executeCommandArraysIndividually(executeCommand, commands)).rejects.toThrow(
      "pipeline commands must be a dense array"
    );
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("bounds individual pipeline fallback concurrency and refills freed slots", async () => {
    const commands: Command[] = Array.from({ length: 5 }, (_unused, index) => ["GET", `key-${index}`]);
    const releases = new Map<number, () => void>();
    const started: number[] = [];
    let active = 0;
    let peak = 0;

    const operation = executeCommandArraysIndividually(async (command) => {
      const key = command[1];
      if (typeof key !== "string") throw new TypeError("expected a string key");
      const index = Number(key.slice("key-".length));
      active += 1;
      peak = Math.max(peak, active);
      started.push(index);
      await new Promise<void>((resolve) => releases.set(index, resolve));
      active -= 1;
      return index;
    }, commands, { fallbackConcurrency: 2 });

    await waitFor(() => started.length === 2);
    expect(started).toEqual([0, 1]);
    releases.get(0)?.();
    await waitFor(() => started.length === 3);
    expect(started).toEqual([0, 1, 2]);
    expect(active).toBe(2);

    releases.get(1)?.();
    releases.get(2)?.();
    await waitFor(() => started.length === 5);
    releases.get(3)?.();
    releases.get(4)?.();

    await expect(operation).resolves.toEqual([0, 1, 2, 3, 4]);
    expect(peak).toBe(2);
  });

  it("bounds individual pipeline fallback concurrency by default", async () => {
    const commands: Command[] = Array.from({ length: 70 }, (_unused, index) => ["GET", `key-${index}`]);
    const releases = new Map<number, () => void>();
    let active = 0;
    let peak = 0;
    let started = 0;

    const operation = executeCommandArraysIndividually(async (command) => {
      const index = started;
      started += 1;
      active += 1;
      peak = Math.max(peak, active);
      await new Promise<void>((resolve) => releases.set(index, resolve));
      active -= 1;
      return command[1];
    }, commands);

    await waitFor(() => started === 64);
    expect(active).toBe(64);
    expect(peak).toBe(64);
    for (let index = 0; index < 64; index += 1) releases.get(index)?.();
    await waitFor(() => started === 70);
    for (let index = 64; index < 70; index += 1) releases.get(index)?.();

    await expect(operation).resolves.toHaveLength(70);
    expect(peak).toBe(64);
  });

  it("refills a dependency fallback slot after an idle prerequisite gap", async () => {
    const commands: Command[] = [["GET", "root"], ["GET", "left"], ["GET", "right"]];
    const started: string[] = [];
    let active = 0;
    let peak = 0;

    const results = await executeCommandArraysIndividually(async (command) => {
      const key = command[1];
      if (typeof key !== "string") throw new TypeError("expected a string key");
      started.push(key);
      active += 1;
      peak = Math.max(peak, active);
      await new Promise<void>((resolve) => setImmediate(resolve));
      active -= 1;
      return key;
    }, commands, {
      fallbackConcurrency: 1,
      fallbackDependencies: [[], [0], [0]]
    });

    expect(results).toEqual(["root", "left", "right"]);
    expect(started).toEqual(["root", "left", "right"]);
    expect(peak).toBe(1);
  });

  it("rejects oversized store commands safely when a custom client lacks array dispatch", async () => {
    const command = vi.fn(async () => 0);
    const store = new KeyValueStore({ codec: new RawCodec(), command });
    const keys = Array.from({ length: 200_000 }, (_unused, index) => `key-${index}`);

    await expect(store.del(keys)).rejects.toThrow(/commandArgs/u);
    expect(command).not.toHaveBeenCalled();
  });

  it("rejects oversized executor commands safely when a custom executor lacks array dispatch", async () => {
    const executeCommand = vi.fn(async () => 0);
    const client = new FerricStoreClient({ executeCommand });
    const keys = Array.from({ length: 200_000 }, (_unused, index) => `key-${index}`);

    await expect(client.kv.del(keys)).rejects.toThrow(/executeCommandArgs/u);
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("orders equivalent named value puts when a partition equals an option token", async () => {
    let pipelineOptions: ExecutePipelineOptions | undefined;
    const client = new FerricStoreClient({
      async executeCommand() {
        return Buffer.from("OK");
      },
      async executePipeline(commands, options) {
        pipelineOptions = options;
        return commands.map(() => Buffer.from("OK"));
      }
    }, { autoBatch: true });

    await Promise.all([
      client.valuePut("first", {
        name: "profile",
        ownerFlowId: "same-owner",
        partitionKey: "OWNER_FLOW_ID"
      }),
      client.command(
        "FLOW.VALUE.PUT",
        Buffer.from("second"),
        "NOW",
        1,
        "OWNER_FLOW_ID",
        "same-owner",
        "PARTITION",
        "OWNER_FLOW_ID",
        "NAME",
        "profile"
      )
    ]);

    expect(pipelineOptions?.fallbackDependencies).toEqual([[], [0]]);
  });

  it("treats non-trailing FLOW.VALUE.MGET option tokens as ordinary refs everywhere", () => {
    let tag = "review-ref-0";
    for (let index = 1; routingSlotForKey(`{${tag}}:a`) === routingSlotForKey("MAXBYTES"); index += 1) {
      tag = `review-ref-${index}`;
    }
    const command: Command = [
      "FLOW.VALUE.MGET",
      `{${tag}}:a`,
      "MAXBYTES",
      `{${tag}}:b`
    ];

    expect(autoBatchOrderingPlan([{ command }])?.accesses.size).toBe(3);
    expect(flowRoutingData("FLOW.VALUE.MGET", command)).toBeUndefined();
  });

  it("lets Workflow worker valueMaxBytes override the state default", async () => {
    const commands: Command[] = [];
    const client = new FerricStoreClient({
      async executeCommand(...args) {
        commands.push(args);
        return [];
      }
    });
    const workflow = new Workflow(client, {
      type: "order",
      valueConfig: { valueMaxBytes: 99 }
    }).state("queued", async () => undefined, { valueMaxBytes: 88 });

    await workflow.worker({ claimDrainBatches: 1, valueMaxBytes: 7 }).runOnce();

    const claim = commands.find((command) => command[0] === "FLOW.CLAIM_DUE");
    expect(optionValue(claim, "VALUE_MAX_BYTES")).toBe(7);
  });

  it("rejects sparse KV key/value tuples before dispatch", async () => {
    const command = vi.fn(async () => Buffer.from("OK"));
    const store = new KeyValueStore({ codec: new RawCodec(), command });
    const sparse = new Array(2) as [string, unknown];

    await expect(store.mset([sparse])).rejects.toThrow(/dense two-item tuples/u);
    expect(command).not.toHaveBeenCalled();
  });

  it("rejects sparse probabilistic-module tuples before dispatch", async () => {
    const executeCommand = vi.fn(async () => [1]);
    const client = new FerricStoreClient({ executeCommand });
    const sparse = new Array(2) as [unknown, number];

    await expect(client.cms.incrBy("counts", [sparse])).rejects.toThrow(/dense two-item tuples/u);
    expect(executeCommand).not.toHaveBeenCalled();
  });

  it("rejects non-binary probabilistic multi-result entries", async () => {
    const client = new FerricStoreClient({
      async executeCommand() {
        return [2];
      }
    });
    const operations = [
      async () => await client.bloom.maddMany("bloom", ["item"]),
      async () => await client.bloom.mexistsMany("bloom", ["item"]),
      async () => await client.cuckoo.mexistsMany("cuckoo", ["item"]),
      async () => await client.topk.queryMany("topk", ["item"])
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow(/binary boolean/u);
    }
  });

  it("rejects dangling TOPK.LIST WITHCOUNT entries", async () => {
    const client = new FerricStoreClient({
      async executeCommand() {
        return [Buffer.from("item"), 1, Buffer.from("dangling")];
      }
    });

    await expect(client.topk.list("topk", { withCount: true })).rejects.toThrow(
      /TOPK\.LIST.*pairs/u
    );
  });

  it("directly routes the core KEY_INFO alias by its first key", () => {
    expect(routingKeyFromArgs("KEY_INFO", ["KEY_INFO", "account:1"])).toEqual({
      handled: true,
      key: "account:1"
    });
  });
});

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error("condition was not reached");
}

function optionValue(command: Command | undefined, option: string): unknown {
  if (command == null) return undefined;
  const index = command.indexOf(option);
  return index < 0 ? undefined : command[index + 1];
}
