import { Buffer } from "node:buffer";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  FerricStoreClient,
  FERRICSTORE_MINIMUM_SERVER_VERSION,
  FERRICSTORE_NATIVE_PROTOCOL_VERSION,
  FERRICSTORE_SDK_VERSION,
  OverloadedError,
  type FetchOrComputeComputeResult,
  type FlowRecord,
  type TopKReserveOptions
} from "../src/index.js";
import { classifyServerError } from "../src/errors.js";
import {
  buildProtocolCommand,
  COMMAND_OPCODES,
  COMPACT_FLOW_RECORD,
  decodeResponse,
  encodeValue,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES
} from "../src/protocol.js";
import { routingSlotForKey } from "../src/topology-utilities.js";
import { routingKeyFromProtocolPayload } from "../src/topology-routing.js";
import { RoutingTopology } from "../src/topology.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStore 0.8 TypeScript contract", () => {
  it("retains the v0.8 features under the current package contract and wire version", () => {
    expect(FERRICSTORE_SDK_VERSION).toBe("0.5.0");
    expect(FERRICSTORE_MINIMUM_SERVER_VERSION).toBe("0.11.0");
    expect(FERRICSTORE_NATIVE_PROTOCOL_VERSION).toBe(1);
  });

  it("requires fenced fetch-or-compute ownership tokens", async () => {
    const token = Buffer.from("ownership-token");
    const executor = new FakeExecutor([
      ["compute", Buffer.from("hint"), token],
      Buffer.from("OK"),
      Buffer.from("OK")
    ]);
    const client = new FerricStoreClient(executor);

    const result = await client.fetchOrCompute("cache", { ttlMs: 1_000 });
    expect(result).toMatchObject({
      computeHint: Buffer.from("hint"),
      computeToken: token,
      hit: false,
      shouldCompute: true
    });
    expectTypeOf<FetchOrComputeComputeResult["computeToken"]>().toEqualTypeOf<Buffer>();

    await client.fetchOrComputeResult("cache", "value", { computeToken: token, ttlMs: 1_000 });
    await client.fetchOrComputeError("cache", "failed", { computeToken: token });
    expect(executor.calls.slice(1)).toEqual([
      ["FETCH_OR_COMPUTE_RESULT", "cache", token, Buffer.from("value"), 1_000],
      ["FETCH_OR_COMPUTE_ERROR", "cache", token, "failed"]
    ]);

    const legacy = new FerricStoreClient(new FakeExecutor([["compute", Buffer.from("hint")]]));
    await expect(legacy.fetchOrCompute("cache", { ttlMs: 1_000 })).rejects.toThrow(
      "FETCH_OR_COMPUTE returned an unexpected response"
    );

    const tokenlessTypeContract = (): void => {
      // @ts-expect-error FerricStore 0.8 removed tokenless completion.
      void client.fetchOrComputeError("cache", "failed", { computeToken: null });
    };
    void tokenlessTypeContract;
  });

  it("uses the v0.8 TOPK.RESERVE grammar without decay", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.topk.reserve("top", 10, { width: 8, depth: 7 })).resolves.toBe(true);
    expect(executor.calls).toEqual([["TOPK.RESERVE", "top", 10, 8, 7]]);

    await expect(client.topk.reserve(
      "top-legacy",
      10,
      { width: 8, depth: 7, decay: 0.9 } as unknown as TopKReserveOptions
    )).rejects.toThrow(/decay.*not supported/u);

    const typeContract = (): void => {
      // @ts-expect-error FerricStore 0.8 removed TopK decay.
      void client.topk.reserve("top", 10, { width: 8, depth: 7, decay: 0.9 });
    };
    void typeContract;
  });

  it("carries maxActiveMs through start-and-claim for integers and infinity", async () => {
    const response = { id: "flow", state: "running", type: "jobs" };
    const executor = new FakeExecutor([response, response]);
    const client = new FerricStoreClient(executor);

    await client.startAndClaim("flow", {
      initialState: "queued",
      maxActiveMs: 30_000,
      type: "jobs",
      worker: "w1"
    });
    await client.startAndClaim("forever", {
      initialState: "queued",
      maxActiveMs: "infinity",
      type: "jobs",
      worker: "w1"
    });

    expect(executor.calls[0]?.slice(-2)).toEqual(["MAX_ACTIVE_MS", 30_000]);
    expect(executor.calls[1]?.slice(-2)).toEqual(["MAX_ACTIVE_MS", "infinity"]);
  });

  it("carries maxActiveMs through child spawning", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await client.spawnChildren("parent", [{ id: "child", payload: Buffer.alloc(0), type: "jobs" }], {
      maxActiveMs: "infinity"
    });

    const index = executor.calls[0]?.indexOf("MAX_ACTIVE_MS") ?? -1;
    expect(executor.calls[0]?.slice(index, index + 2)).toEqual(["MAX_ACTIVE_MS", "infinity"]);
  });

  it("decodes max_active_ms failure details while retaining unknown record extensions", async () => {
    const raw = new Map<unknown, unknown>([
      ["id", "expired"],
      ["type", "jobs"],
      ["state", "failed"],
      ["error", new Map<unknown, unknown>([["reason", "max_active_ms"], ["max_active_ms", 500]])],
      ["future_extension", Buffer.from("future")]
    ]);
    const client = new FerricStoreClient(new FakeExecutor([raw]));

    const record = await client.get("expired");
    expect(record).toMatchObject({
      error: { reason: "max_active_ms", maxActiveMs: 500 },
      failureReason: "max_active_ms"
    });
    expect((record?.raw as Map<unknown, unknown>).get("future_extension")).toEqual(Buffer.from("future"));
    expectTypeOf<FlowRecord["failureReason"]>().toEqualTypeOf<string | undefined>();
  });

  it("tolerates unknown compact Flow record field ids", () => {
    const count = Buffer.allocUnsafe(4);
    count.writeUInt32BE(1);
    const body = Buffer.concat([
      Buffer.from([0, 0, COMPACT_FLOW_RECORD]),
      count,
      Buffer.from([250]),
      encodeValue(Buffer.from("future"))
    ]);

    expect(decodeResponse({
      body,
      bodyLength: body.byteLength,
      flags: FLAG_CUSTOM_PAYLOAD,
      laneId: 1,
      opcode: OPCODES.flowGet,
      requestId: 1n
    }, OPCODES.flowGet, {
      compactResponseOpcodes: new Map([["flow_record_v1", new Set([OPCODES.flowGet])]])
    })).toEqual({ field_250: Buffer.from("future") });
  });

  it("emits a typed FLOW.SIGNAL payload with id and signal", () => {
    expect(buildProtocolCommand([
      "FLOW.SIGNAL", "flow-1", "SIGNAL", "paid", "IDEMPOTENCY", "evt-1",
      "IF_STATE", "waiting", "IF_STATE", "pending", "PARTITION", "tenant-a"
    ])).toEqual({
      opcode: OPCODES.flowSignal,
      payload: {
        id: "flow-1",
        idempotency_key: "evt-1",
        if_state: ["waiting", "pending"],
        partition_key: "tenant-a",
        signal: "paid"
      }
    });
  });

  it("keeps only canonical lineage fields in typed payloads and routing", () => {
    const canonical = {
      opcode: OPCODES.flowCreate,
      payload: { parent_flow_id: "parent", root_flow_id: "root" }
    };
    const legacy = {
      opcode: OPCODES.flowCreate,
      payload: { parent_id: "parent", root_id: "root" }
    };

    expect(routingKeyFromProtocolPayload("FLOW.CREATE", canonical)).toBe("parent");
    expect(routingKeyFromProtocolPayload("FLOW.CREATE", legacy)).toBeUndefined();
    expect(buildProtocolCommand([
      "FLOW.START_AND_CLAIM", "flow", "TYPE", "jobs", "INITIAL_STATE", "queued",
      "WORKER", "w1", "PARENT_ID", "legacy"
    ])).toMatchObject({ opcode: COMMAND_OPCODES.COMMAND_EXEC });
  });

  it("does not synthesize a partition for Flow effect reads", async () => {
    const executor = new FakeExecutor([null, []]);
    const client = new FerricStoreClient(executor);

    await client.effectGet("flow-1", "charge");
    await client.governanceLedger("flow-1");

    expect(executor.calls).toEqual([
      ["FLOW.EFFECT.GET", "flow-1", "EFFECT_KEY", "charge"],
      ["FLOW.GOVERNANCE.LEDGER", "flow-1"]
    ]);
  });

  it("rejects cross-slot MSET and MSETNX locally before dispatch", async () => {
    const first = "{slot-a}:one";
    let second = "{slot-b}:two";
    for (let index = 0; RoutingTopology.slotForKey(first) === RoutingTopology.slotForKey(second); index += 1) {
      second = `{slot-b-${index}}:two`;
    }
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.kv.mset([[first, "1"], [second, "2"]])).rejects.toThrow(/share a slot/u);
    await expect(client.kv.msetnx([[first, "1"], [second, "2"]])).rejects.toThrow(/share a slot/u);
    await expect(client.command("MSET", first, "1", second, "2")).rejects.toThrow(/share a slot/u);
    await expect(client.commandArgs(["MSETNX", first, "1", second, "2"])).rejects.toThrow(/share a slot/u);
    await expect(client.pipeline([
      ["GET", "safe"],
      ["MSET", first, "1", second, "2"]
    ])).rejects.toThrow(/share a slot/u);
    expect(() => buildProtocolCommand(["MSET", first, "1", second, "2"])).toThrow(/share a slot/u);
    expect(() => buildProtocolCommand(["MSETNX", first, "1", second, "2"])).toThrow(/share a slot/u);
    expect(executor.calls).toEqual([]);
    expect(executor.pipelineCalls).toEqual([]);
  });

  it("computes string slots exactly like native UTF-8 bytes", () => {
    for (const key of ["plain-😀", "{租户}:key", "f:{流程}:state", "X:f:{流程}:value", "bad-\ud800-key"]) {
      expect(routingSlotForKey(key), key).toBe(routingSlotForKey(Buffer.from(key)));
    }
  });

  it("exposes and follows structured server retry metadata", () => {
    const error = classifyServerError("ERR busy", {
      code: "busy",
      retry_after_ms: 125,
      retryable: true,
      safe_to_retry: true
    }, undefined, 4);

    expect(error).toBeInstanceOf(OverloadedError);
    expect(error).toMatchObject({ retryable: true, safeToRetry: true, retryAfterMs: 125 });

    const unsafe = classifyServerError("ERR busy", {
      code: "busy",
      retry_after_ms: 125,
      retryable: true,
      safe_to_retry: false
    }, undefined, 4);
    expect(unsafe).toMatchObject({ retryable: true, safeToRetry: false });
  });
});
