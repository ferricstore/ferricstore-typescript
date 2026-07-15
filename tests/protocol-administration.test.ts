import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { COMMAND_OPCODES, OPCODES, buildProtocolCommand } from "../src/protocol.js";

describe("native administration protocol codec", () => {
  it("preserves bigint fencing tokens in typed Flow mutations", () => {
    const fencingToken = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const command = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      Buffer.from("lease"),
      "FENCING",
      fencingToken,
      "NOW",
      100
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowComplete,
      payload: { fencing_token: fencingToken }
    });
  });

  it("rejects inexact or out-of-range fencing command arguments", () => {
    const command = (fencingToken: number | bigint) => buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      Buffer.from("lease"),
      "FENCING",
      fencingToken,
      "NOW",
      100
    ]);

    expect(() => command(Number.MAX_SAFE_INTEGER + 1)).toThrow("use bigint");
    expect(() => command(9_223_372_036_854_775_808n)).toThrow("signed 64-bit range");
  });

  it("builds direct native fused workflow commands", () => {
    const lease = Buffer.from("lease");
    expect(buildProtocolCommand([
      "FLOW.START_AND_CLAIM",
      "flow-1",
      "TYPE",
      "order",
      "INITIAL_STATE",
      "created",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "NOW",
      100,
      "ATTRIBUTE",
      "phases",
      ["created", "charged"]
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.START_AND_CLAIM"],
      payload: {
        attributes: { phases: ["created", "charged"] },
        id: "flow-1",
        initial_state: "created",
        lease_ms: 30_000,
        now_ms: 100,
        type: "order",
        worker: "worker-1"
      }
    });

    expect(buildProtocolCommand([
      "FLOW.STEP_CONTINUE", "flow-1", lease, "created", "charged",
      "FENCING", 7, "LEASE_MS", 30_000, "NOW", 101,
      "ATTRIBUTE_DELETE", "temporary", "RETURN", "JOBS_COMPACT"
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.STEP_CONTINUE"],
      payload: {
        attributes_delete: ["temporary"],
        fencing_token: 7,
        from_state: "created",
        id: "flow-1",
        lease_ms: 30_000,
        lease_token: lease,
        now_ms: 101,
        return: "JOBS_COMPACT",
        to_state: "charged"
      }
    });

    const items = [{ id: "flow-2", partition_key: "tenant-a" }];
    expect(buildProtocolCommand([
      "FLOW.RUN_STEPS_MANY", "TYPE", "order", "STATES", ["created", "charged"],
      "WORKER", "worker-1", "LEASE_MS", 30_000, "NOW", 102, "ITEMS", items
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.RUN_STEPS_MANY"],
      payload: {
        items,
        lease_ms: 30_000,
        now_ms: 102,
        states: ["created", "charged"],
        type: "order",
        worker: "worker-1"
      }
    });
  });

  it("builds direct native Flow schedule, query, and governance commands", () => {
    expect(buildProtocolCommand([
      "FLOW.HISTORY", "flow-1", "PARTITION", "tenant-a", "FROM_VERSION", 2,
      "TO_VERSION", 8, "INCLUDE_COLD", true, "VALUES", false, "PAYLOAD_MAX_BYTES", 64_000
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.HISTORY"],
      payload: {
        from_version: 2,
        id: "flow-1",
        include_cold: true,
        partition_key: "tenant-a",
        payload_max_bytes: 64_000,
        to_version: 8,
        values: false
      }
    });
    expect(buildProtocolCommand(["FLOW.STATS", "order", "STATE", "queued"])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.STATS"],
      payload: { state: "queued", type: "order" }
    });
    expect(buildProtocolCommand([
      "FLOW.ATTRIBUTE_VALUES", "order", "tenant", "COUNT", 10
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.ATTRIBUTE_VALUES"],
      payload: { attribute: "tenant", count: 10, type: "order" }
    });
    expect(buildProtocolCommand([
      "FLOW.SCHEDULE.CREATE", "schedule-1", "KIND", "cron", "CRON", "*/5 * * * *", "OVERWRITE", true
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.SCHEDULE.CREATE"],
      payload: { cron: "*/5 * * * *", id: "schedule-1", kind: "cron", overwrite: true }
    });
    expect(buildProtocolCommand([
      "FLOW.SCHEDULE.FIRE_DUE", "WORKER", "scheduler-1", "BLOCK", 50, "LIMIT", 10
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.SCHEDULE.FIRE_DUE"],
      payload: { block_ms: 50, limit: 10, worker: "scheduler-1" },
      serverBlockMs: 50
    });
    expect(buildProtocolCommand([
      "FLOW.EFFECT.RESERVE", "flow-1", "EFFECT_KEY", "charge", "EFFECT_TYPE", "payment",
      "OPERATION_DIGEST", "sha256:1"
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.EFFECT.RESERVE"],
      payload: {
        effect_key: "charge",
        effect_type: "payment",
        id: "flow-1",
        operation_digest: "sha256:1"
      }
    });
    expect(buildProtocolCommand([
      "FLOW.BUDGET.COMMIT", "payments", "RESERVATION_ID", "reservation-1",
      "ACTUAL_AMOUNT", 8, "USAGE", { tokens: 8 }
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.BUDGET.COMMIT"],
      payload: {
        actual_amount: 8,
        reservation_id: "reservation-1",
        scope: "payments",
        usage: { tokens: 8 }
      }
    });
    expect(buildProtocolCommand([
      "FLOW.LIMIT.RELEASE", "payments", "SHARD_ID", 1, "AMOUNT", 2,
      "RESERVATION_IDS", 2, "lease:1", "lease:2"
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.LIMIT.RELEASE"],
      payload: {
        amount: 2,
        reservation_ids: ["lease:1", "lease:2"],
        scope: "payments",
        shard_id: 1
      }
    });
  });

  it("uses dedicated native client metadata opcodes", () => {
    const setName = buildProtocolCommand(["CLIENT", "SETNAME", "worker-a"]);
    const info = buildProtocolCommand(["CLIENT", "INFO"]);

    expect(setName).toMatchObject({ laneId: 0, opcode: OPCODES.clientSetName, payload: { name: "worker-a" } });
    expect(info).toMatchObject({ laneId: 0, opcode: OPCODES.clientInfo, payload: {} });
  });

  it("uses dedicated native topology opcodes", () => {
    expect(buildProtocolCommand(["SHARDS"])).toMatchObject({ laneId: 0, opcode: OPCODES.shards, payload: {} });
    expect(buildProtocolCommand(["ROUTE", "tenant-key"])).toMatchObject({
      laneId: 0,
      opcode: OPCODES.route,
      payload: { key: "tenant-key" }
    });
    expect(buildProtocolCommand(["ROUTE_BATCH", "tenant-a", "tenant-b"])).toMatchObject({
      laneId: 0,
      opcode: OPCODES.routeBatch,
      payload: { keys: ["tenant-a", "tenant-b"] }
    });
  });

  it("uses dedicated native authentication and options opcodes", () => {
    expect(buildProtocolCommand(["AUTH", "secret"])).toMatchObject({
      laneId: 0,
      opcode: OPCODES.auth,
      payload: { password: "secret", username: "default" }
    });
    expect(buildProtocolCommand(["AUTH", "svc", "secret"])).toMatchObject({
      laneId: 0,
      opcode: OPCODES.auth,
      payload: { password: "secret", username: "svc" }
    });
    expect(buildProtocolCommand(["OPTIONS"])).toMatchObject({ laneId: 0, opcode: OPCODES.options, payload: {} });
  });

});
