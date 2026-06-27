import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import {
  OPCODES,
  buildProtocolCommand,
  decodeResponse,
  encodeRequest,
  encodeValue,
  pipelineCommand,
  unwrapPipelineResponse,
  type ResponseFrame
} from "../src/protocol.js";
import { CLAIMED_ITEM_WIRE, claimedItemFromResp } from "../src/types.js";

describe("native protocol codec", () => {
  it("encodes request frames with FerricStore native header", () => {
    const frame = encodeRequest({ laneId: 0, opcode: OPCODES.ping, payload: { message: "hello" } }, 7n);

    expect(frame.toString("ascii", 0, 4)).toBe("FSNP");
    expect(frame.readUInt8(4)).toBe(1);
    expect(frame.readUInt32BE(6)).toBe(0);
    expect(frame.readUInt16BE(10)).toBe(OPCODES.ping);
    expect(frame.readBigUInt64BE(12)).toBe(7n);
    expect(frame.readUInt32BE(20)).toBe(frame.byteLength - 24);
  });

  it("builds direct compact-capable KV commands", () => {
    const get = buildProtocolCommand(["GET", "user:1"]);
    const set = buildProtocolCommand(["SET", "user:1", Buffer.from("value"), "PX", 1000]);

    expect(get).toMatchObject({ opcode: OPCODES.get, payload: { key: "user:1" } });
    expect(set.opcode).toBe(OPCODES.set);
    expect(set.payload).toMatchObject({ key: "user:1", ttl: 1000 });
  });

  it("uses dedicated native client metadata opcodes", () => {
    const setName = buildProtocolCommand(["CLIENT", "SETNAME", "worker-a"]);

    expect(setName).toMatchObject({ laneId: 0, opcode: OPCODES.clientSetName, payload: { name: "worker-a" } });
  });

  it("decodes compact GET responses", () => {
    const value = Buffer.from("value");
    const body = Buffer.concat([Buffer.from([0, 0, 0x82, 1]), u32(value.byteLength), value]);
    const decoded = decodeResponse(responseFrame(OPCODES.get, body), OPCODES.get);

    expect(Buffer.isBuffer(decoded)).toBe(true);
    expect((decoded as Buffer).toString("utf8")).toBe("value");
  });

  it("builds compact homogeneous pipelines and unwraps response pairs", () => {
    const pipeline = pipelineCommand([
      ["GET", "a"],
      ["GET", "b"]
    ]);

    expect(pipeline.opcode).toBe(OPCODES.pipeline);
    expect(pipeline.flags).toBe(0x02);
    expect(unwrapPipelineResponse([["ok", Buffer.from("a")], ["ok", null]])).toEqual([Buffer.from("a"), null]);
  });

  it("decodes compact pipeline successes without status tuple allocation", () => {
    const value = Buffer.from("a");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x95]),
      u32(2),
      Buffer.from([0, 1]),
      binary(value),
      Buffer.from([0, 0])
    ]);
    const decoded = decodeResponse(responseFrame(OPCODES.pipeline, body), OPCODES.pipeline);

    expect(decoded).toEqual([value, null]);
    expect(unwrapPipelineResponse(decoded)).toBe(decoded);
    expect(unwrapPipelineResponse(decoded)).toEqual([value, null]);
  });

  it("builds direct native FLOW.CREATE_MANY for mixed partition batches", () => {
    const payload = Buffer.from("payload");
    const command = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "MIXED",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "INDEPENDENT",
      true,
      "ITEMS",
      "flow-1",
      "p1",
      payload,
      "flow-2",
      "p2",
      payload
    ]);

    expect(command.opcode).toBe(OPCODES.flowCreateMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x9e);
  });

  it("builds direct native FLOW.CLAIM_DUE with compact job return options", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      100,
      "PARTITIONS",
      2,
      "p1",
      "p2",
      "RETURN",
      "JOBS_COMPACT",
      "NOPAYLOAD"
    ]);

    expect(command.opcode).toBe(OPCODES.flowClaimDue);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x91);
  });

  it("keeps full-record FLOW.CLAIM_DUE on generic execution", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      1
    ]);

    expect(command.opcode).toBe(OPCODES.commandExec);
  });

  it("builds direct native FLOW.CREATE for simple queue creates", () => {
    const command = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "PARTITION",
      "p1",
      "RUN_AT",
      1000,
      "PRIORITY",
      1
    ]);

    expect(command.opcode).toBe(OPCODES.flowCreate);
    expect(command.payload).toMatchObject({
      id: "flow-1",
      type: "email",
      state: "queued",
      now_ms: 1000,
      partition_key: "p1",
      run_at_ms: 1000,
      priority: 1
    });
  });

  it("keeps payload-bearing FLOW.CREATE on generic execution", () => {
    const command = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "PAYLOAD",
      Buffer.from("payload")
    ]);

    expect(command.opcode).toBe(OPCODES.commandExec);
  });

  it("builds direct native FLOW.COMPLETE for simple claimed completions", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      lease,
      "FENCING",
      7,
      "NOW",
      2000,
      "PARTITION",
      "p1"
    ]);

    expect(command.opcode).toBe(OPCODES.flowComplete);
    expect(command.payload).toMatchObject({
      id: "flow-1",
      lease_token: lease,
      fencing_token: 7,
      now_ms: 2000,
      partition_key: "p1"
    });
  });

  it("keeps result-bearing FLOW.COMPLETE on generic execution", () => {
    const command = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      Buffer.from("lease-token"),
      "FENCING",
      7,
      "NOW",
      2000,
      "RESULT",
      Buffer.from("result")
    ]);

    expect(command.opcode).toBe(OPCODES.commandExec);
  });

  it("builds direct native FLOW.COMPLETE_MANY for claimed mixed batches", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "INDEPENDENT",
      true,
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x92);
  });

  it("keeps FLOW.COMPLETE_MANY OK-on-success on the direct native opcode", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x93);
  });

  it("keeps TTL-bearing FLOW.COMPLETE_MANY on generic native encoding", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "TTL",
      5000,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBeUndefined();
    expect(command.payload).toMatchObject({
      return: "OK_ON_SUCCESS",
      ttl_ms: 5000
    });
  });

  it("decodes compact claim jobs with attributes", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const attrs = { tenant: Buffer.from("acme") };
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x80]),
      u32(1),
      binary(id),
      binary(partition),
      binary(lease),
      i64(9n),
      encodeValue(attrs)
    ]);

    const decoded = decodeResponse(responseFrame(OPCODES.flowClaimDue, body), OPCODES.flowClaimDue);

    expect(decoded).toEqual([[id, partition, lease, 9, null, attrs]]);
  });

  it("preserves raw compact claimed item buffers for follow-up completion", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const item = claimedItemFromResp([id, partition, lease, 9]);

    expect(item.id).toBe("flow-1");
    expect(item.partitionKey).toBe("p1");
    expect(item[CLAIMED_ITEM_WIRE]).toMatchObject({
      id,
      partitionKey: partition,
      leaseToken: lease,
      fencingToken: 9
    });
  });

  it("round-trips typed protocol maps", () => {
    const encoded = encodeValue({ a: 1, b: [true, Buffer.from("x")] });
    const decoded = decodeResponse(responseFrame(OPCODES.commandExec, Buffer.concat([Buffer.from([0, 0]), encoded])), OPCODES.commandExec);

    expect(decoded).toEqual({ a: 1, b: [true, Buffer.from("x")] });
  });
});

function responseFrame(opcode: number, body: Buffer): ResponseFrame {
  return { body, bodyLength: body.byteLength, flags: 0, laneId: opcode < 0x0100 ? 0 : 1, opcode, requestId: 1n };
}

function u32(value: number): Buffer {
  const buffer = Buffer.allocUnsafe(4);
  buffer.writeUInt32BE(value, 0);
  return buffer;
}

function i64(value: bigint): Buffer {
  const buffer = Buffer.allocUnsafe(8);
  buffer.writeBigInt64BE(value, 0);
  return buffer;
}

function binary(value: Buffer): Buffer {
  return Buffer.concat([u32(value.byteLength), value]);
}
