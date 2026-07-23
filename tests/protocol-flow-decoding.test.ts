import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import {
  OPCODES,
  buildProtocolCommand,
  decodeResponse,
  decodeValue,
  encodeValue,
  type ResponseFrame
} from "../src/protocol.js";
import { CLAIMED_ITEM_WIRE, claimedItemFromResp } from "../src/types.js";
import { compactResponseHints } from "./compact-response-test-support.js";

describe("native Flow protocol decoding limits", () => {
  it("rejects compact Flow response kinds on unrelated opcodes", () => {
    const compactBodies = [
      Buffer.concat([Buffer.from([0, 0, 0x80]), u32(0)]),
      Buffer.concat([Buffer.from([0, 0, 0x84]), u32(0)]),
      Buffer.concat([Buffer.from([0, 0, 0x85]), u32(0)])
    ];

    for (const body of compactBodies) {
      expect(() => decodeResponse(
        responseFrame(OPCODES.commandExec, body),
        OPCODES.commandExec
      )).toThrow("unknown protocol value tag");
    }

    const recordBody = Buffer.concat([Buffer.from([0, 0, 0x84]), u32(0)]);
    const recordListBody = Buffer.concat([Buffer.from([0, 0, 0x85]), u32(0)]);
    expect(decodeResponse(
      responseFrame(OPCODES.flowGet, recordBody), OPCODES.flowGet, compactResponseHints
    )).toEqual({});
    expect(decodeResponse(
      responseFrame(OPCODES.pipeline, recordListBody),
      OPCODES.pipeline,
      compactResponseHints
    )).toEqual([]);
  });

  it("preserves compact claim fencing tokens outside the JavaScript safe range", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const unsafe = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x80]),
      u32(1),
      binary(id),
      binary(partition),
      binary(lease),
      i64(unsafe)
    ]);

    expect(decodeResponse(
      responseFrame(OPCODES.flowClaimDue, body), OPCODES.flowClaimDue, compactResponseHints
    )).toEqual([
      [id, partition, lease, unsafe]
    ]);
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

  it("preserves signed 64-bit integers outside the JavaScript safe range", () => {
    const positive = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const negative = BigInt(Number.MIN_SAFE_INTEGER) - 1n;

    expect(decodeValue(encodeValue(positive)).value).toBe(positive);
    expect(decodeValue(encodeValue(negative)).value).toBe(negative);
    expect(decodeValue(encodeValue(Number.MAX_SAFE_INTEGER)).value).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("preserves the full unsigned 64-bit native integer domain", () => {
    const maximum = 18_446_744_073_709_551_615n;
    const encoded = encodeValue(maximum);

    expect(encoded.readUInt8(0)).toBe(8);
    expect(decodeValue(encoded).value).toBe(maximum);
  });

  it("rejects unsafe integer numbers and accepts bigint instead", () => {
    expect(() => encodeValue(Number.MAX_SAFE_INTEGER + 1)).toThrow("unsafe integer number");
    expect(() => encodeValue(BigInt(Number.MAX_SAFE_INTEGER) + 1n)).not.toThrow();
    expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "order",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      "LIMIT",
      1,
      "RETURN",
      "JOBS_COMPACT"
    ])).toThrow("numeric command argument exceeds the JavaScript safe range");
  });

  it("decodes protocol maps without prototype mutation", () => {
    const source = Object.create(null) as Record<string, unknown>;
    source.__proto__ = { inheritedCapability: "attacker-controlled" };

    const decoded = decodeValue(encodeValue(source)).value as Record<string, unknown>;

    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    expect(Object.hasOwn(decoded, "__proto__")).toBe(true);
    expect(decoded.inheritedCapability).toBeUndefined();
  });

  it("rejects oversized and over-nested protocol value containers", () => {
    const oversized = Buffer.concat([Buffer.from([5]), u32(100_001)]);
    const nested = Buffer.concat([
      ...Array.from({ length: 65 }, () => Buffer.concat([Buffer.from([5]), u32(1)])),
      Buffer.from([0])
    ]);

    expect(() => decodeValue(oversized)).toThrow("exceeds max items");
    expect(() => decodeValue(nested)).toThrow("nesting exceeds max depth");
  });

  it("bounds outbound protocol containers and rejects cycles before recursion overflow", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    let nested: unknown = null;
    for (let depth = 0; depth < 65; depth += 1) nested = [nested];
    const shared = { value: 1 };

    expect(() => encodeValue(circular)).toThrow("circular reference");
    expect(() => encodeValue(nested)).toThrow("nesting exceeds max depth");
    expect(() => encodeValue(Array.from({ length: 100_001 }, () => null))).toThrow(
      "container exceeds max items"
    );
    expect(() => encodeValue([
      Array.from({ length: 50_000 }, () => null),
      Array.from({ length: 50_000 }, () => null)
    ])).toThrow("total items exceed max items");
    expect(decodeValue(encodeValue([shared, shared])).value).toEqual([shared, shared]);
  });

  it("applies the protocol item limit cumulatively across nested containers", () => {
    const nested = encodeValue([[null, null], [null, null]]);

    expect(() => decodeValue(nested, 0, { maxItems: 4 })).toThrow(
      "total items exceed max items"
    );
  });

  it("shares the cumulative item budget across compact record fields", () => {
    const values = encodeValue(Array.from({ length: 50_000 }, () => null));
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x84]),
      u32(2),
      Buffer.from([1]),
      values,
      Buffer.from([2]),
      values
    ]);

    expect(() => decodeResponse(
      responseFrame(OPCODES.flowGet, body), OPCODES.flowGet, compactResponseHints
    )).toThrow(
      "total items exceed max items"
    );
  });

  it("rejects compact response counts above the decoder item limit", () => {
    const body = Buffer.concat([Buffer.from([0, 0, 0x81]), u32(100_001)]);

    expect(() => decodeResponse(
      responseFrame(OPCODES.pipeline, body), OPCODES.pipeline, compactResponseHints
    )).toThrow(
      "exceeds max items"
    );
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
