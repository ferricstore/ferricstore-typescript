import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { readCompactBinaryMap } from "../src/protocol-compact-collections.js";
import {
  COMPACT_FLOW_RECORD,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  decodeResponse,
  decodeValue,
  encodeValue,
  type ResponseFrame
} from "../src/protocol.js";

describe("native map decoding", () => {
  it("rejects duplicate generic map keys instead of overwriting values", () => {
    const payload = genericMap([
      [Buffer.from("field"), encodeValue(1)],
      [Buffer.from("field"), encodeValue(2)]
    ]);

    expect(() => decodeValue(payload)).toThrow("duplicate map key");
  });

  it("rejects malformed UTF-8 generic map keys instead of merging replacement keys", () => {
    const payload = genericMap([[Buffer.from([0xff]), encodeValue(1)]]);

    expect(() => decodeValue(payload)).toThrow("valid UTF-8");
  });

  it("rejects duplicate and malformed compact binary-map keys", () => {
    const budget = (): { remainingItems: number } => ({ remainingItems: 100 });
    expect(() => readCompactBinaryMap(compactMap([
      [Buffer.from("field"), Buffer.from("a")],
      [Buffer.from("field"), Buffer.from("b")]
    ]), 0, budget())).toThrow("duplicate map key");
    expect(() => readCompactBinaryMap(compactMap([
      [Buffer.from([0xff]), Buffer.from("a")]
    ]), 0, budget())).toThrow("valid UTF-8");
  });

  it("rejects duplicate and malformed compact Flow-record extension keys", () => {
    expect(() => decodeCompactFlowRecord(Buffer.concat([
      Buffer.from([COMPACT_FLOW_RECORD]),
      u32(2),
      Buffer.from([0]), binary(Buffer.from("id")), encodeValue("extension"),
      Buffer.from([1]), encodeValue("canonical")
    ]))).toThrow("duplicate map key");
    expect(() => decodeCompactFlowRecord(Buffer.concat([
      Buffer.from([COMPACT_FLOW_RECORD]),
      u32(1),
      Buffer.from([0]), binary(Buffer.from([0xff])), encodeValue("value")
    ]))).toThrow("valid UTF-8");
  });

  it("keeps prototype-looking keys as safe own data properties", () => {
    const decoded = decodeValue(genericMap([
      [Buffer.from("__proto__"), encodeValue("safe")],
      [Buffer.from("constructor"), encodeValue("also-safe")]
    ])).value as Record<string, unknown>;

    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    expect(Object.hasOwn(decoded, "__proto__")).toBe(true);
    expect(decoded.__proto__).toEqual(Buffer.from("safe"));
    expect(decoded.constructor).toEqual(Buffer.from("also-safe"));
  });
});

function genericMap(entries: readonly (readonly [Buffer, Buffer])[]): Buffer {
  return Buffer.concat([
    Buffer.from([6]),
    u32(entries.length),
    ...entries.flatMap(([key, value]) => [binary(key), value])
  ]);
}

function compactMap(entries: readonly (readonly [Buffer, Buffer])[]): Buffer {
  return Buffer.concat([
    u32(entries.length),
    ...entries.flatMap(([key, value]) => [binary(key), binary(value)])
  ]);
}

function binary(value: Buffer): Buffer {
  return Buffer.concat([u32(value.byteLength), value]);
}

function u32(value: number): Buffer {
  const output = Buffer.allocUnsafe(4);
  output.writeUInt32BE(value);
  return output;
}

function decodeCompactFlowRecord(value: Buffer): unknown {
  const body = Buffer.concat([Buffer.from([0, 0]), value]);
  const frame: ResponseFrame = {
    body,
    bodyLength: body.byteLength,
    flags: FLAG_CUSTOM_PAYLOAD,
    laneId: 1,
    opcode: OPCODES.flowGet,
    requestId: 1n
  };
  return decodeResponse(frame, OPCODES.flowGet, {
    compactResponseOpcodes: new Map([["flow_record_v1", new Set([OPCODES.flowGet])]])
  });
}
