import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { nativeNegotiation } from "../src/native-negotiation.js";
import { v010Startup } from "./adapter-test-support.js";

describe("HELLO compact response codec bounds", () => {
  it("accepts unknown bounded future codecs for forward compatibility", () => {
    const startup = withCodecTable({ future_codec_v2: [0x7fff] });

    expect(nativeNegotiation(startup).compactResponseOpcodes.get("future_codec_v2"))
      .toEqual(new Set([0x7fff]));
  });

  it("rejects oversized codec tables and total opcode collections", () => {
    expect(() => nativeNegotiation(withCodecTable(Object.fromEntries(
      Array.from({ length: 33 }, (_, index) => [`codec_${index}`, [index]])
    )))).toThrow("at most 32 codecs");
    expect(() => nativeNegotiation(withCodecTable({
      future_codec_v2: Array.from({ length: 1_025 }, (_, index) => index)
    }))).toThrow("at most 1024 opcodes");
  });

  it("rejects invalid names, sparse arrays, duplicate opcodes, and non-uint16 values", () => {
    const sparse = new Array<number>(2);
    sparse[1] = 1;
    for (const table of [
      { ["x".repeat(129)]: [1] },
      new Map([[Buffer.from([0xff]), [1]]]),
      { codec: sparse },
      { codec: [1, 1] },
      { codec: [0x1_0000] },
      { codec: ["1"] }
    ]) {
      expect(() => nativeNegotiation(withCodecTable(table))).toThrow(
        "incompatible FerricStore server"
      );
    }
  });

  it("rejects duplicate normalized codec names", () => {
    expect(() => nativeNegotiation(withCodecTable(new Map<unknown, unknown>([
      ["codec", [1]],
      [Buffer.from("codec"), [2]]
    ])))).toThrow("duplicate codec");
  });
});

function withCodecTable(table: unknown): Record<string, unknown> {
  const startup = v010Startup();
  const capabilities = startup.capabilities as Record<string, unknown>;
  capabilities.response_codecs = { compact_response_opcodes: table };
  return startup;
}
