import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";

import {
  COMPACT_PIPELINE_RESPONSE,
  DEFAULT_MAX_VALUE_ITEMS,
  COMPACT_KV_GET,
  FLAG_CUSTOM_PAYLOAD,
  NULL_U32,
  OPCODES,
  decodeResponse,
  type ResponseFrame
} from "../src/protocol.js";
import { compactResponseHints } from "./compact-response-test-support.js";

function responseFrame(opcode: number, payload: Buffer): ResponseFrame {
  const body = Buffer.concat([Buffer.from([0, 0]), payload]);
  return {
    body,
    bodyLength: body.byteLength,
    flags: FLAG_CUSTOM_PAYLOAD,
    laneId: 1,
    opcode,
    requestId: 1n
  };
}

describe("compact response validation", () => {
  it("rejects trailing bytes after a missing compact GET marker", () => {
    const payload = Buffer.from([COMPACT_KV_GET, 0, 0xff]);

    expect(() => decodeResponse(
      responseFrame(OPCODES.get, payload), OPCODES.get, compactResponseHints
    )).toThrow(
      "trailing compact GET bytes"
    );
  });

  it("charges compact Flow value-reference properties against the shared item budget", () => {
    const header = Buffer.allocUnsafe(7);
    header.writeUInt8(COMPACT_PIPELINE_RESPONSE, 0);
    header.writeUInt32BE(DEFAULT_MAX_VALUE_ITEMS, 1);
    header.writeUInt8(0, 5);
    header.writeUInt8(5, 6);
    const valueRef = Buffer.allocUnsafe(12);
    valueRef.writeUInt32BE(0, 0);
    valueRef.writeUInt32BE(NULL_U32, 4);
    valueRef.writeUInt32BE(NULL_U32, 8);

    expect(() =>
      decodeResponse(
        responseFrame(OPCODES.pipeline, Buffer.concat([
          header,
          valueRef,
          Buffer.alloc((DEFAULT_MAX_VALUE_ITEMS - 1) * 2)
        ])),
        OPCODES.pipeline,
        compactResponseHints
      )
    ).toThrow("total items exceed max items");
  });
});
