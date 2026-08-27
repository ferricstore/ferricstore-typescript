import { Buffer } from "node:buffer";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  QUALITY_FIELDS,
  RECORD_FIELDS,
  USAGE_FIELDS
} from "../src/protocol-flow-query-result.js";
import {
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  decodeResponse,
  encodeValue,
  type ResponseDecodeHints,
  type ResponseFrame
} from "../src/protocol.js";

const hints: ResponseDecodeHints = {
  compactResponseOpcodes: new Map([
    ["flow_query_result_v1", new Set([OPCODES.commandExec, OPCODES.flowQuery])]
  ])
};

describe("compact FLOW.QUERY results", () => {
  it("decodes the shared server golden corpus without schema drift", () => {
    const corpus = JSON.parse(
      readFileSync(new URL("./fixtures/flow_query_result_v1.json", import.meta.url), "utf8")
    ) as {
      readonly tag: number;
      readonly record_fields: readonly string[];
      readonly quality_fields: readonly string[];
      readonly usage_fields: readonly string[];
      readonly vectors: readonly {
        readonly name: string;
        readonly payload_hex: string;
        readonly count_decimal?: string;
      }[];
    };

    expect(corpus.tag).toBe(0xa0);
    expect(corpus.record_fields).toEqual(RECORD_FIELDS);
    expect(corpus.quality_fields).toEqual(QUALITY_FIELDS);
    expect(corpus.usage_fields).toEqual(USAGE_FIELDS);

    const [pageVector, countVector] = corpus.vectors;
    if (pageVector == null || countVector?.count_decimal == null) {
      throw new TypeError("shared compact query result corpus is incomplete");
    }
    const page = decodeResponse(
      frame(OPCODES.flowQuery, Buffer.from(pageVector.payload_hex, "hex")),
      OPCODES.flowQuery,
      hints
    ) as Record<string, unknown>;
    const count = decodeResponse(
      frame(OPCODES.commandExec, Buffer.from(countVector.payload_hex, "hex")),
      OPCODES.commandExec,
      hints
    ) as Record<string, unknown>;

    expect(page.records).toEqual([{
      id: Buffer.from("run-1"),
      state: Buffer.from("failed"),
      fields: { invoice_total: 42 }
    }]);
    expect(count.result).toEqual({
      kind: Buffer.from("count"),
      value: BigInt(countVector.count_decimal)
    });
  });

  it("decodes a projected record page into the existing result contract", () => {
    const payload = pagePayload();
    const result = decodeResponse(frame(OPCODES.flowQuery, payload), OPCODES.flowQuery, hints) as Record<string, unknown>;

    expect(text(result.version)).toBe("ferric.flow.query.result/v1");
    expect(result.page).toEqual({ has_more: false, cursor: null });
    expect(result.quality).toEqual({
      exactness: Buffer.from("authoritative"),
      freshness: Buffer.from("current"),
      coverage: Buffer.from("complete"),
      pagination: Buffer.from("authenticated_seek")
    });
    expect(result.records).toEqual([{
      id: Buffer.from("run-1"),
      state: Buffer.from("failed"),
      fields: { invoice_total: 42 }
    }]);
    expect((result.usage as Record<string, unknown>).result_records).toBe(1);
    expect((result.usage as Record<string, unknown>).response_bytes).toBe(payload.byteLength);
  });

  it("decodes count results and preserves integers outside JavaScript's safe range", () => {
    const count = BigInt(Number.MAX_SAFE_INTEGER) + 7n;
    const result = decodeResponse(
      frame(OPCODES.commandExec, countPayload(count)),
      OPCODES.commandExec,
      hints
    ) as Record<string, unknown>;

    expect(result.result).toEqual({ kind: Buffer.from("count"), value: count });
  });

  it("does not expose shared mutable contract buffers across responses", () => {
    const first = decodeResponse(
      frame(OPCODES.flowQuery, countPayload(1n)),
      OPCODES.flowQuery,
      hints
    ) as Record<string, unknown>;
    const second = decodeResponse(
      frame(OPCODES.flowQuery, countPayload(2n)),
      OPCODES.flowQuery,
      hints
    ) as Record<string, unknown>;
    const firstVersion = first.version as Buffer;
    const secondVersion = second.version as Buffer;

    expect(firstVersion).not.toBe(secondVersion);
    firstVersion[0] = 0x58;
    expect(secondVersion.toString("utf8")).toBe("ferric.flow.query.result/v1");
  });

  it("does not let retained small record values pin the complete compact page", () => {
    const response = frame(OPCODES.flowQuery, pagePayload());
    const result = decodeResponse(response, OPCODES.flowQuery, hints) as Record<string, unknown>;
    const record = (result.records as Record<string, unknown>[])[0];
    const id = record?.id as Buffer;

    expect(id).toEqual(Buffer.from("run-1"));
    expect(id.buffer).not.toBe(response.body.buffer);
    expect(id.buffer.byteLength).toBe(id.byteLength);
  });

  it("rejects reserved record bits, truncated payloads, and trailing bytes", () => {
    const valid = pagePayload();
    const reserved = Buffer.from(valid);
    reserved.writeUInt32BE(reserved.readUInt32BE(103) | (1 << 20), 103);

    const usageOutOfRange = Buffer.from(valid);
    usageOutOfRange.writeBigUInt64BE(1n << 63n, 6);
    const hydratedBeyondScan = Buffer.from(valid);
    hydratedBeyondScan.writeBigUInt64BE(2n, 38);
    const wrongRecords = Buffer.from(valid);
    wrongRecords.writeBigUInt64BE(2n, 62);
    const countUsageMismatch = countPayload(42n);
    countUsageMismatch.writeBigUInt64BE(0n, 62);

    for (const payload of [
      reserved,
      valid.subarray(0, valid.byteLength - 1),
      Buffer.concat([valid, Buffer.from([0])]),
      cursorPagePayload("fqc1_short"),
      cursorPagePayload("other_cursor_token"),
      cursorPagePayload(Buffer.concat([Buffer.from("fqc1_"), Buffer.alloc(11, 0xff)])),
      usageOutOfRange,
      hydratedBeyondScan,
      wrongRecords,
      countUsageMismatch,
      countPayload(1n << 63n),
    ]) {
      expect(() => decodeResponse(frame(OPCODES.flowQuery, payload), OPCODES.flowQuery, hints)).toThrow();
    }
  });

  it("does not interpret the custom tag unless the server advertised it for the opcode", () => {
    expect(() => decodeResponse(frame(OPCODES.flowQuery, pagePayload()), OPCODES.flowQuery)).toThrow(
      "unsupported or malformed custom protocol response"
    );
  });

  it("rejects a compact result when the custom-payload frame flag is missing", () => {
    expect(() =>
      decodeResponse(frame(OPCODES.flowQuery, pagePayload(), 0), OPCODES.flowQuery, hints)
    ).toThrow("unknown protocol value tag 160");
  });

  it("rejects an ordinary typed value when the custom-payload frame flag is set", () => {
    expect(() =>
      decodeResponse(
        frame(OPCODES.flowQuery, encodeValue({ records: [] })),
        OPCODES.flowQuery,
        hints
      )
    ).toThrow("unsupported or malformed custom protocol response");
  });
});

function pagePayload(): Buffer {
  const values = [encodeValue("run-1"), encodeValue("failed"), encodeValue({ invoice_total: 42 })];
  const payload = Buffer.concat([
    Buffer.from([0xa0, 0, 0, 0, 0, 2]),
    usage(1),
    Buffer.from([0, 0xff, 0xff, 0xff, 0xff]),
    u32(1),
    u32((1 << 0) | (1 << 2) | (1 << 19)),
    ...values
  ]);
  payload.writeBigUInt64BE(BigInt(payload.byteLength), 70);
  return payload;
}

function countPayload(count: bigint): Buffer {
  const payload = Buffer.concat([
    Buffer.from([0xa0, 1, 2, 1, 0, 0]),
    usage(1),
    u64(count)
  ]);
  payload.writeBigUInt64BE(BigInt(payload.byteLength), 70);
  return payload;
}

function cursorPagePayload(cursor: string | Buffer): Buffer {
  const rawCursor = Buffer.isBuffer(cursor) ? cursor : Buffer.from(cursor);
  const payload = Buffer.concat([
    Buffer.from([0xa0, 0, 0, 0, 0, 2]),
    usage(0),
    Buffer.from([1]),
    u32(rawCursor.byteLength),
    rawCursor,
    u32(0),
  ]);
  payload.writeBigUInt64BE(BigInt(payload.byteLength), 70);
  return payload;
}

function usage(resultRecords: number): Buffer {
  const values = new Array<bigint>(11).fill(0n);
  values[2] = BigInt(resultRecords);
  values[4] = BigInt(resultRecords);
  values[7] = BigInt(resultRecords);
  return Buffer.concat(values.map(u64));
}

function u32(value: number): Buffer {
  const output = Buffer.allocUnsafe(4);
  output.writeUInt32BE(value);
  return output;
}

function u64(value: bigint): Buffer {
  const output = Buffer.allocUnsafe(8);
  output.writeBigUInt64BE(value);
  return output;
}

function frame(
  opcode: number,
  payload: Buffer,
  flags: number = FLAG_CUSTOM_PAYLOAD
): ResponseFrame {
  const body = Buffer.concat([Buffer.from([0, 0]), payload]);
  return {
    flags,
    laneId: 1,
    opcode,
    requestId: 1n,
    bodyLength: body.byteLength,
    body
  };
}

function text(value: unknown): string {
  return Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
}
