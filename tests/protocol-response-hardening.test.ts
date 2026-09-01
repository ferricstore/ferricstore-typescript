import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { protocolErrorMessage } from "../src/protocol-error-message.js";
import { durableMutationMayHaveCommitted } from "../src/client-durable-step.js";
import {
  COMPACT_KV_MGET_FIXED,
  COMPACT_OK_LIST,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  buildProtocolCommand,
  decodeResponse,
  encodeValue,
  tryPipelineCommand,
  unwrapPipelineResponse,
  type ResponseDecodeHints,
  type ResponseFrame
} from "../src/protocol.js";

const okHints: ResponseDecodeHints = {
  compactResponseOpcodes: new Map<string, ReadonlySet<number>>([
    ["ok_list_v1", new Set([OPCODES.set, OPCODES.flowCreateMany, OPCODES.pipeline])],
    ["kv_mget_v1", new Set([OPCODES.mget])],
    ["pipeline_v1", new Set([OPCODES.pipeline])]
  ])
};

describe("native response hardening", () => {
  it("does not run negotiated compact success decoders for error responses", () => {
    const compactLookingError = Buffer.concat([
      Buffer.from([COMPACT_OK_LIST]),
      u32(100_000)
    ]);

    const error = captureError(() => decodeResponse(
      response(OPCODES.set, 1, compactLookingError),
      OPCODES.set,
      okHints
    ));

    expect(error.message.length).toBeLessThan(256);
    expect(error.message).not.toContain("OK,OK");
  });

  it("requires one compact OK value for scalar SET and MSET responses", () => {
    for (const opcode of [OPCODES.set, OPCODES.mset]) {
      const hints: ResponseDecodeHints = {
        compactResponseOpcodes: new Map([["ok_list_v1", new Set([opcode])]])
      };
      expect(() => decodeResponse(
        response(
          opcode,
          0,
          Buffer.concat([Buffer.from([COMPACT_OK_LIST]), u32(2)]),
          FLAG_CUSTOM_PAYLOAD
        ),
        opcode,
        hints
      )).toThrow("expected 1 item");
    }
  });

  it("checks correlated compact collection counts before allocating results", () => {
    const manyHints = { ...okHints, compactResponseItems: 2 };
    expect(() => decodeResponse(
      response(
        OPCODES.flowCreateMany,
        0,
        Buffer.concat([Buffer.from([COMPACT_OK_LIST]), u32(100_000)]),
        FLAG_CUSTOM_PAYLOAD
      ),
      OPCODES.flowCreateMany,
      manyHints
    )).toThrow("expected 2 items");

    const fixedEmptyMget = Buffer.concat([
      Buffer.from([COMPACT_KV_MGET_FIXED]),
      u32(100_000),
      u32(0)
    ]);
    expect(() => decodeResponse(
      response(OPCODES.mget, 0, fixedEmptyMget, FLAG_CUSTOM_PAYLOAD),
      OPCODES.mget,
      manyHints
    )).toThrow("expected 2 items");
  });

  it("carries exact compact response counts from requests to response correlation", () => {
    expect(buildProtocolCommand(["MGET", "a", "b"])).toMatchObject({ compactResponseItems: 2 });
    expect(buildProtocolCommand(["MSET", "{p}:a", "1", "{p}:b", "2"])).toMatchObject({
      compactResponseItems: 1
    });
    expect(buildProtocolCommand([
      "FLOW.VALUE.MGET", "ref-a", "ref-b", "MAXBYTES", 1_024
    ])).toMatchObject({ compactResponseItems: 2 });
    expect(buildProtocolCommand([
      "FLOW.CREATE_MANY", "partition", "TYPE", "task", "STATE", "queued", "NOW", 1,
      "ITEMS", "a", "payload-a", "b", "payload-b"
    ])).toMatchObject({ compactResponseItems: 2 });
    expect(tryPipelineCommand([["GET", "a"], ["GET", "b"]])).toMatchObject({
      compactResponseItems: 2
    });
  });

  it("bounds error messages while retaining the raw server value", () => {
    const raw = Buffer.alloc(100_000, 0x78);
    const error = captureError(() => decodeResponse(
      response(OPCODES.get, 1, encodeValue(raw)),
      OPCODES.get
    ));

    expect(error.message.length).toBeLessThanOrEqual(4_120);
    expect(error.message.endsWith("…")).toBe(true);
    expect(Buffer.isBuffer(error.raw)).toBe(true);
    expect((error.raw as Buffer).byteLength).toBe(raw.byteLength);
    expect((error.raw as Buffer).subarray(0, 32)).toEqual(raw.subarray(0, 32));
  });

  it("decodes only a bounded prefix of binary error messages", () => {
    const raw: Buffer = Buffer.alloc(1_000_000, 0x78);
    const originalSubarray = raw.subarray.bind(raw);
    let selectedBytes = raw.byteLength;
    raw.subarray = (start?: number, end?: number): Buffer => {
      selectedBytes = (end ?? raw.byteLength) - (start ?? 0);
      return originalSubarray(start, end);
    };

    expect(protocolErrorMessage(1, raw)).toHaveLength(4_096);
    expect(selectedBytes).toBeLessThanOrEqual(16_384);
  });

  it("treats a future native response status as an uncertain outcome", () => {
    const error = captureError(() => decodeResponse(
      response(OPCODES.flowStepContinue, 65_000, encodeValue({
        message: "future status",
        retryable: true,
        safe_to_retry: true
      })),
      OPCODES.flowStepContinue
    ));

    expect(error).toMatchObject({ retryable: false, safeToRetry: false });
    expect(error.message).toMatch(/unknown native response status 65000/iu);
    expect(durableMutationMayHaveCommitted(error)).toBe(true);
  });

  it("does not return a future native pipeline item status as a successful value", () => {
    const error = captureError(() => unwrapPipelineResponse([
      ["ok", "confirmed"],
      ["future_status", { safe_to_retry: true }]
    ]));

    expect(error).toMatchObject({ retryable: false, safeToRetry: false });
    expect(error.message).toMatch(/unknown native pipeline status/iu);
    expect(durableMutationMayHaveCommitted(error)).toBe(true);
  });
});

function response(
  opcode: number,
  status: number,
  value: Buffer,
  flags = 0
): ResponseFrame {
  const body = Buffer.allocUnsafe(2 + value.byteLength);
  body.writeUInt16BE(status, 0);
  value.copy(body, 2);
  return {
    body,
    bodyLength: body.byteLength,
    flags,
    laneId: 1,
    opcode,
    requestId: 1n
  };
}

function u32(value: number): Buffer {
  const output = Buffer.allocUnsafe(4);
  output.writeUInt32BE(value);
  return output;
}

function captureError(action: () => unknown): Error & { readonly raw?: unknown } {
  try {
    action();
  } catch (error) {
    if (error instanceof Error) return error;
    throw error;
  }
  throw new Error("expected action to throw");
}
