import type { AddressInfo } from "node:net";
import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import {
  COMPACT_BINARY_LIST_LIST,
  COMPACT_BINARY_MAP_LIST,
  COMPACT_FLOW_CLAIM_JOBS,
  COMPACT_FLOW_RECORD_LIST,
  COMPACT_INTEGER_LIST,
  COMPACT_KV_GET,
  COMPACT_KV_MGET,
  COMPACT_KV_MGET_FIXED,
  COMPACT_OK_LIST,
  FLAG_MORE_CHUNKS,
  OPCODES,
  type ResponseFrame,
  decodeResponse,
  encodeValue,
  unwrapPipelineResponse
} from "../src/protocol.js";
import {
  NO_RESPONSE,
  responseFrameFromBody,
  startCountingServer
} from "./adapter-test-support.js";

function response(opcode: number, body: Buffer): ResponseFrame {
  return { body, bodyLength: body.byteLength, flags: 0, laneId: 1, opcode, requestId: 1n };
}

function compactGet(value: Buffer): Buffer {
  const size = Buffer.allocUnsafe(4);
  size.writeUInt32BE(value.byteLength);
  return Buffer.concat([Buffer.from([0, 0, COMPACT_KV_GET, 1]), size, value]);
}

function uint32(value: number): Buffer {
  const encoded = Buffer.allocUnsafe(4);
  encoded.writeUInt32BE(value);
  return encoded;
}

function binary(value: string): Buffer {
  const encoded = Buffer.from(value);
  return Buffer.concat([uint32(encoded.byteLength), encoded]);
}

const pipelineHints = {
  compactResponseOpcodes: new Map([["pipeline_v1", new Set([OPCODES.pipeline])]])
};

describe("FerricStore 0.8 HELLO negotiation", () => {
  it("decodes compact responses only when HELLO advertises that codec/opcode", () => {
    const body = compactGet(Buffer.from("value"));
    expect(decodeResponse(response(OPCODES.get, body), OPCODES.get, {
      compactResponseOpcodes: new Map([["kv_get_v1", new Set([OPCODES.get])]])
    })).toEqual(Buffer.from("value"));
    expect(() => decodeResponse(response(OPCODES.get, body), OPCODES.get, {
      compactResponseOpcodes: new Map()
    })).toThrow();
  });

  it("uses pipeline_v1 to decode the server's fixed-width MGET pipeline shortcut", () => {
    const payload = Buffer.concat([
      Buffer.from([COMPACT_KV_MGET_FIXED]),
      uint32(2),
      uint32(2),
      Buffer.from("v1v2")
    ]);

    expect(decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), payload
    ])), OPCODES.pipeline, pipelineHints)).toEqual([Buffer.from("v1"), Buffer.from("v2")]);
    expect(() => decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), payload
    ])), OPCODES.pipeline, { compactResponseOpcodes: new Map() })).toThrow();
  });

  it("decodes every values-only pipeline shortcut advertised by pipeline_v1", () => {
    const ok = Buffer.concat([Buffer.from([COMPACT_OK_LIST]), uint32(2)]);
    expect(unwrapPipelineResponse(decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), ok
    ])), OPCODES.pipeline, pipelineHints))).toEqual([Buffer.from("OK"), Buffer.from("OK")]);

    const mget = Buffer.concat([
      Buffer.from([COMPACT_KV_MGET]),
      uint32(2),
      Buffer.from([1]),
      binary("value"),
      Buffer.from([0])
    ]);
    expect(unwrapPipelineResponse(decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), mget
    ])), OPCODES.pipeline, pipelineHints))).toEqual([Buffer.from("value"), null]);

    const integers = Buffer.allocUnsafe(5 + 3 * 8);
    integers.writeUInt8(COMPACT_INTEGER_LIST, 0);
    integers.writeUInt32BE(3, 1);
    integers.writeBigInt64BE(42n, 5);
    integers.writeBigInt64BE(9_007_199_254_740_992n, 13);
    integers.writeBigInt64BE(-9_007_199_254_740_992n, 21);
    expect(decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), integers
    ])), OPCODES.pipeline, pipelineHints)).toEqual([
      42,
      9_007_199_254_740_992n,
      -9_007_199_254_740_992n
    ]);

    const lists = Buffer.concat([
      Buffer.from([COMPACT_BINARY_LIST_LIST]),
      uint32(2),
      uint32(2),
      binary("ok"),
      binary("user-value"),
      uint32(0)
    ]);
    const decodedLists = decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), lists
    ])), OPCODES.pipeline, pipelineHints);
    expect(unwrapPipelineResponse(decodedLists)).toEqual([
      [Buffer.from("ok"), Buffer.from("user-value")],
      []
    ]);

    const maps = Buffer.concat([
      Buffer.from([COMPACT_BINARY_MAP_LIST]),
      uint32(2),
      uint32(1),
      binary("field"),
      binary("value"),
      uint32(0)
    ]);
    expect(decodeResponse(response(OPCODES.pipeline, Buffer.concat([
      Buffer.from([0, 0]), maps
    ])), OPCODES.pipeline, pipelineHints)).toEqual([
      { field: Buffer.from("value") },
      {}
    ]);

    for (const tag of [COMPACT_FLOW_CLAIM_JOBS, COMPACT_FLOW_RECORD_LIST]) {
      const empty = Buffer.concat([Buffer.from([tag]), uint32(0)]);
      expect(unwrapPipelineResponse(decodeResponse(response(
        OPCODES.pipeline,
        Buffer.concat([Buffer.from([0, 0]), empty])
      ), OPCODES.pipeline, pipelineHints))).toEqual([]);
    }
  });

  it("uses negotiated compact response opcodes on a live adapter", async () => {
    const server = await startCountingServer((request, socket) => {
      if (request.opcode === OPCODES.startup) {
        return {
          capabilities: {
            limits: { max_response_bytes: 1_024 },
            response_codecs: { compact_response_opcodes: { kv_get_v1: [OPCODES.get] } }
          }
        };
      }
      if (request.opcode === OPCODES.get) {
        socket.write(responseFrameFromBody(
          request.opcode,
          request.laneId,
          request.requestId,
          compactGet(Buffer.from("value"))
        ));
        return NO_RESPONSE;
      }
      return undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;
    const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

    try {
      await expect(adapter.executeCommand("GET", "key")).resolves.toEqual(Buffer.from("value"));
    } finally {
      await adapter.close();
    }
  });

  it("rejects an unadvertised compact response instead of consulting a static table", async () => {
    const server = await startCountingServer((request, socket) => {
      if (request.opcode === OPCODES.startup) {
        return { capabilities: { response_codecs: { compact_response_opcodes: {} } } };
      }
      if (request.opcode === OPCODES.get) {
        socket.write(responseFrameFromBody(
          request.opcode,
          request.laneId,
          request.requestId,
          compactGet(Buffer.from("value"))
        ));
        return NO_RESPONSE;
      }
      return undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;
    const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

    try {
      await expect(adapter.executeCommand("GET", "key")).rejects.toThrow();
    } finally {
      await adapter.close();
    }
  });

  it("enforces negotiated aggregate response bytes across chunks", async () => {
    const encoded = encodeValue(Buffer.alloc(64, 1));
    const body = Buffer.concat([Buffer.from([0, 0]), encoded]);
    const server = await startCountingServer((request, socket) => {
      if (request.opcode === OPCODES.startup) {
        return { capabilities: { limits: { max_response_bytes: 32 } } };
      }
      if (request.opcode === OPCODES.get) {
        const middle = Math.floor(body.byteLength / 2);
        socket.write(responseFrameFromBody(
          request.opcode, request.laneId, request.requestId, body.subarray(0, middle), FLAG_MORE_CHUNKS
        ));
        socket.write(responseFrameFromBody(
          request.opcode, request.laneId, request.requestId, body.subarray(middle)
        ));
        return NO_RESPONSE;
      }
      return undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;
    const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

    try {
      await expect(adapter.executeCommand("GET", "key")).rejects.toThrow(/exceeded 32 bytes/u);
    } finally {
      await adapter.close();
    }
  });

  it("reassembles compact MGET and FLOW.VALUE.MGET chunks before decoding", async () => {
    const size = Buffer.allocUnsafe(4);
    size.writeUInt32BE(1);
    const itemSize = Buffer.allocUnsafe(4);
    itemSize.writeUInt32BE(5);
    const body = Buffer.concat([
      Buffer.from([0, 0, COMPACT_KV_MGET]), size, Buffer.from([1]), itemSize, Buffer.from("value")
    ]);
    const server = await startCountingServer((request, socket) => {
      if (request.opcode === OPCODES.startup) {
        return {
          capabilities: {
            response_codecs: {
              compact_response_opcodes: {
                kv_mget_v1: [OPCODES.mget, OPCODES.flowValueMGet]
              }
            }
          }
        };
      }
      if (request.opcode === OPCODES.mget || request.opcode === OPCODES.flowValueMGet) {
        const middle = 7;
        socket.write(responseFrameFromBody(
          request.opcode, request.laneId, request.requestId, body.subarray(0, middle), FLAG_MORE_CHUNKS
        ));
        socket.write(responseFrameFromBody(
          request.opcode, request.laneId, request.requestId, body.subarray(middle)
        ));
        return NO_RESPONSE;
      }
      return undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;
    const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

    try {
      await expect(adapter.executeCommand("MGET", "key")).resolves.toEqual([Buffer.from("value")]);
      await expect(adapter.executeCommand("FLOW.VALUE.MGET", "ref")).resolves.toEqual([
        Buffer.from("value")
      ]);
    } finally {
      await adapter.close();
    }
  });

  it("keeps interleaved chunk assembly isolated by lane, opcode, and request id", () => {
    const assembler = new NativeChunkAssembler(1_024, 16, 1_024);
    const frame = (laneId: number, opcode: number, body: string, flags: number) => ({
      body: Buffer.from(body),
      bodyLength: Buffer.byteLength(body),
      flags,
      laneId,
      opcode,
      requestId: 9n
    });

    expect(assembler.assemble(frame(1, OPCODES.mget, "mget-", FLAG_MORE_CHUNKS))).toBeUndefined();
    expect(assembler.assemble(
      frame(2, OPCODES.flowValueMGet, "flow-", FLAG_MORE_CHUNKS)
    )).toBeUndefined();
    expect(assembler.assemble(frame(1, OPCODES.mget, "done", 0))?.body.toString()).toBe("mget-done");
    expect(assembler.assemble(
      frame(2, OPCODES.flowValueMGet, "done", 0)
    )?.body.toString()).toBe("flow-done");
  });

  it("never submits an unauthenticated request above 64 KiB", async () => {
    let authRequests = 0;
    const server = await startCountingServer((request) => {
      if (request.opcode === OPCODES.startup) {
        return {
          auth_required: true,
          capabilities: { limits: { max_frame_bytes: 16 * 1024 * 1024 } }
        };
      }
      if (request.opcode === OPCODES.auth) authRequests += 1;
      return undefined;
    }, { fragmentResponses: false });
    const address = server.address() as AddressInfo;

    await expect(NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
      password: "x".repeat(70 * 1024)
    }).then(async (adapter) => {
      await adapter.close();
      throw new Error("oversized AUTH was submitted");
    })).rejects.toThrow(/65536-byte/u);
    expect(authRequests).toBe(0);
  });
});
