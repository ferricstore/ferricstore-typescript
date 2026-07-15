import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";

import { NativeReceiveBuffer } from "../src/native-receive-buffer.js";
import { DEFAULT_MAX_FRAME_BYTES, OPCODES } from "../src/protocol.js";
import { responseFrameFromBody } from "./adapter-test-support.js";

describe("NativeReceiveBuffer", () => {
  it("bounds retained fragment metadata while an incomplete frame arrives", () => {
    const body = Buffer.alloc(16 * 1024, 0x61);
    const encoded = responseFrameFromBody(OPCODES.ping, 0, 1n, body);
    const receive = new NativeReceiveBuffer();
    let decoded;
    let maxRetainedFragments = 0;

    for (let index = 0; index < encoded.byteLength; index += 1) {
      receive.append(encoded.subarray(index, index + 1));
      decoded ??= receive.nextFrame(DEFAULT_MAX_FRAME_BYTES);
      maxRetainedFragments = Math.max(maxRetainedFragments, retainedFragments(receive));
    }

    expect(decoded?.body).toEqual(body);
    expect(maxRetainedFragments).toBeLessThanOrEqual(32);
  });

  it("keeps complete single-buffer frames on the zero-copy path", () => {
    const body = Buffer.from("PONG");
    const encoded = responseFrameFromBody(OPCODES.ping, 0, 1n, body);
    const receive = new NativeReceiveBuffer();

    receive.append(encoded);
    const decoded = receive.nextFrame(DEFAULT_MAX_FRAME_BYTES);

    expect(decoded?.body).toEqual(body);
    expect(decoded?.body.buffer).toBe(encoded.buffer);
  });
});

function retainedFragments(receive: NativeReceiveBuffer): number {
  const internals = receive as unknown as {
    readonly assembly?: { readonly pages: readonly Buffer[] };
    readonly chunkIndex: number;
    readonly chunks: readonly Buffer[];
  };
  return internals.chunks.length - internals.chunkIndex + (internals.assembly?.pages.length ?? 0);
}
