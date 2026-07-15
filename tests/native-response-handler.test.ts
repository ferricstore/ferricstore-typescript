import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import { NativeChunkAssembler } from "../src/native-chunk-assembler.js";
import { NativeHeartbeat } from "../src/native-heartbeat.js";
import type { PendingRequest } from "../src/native-pending-request.js";
import { NativeResponseHandler } from "../src/native-response-handler.js";
import { DEFAULT_MAX_FRAME_BYTES, OPCODES } from "../src/protocol.js";
import { responseFrame } from "./adapter-test-support.js";

describe("NativeResponseHandler", () => {
  it("yields after a bounded response-frame batch and resumes buffered work", async () => {
    const pending = new Map<bigint, PendingRequest>();
    const frames: Buffer[] = [];
    let resolved = 0;
    for (let index = 1; index <= 129; index += 1) {
      const requestId = BigInt(index);
      pending.set(requestId, {
        hasFlowControlCredit: true,
        indefinite: false,
        laneId: 1,
        opcode: OPCODES.ping,
        reject: () => undefined,
        resolve: () => { resolved += 1; }
      });
      frames.push(responseFrame(OPCODES.ping, 1, requestId, "PONG"));
    }
    const pause = vi.fn();
    const resume = vi.fn();
    const handler = new NativeResponseHandler({
      applyFlowControlLimits: () => undefined,
      beginDraining: () => undefined,
      chunkAssembler: new NativeChunkAssembler(1_000_000, 1_000, 1_000_000),
      destroy: () => undefined,
      failAll: () => undefined,
      heartbeat: new NativeHeartbeat(undefined, async () => undefined, () => undefined),
      maxChunkBytes: 1_000_000,
      maxChunkFrames: 1_000,
      maxFrameBytes: DEFAULT_MAX_FRAME_BYTES,
      maxResponseBytes: 1_000_000,
      pause,
      pending,
      resume,
      takePending: (requestId) => {
        const request = pending.get(requestId);
        pending.delete(requestId);
        return request;
      }
    });

    handler.onData(Buffer.concat(frames));

    expect(resolved).toBe(128);
    expect(pause).toHaveBeenCalledOnce();
    expect(resume).not.toHaveBeenCalled();

    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(resolved).toBe(129);
    expect(pending.size).toBe(0);
    expect(resume).toHaveBeenCalledOnce();
  });
});
