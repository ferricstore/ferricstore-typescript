import type { AddressInfo } from "node:net";
import { expect, test } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import { ReconnectingExecutor } from "../src/reconnecting-executor.js";
import { OPCODES } from "../src/protocol.js";
import {
  NO_RESPONSE,
  startCountingServer,
  v010Startup
} from "./adapter-test-support.js";

test("native EOF after request receipt remains possibly sent and is never replayed", async () => {
  let effects = 0;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) return v010Startup();
    if (request.opcode === OPCODES.set) {
      effects += 1;
      setImmediate(() => socket.destroy());
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const executor = new ReconnectingExecutor(
    async () => await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`),
    { baseDelayMs: 0, jitterPct: 0, maxDelayMs: 0, maxRetries: 2 }
  );
  await executor.ready();

  try {
    await expect(executor.executeCommand("SET", "key", "value")).rejects.toMatchObject({
      requestDisposition: "possibly_sent"
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(effects).toBe(1);
  } finally {
    await executor.close();
  }
});

test("native request encoding limits fail as unsent without a network exchange", async () => {
  let effects = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return v010Startup({ limits: { max_frame_bytes: 256 } });
    }
    if (request.opcode === OPCODES.set) effects += 1;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await expect(adapter.executeCommand("SET", "key", Buffer.alloc(2_048))).rejects.toMatchObject({
      requestDisposition: "unsent",
      safeToRetry: true
    });
    expect(effects).toBe(0);
  } finally {
    await adapter.close();
  }
});
