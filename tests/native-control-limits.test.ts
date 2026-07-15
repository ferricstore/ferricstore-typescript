import { Buffer } from "node:buffer";
import type { AddressInfo, Socket } from "node:net";
import { expect, test } from "vitest";

import { NativeAdapter } from "../src/adapters.js";
import { OverloadedError } from "../src/errors.js";
import { OPCODES } from "../src/protocol.js";
import {
  NO_RESPONSE,
  type TestRequest,
  responseFrame,
  startCountingServer,
  waitFor
} from "./adapter-test-support.js";

test("NativeAdapter bounds pending control requests and immediately reuses released slots", async () => {
  const pendingPings: { readonly request: TestRequest; readonly socket: Socket }[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode !== OPCODES.ping) return undefined;
    pendingPings.push({ request, socket });
    if (pendingPings.length <= 2) return NO_RESPONSE;
    return Buffer.from("PONG");
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxPendingControlRequests: 2,
    timeoutMs: 500
  });
  const first = adapter.executeCommand("PING");
  const second = adapter.executeCommand("PING");
  void first.catch(() => undefined);
  void second.catch(() => undefined);

  try {
    await waitFor(() => pendingPings.length === 2);
    const overloaded = await adapter.executeCommand("PING").catch((error: unknown) => error);

    expect(overloaded).toBeInstanceOf(OverloadedError);
    expect((overloaded as OverloadedError).reason).toBe("client_control_requests_full");
    expect(pendingPings).toHaveLength(2);

    const completed = pendingPings[0];
    if (completed == null) throw new Error("first pending PING was not captured");
    completed.socket.write(responseFrame(
      completed.request.opcode,
      completed.request.laneId,
      completed.request.requestId,
      Buffer.from("PONG")
    ));
    await expect(first).resolves.toEqual(Buffer.from("PONG"));

    await expect(adapter.executeCommand("PING")).resolves.toEqual(Buffer.from("PONG"));
    expect(pendingPings).toHaveLength(3);
  } finally {
    await adapter.close();
    await Promise.allSettled([first, second]);
  }
});
