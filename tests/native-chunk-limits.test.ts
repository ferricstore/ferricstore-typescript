import type { AddressInfo, Socket } from "node:net";
import { expect, test, vi } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import { OPCODES } from "../src/protocol.js";
import {
  type TestRequest,
  BackpressureSocket,
  NO_RESPONSE,
  chunkAssemblerInternals,
  directNativeAdapter,
  encodedResponseBody,
  responseFrame,
  responseFrameFromBody,
  startCountingServer,
  startFragmentingServer,
  waitFor
} from "./adapter-test-support.js";

test("NativeAdapter cleans chunk buffers when a chunked request times out", async () => {
  const server = await startFragmentingServer({ chunkOnlyPing: true });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 4,
    timeoutMs: 20
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("timed out");
    expect(chunkAssemblerInternals(adapter).chunks.size).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter cleans timed-out chunks through its request index", async () => {
  const server = await startFragmentingServer({ chunkOnlyPing: true });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    timeoutMs: 20
  });
  const { chunks } = chunkAssemblerInternals(adapter);
  const globalScan = vi.spyOn(chunks, "keys");

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("timed out");
    expect(globalScan).not.toHaveBeenCalled();
    expect(chunks.size).toBe(0);
  } finally {
    globalScan.mockRestore();
    await adapter.close();
  }
});

test("NativeAdapter rejects oversized chunked responses", async () => {
  const server = await startFragmentingServer({ chunkOnlyPing: true });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxChunkBytes: 4,
    protocolLanes: 4,
    timeoutMs: 100
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("chunked response exceeded");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter bounds zero-byte chunk frame metadata", async () => {
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode !== OPCODES.ping) return undefined;
      for (let index = 0; index < 4; index++) {
        socket.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, Buffer.alloc(0), 0x20));
      }
      return NO_RESPONSE;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxChunkFrames: 3,
    timeoutMs: 100
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("chunked response exceeded 3 frames");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter appends chunk metadata without copying the growing list", async () => {
  let pingRequest: TestRequest | undefined;
  let pingSocket: Socket | undefined;
  const body = encodedResponseBody("PONG");
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode !== OPCODES.ping) return undefined;
      pingRequest = request;
      pingSocket = socket;
      socket.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, body.subarray(0, 1), 0x20));
      return NO_RESPONSE;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    const response = adapter.executeCommand("PING");
    await waitFor(() => pingRequest != null && pingSocket != null);
    const request = pingRequest;
    if (request == null) throw new Error("PING request was not observed");
    const key = `${request.requestId}:${request.opcode}:${request.laneId}`;
    const { chunks } = chunkAssemblerInternals(adapter);
    await waitFor(() => chunks.get(key)?.length === 1);
    const initialList = chunks.get(key);

    pingSocket?.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, body.subarray(1, 2), 0x20));
    await waitFor(() => chunks.get(key)?.length === 2);
    expect(chunks.get(key)).toBe(initialList);

    pingSocket?.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, body.subarray(2)));
    await expect(response).resolves.toEqual(Buffer.from("PONG"));
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter applies the response cap while chunks are still arriving", async () => {
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode === OPCODES.startup) {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, null));
        return NO_RESPONSE;
      }
      if (request.opcode === OPCODES.ping) {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "partial-response", 0x20));
        return NO_RESPONSE;
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxChunkBytes: 1_024,
    maxResponseBytes: 4,
    timeoutMs: 100
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("response exceeded 4 bytes");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter includes the final chunk in the chunked response byte limit", async () => {
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode !== OPCODES.ping) return undefined;
      const body = encodedResponseBody("chunked-value");
      socket.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, body.subarray(0, 4), 0x20));
      socket.write(responseFrameFromBody(request.opcode, request.laneId, request.requestId, body.subarray(4)));
      return NO_RESPONSE;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxChunkBytes: 8
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("chunked response exceeded");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter applies the global chunk cap before accepting a final chunk", async () => {
  const adapter = directNativeAdapter(new BackpressureSocket(), 1024 * 1024, 20, 8);
  const { assembler } = chunkAssemblerInternals(adapter);
  const frame = (requestId: bigint, flags: number) => ({
    body: Buffer.alloc(4),
    bodyLength: 4,
    flags,
    laneId: 1,
    opcode: OPCODES.ping,
    requestId
  });

  try {
    expect(assembler.assemble(frame(1n, 0x20))).toBeUndefined();
    expect(assembler.assemble(frame(2n, 0x20))).toBeUndefined();
    expect(() => assembler.assemble(frame(1n, 0))).toThrow(
      "buffered chunk responses exceeded 8 bytes"
    );
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter retains final-chunk accounting through contiguous assembly", async () => {
  const adapter = directNativeAdapter(new BackpressureSocket(), 1024 * 1024, 20, 8);
  const { assembler } = chunkAssemblerInternals(adapter);
  const internals = assembler as unknown as { totalBytes: number };
  const originalConcat = Buffer.concat.bind(Buffer);
  let bytesDuringAssembly = -1;
  const concat = vi.spyOn(Buffer, "concat").mockImplementation((chunks, totalLength) => {
    bytesDuringAssembly = internals.totalBytes;
    return originalConcat(chunks, totalLength);
  });

  try {
    expect(assembler.assemble({
      body: Buffer.alloc(4),
      bodyLength: 4,
      flags: 0x20,
      laneId: 1,
      opcode: OPCODES.ping,
      requestId: 1n
    })).toBeUndefined();
    expect(assembler.assemble({
      body: Buffer.alloc(4),
      bodyLength: 4,
      flags: 0,
      laneId: 1,
      opcode: OPCODES.ping,
      requestId: 1n
    })).toMatchObject({ bodyLength: 8 });
    expect(bytesDuringAssembly).toBe(8);
    expect(internals.totalBytes).toBe(0);
  } finally {
    concat.mockRestore();
    await adapter.close();
  }
});

test("NativeAdapter bounds unchunked response bodies", async () => {
  const server = await startCountingServer((request) =>
    request.opcode === OPCODES.ping ? Buffer.alloc(128, 0x61) : undefined
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxResponseBytes: 32
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("response exceeded");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter ignores unsolicited chunk frames", async () => {
  let unsolicitedSent = false;
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode === OPCODES.startup) {
        setImmediate(() => {
          socket.write(responseFrame(OPCODES.ping, 1, 999n, "unsolicited", 0x20));
          unsolicitedSent = true;
        });
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    await waitFor(() => unsolicitedSent);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(chunkAssemblerInternals(adapter).chunks.size).toBe(0);
  } finally {
    await adapter.close();
  }
});
