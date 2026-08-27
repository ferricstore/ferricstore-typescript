import net from "node:net";
import type { AddressInfo, Socket } from "node:net";
import { expect, test, vi } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import {
  COMMAND_OPCODES,
  OPCODES
} from "../src/protocol.js";
import {
  type TestRequest,
  BackpressureSocket,
  NO_RESPONSE,
  activeConnections,
  commandExecName,
  directNativeAdapter,
  encodedResponseBody,
  handleSocket,
  listen,
  responseFrame,
  responseFrameFromBody,
  servers,
  startCountingServer,
  v010Startup,
  waitFor
} from "./adapter-test-support.js";

test("NativeAdapter stamps FLOW.QUERY deadlines from the remaining response budget", async () => {
  let queryPayload: Record<string, unknown> | undefined;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.flowQuery) {
      queryPayload = request.payload as Record<string, unknown>;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    timeoutMs: 500
  });
  const startedAtMs = Date.now();

  try {
    await adapter.executeCommand(
      "FLOW.QUERY",
      "FQL1",
      "FROM runs WHERE run_id = @run RETURN RECORDS",
      "run",
      "run-1"
    );
    const deadlineMs = queryPayload?.deadline_ms;
    expect(deadlineMs).toBeTypeOf("number");
    expect(deadlineMs as number).toBeGreaterThanOrEqual(startedAtMs + 450);
    expect(deadlineMs as number).toBeLessThanOrEqual(Date.now() + 500);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter can keep idle sockets active with heartbeat pings", async () => {
  let pingCount = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.ping) {
      pingCount++;
    }
  });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 10
  });

  try {
    await waitFor(() => pingCount > 0);
    expect(pingCount).toBeGreaterThan(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter starts heartbeats only after STARTUP and AUTH complete", async () => {
  let authenticated = false;
  let postAuthPings = 0;
  let preAuthPings = 0;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      setTimeout(() => {
        socket.write(responseFrame(
          request.opcode,
          request.laneId,
          request.requestId,
          v010Startup({ auth_required: true })
        ));
      }, 25);
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.auth) {
      setTimeout(() => {
        authenticated = true;
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, "OK"));
      }, 25);
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.ping) {
      if (authenticated) postAuthPings += 1;
      else preAuthPings += 1;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 5,
    password: "secret",
    timeoutMs: 200
  });

  try {
    await waitFor(() => postAuthPings > 0);
    expect(preAuthPings).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter heartbeats while an indefinite request is pending", async () => {
  let blockingSeen = false;
  let pingCount = 0;
  const server = await startCountingServer((request) => {
    if (commandExecName(request) === "BLPOP") {
      blockingSeen = true;
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.ping) pingCount++;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 10,
    timeoutMs: 30
  });
  const blocking = adapter.executeCommand("BLPOP", "queue", 0);
  void blocking.catch(() => undefined);

  try {
    await waitFor(() => blockingSeen && pingCount > 0);
    expect(pingCount).toBeGreaterThan(0);
  } finally {
    await adapter.close();
    await Promise.allSettled([blocking]);
  }
});

test("NativeAdapter retirement cancels indefinite requests instead of retaining the connection", async () => {
  let blockingSeen = false;
  const server = await startCountingServer((request) => {
    if (commandExecName(request) === "BLPOP") {
      blockingSeen = true;
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 10,
    timeoutMs: 30
  });
  const blocking = adapter.executeCommand("BLPOP", "queue", 0);
  void blocking.catch(() => undefined);

  try {
    await waitFor(() => blockingSeen);
    const retirement = adapter.retire();
    const retiredPromptly = await Promise.race([
      retirement.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 30))
    ]);

    expect(retiredPromptly).toBe(true);
    await expect(blocking).rejects.toThrow("connection closed");
    await waitFor(async () => (await activeConnections(server)) === 0);
  } finally {
    await adapter.close();
    await Promise.allSettled([blocking]);
  }
});

test("NativeAdapter keeps heartbeating while finite requests drain during retirement", async () => {
  let pendingGet: { request: TestRequest; socket: Socket } | undefined;
  let pingCount = 0;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.get) {
      pendingGet = { request, socket };
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.ping) pingCount++;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    heartbeatIntervalMs: 10,
    timeoutMs: 500
  });
  const pending = adapter.executeCommand("GET", "key");
  void pending.catch(() => undefined);

  try {
    await waitFor(() => pendingGet != null);
    const retirement = adapter.retire();
    await waitFor(() => pingCount > 0);

    const captured = pendingGet;
    if (captured == null) throw new Error("pending GET was not captured");
    captured.socket.write(responseFrame(
      captured.request.opcode,
      captured.request.laneId,
      captured.request.requestId,
      Buffer.from("value")
    ));
    await expect(pending).resolves.toEqual(Buffer.from("value"));
    await expect(retirement).resolves.toBeUndefined();
  } finally {
    await adapter.close();
    await Promise.allSettled([pending]);
  }
});

test("NativeAdapter closes a blackholed connection after heartbeat timeout", async () => {
  const server = await startCountingServer(
    (request) => (request.opcode === OPCODES.ping ? NO_RESPONSE : undefined),
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 10,
    timeoutMs: 20
  });

  try {
    await new Promise((resolve) => setTimeout(resolve, 50));
    await expect(adapter.executeCommand("PING")).rejects.toThrow("connection is closed");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter heartbeats a blackholed connection during sustained outbound traffic", async () => {
  let pingCount = 0;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.ping) {
      pingCount += 1;
      return NO_RESPONSE;
    }
    if (request.opcode === OPCODES.get) return NO_RESPONSE;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 10,
    timeoutMs: 15
  });
  const requests: Promise<unknown>[] = [];
  const traffic = setInterval(() => {
    const request = adapter.executeCommand("GET", `busy-${requests.length}`);
    void request.catch(() => undefined);
    requests.push(request);
  }, 1);

  try {
    await new Promise((resolve) => setTimeout(resolve, 70));
    expect(pingCount).toBeGreaterThan(0);
  } finally {
    clearInterval(traffic);
    await adapter.close();
    await Promise.allSettled(requests);
  }
});

test("NativeAdapter retires credits whose timed-out response never arrives", async () => {
  const server = await startCountingServer((request) =>
    request.opcode === OPCODES.get ? NO_RESPONSE : undefined
  , { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 0,
    timeoutMs: 10
  });

  try {
    await expect(adapter.executeCommand("GET", "lost-response")).rejects.toMatchObject({
      code: "request_timeout",
      requestDisposition: "possibly_sent"
    });
    await waitFor(async () => (await activeConnections(server)) === 0);
    await expect(adapter.executeCommand("PING")).rejects.toThrow("connection is closed");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter marks a flow-control waiter timeout as unsent", async () => {
  let blockingSeen = false;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 1,
          max_inflight_per_lane: 1
        }
      };
    }
    if (commandExecName(request) === "BLPOP") {
      blockingSeen = true;
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    protocolLanes: 1,
    timeoutMs: 20
  });
  const blocking = adapter.executeCommand("BLPOP", "queue", 0);
  void blocking.catch(() => undefined);

  try {
    await waitFor(() => blockingSeen);
    await expect(adapter.executeCommand("GET", "queued")).rejects.toMatchObject({
      code: "request_timeout",
      requestDisposition: "unsent"
    });
  } finally {
    await adapter.close();
    await Promise.allSettled([blocking]);
  }
});

test("NativeAdapter marks a dispatched control-command timeout as possibly sent", async () => {
  const server = await startCountingServer((request) =>
    request.opcode === OPCODES.ping ? NO_RESPONSE : undefined
  , { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 0,
    timeoutMs: 10
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toMatchObject({
      code: "request_timeout",
      requestDisposition: "possibly_sent"
    });
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter marks requests dispatched before a protocol failure as possibly sent", async () => {
  let mutationsSeen = 0;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode !== OPCODES.set) return undefined;
    mutationsSeen += 1;
    if (mutationsSeen === 2) {
      const malformedFrame = Buffer.alloc(24);
      malformedFrame.write("NOPE", 0, "ascii");
      socket.write(malformedFrame);
    }
    return NO_RESPONSE;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    timeoutMs: 100
  });

  try {
    const settled = await Promise.allSettled([
      adapter.executeCommand("SET", "one", "1"),
      adapter.executeCommand("SET", "two", "2")
    ]);

    expect(mutationsSeen).toBe(2);
    for (const result of settled) {
      expect(result.status).toBe("rejected");
      if (result.status !== "rejected") throw new Error("expected request rejection");
      expect(result.reason as unknown).toMatchObject({
        code: "connection_closed",
        requestDisposition: "possibly_sent"
      });
    }
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter rejects pending requests immediately when explicitly closed", async () => {
  let pingSeen = false;
  const server = await startCountingServer(
    (request) => {
      if (request.opcode === OPCODES.ping) {
        pingSeen = true;
        return NO_RESPONSE;
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, { timeoutMs: 500 });
  const pending = adapter.executeCommand("PING");
  await waitFor(() => pingSeen);
  const rejected = expect(pending).rejects.toThrow("connection closed");

  await adapter.close();
  await rejected;
});

test("NativeAdapter fully closes a socket when the peer remains half-open", async () => {
  const sockets = new Set<Socket>();
  const server = net.createServer({ allowHalfOpen: true }, (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    handleSocket(socket, { fragmentResponses: false });
  });
  servers.push(server);
  await listen(server);
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`);

  try {
    expect(await activeConnections(server)).toBe(1);
    await adapter.close();
    expect((adapter as unknown as { socket: Socket }).socket.destroyed).toBe(true);
  } finally {
    for (const socket of sockets) socket.destroy();
  }
});

test("NativeAdapter pauses outbound writes until the socket drains", async () => {
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 1024 * 1024);
  const pending = [
    adapter.executeCommand("SET", "one", Buffer.alloc(32)),
    adapter.executeCommand("SET", "two", Buffer.alloc(32)),
    adapter.executeCommand("SET", "three", Buffer.alloc(32))
  ];
  for (const request of pending) void request.catch(() => undefined);

  await new Promise((resolve) => setImmediate(resolve));
  expect(socket.writes).toHaveLength(1);

  socket.emit("drain");
  await new Promise((resolve) => setImmediate(resolve));
  expect(socket.writes).toHaveLength(3);

  await adapter.close();
  await Promise.allSettled(pending);
});

test("NativeAdapter distinguishes possibly-sent writes from socket-queued requests on connection failure", async () => {
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 1024 * 1024);
  const dispatched = adapter.executeCommand("SET", "one", Buffer.alloc(32));
  const queued = adapter.executeCommand("SET", "two", Buffer.alloc(32));
  const settled = Promise.allSettled([dispatched, queued]);

  socket.emit("error", new Error("socket failed"));
  const [dispatchedResult, queuedResult] = await settled;

  expect(dispatchedResult).toMatchObject({
    status: "rejected",
    reason: {
      cause: { message: "socket failed" },
      requestDisposition: "possibly_sent"
    }
  });
  expect(queuedResult).toMatchObject({
    status: "rejected",
    reason: { requestDisposition: "unsent" }
  });
});

test("NativeAdapter marks a synchronous socket write failure as unsent", async () => {
  class ThrowingWriteSocket extends BackpressureSocket {
    override write(): boolean {
      throw new Error("synchronous write failed");
    }
  }

  const adapter = directNativeAdapter(new ThrowingWriteSocket(), 1024 * 1024);

  try {
    await expect(adapter.executeCommand("SET", "key", "value")).rejects.toMatchObject({
      requestDisposition: "unsent",
      cause: { message: "synchronous write failed" }
    });
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter marks an asynchronous socket write failure as possibly sent", async () => {
  class CallbackFailingWriteSocket extends BackpressureSocket {
    override write(frame: Buffer, callback?: (error?: Error | null) => void): boolean {
      this.writes.push(frame);
      queueMicrotask(() => callback?.(new Error("asynchronous write failed")));
      return true;
    }
  }

  const adapter = directNativeAdapter(new CallbackFailingWriteSocket(), 1024 * 1024);

  try {
    await expect(adapter.executeCommand("SET", "key", "value")).rejects.toMatchObject({
      requestDisposition: "possibly_sent",
      cause: { message: "asynchronous write failed" }
    });
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter bounds bytes waiting behind socket backpressure", async () => {
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 1);
  const first = adapter.executeCommand("SET", "one", Buffer.alloc(32));
  void first.catch(() => undefined);

  try {
    await expect(adapter.executeCommand("SET", "two", Buffer.alloc(32))).rejects.toMatchObject({
      reason: "client_write_queue_full"
    });
    expect(socket.writes).toHaveLength(1);
  } finally {
    await adapter.close();
    await Promise.allSettled([first]);
  }
});

test("NativeAdapter compacts timed-out writes while a socket remains stalled", async () => {
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 16 * 1024 * 1024, 1);
  const requests = Array.from({ length: 2_000 }, (_, index) =>
    adapter.executeCommand("SET", `stalled-${index}`, Buffer.alloc(8))
  );

  try {
    await Promise.allSettled(requests);
    const { writeQueue: queue } = adapter as unknown as { writeQueue: {
      queuedWriteBytes: number;
      queuedWrites: unknown[];
      queuedWritesByRequest: Map<bigint, unknown>;
    } };
    expect(queue.queuedWritesByRequest.size).toBe(0);
    expect(queue.queuedWriteBytes).toBe(0);
    expect(queue.queuedWrites).toHaveLength(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter bounds write tombstones while one queued request remains live", async () => {
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 16 * 1024 * 1024, 1);
  const first = adapter.executeCommand("PING");
  const anchor = adapter.executeCommand("BLPOP", "anchor", 0);
  void first.catch(() => undefined);
  void anchor.catch(() => undefined);

  try {
    for (let wave = 0; wave < 4; wave += 1) {
      await Promise.allSettled(Array.from({ length: 1_000 }, (_, index) =>
        adapter.executeCommand("SET", `wave-${wave}-${index}`, Buffer.alloc(8))
      ));
    }
    const { writeQueue: queue } = adapter as unknown as { writeQueue: {
      queuedWriteTombstones: number;
      queuedWrites: unknown[];
      queuedWritesByRequest: Map<bigint, unknown>;
    } };
    expect(queue.queuedWritesByRequest.size).toBe(1);
    expect(queue.queuedWriteTombstones).toBeLessThan(1_024);
    expect(queue.queuedWrites.length).toBeLessThanOrEqual(1_024);
  } finally {
    await adapter.close();
    await Promise.allSettled([first, anchor]);
  }
});

test("NativeAdapter chunks request timers above Node's maximum delay", async () => {
  const timeout = vi.spyOn(globalThis, "setTimeout");
  const socket = new BackpressureSocket();
  const adapter = directNativeAdapter(socket, 1024, 3_000_000_000);
  const pending = adapter.executeCommand("PING");
  void pending.catch(() => undefined);

  try {
    expect(timeout.mock.calls.some((call) => call[1] === 2_147_483_647)).toBe(true);
    expect(timeout.mock.calls.some((call) => call[1] === 3_000_000_000)).toBe(false);
  } finally {
    await adapter.close();
    await Promise.allSettled([pending]);
    timeout.mockRestore();
  }
});

test("NativeAdapter gives finite blocking claims their server wait plus transport timeout", async () => {
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.flowClaimDue) {
      setTimeout(() => {
        socket.write(responseFrame(request.opcode, request.laneId, request.requestId, []));
      }, 50);
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    timeoutMs: 20
  });

  try {
    await expect(adapter.executeCommand(
      "FLOW.CLAIM_DUE",
      "email",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30_000,
      "LIMIT",
      1,
      "BLOCK",
      200,
      "RETURN",
      "JOBS_COMPACT"
    )).resolves.toEqual([]);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter enforces advertised connection and lane credits with a bounded waiter queue", async () => {
  const gets: { request: TestRequest; socket: Socket }[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 1,
          max_inflight_per_lane: 1
        }
      };
    }
    if (request.opcode === OPCODES.get) {
      gets.push({ request, socket });
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxQueuedRequests: 1,
    protocolLanes: 1,
    timeoutMs: 500
  });
  const first = adapter.executeCommand("GET", "first");
  const second = adapter.executeCommand("GET", "second");
  void first.catch(() => undefined);
  void second.catch(() => undefined);

  try {
    await waitFor(() => gets.length === 1);
    await expect(adapter.executeCommand("GET", "third")).rejects.toMatchObject({
      reason: "client_queue_full"
    });
    expect(gets).toHaveLength(1);

    const firstGet = gets[0];
    if (firstGet == null) throw new Error("first GET was not captured");
    firstGet.socket.write(responseFrame(
      firstGet.request.opcode,
      firstGet.request.laneId,
      firstGet.request.requestId,
      Buffer.from("value-1")
    ));
    await expect(first).resolves.toEqual(Buffer.from("value-1"));

    await waitFor(() => gets.length === 2);
    const secondGet = gets[1];
    if (secondGet == null) throw new Error("second GET was not captured");
    secondGet.socket.write(responseFrame(
      secondGet.request.opcode,
      secondGet.request.laneId,
      secondGet.request.requestId,
      Buffer.from("value-2")
    ));
    await expect(second).resolves.toEqual(Buffer.from("value-2"));
  } finally {
    await adapter.close();
    await Promise.allSettled([first, second]);
  }
});

test("NativeAdapter retains flow-control credit until a timed-out sent request finishes", async () => {
  const gets: { request: TestRequest; socket: Socket }[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 1,
          max_inflight_per_lane: 1
        }
      };
    }
    if (request.opcode === OPCODES.get) {
      gets.push({ request, socket });
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    protocolLanes: 1,
    timeoutMs: 50
  });

  try {
    await expect(adapter.executeCommand("GET", "slow-first")).rejects.toThrow("timed out");
    expect(gets).toHaveLength(1);

    const second = adapter.executeCommand("GET", "second");
    void second.catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 10));
    const requestsBeforeLateResponse = gets.length;

    const firstGet = gets[0];
    if (firstGet == null) throw new Error("first GET was not captured");
    firstGet.socket.write(responseFrame(
      firstGet.request.opcode,
      firstGet.request.laneId,
      firstGet.request.requestId,
      Buffer.from("late-first")
    ));

    await waitFor(() => gets.length === 2);
    const secondGet = gets[1];
    if (secondGet == null) throw new Error("second GET was not captured");
    secondGet.socket.write(responseFrame(
      secondGet.request.opcode,
      secondGet.request.laneId,
      secondGet.request.requestId,
      Buffer.from("value-2")
    ));

    await expect(second).resolves.toEqual(Buffer.from("value-2"));
    expect(requestsBeforeLateResponse).toBe(1);
    expect((adapter as unknown as { pending: Map<bigint, unknown> }).pending.size).toBe(0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter honors the STARTUP lane queue limit", async () => {
  const gets: { request: TestRequest; socket: Socket }[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 10,
          max_inflight_per_lane: 10
        },
        limits: { max_lane_queue: 1 }
      };
    }
    if (request.opcode === OPCODES.get) {
      gets.push({ request, socket });
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 1,
    timeoutMs: 500
  });
  const requests = [
    adapter.executeCommand("GET", "first"),
    adapter.executeCommand("GET", "second"),
    adapter.executeCommand("GET", "third")
  ];
  for (const request of requests) void request.catch(() => undefined);

  try {
    await waitFor(() => gets.length >= 1);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(gets).toHaveLength(1);

    for (let index = 0; index < requests.length; index += 1) {
      await waitFor(() => gets.length > index);
      const get = gets[index];
      if (get == null) throw new Error(`GET ${index} was not captured`);
      get.socket.write(responseFrame(
        get.request.opcode,
        get.request.laneId,
        get.request.requestId,
        Buffer.from(`value-${index + 1}`)
      ));
      await expect(requests[index]).resolves.toEqual(Buffer.from(`value-${index + 1}`));
    }
  } finally {
    await adapter.close();
    await Promise.allSettled(requests);
  }
});

test("NativeAdapter rejects immediately when STARTUP disables the lane queue", async () => {
  let getSeen = false;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 10,
          max_inflight_per_lane: 10
        },
        limits: { max_lane_queue: 0 }
      };
    }
    if (request.opcode === OPCODES.get) getSeen = true;
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    timeoutMs: 500
  });

  try {
    await expect(adapter.executeCommand("GET", "key")).rejects.toMatchObject({
      reason: "client_lane_queue_full"
    });
    expect(getSeen).toBe(false);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter applies limits returned by WINDOW_UPDATE", async () => {
  const gets: { request: TestRequest; socket: Socket }[] = [];
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 2,
          max_inflight_per_lane: 2
        }
      };
    }
    if (request.opcode === OPCODES.windowUpdate) {
      return {
        accepted: true,
        limits: {
          max_inflight_per_connection: 1,
          max_inflight_per_lane: 1
        }
      };
    }
    if (request.opcode === OPCODES.get) {
      gets.push({ request, socket });
      return NO_RESPONSE;
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    protocolLanes: 1,
    timeoutMs: 500
  });

  await adapter.executeCommand(
    "WINDOW_UPDATE",
    "MAX_INFLIGHT_PER_CONNECTION",
    1,
    "MAX_INFLIGHT_PER_LANE",
    1
  );
  const first = adapter.executeCommand("GET", "first");
  const second = adapter.executeCommand("GET", "second");
  void first.catch(() => undefined);
  void second.catch(() => undefined);

  try {
    await waitFor(() => gets.length === 1);
    expect(gets).toHaveLength(1);
    const firstGet = gets[0];
    if (firstGet == null) throw new Error("first GET was not captured");
    firstGet.socket.write(responseFrame(
      firstGet.request.opcode,
      firstGet.request.laneId,
      firstGet.request.requestId,
      Buffer.from("value-1")
    ));
    await first;

    await waitFor(() => gets.length === 2);
    const secondGet = gets[1];
    if (secondGet == null) throw new Error("second GET was not captured");
    secondGet.socket.write(responseFrame(
      secondGet.request.opcode,
      secondGet.request.laneId,
      secondGet.request.requestId,
      Buffer.from("value-2")
    ));
    await second;
  } finally {
    await adapter.close();
    await Promise.allSettled([first, second]);
  }
});

test("NativeAdapter can reopen a zero-sized data window with WINDOW_UPDATE", async () => {
  let getSeen = false;
  const server = await startCountingServer((request) => {
    if (request.opcode === OPCODES.startup) {
      return {
        flow_control: {
          max_inflight_per_connection: 0,
          max_inflight_per_lane: 0
        }
      };
    }
    if (request.opcode === OPCODES.windowUpdate) {
      return {
        accepted: true,
        limits: {
          max_inflight_per_connection: 1,
          max_inflight_per_lane: 1
        }
      };
    }
    if (request.opcode === OPCODES.get) {
      getSeen = true;
      return Buffer.from("value");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    maxQueuedRequests: 0,
    timeoutMs: 100
  });

  try {
    await adapter.executeCommand(
      "WINDOW_UPDATE",
      "MAX_INFLIGHT_PER_CONNECTION",
      1,
      "MAX_INFLIGHT_PER_LANE",
      1
    );
    await expect(adapter.executeCommand("GET", "key")).resolves.toEqual(Buffer.from("value"));
    expect(getSeen).toBe(true);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter surfaces management events and drains on GOAWAY", async () => {
  const events: { opcode: number; value: unknown }[] = [];
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode === OPCODES.startup) {
        setImmediate(() => {
          socket.write(responseFrame(COMMAND_OPCODES.GOAWAY, 0, 0n, { reason: "draining" }));
        });
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    onEvent: (event) => events.push(event)
  });

  try {
    await waitFor(() => events.length === 1);
    expect(events[0]).toMatchObject({ opcode: COMMAND_OPCODES.GOAWAY });
    await expect(adapter.executeCommand("PING")).rejects.toThrow("connection is closed");
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter enters GOAWAY draining state before invoking the event callback", async () => {
  let callbackAttempt: Promise<unknown> | undefined;
  let eventSeen = false;
  let pingSeen = false;
  const server = await startCountingServer((request, socket) => {
    if (request.opcode === OPCODES.options) {
      setImmediate(() => {
        socket.write(responseFrame(COMMAND_OPCODES.GOAWAY, 0, 0n, { reason: "draining" }));
      });
      return {};
    }
    if (request.opcode === OPCODES.ping) {
      pingSeen = true;
      return Buffer.from("PONG");
    }
    return undefined;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    onEvent: (event) => {
      if (event.opcode === COMMAND_OPCODES.GOAWAY) {
        eventSeen = true;
        const attempt = adapter.executeCommand("PING");
        void attempt.catch(() => undefined);
        callbackAttempt = attempt;
      }
    }
  });

  try {
    await adapter.executeCommand("OPTIONS");
    await waitFor(() => eventSeen && callbackAttempt != null);
    await expect(callbackAttempt).rejects.toThrow("connection is closed");
    await new Promise((resolve) => setImmediate(resolve));
    expect(pingSeen).toBe(false);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter finishes draining when the last pending request times out", async () => {
  const server = await startCountingServer((request, socket) => {
    if (request.opcode !== OPCODES.ping) return undefined;
    socket.write(responseFrame(COMMAND_OPCODES.GOAWAY, 0, 0n, { reason: "draining" }));
    return NO_RESPONSE;
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    connectTimeoutMs: 1_000,
    timeoutMs: 20
  });

  try {
    await expect(adapter.executeCommand("PING")).rejects.toThrow("timed out");
    await waitFor(async () => (await activeConnections(server)) === 0);
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter assembles chunked management events", async () => {
  const events: { opcode: number; value: unknown }[] = [];
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode === OPCODES.startup) {
        setImmediate(() => {
          const body = encodedResponseBody({ event: "TOPOLOGY_CHANGED", payload: { route_epoch: 2 } });
          socket.write(responseFrameFromBody(COMMAND_OPCODES.EVENT, 0, 0n, body.subarray(0, 5), 0x20));
          socket.write(responseFrameFromBody(COMMAND_OPCODES.EVENT, 0, 0n, body.subarray(5)));
        });
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    onEvent: (event) => events.push(event)
  });

  try {
    await waitFor(() => events.length === 1);
    expect(events[0]).toMatchObject({
      opcode: COMMAND_OPCODES.EVENT,
      value: { event: Buffer.from("TOPOLOGY_CHANGED"), payload: { route_epoch: 2 } }
    });
  } finally {
    await adapter.close();
  }
});

test("NativeAdapter isolates rejected asynchronous event callbacks", async () => {
  let eventSent = false;
  let unhandled: unknown;
  const onUnhandled = (error: unknown): void => {
    unhandled = error;
  };
  process.once("unhandledRejection", onUnhandled);
  const server = await startCountingServer(
    (request, socket) => {
      if (request.opcode === OPCODES.startup) {
        setImmediate(() => {
          socket.write(responseFrame(COMMAND_OPCODES.EVENT, 0, 0n, { event: "FLOW_WAKE" }));
          eventSent = true;
        });
      }
      return undefined;
    },
    { fragmentResponses: false }
  );
  const address = server.address() as AddressInfo;
  const adapter = await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`, {
    onEvent: async () => {
      throw new Error("event callback failed");
    }
  });

  try {
    await waitFor(() => eventSent);
    await new Promise((resolve) => setImmediate(resolve));
    expect(unhandled).toBeUndefined();
  } finally {
    process.off("unhandledRejection", onUnhandled);
    await adapter.close();
  }
});
