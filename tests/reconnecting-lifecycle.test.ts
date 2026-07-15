import type { AddressInfo } from "node:net";
import { expect, test } from "vitest";
import { NativeAdapter } from "../src/adapters.js";
import { ReconnectingExecutor } from "../src/reconnecting-executor.js";
import { ConnectionClosedError } from "../src/errors.js";
import type { CommandArgument } from "../src/internal.js";
import { COMMAND_OPCODES } from "../src/protocol.js";
import {
  activeConnections,
  NO_RESPONSE,
  commandExecName,
  responseFrame,
  startCountingServer,
  startStartupClosingServer,
  waitFor
} from "./adapter-test-support.js";

test("ReconnectingExecutor reconnects when the native adapter was closed while idle", async () => {
  const server = await startStartupClosingServer();
  const address = server.address() as AddressInfo;
  const executor = new ReconnectingExecutor(async () => await NativeAdapter.fromUrl(`ferric://127.0.0.1:${address.port}`));

  try {
    await waitFor(() => server.connectionCount > 0);
    await waitFor(async () => (await activeConnections(server)) === 0);
    const response = await executor.executeCommand("PING");
    expect(Buffer.isBuffer(response)).toBe(true);
    expect((response as Buffer).toString("utf8")).toBe("PONG");
    expect(server.connectionCount).toBeGreaterThan(1);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor never replays a dispatched indefinite command after GOAWAY", async () => {
  let blockingCalls = 0;
  const server = await startCountingServer((request, socket) => {
    if (commandExecName(request) !== "BLPOP") return undefined;
    blockingCalls += 1;
    if (blockingCalls === 1) {
      socket.write(responseFrame(COMMAND_OPCODES.GOAWAY, 0, 0n, { reason: "draining" }));
      return NO_RESPONSE;
    }
    return [Buffer.from("queue"), Buffer.from("second-item")];
  }, { fragmentResponses: false });
  const address = server.address() as AddressInfo;
  const executor = new ReconnectingExecutor(async () => await NativeAdapter.fromUrl(
    `ferric://127.0.0.1:${address.port}`,
    { timeoutMs: 100 }
  ));

  try {
    await expect(executor.executeCommand("BLPOP", "queue", 0)).rejects.toThrow(
      "FerricStore connection closed"
    );
    expect(blockingCalls).toBe(1);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor does not replay an uncertain in-flight operation", async () => {
  let createCount = 0;
  const calls: string[][] = [];
  let firstCalls = 0;
  const executor = new ReconnectingExecutor(async () => {
    createCount += 1;
    if (createCount === 1) {
      return {
        async executeCommand(...args: CommandArgument[]): Promise<unknown> {
          calls.push(args.map(String));
          firstCalls += 1;
          if (firstCalls === 1) throw new Error("FerricStore connection closed");
          throw new ConnectionClosedError("unsent");
        }
      };
    }
    return {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args.map(String));
        return Buffer.from("PONG");
      }
    };
  });

  try {
    await expect(executor.executeCommand("INCR", "counter")).rejects.toThrow(
      "FerricStore connection closed"
    );
    await expect(executor.executeCommand("PING")).resolves.toEqual(Buffer.from("PONG"));

    expect(createCount).toBe(2);
    expect(calls).toEqual([
      ["INCR", "counter"],
      ["PING"],
      ["PING"]
    ]);
  } finally {
    await executor.close();
  }
});

test("ReconnectingExecutor closes a replacement created after client close", async () => {
  let createCount = 0;
  let secondClosed = false;
  let releaseSecond: (() => void) | undefined;
  let markSecondStarted: (() => void) | undefined;
  const secondStarted = new Promise<void>((resolve) => {
    markSecondStarted = resolve;
  });
  const secondGate = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const executor = new ReconnectingExecutor(async () => {
    createCount++;
    if (createCount === 1) {
      return {
        async executeCommand(): Promise<unknown> {
          throw new ConnectionClosedError("unsent");
        }
      };
    }
    markSecondStarted?.();
    await secondGate;
    return {
      async close(): Promise<void> {
        secondClosed = true;
      },
      async executeCommand(): Promise<unknown> {
        return "unexpected success";
      }
    };
  });
  const operation = executor.executeCommand("PING");

  await secondStarted;
  let closeFinished = false;
  const closing = executor.close().then(() => {
    closeFinished = true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  expect(closeFinished).toBe(false);
  releaseSecond?.();

  await closing;
  await expect(operation).rejects.toThrow("client is closed");
  expect(secondClosed).toBe(true);
});
