import { describe, expect, it } from "vitest";

import {
  FerricStoreClient,
  QueueClient,
  QueueCompletionError
} from "../src/index.js";
import type { CommandExecutor } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";

describe("Queue asynchronous completion errors", () => {
  it("reports successful completions when a later completion fails", async () => {
    const failure = new Error("second completion failed");
    let completionCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          return [
            ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
            ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
          ];
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          completionCalls += 1;
          if (completionCalls === 2) throw failure;
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      leaseRenewal: false,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({ completed: 0 });
    let caught: unknown;
    try {
      await worker.flush();
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(QueueCompletionError);
    expect(caught).toMatchObject({ cause: failure, completed: 1 });
    await expect(worker.flush()).resolves.toBe(0);
  });

  it("drains later successes before reporting an earlier failure", async () => {
    const failure = new Error("first completion failed");
    let completionCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          return [
            ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
            ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
          ];
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          completionCalls += 1;
          if (completionCalls === 1) throw failure;
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      leaseRenewal: false,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({ completed: 0 });
    await expect(worker.flush()).rejects.toMatchObject({ cause: failure, completed: 1 });
    await expect(worker.flush()).resolves.toBe(0);
  });
});
