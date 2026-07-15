import { describe, expect, it } from "vitest";
import type { CommandExecutor } from "../src/adapters.js";
import { FerricStoreClient, QueueClient, transition } from "../src/index.js";
import type { CommandArgument } from "../src/internal.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Queue worker shutdown and terminal errors", () => {
  it("flushes pending async completions when run loop stops", async () => {
    const signal = new AbortController();
    const executor = new FakeExecutor([
      [["email-1", "tenant-a", Buffer.from("lease-1"), 1]],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 1,
      signal: signal.signal,
      worker: "worker-1"
    });

    const task = worker.run(() => {
      signal.abort();
    });

    await expect(task).resolves.toBeUndefined();
    expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.CLAIM_DUE", "FLOW.COMPLETE_MANY"]);
  });

  it("propagates terminal completion write errors instead of retrying completed handler results", async () => {
    const terminalError = new Error("terminal write failed");
    const executor = new FakeExecutor([
      [["email-1", "tenant-a", Buffer.from("lease-1"), 1]],
      terminalError
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(
      queue.worker({ batchSize: 1, claimPayload: false, worker: "worker-1" }).runBatchOnce(() => undefined)
    ).rejects.toThrow("terminal write failed");

    expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.CLAIM_DUE", "FLOW.COMPLETE_MANY"]);
  });

  it("rejects workflow transitions in queue handlers", async () => {
    const executor = new FakeExecutor([[flow("email-1", 1)]]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    await expect(
      queue.worker({ exceptionPolicy: "raise", worker: "worker-1" }).runOnce(() => transition("next"))
    ).rejects.toThrow("Queue handlers cannot return transition");
  });

  it("does not treat a null async completion rejection as success", async () => {
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          return [["email-1", "tenant-a", Buffer.from("lease-1"), 1]];
        }
        if (args[0] === "FLOW.COMPLETE_MANY") {
          // eslint-disable-next-line @typescript-eslint/only-throw-error -- JavaScript promises may reject with any value
          throw null;
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 1,
      leaseRenewal: false,
      worker: "worker-1"
    });

    await worker.runBatchOnce(() => undefined);
    await expect(worker.flush()).rejects.toMatchObject({
      cause: null,
      completed: 0,
      name: "QueueCompletionError"
    });
  });
});

function flow(id: string, fencingToken: number): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", id],
    ["type", "email"],
    ["state", "queued"],
    ["partition_key", "tenant-a"],
    ["lease_token", Buffer.from(`lease-${fencingToken}`)],
    ["fencing_token", fencingToken]
  ]);
}
