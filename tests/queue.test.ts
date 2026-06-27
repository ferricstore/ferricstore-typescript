import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec, QueueClient, fail, retry, transition } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Queue", () => {
  it("enqueues a durable queue item through FLOW.CREATE", async () => {
    const executor = new FakeExecutor();
    const queue = new QueueClient(new FerricStoreClient(executor, { codec: new JsonCodec() })).queue("email");

    await queue.enqueue("email-1", {
      idempotent: true,
      nowMs: 100,
      payload: { to: "user@example.com" }
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.CREATE",
      "email-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      100,
      "PAYLOAD",
      Buffer.from('{"to":"user@example.com"}'),
      "RUN_AT",
      100,
      "PRIORITY",
      0,
      "IDEMPOTENT",
      "true"
    ]);
  });

  it("can retry and fail jobs from handler outcomes", async () => {
    const executor = new FakeExecutor([
      [
        flow("email-1", 1),
        flow("email-2", 2)
      ],
      Buffer.from("OK"),
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({ batchSize: 2, worker: "worker-1" }).runOnce((job) => {
      if (job.id === "email-1") {
        return retry({ error: "rate limited" });
      }
      return fail({ error: "bad address" });
    });

    expect(result).toEqual({ claimed: 2, completed: 0, failed: 1, retried: 1 });
    expect(executor.calls.at(1)?.[0]).toBe("FLOW.RETRY");
    expect(executor.calls.at(2)?.[0]).toBe("FLOW.FAIL");
  });

  it("uses compact claims and batched completion when payload is not requested", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", lease1, 1],
        ["email-2", "tenant-a", lease2, 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      worker: "worker-1"
    }).runOnce(() => undefined);

    expect(result).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("JOBS_COMPACT");
    expect(executor.calls[0]).toContain("NOPAYLOAD");
    expect(executor.calls[1]).toEqual([
      "FLOW.COMPLETE_MANY",
      "tenant-a",
      "NOW",
      expect.any(Number),
      "INDEPENDENT",
      "true",
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      Buffer.from("email-1"),
      lease1,
      1,
      Buffer.from("email-2"),
      lease2,
      2
    ]);
  });

  it("runs queue batch handlers with one handler call and one completion batch", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const seen: string[][] = [];

    const result = await queue.worker({
      batchSize: 2,
      claimPayload: false,
      worker: "worker-1"
    }).runBatchOnce((jobs) => {
      seen.push(jobs.map((job) => job.id));
    });

    expect(seen).toEqual([["email-1", "email-2"]]);
    expect(result).toEqual({ claimed: 2, completed: 2, failed: 0, retried: 0 });
    expect(executor.calls).toHaveLength(2);
    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE_MANY");
  });

  it("drains async queue completions with explicit flush", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK")
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 2,
      completed: 0
    });
    await expect(worker.flush()).resolves.toBe(2);
  });

  it("drains pending async completions before claiming more jobs", async () => {
    const executor = new FakeExecutor([
      [
        ["email-1", "tenant-a", Buffer.from("lease-1"), 1],
        ["email-2", "tenant-a", Buffer.from("lease-2"), 2]
      ],
      Buffer.from("OK"),
      []
    ]);
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const worker = queue.worker({
      batchSize: 1,
      claimPayload: false,
      completeAsyncDepth: 2,
      worker: "worker-1"
    });

    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 2,
      completed: 0
    });
    await expect(worker.runBatchOnce(() => undefined)).resolves.toMatchObject({
      claimed: 0,
      completed: 2
    });

    expect(executor.calls[0]?.[0]).toBe("FLOW.CLAIM_DUE");
    expect(executor.calls[1]?.[0]).toBe("FLOW.COMPLETE_MANY");
    expect(executor.calls[2]?.[0]).toBe("FLOW.COMPLETE_MANY");
    expect(executor.calls[3]?.[0]).toBe("FLOW.CLAIM_DUE");
  });

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
