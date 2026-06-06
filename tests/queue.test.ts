import { describe, expect, it } from "vitest";
import { FlowClient, JsonCodec, QueueClient, fail, retry, transition } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Queue", () => {
  it("enqueues a durable queue item through FLOW.CREATE", async () => {
    const executor = new FakeExecutor();
    const queue = new QueueClient(new FlowClient(executor, { codec: new JsonCodec() })).queue("email");

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
    const queue = new QueueClient(new FlowClient(executor)).queue("email");

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

  it("rejects workflow transitions in queue handlers", async () => {
    const executor = new FakeExecutor([[flow("email-1", 1)]]);
    const queue = new QueueClient(new FlowClient(executor)).queue("email");

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
