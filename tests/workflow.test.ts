import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec, WorkflowClient, complete, transition } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("Workflow", () => {
  it("claims a state and applies an explicit transition outcome", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-1"],
          ["type", "order"],
          ["state", "created"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 11],
          ["payload", Buffer.from('{"amount":42}')]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({
      initialState: "created",
      type: "order",
      worker: "worker-1"
    });

    workflow.state("created", (ctx) => {
      expect(ctx.payload).toEqual({ amount: 42 });
      return transition("charged", { values: { receipt: { ok: true } } });
    });

    const result = await workflow.worker({ batchSize: 1 }).runOnce();

    expect(result).toEqual({ applied: 1, claimCalls: 1, claimed: 1, emptyClaims: 0 });
    expect(executor.calls[1]).toEqual([
      "FLOW.TRANSITION",
      "order-1",
      "created",
      "charged",
      "LEASE_TOKEN",
      Buffer.from("lease"),
      "FENCING",
      11,
      "NOW",
      expect.any(Number),
      "PARTITION",
      "tenant-a",
      "RUN_AT",
      expect.any(Number),
      "VALUE",
      "receipt",
      Buffer.from('{"ok":true}')
    ]);
  });

  it("treats plain handler return values as complete results", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-2"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 12]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ initialState: "created", type: "order" });

    workflow.state("charged", () => ({ done: true }));

    await workflow.worker({ batchSize: 1, states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toContain("FLOW.COMPLETE");
    expect(executor.calls[1]).toContain("RESULT");
    expect(executor.calls[1]).toContainEqual(Buffer.from('{"done":true}'));
  });

  it("allows handlers to return complete explicitly", async () => {
    const executor = new FakeExecutor([
      [
        new Map<unknown, unknown>([
          ["id", "order-3"],
          ["type", "order"],
          ["state", "charged"],
          ["partition_key", "tenant-a"],
          ["lease_token", Buffer.from("lease")],
          ["fencing_token", 13]
        ])
      ],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const workflow = new WorkflowClient(flow).workflow({ type: "order" });

    workflow.state("charged", () => complete({ result: { ok: true }, ttlMs: 60_000 }));

    await workflow.worker({ states: ["charged"], worker: "worker-1" }).runOnce();

    expect(executor.calls[1]).toContain("TTL");
    expect(executor.calls[1]).toContain(60_000);
  });
});
