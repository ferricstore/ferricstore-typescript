import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  JsonCodec,
  WorkflowClient,
  complete,
  isWaitForChildrenOutcome,
  waitForChildren
} from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

function claimedImage(): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", "image-1"],
    ["type", "image"],
    ["state", "received"],
    ["partition_key", "tenant-a"],
    ["lease_token", Buffer.from("lease")],
    ["fencing_token", 11],
    ["payload", Buffer.from("{}")]
  ]);
}

function parentWithGroup(group: Map<unknown, unknown>): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["id", Buffer.from("image-1")],
    ["type", Buffer.from("image")],
    ["state", Buffer.from("resized")],
    ["partition_key", Buffer.from("tenant-a")],
    ["child_groups", new Map<unknown, unknown>([[Buffer.from("resizes"), group]])]
  ]);
}

describe("waitForChildren", () => {
  it("spawns the children and parks the parent in one fenced command", async () => {
    const executor = new FakeExecutor([[claimedImage()], Buffer.from("OK")]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const image = new WorkflowClient(flow).workflow({ type: "image", worker: "worker-1" });

    image.state("received", (ctx) => waitForChildren(
      [
        { id: `${ctx.id}:small`, payload: { size: "small" }, type: "resize" },
        { id: `${ctx.id}:large`, payload: { size: "large" }, type: "resize" }
      ],
      { failure: "resize_failed", groupId: "resizes", success: "resized", waitState: "waiting_for_resizes" }
    ));

    await expect(image.worker({ batchSize: 1, states: ["received"] }).runOnce())
      .resolves.toEqual({ applied: 1, claimCalls: 1, claimed: 1, emptyClaims: 0 });

    expect(executor.calls).toHaveLength(2);
    expect(executor.calls[1]).toEqual([
      "FLOW.SPAWN_CHILDREN",
      "image-1",
      "GROUP",
      "resizes",
      "WAIT",
      "all",
      "NOW",
      expect.any(Number),
      "PARTITION",
      "tenant-a",
      "LEASE_TOKEN",
      Buffer.from("lease"),
      "FENCING",
      11,
      "WAIT_STATE",
      "waiting_for_resizes",
      "SUCCESS",
      "resized",
      "FAILURE",
      "resize_failed",
      "ITEMS",
      "image-1:small",
      "resize",
      Buffer.from('{"size":"small"}'),
      "image-1:large",
      "resize",
      Buffer.from('{"size":"large"}')
    ]);
  });

  it("passes the child failure and parent-close policies through", async () => {
    const executor = new FakeExecutor([[claimedImage()], Buffer.from("OK")]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const image = new WorkflowClient(flow).workflow({ type: "image", worker: "worker-1" });

    image.state("received", () => waitForChildren(
      [{ id: "image-1:small", type: "resize" }],
      {
        failure: "resize_failed",
        onChildFailed: "ignore",
        onParentClosed: "abandon_children",
        success: "resized",
        wait: "any",
        waitState: "waiting_for_resizes"
      }
    ));

    await image.worker({ batchSize: 1, states: ["received"] }).runOnce();

    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "WAIT", "any", "ON_CHILD_FAILED", "ignore", "ON_PARENT_CLOSED", "abandon_children"
    ]));
  });

  it("requires the wait, success, and failure states and at least one child", () => {
    const states = { failure: "failed_children", success: "children_done", waitState: "waiting" };

    expect(() => waitForChildren([], states)).toThrow("waitForChildren children must be non-empty");
    for (const name of ["waitState", "success", "failure"] as const) {
      expect(() => waitForChildren([{ id: "c", type: "t" }], { ...states, [name]: "" }))
        .toThrow(`waitForChildren ${name} must be a non-empty string`);
    }
    expect(() => waitForChildren([{ id: "c", type: "t" }], { ...states, wait: "none" as never }))
      .toThrow("waitForChildren wait must be 'all' or 'any'");
  });

  it("is a workflow outcome only when created by the helper", () => {
    const outcome = waitForChildren([{ id: "c", type: "t" }], {
      failure: "failed_children",
      success: "children_done",
      waitState: "waiting"
    });

    expect(isWaitForChildrenOutcome(outcome)).toBe(true);
    expect(isWaitForChildrenOutcome({ ...outcome })).toBe(false);
    expect(outcome.kind).toBe("wait_for_children");
  });
});

describe("childResults", () => {
  it("decodes each child's status and result or error in one value read", async () => {
    const group = new Map<unknown, unknown>([
      [Buffer.from("wait"), Buffer.from("all")],
      [Buffer.from("resolved"), Buffer.from("failure")],
      [Buffer.from("summary"), new Map<unknown, unknown>([
        [Buffer.from("total"), 3],
        [Buffer.from("completed"), 1],
        [Buffer.from("failed"), 1],
        [Buffer.from("cancelled"), 0]
      ])],
      [Buffer.from("children"), new Map<unknown, unknown>([
        [Buffer.from("image-1:small"), Buffer.from("completed")],
        [Buffer.from("image-1:large"), Buffer.from("failed")],
        [Buffer.from("image-1:huge"), Buffer.from("running")]
      ])],
      [Buffer.from("child_partitions"), new Map<unknown, unknown>([
        [Buffer.from("image-1:small"), Buffer.from("tenant-a")],
        [Buffer.from("image-1:large"), Buffer.from("tenant-a")],
        [Buffer.from("image-1:huge"), Buffer.from("tenant-b")]
      ])],
      [Buffer.from("results"), new Map<unknown, unknown>([
        [Buffer.from("image-1:small"), new Map<unknown, unknown>([
          [Buffer.from("status"), Buffer.from("completed")],
          [Buffer.from("result_ref"), Buffer.from("ref-small")]
        ])],
        [Buffer.from("image-1:large"), new Map<unknown, unknown>([
          [Buffer.from("status"), Buffer.from("failed")],
          [Buffer.from("error_ref"), Buffer.from("ref-large")]
        ])]
      ])]
    ]);
    const executor = new FakeExecutor([
      parentWithGroup(group),
      [Buffer.from('{"url":"s3://small"}'), Buffer.from('{"message":"too large"}')]
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(flow.childResults("image-1", { groupId: "resizes", partitionKey: "tenant-a" })).resolves.toEqual({
      children: [
        { id: "image-1:huge", partitionKey: "tenant-b", status: "running" },
        { error: { message: "too large" }, id: "image-1:large", partitionKey: "tenant-a", status: "failed" },
        { id: "image-1:small", partitionKey: "tenant-a", result: { url: "s3://small" }, status: "completed" }
      ],
      groupId: "resizes",
      resolved: "failure",
      summary: { cancelled: 0, completed: 1, failed: 1, total: 3 },
      wait: "all"
    });
    expect(executor.calls).toEqual([
      ["FLOW.GET", "image-1", "PARTITION", "tenant-a"],
      ["FLOW.VALUE.MGET", "ref-small", "ref-large"]
    ]);
  });

  it("returns undefined for a missing parent or group", async () => {
    const executor = new FakeExecutor([null, parentWithGroup(new Map())]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(flow.childResults("missing")).resolves.toBeUndefined();
    await expect(flow.childResults("image-1", { groupId: "other" })).resolves.toBeUndefined();
  });

  it("reads the current Flow's group from a workflow handler", async () => {
    const group = new Map<unknown, unknown>([
      ["wait", "all"],
      ["resolved", "success"],
      ["summary", { cancelled: 0, completed: 1, failed: 0, total: 1 }],
      ["children", { "image-1:small": "completed" }],
      ["child_partitions", { "image-1:small": "tenant-a" }],
      ["results", { "image-1:small": { result_ref: "ref-small", status: "completed" } }]
    ]);
    const claimed = claimedImage();
    claimed.set("state", "resized");
    const executor = new FakeExecutor([
      [claimed],
      parentWithGroup(group),
      [Buffer.from('{"url":"s3://small"}')],
      Buffer.from("OK")
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const image = new WorkflowClient(flow).workflow({ type: "image", worker: "worker-1" });
    let urls: unknown[] = [];

    image.state("resized", async (ctx) => {
      const resizes = await ctx.flow.childResults("resizes");
      urls = resizes.children.map((child) => (child.result as { url: string }).url);
      return complete({ result: { urls } });
    });

    await image.worker({ batchSize: 1, states: ["resized"] }).runOnce();

    expect(urls).toEqual(["s3://small"]);
    expect(executor.calls[1]).toEqual(["FLOW.GET", "image-1", "PARTITION", "tenant-a"]);
  });

  it("fails clearly when the current Flow has no such group", async () => {
    const claimed = claimedImage();
    const parent = parentWithGroup(new Map());
    parent.set("child_groups", new Map());
    const executor = new FakeExecutor([[claimed], parent, Buffer.from("OK")]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const image = new WorkflowClient(flow).workflow({ type: "image", worker: "worker-1" });
    let failure: unknown;

    image.state("received", async (ctx) => {
      try {
        await ctx.flow.childResults("resizes");
      } catch (error) {
        failure = error;
      }
      return complete();
    });

    await image.worker({ batchSize: 1, states: ["received"] }).runOnce();

    expect(String(failure)).toContain("Flow image-1 has no child group resizes");
  });
});

describe("FlowRecord result", () => {
  it("decodes a terminal Flow's result when the read returns it", async () => {
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([
        ["id", Buffer.from("image-1")],
        ["type", Buffer.from("image")],
        ["state", Buffer.from("completed")],
        ["result", Buffer.from('{"urls":["s3://small"]}')]
      ]),
      new Map<unknown, unknown>([
        ["id", Buffer.from("image-2")],
        ["type", Buffer.from("image")],
        ["state", Buffer.from("received")]
      ])
    ]);
    const flow = new FerricStoreClient(executor, { codec: new JsonCodec() });

    expect((await flow.get("image-1", { full: true }))?.result).toEqual({ urls: ["s3://small"] });
    expect(await flow.get("image-2", { full: true })).not.toHaveProperty("result");
  });
});
