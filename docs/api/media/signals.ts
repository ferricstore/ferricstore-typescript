import { FlowClient, JsonCodec, WorkflowClient, complete, retry } from "../src/index.js";

const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const review = new WorkflowClient(flow).workflow({
  initialState: "pending_review",
  type: "document_review",
  worker: "review-worker-1"
});

review.state("pending_review", async () => {
  return retry({ error: "waiting for signal", runAtMs: Date.now() + 60_000 });
});

review.state(
  "approved",
  async (ctx) => {
    return complete({
      result: {
        approvedBy: await ctx.value("approvedBy"),
        approvedAt: await ctx.value("approvedAt")
      }
    });
  },
  { claimValues: ["approvedBy", "approvedAt"] }
);

await review.start("review-1", {
  idempotent: true,
  payload: { documentId: "doc-1" }
});

await review.signal("review-1", {
  ifState: "pending_review",
  signal: "approve",
  transitionTo: "approved",
  values: {
    approvedAt: new Date().toISOString(),
    approvedBy: "user-1"
  }
});

console.log(await review.worker({ batchSize: 10, states: ["approved"] }).runOnce());

await flow.close();
