import { FlowClient, JsonCodec, WorkflowClient, complete, transition } from "../src/index.js";

const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const workflows = new WorkflowClient(flow);

const image = workflows.workflow({
  initialState: "received",
  type: "image",
  worker: "image-parent-worker-1"
});

image.state("received", async (ctx) => {
  await ctx.flow.spawnChildren(
    [
      { id: `${ctx.id}:small`, type: "resize", payload: { imageId: ctx.id, size: "small" } },
      { id: `${ctx.id}:large`, type: "resize", payload: { imageId: ctx.id, size: "large" } }
    ],
    { wait: "all" }
  );

  return transition("waiting_for_resizes");
});

image.state("waiting_for_resizes", async () => {
  return complete({ result: { fanoutStarted: true } });
});

await image.start("image-1", {
  idempotent: true,
  payload: { uploadId: "upload-1" }
});

console.log(await image.worker({ batchSize: 5, states: ["received", "waiting_for_resizes"] }).runOnce());

await flow.close();
