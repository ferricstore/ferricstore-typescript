import { FlowClient, JsonCodec, QueueClient, retry } from "../src/index.js";

const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const queue = new QueueClient(flow).queue({ type: "thumbnail", worker: "thumbnail-worker-1" });

await queue.enqueue("thumbnail-1", {
  idempotent: true,
  payload: { imageId: "img-1", size: "small" },
  retentionTtlMs: 86_400_000
});

const result = await queue.worker({ batchSize: 25, reclaimExpired: true }).runOnce(async (job) => {
  if (job.payload == null) {
    return retry({ error: "missing payload", runAtMs: Date.now() + 10_000 });
  }
  return { generated: true, jobId: job.id };
});

console.log(result);

await flow.close();
