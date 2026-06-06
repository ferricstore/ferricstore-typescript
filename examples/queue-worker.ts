import { FlowClient, JsonCodec, QueueClient } from "../src/index.js";

const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "redis://127.0.0.1:6379/0", {
  codec: new JsonCodec()
});

const emails = new QueueClient(flow).queue("email");

await emails.enqueue("email-1", {
  idempotent: true,
  payload: { template: "welcome", userId: "user-1" }
});

const result = await emails.worker({ batchSize: 100, worker: "email-worker-1" }).runOnce(async (job) => {
  console.log("send", job.id, job.payload);
  return { sent: true };
});

console.log(result);

await flow.close();
