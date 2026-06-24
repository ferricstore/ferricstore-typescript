import { FlowClient, JsonCodec, WorkflowClient, complete, transition } from "../src/index.js";

const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const order = new WorkflowClient(flow).workflow({
  initialState: "created",
  type: "order",
  worker: "order-worker-1"
});

order.state("created", async (ctx) => {
  console.log("charge", ctx.id, ctx.payload);
  return transition("charged");
});

order.state("charged", async (ctx) => {
  console.log("receipt", ctx.id);
  return complete({ result: { ok: true } });
});

await order.start("order-1", {
  idempotent: true,
  payload: { amount: 42, userId: "user-1" }
});

const result = await order.worker({ batchSize: 10, states: ["created", "charged"] }).runOnce();
console.log(result);

await flow.close();
