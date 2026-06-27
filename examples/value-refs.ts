import { FerricStoreClient, JsonCodec, WorkflowClient, complete } from "../src/index.js";

const flow = await FerricStoreClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const rawRef = await flow.valuePut(
  {
    plan: "enterprise",
    userId: "user-1"
  },
  { name: "profile", ttlMs: 3_600_000 }
);
const profileRef = Buffer.isBuffer(rawRef) ? rawRef.toString("utf8") : String(rawRef);

const account = new WorkflowClient(flow).workflow({
  initialState: "hydrate",
  type: "account"
});

account.state("hydrate", async (ctx) => {
  const profile = (await ctx.value("profile")) as { plan?: string; userId?: string } | undefined;
  return complete({
    result: {
      plan: profile?.plan,
      userId: profile?.userId
    }
  });
});

await account.start("account-1", {
  idempotent: true,
  valueRefs: { profile: profileRef }
});

console.log(await account.worker({ batchSize: 10, states: ["hydrate"] }).runOnce());

await flow.close();
