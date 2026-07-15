import { FerricStoreClient, JsonCodec } from "../src/index.js";

const client = await FerricStoreClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
  codec: new JsonCodec()
});

const suffix = Date.now().toString(36);

await client.kv.set(`session:${suffix}`, { userId: "user-1" }, { px: 60_000 });
await client.hash.hset(`user:${suffix}`, { email: "ada@example.com", plan: "pro" });
await client.lists.rpush(`jobs:${suffix}`, { id: "job-1" }, { id: "job-2" });
await client.sets.sadd(`seen:${suffix}`, "user-1", "user-2");
await client.zset.zadd(`scores:${suffix}`, [{ member: "user-1", score: 42 }]);
await client.stream.xadd(`events:${suffix}`, "*", { id: "evt-1", type: "created" });
await client.kv.set(`json:user:${suffix}`, { id: "user-1", flags: ["beta"] });
await client.bloom.add(`bf:seen:${suffix}`, "user-1");
await client.tdigest.create(`latency:${suffix}`);
await client.tdigest.add(`latency:${suffix}`, 12, 18, 31);

console.log({
  json: await client.kv.get(`json:user:${suffix}`),
  session: await client.kv.get(`session:${suffix}`),
  tdigestP95: await client.tdigest.quantile(`latency:${suffix}`, 0.95)
});

await client.close();
