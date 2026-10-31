import { describe, expect, it } from "vitest";

import { integrationClient, suffix } from "./live-support.js";

describe("FerricStore compact Pub/Sub pipeline integration", () => {
  it("publishes to multiple channels in one physical pipeline", async () => {
    const client = await integrationClient();
    const prefix = `ts-sdk:pubsub-pipeline:${suffix()}:`;

    try {
      await expect(client.pipeline([
        ["PUBLISH", `${prefix}first`, "one"],
        ["PUBLISH", `${prefix}second`, "two"],
        ["PUBLISH", `${prefix}first`, "three"],
      ])).resolves.toEqual([0, 0, 0]);
    } finally {
      await client.close();
    }
  });

  it("publishes to shard channels, which this client cannot subscribe to", async (context) => {
    const client = await integrationClient();
    const channel = `ts-sdk:shard:${suffix()}`;

    try {
      let published: number;
      try {
        published = await client.spublish(channel, "shard");
      } catch (error) {
        // Servers before sharded Pub/Sub (the 0.11 and 0.12.0 release images).
        if (/unknown command/i.test(String(error))) context.skip();
        throw error;
      }
      expect(published).toBe(0);
      await expect(client.pubsubShardChannels(channel)).resolves.toEqual([]);
      const counts = await client.pubsubShardNumSub(channel);
      expect(counts.map((value) => (Buffer.isBuffer(value) ? value.toString() : value))).toEqual([channel, 0]);
      // SSUBSCRIBE, like SUBSCRIBE, needs a pinned connection: refused unsent.
      const refused = /pinned connection|persistent native/;
      await expect(client.command("SSUBSCRIBE", channel)).rejects.toThrow(refused);
      await expect(client.command("SUNSUBSCRIBE", channel)).rejects.toThrow(refused);
    } finally {
      await client.close();
    }
  });
});
