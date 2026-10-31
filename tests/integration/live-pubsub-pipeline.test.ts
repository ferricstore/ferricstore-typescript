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

  it("publishes to shard channels, which this client cannot subscribe to", async () => {
    const client = await integrationClient();
    const channel = `ts-sdk:shard:${suffix()}`;

    try {
      // SSUBSCRIBE, like SUBSCRIBE, needs a pinned connection: refused unsent.
      await expect(client.command("SSUBSCRIBE", channel)).rejects.toThrow(/pinned connection/);
      await expect(client.command("SUNSUBSCRIBE", channel)).rejects.toThrow(/pinned connection/);
      await expect(client.spublish(channel, "shard")).resolves.toBe(0);
      await expect(client.pubsubShardChannels(channel)).resolves.toEqual([]);
      await expect(client.pubsubShardNumSub(channel)).resolves.toEqual([Buffer.from(channel), 0]);
    } finally {
      await client.close();
    }
  });
});
