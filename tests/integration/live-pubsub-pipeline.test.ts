import { describe, expect, it } from "vitest";

import { FerricStoreClient } from "../../src/index.js";
import { suffix, url } from "./live-support.js";

describe("FerricStore compact Pub/Sub pipeline integration", () => {
  it("publishes to multiple channels in one physical pipeline", async () => {
    const client = await FerricStoreClient.fromUrl(url());
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
});
