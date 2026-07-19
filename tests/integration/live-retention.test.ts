import { describe, expect, it } from "vitest";
import { FerricStoreClient } from "../../src/index.js";
import { url } from "./live-support.js";

describe("FerricStore Flow retention integration", () => {
  it("runs retention cleanup on an isolated store", async () => {
    const client = await FerricStoreClient.fromUrl(url());
    try {
      await expect(client.retentionCleanup()).resolves.toBeTypeOf("object");
    } finally {
      await client.close();
    }
  });
});
