import { describe, expect, it } from "vitest";
import { integrationClient } from "./live-support.js";

describe("FerricStore Flow retention integration", () => {
  it("runs retention cleanup on an isolated store", async () => {
    const client = await integrationClient();
    try {
      await expect(client.retentionCleanup()).resolves.toBeTypeOf("object");
    } finally {
      await client.close();
    }
  });
});
