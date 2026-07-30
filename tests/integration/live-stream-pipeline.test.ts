import { describe, expect, it } from "vitest";

import { FerricStoreClient } from "../../src/index.js";
import { suffix, text, url } from "./live-support.js";

describe("FerricStore compact Stream pipeline integration", () => {
  it("spans multiple topics", async () => {
    const client = await FerricStoreClient.fromUrl(url());
    const prefix = `ts-sdk:stream-pipeline:${suffix()}:`;
    const first = `${prefix}{a}:first`;
    const second = `${prefix}{b}:second`;

    try {
      const results = await client.pipeline([
        ["XADD", first, "*", "field", "one"],
        ["XADD", second, "*", "field", "two"],
        ["XADD", first, "*", "field", "three"]
      ]);

      expect(results).toHaveLength(3);
      expect(results.every((result) => text(result).length > 0)).toBe(true);
      await expect(client.stream.xlen(first)).resolves.toBe(2);
      await expect(client.stream.xlen(second)).resolves.toBe(1);
    } finally {
      await client.command("DEL", first).catch(() => undefined);
      await client.command("DEL", second).catch(() => undefined);
      await client.close();
    }
  });
});
