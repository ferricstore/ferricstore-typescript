import http from "node:http";
import { expect, test } from "vitest";

import { HTTPAdapter } from "../src/index.js";

const BLOCK_MS_ERROR = "blockMs must be a safe non-negative integer no greater than 4294967295";

test("HTTP fallback rejects truncated counted options before dispatch", async () => {
  let requests = 0;
  const server = http.createServer((_request, response) => {
    requests += 1;
    response.statusCode = 500;
    response.end();
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") throw new Error("HTTP server did not expose an address");
  const adapter = await HTTPAdapter.fromUrl(`http://127.0.0.1:${address.port}`);
  try {
    for (const countOption of ["STATES", "PARTITIONS"] as const) {
      for (const blockOption of ["BLOCK", "BLOCK_MS"] as const) {
        for (const args of [
          [countOption, 3, blockOption],
          [countOption, 3, blockOption, false]
        ] as const) {
          await expect(adapter.executeCommand(
            "FLOW.CLAIM_DUE", "email", ...args
          )).rejects.toThrow(BLOCK_MS_ERROR);
        }
      }
    }
    expect(requests).toBe(0);
  } finally {
    await adapter.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
