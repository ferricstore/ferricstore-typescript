import { describe, expect, it } from "vitest";
import { eventually } from "./integration/live-support.js";

describe("live integration support", () => {
  it("retries transient errors and stale values until the predicate is ready", async () => {
    let attempts = 0;

    const result = await eventually(
      async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("projection is building");
        return attempts;
      },
      (value) => value === 3,
      "projection did not become ready",
      { intervalMs: 0, timeoutMs: 100 }
    );

    expect(result).toBe(3);
    expect(attempts).toBe(3);
  });
});
