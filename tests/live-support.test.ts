import { describe, expect, it } from "vitest";
import { eventually, waitForAclProjection } from "./integration/live-support.js";

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

  it("waits through the fail-closed ACL projection transition", async () => {
    let attempts = 0;

    const result = await waitForAclProjection(async () => {
      attempts += 1;
      if (attempts < 3) throw new Error("NOPERM ACL catalog projection unavailable");
      return "default";
    }, { intervalMs: 0, timeoutMs: 100 });

    expect(result).toBe("default");
    expect(attempts).toBe(3);
  });

  it("does not retry unrelated permission failures", async () => {
    let attempts = 0;

    await expect(waitForAclProjection(async () => {
      attempts += 1;
      throw new Error("NOPERM command denied by ACL");
    }, { intervalMs: 0, timeoutMs: 100 })).rejects.toThrow("NOPERM command denied by ACL");

    expect(attempts).toBe(1);
  });
});
