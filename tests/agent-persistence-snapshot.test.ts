import { describe, expect, it } from "vitest";

import { encodeSnapshot, snapshotDigest } from "../src/agent-persistence/snapshot.js";

describe("agent persistence snapshots", () => {
  it("orders object keys without depending on the process locale", () => {
    const first = { z: 1, "ä": 2 };
    const second = { "ä": 2, z: 1 };
    const encoded = JSON.parse(encodeSnapshot(first).toString("utf8")) as unknown;

    expect(encoded).toEqual([
      "object",
      [["z", ["number", 1]], ["ä", ["number", 2]]]
    ]);
    expect(snapshotDigest(first)).toBe(snapshotDigest(second));
  });
});
