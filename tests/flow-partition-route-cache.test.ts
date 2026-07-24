import { Buffer } from "node:buffer";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  flowLogicalPartitionRoutingKey,
  flowPartitionRouteCacheStats,
  resetFlowPartitionRouteCache
} from "../src/flow-partition-route-cache.js";

describe("Flow partition route cache", () => {
  beforeEach(() => resetFlowPartitionRouteCache());

  it("does not retain oversized partition keys", () => {
    for (let index = 0; index < 64; index += 1) {
      const key = Buffer.alloc(65_535, index);
      key.writeUInt32BE(index, 0);
      expect(flowLogicalPartitionRoutingKey(key)).toMatch(/^\{f:[A-Za-z0-9_-]+\}$/u);
    }

    expect(flowPartitionRouteCacheStats()).toMatchObject({ bytes: 0, entries: 0 });
  });

  it("keeps cached storage within both entry and byte budgets", () => {
    for (let index = 0; index < 2_000; index += 1) {
      flowLogicalPartitionRoutingKey(`partition-${index}-${"x".repeat(1_024)}`);
    }

    const stats = flowPartitionRouteCacheStats();
    expect(stats.entries).toBeLessThanOrEqual(stats.maxEntries);
    expect(stats.bytes).toBeLessThanOrEqual(stats.maxBytes);
    expect(stats.entries).toBeLessThan(2_000);
  });

  it("retains the short repeated-key fast path without stale mutable buffers", () => {
    const short = Buffer.from("tenant-a");
    const first = flowLogicalPartitionRoutingKey(short);
    for (let index = 0; index < 1_000; index += 1) {
      expect(flowLogicalPartitionRoutingKey(short)).toBe(first);
    }
    expect(flowPartitionRouteCacheStats()).toMatchObject({ entries: 1, hits: 1_000, misses: 1 });

    short.write("tenant-b");
    expect(flowLogicalPartitionRoutingKey(short)).not.toBe(first);
  });

  it("recognizes binary auto partitions without converting the full value to text", () => {
    const auto = Buffer.from("__flow_auto__:255");
    const toString = vi.spyOn(auto, "toString");

    expect(flowLogicalPartitionRoutingKey(auto)).toBe("{fa:255}");
    expect(toString).not.toHaveBeenCalled();
  });
});
