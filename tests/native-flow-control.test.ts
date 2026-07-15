import { afterEach, describe, expect, it, vi } from "vitest";

import { NativeFlowControl } from "../src/native-flow-control.js";

describe("NativeFlowControl", () => {
  afterEach(() => vi.useRealTimers());

  it("bounds historical waiter storage while a saturated lane stays occupied", async () => {
    const laneId = 1;
    const flowControl = new NativeFlowControl(16);
    flowControl.updateLimits(1, 1);
    expect(flowControl.tryAcquire(laneId)).toBe(true);

    let next = flowControl.wait(laneId, 60_000);
    let spare = flowControl.wait(laneId, 60_000);
    for (let grant = 0; grant < 20_000; grant += 1) {
      flowControl.release(laneId);
      await next;
      next = spare;
      spare = flowControl.wait(laneId, 60_000);
    }

    const internals = flowControl as unknown as {
      readonly queuesByLane: Map<number, {
        readonly head: number;
        readonly waiters: readonly unknown[];
        readonly waiting: number;
      }>;
    };
    const queue = internals.queuesByLane.get(laneId);
    expect(queue).toBeDefined();
    expect(queue?.waiting).toBe(2);
    expect((queue?.waiters.length ?? 0) - (queue?.head ?? 0)).toBe(2);
    expect(queue?.waiters.length).toBeLessThan(2_048);

    flowControl.close(new Error("test complete"));
    await Promise.allSettled([next, spare]);
  });

  it("compacts timed-out tombstones behind a live waiter", async () => {
    vi.useFakeTimers();
    const laneId = 1;
    const flowControl = new NativeFlowControl(16);
    flowControl.updateLimits(0, 1);
    const anchor = flowControl.wait(laneId, 60_000);
    const anchorSettled = anchor.catch((error: unknown) => error);

    for (let cycle = 0; cycle < 200; cycle += 1) {
      const timedOut = Array.from({ length: 15 }, () =>
        flowControl.wait(laneId, 0).catch((error: unknown) => error)
      );
      await vi.advanceTimersByTimeAsync(0);
      await Promise.all(timedOut);
    }

    const internals = flowControl as unknown as {
      readonly queuesByLane: Map<number, {
        readonly waiters: readonly unknown[];
        readonly waiting: number;
      }>;
    };
    const queue = internals.queuesByLane.get(laneId);
    expect(queue?.waiting).toBe(1);
    expect(queue?.waiters.length).toBeLessThan(2_048);

    flowControl.close(new Error("test complete"));
    await anchorSettled;
  });
});
