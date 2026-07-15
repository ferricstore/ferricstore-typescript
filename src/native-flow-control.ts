import { OverloadedError, RequestTimeoutError } from "./errors.js";
import { setLongTimeout, type LongTimer } from "./internal.js";

const DEFAULT_MAX_INFLIGHT_PER_CONNECTION = 4_096;
const DEFAULT_MAX_INFLIGHT_PER_LANE = 1_024;
const WAITER_COMPACTION_THRESHOLD = 1_024;

interface FlowControlWaiter {
  reject: (reason: unknown) => void;
  resolve: () => void;
  settled: boolean;
  timer: LongTimer;
}

interface FlowControlLaneQueue {
  head: number;
  waiting: number;
  readonly waiters: FlowControlWaiter[];
}

/** Server-advertised native request credits with an allocation-free fast path. */
export class NativeFlowControl {
  private activeTotal = 0;
  private readonly activeByLane = new Map<number, number>();
  private closedError?: Error;
  private connectionLimit = DEFAULT_MAX_INFLIGHT_PER_CONNECTION;
  private laneQueueLimit = Number.MAX_SAFE_INTEGER;
  private laneLimit = DEFAULT_MAX_INFLIGHT_PER_LANE;
  private laneWindowLimit = DEFAULT_MAX_INFLIGHT_PER_LANE;
  private readonly queuesByLane = new Map<number, FlowControlLaneQueue>();
  private readonly readyLanes = new Set<number>();
  private queuedTotal = 0;

  constructor(private readonly maxQueuedRequests: number) {}

  tryAcquire(laneId: number): boolean {
    if (
      this.closedError != null ||
      this.activeTotal >= this.connectionLimit ||
      (this.activeByLane.get(laneId) ?? 0) >= this.laneLimit
    ) return false;
    this.activate(laneId);
    return true;
  }

  wait(laneId: number, timeoutMs: number): Promise<void> {
    if (this.closedError != null) return Promise.reject(this.closedError);
    if (this.laneQueueLimit === 0) {
      return Promise.reject(new OverloadedError("FerricStore server lane queue is disabled", {
        reason: "client_lane_queue_full"
      }));
    }
    if (this.queuedTotal >= this.maxQueuedRequests) {
      return Promise.reject(new OverloadedError("FerricStore client request queue is full", {
        reason: "client_queue_full"
      }));
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setLongTimeout(() => {
        if (waiter.settled) return;
        waiter.settled = true;
        this.queuedTotal -= 1;
        const queue = this.queuesByLane.get(laneId);
        if (queue != null) {
          queue.waiting -= 1;
          if (queue.waiting === 0) {
            this.queuesByLane.delete(laneId);
            this.readyLanes.delete(laneId);
          } else {
            this.compactWaiters(queue);
          }
        }
        reject(new RequestTimeoutError(timeoutMs, "unsent"));
      }, timeoutMs);
      timer.unref();
      const waiter: FlowControlWaiter = { reject, resolve, settled: false, timer };
      const queue = this.queuesByLane.get(laneId);
      if (queue == null) {
        this.queuesByLane.set(laneId, { head: 0, waiting: 1, waiters: [waiter] });
        this.readyLanes.add(laneId);
      } else {
        queue.waiting += 1;
        queue.waiters.push(waiter);
      }
      this.queuedTotal += 1;
    });
  }

  release(laneId: number): void {
    const active = this.activeByLane.get(laneId) ?? 0;
    if (active <= 0) return;
    this.activeTotal -= 1;
    if (active === 1) this.activeByLane.delete(laneId);
    else this.activeByLane.set(laneId, active - 1);
    if (this.closedError == null) this.pump();
  }

  updateLimits(connectionLimit?: number, laneLimit?: number): void {
    if (connectionLimit != null) this.connectionLimit = connectionLimit;
    if (laneLimit != null) this.laneWindowLimit = laneLimit;
    this.laneLimit = Math.min(this.laneWindowLimit, this.laneQueueLimit);
    this.pump();
  }

  updateLaneQueueLimit(laneQueueLimit: number): void {
    this.laneQueueLimit = laneQueueLimit;
    this.laneLimit = Math.min(this.laneWindowLimit, this.laneQueueLimit);
    this.pump();
  }

  cancelQueued(error: Error): void {
    for (const queue of this.queuesByLane.values()) {
      for (let index = queue.head; index < queue.waiters.length; index += 1) {
        const waiter = queue.waiters[index];
        if (waiter == null || waiter.settled) continue;
        waiter.settled = true;
        waiter.timer.cancel();
        waiter.reject(error);
      }
    }
    this.queuesByLane.clear();
    this.readyLanes.clear();
    this.queuedTotal = 0;
  }

  close(error: Error): void {
    if (this.closedError != null) return;
    this.closedError = error;
    this.cancelQueued(error);
  }

  private activate(laneId: number): void {
    this.activeTotal += 1;
    this.activeByLane.set(laneId, (this.activeByLane.get(laneId) ?? 0) + 1);
  }

  private pump(): void {
    while (this.closedError == null && this.activeTotal < this.connectionLimit && this.readyLanes.size > 0) {
      const laneId = this.nextGrantableLane();
      if (laneId == null) return;
      const queue = this.queuesByLane.get(laneId);
      if (queue == null) continue;
      const waiter = this.takeWaiter(queue);
      if (waiter == null) {
        this.queuesByLane.delete(laneId);
        continue;
      }
      queue.waiting -= 1;
      this.queuedTotal -= 1;
      waiter.settled = true;
      if (queue.waiting > 0) {
        this.compactWaiters(queue);
        this.readyLanes.add(laneId);
      } else {
        this.queuesByLane.delete(laneId);
      }
      waiter.timer.cancel();
      this.activate(laneId);
      waiter.resolve();
    }
  }

  private nextGrantableLane(): number | undefined {
    const attempts = this.readyLanes.size;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const laneId = this.readyLanes.values().next().value;
      if (laneId == null) return undefined;
      this.readyLanes.delete(laneId);
      const queue = this.queuesByLane.get(laneId);
      if (queue == null || queue.waiting === 0) continue;
      if ((this.activeByLane.get(laneId) ?? 0) < this.laneLimit) return laneId;
      this.readyLanes.add(laneId);
    }
    return undefined;
  }

  private takeWaiter(queue: FlowControlLaneQueue): FlowControlWaiter | undefined {
    while (queue.head < queue.waiters.length) {
      const waiter = queue.waiters[queue.head++];
      if (waiter != null && !waiter.settled) return waiter;
    }
    return undefined;
  }

  private compactWaiters(queue: FlowControlLaneQueue): void {
    const remainingSlots = queue.waiters.length - queue.head;
    const tombstones = Math.max(0, remainingSlots - queue.waiting);
    const compactConsumed = queue.head >= WAITER_COMPACTION_THRESHOLD
      && queue.head * 2 >= queue.waiters.length;
    const compactTombstones = tombstones >= WAITER_COMPACTION_THRESHOLD
      && tombstones * 2 >= remainingSlots;
    if (!compactConsumed && !compactTombstones) return;

    let writeIndex = 0;
    for (let index = queue.head; index < queue.waiters.length; index += 1) {
      const waiter = queue.waiters[index];
      if (waiter != null && !waiter.settled) {
        queue.waiters[writeIndex] = waiter;
        writeIndex += 1;
      }
    }
    queue.waiters.length = writeIndex;
    queue.head = 0;
  }
}
