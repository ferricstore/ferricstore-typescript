import type { FerricStoreClient } from "./client.js";
import { setLongInterval, type LongTimer } from "./internal.js";
import {
  CLAIMED_ITEM_WIRE,
  type ClaimedItem,
  type FlowRecord,
  type WorkerConfig
} from "./types.js";

type LeasedJob = FlowRecord | ClaimedItem;

/** Raised when a worker loses the ability to renew a claimed job's lease. */
export class LeaseRenewalError extends Error {
  constructor(cause: unknown) {
    super("FerricStore lease renewal failed", { cause });
    this.name = "LeaseRenewalError";
  }
}

/** Keeps a claim alive and safely hands renewal across lease-rotating mutations. */
export class LeaseRenewalGuard {
  private error?: LeaseRenewalError;
  private inFlight?: Promise<void>;
  private currentJob: ClaimedItem;
  private readonly intervalMs: number;
  private readonly renewalEnabled: boolean;
  private state: "active" | "paused" | "stopped" = "active";
  private timer?: LongTimer;

  constructor(
    private readonly client: FerricStoreClient,
    leasedJob: LeasedJob,
    private readonly leaseMs: number,
    options: WorkerConfig
  ) {
    this.currentJob = snapshotLeasedJob(leasedJob);
    this.renewalEnabled = options.leaseRenewal !== false;
    this.intervalMs = positiveInteger(options.leaseRenewIntervalMs, Math.max(1, Math.trunc(leaseMs / 2)));
    this.startRenewal();
  }

  get job(): ClaimedItem {
    return this.currentJob;
  }

  assertActive(): void {
    if (this.error != null) throw this.error;
  }

  async stop(): Promise<void> {
    this.state = "stopped";
    this.cancelRenewal();
    await this.inFlight;
    this.assertActive();
  }

  async pauseForLeaseMutation(): Promise<void> {
    if (this.state === "stopped") throw new Error("lease renewal guard is stopped");
    if (this.state === "paused") return;
    this.state = "paused";
    this.cancelRenewal();
    await this.inFlight;
    this.assertActive();
  }

  resumeWith(job: ClaimedItem): void {
    if (this.state !== "paused") throw new Error("lease renewal guard is not paused");
    this.currentJob = snapshotLeasedJob(job);
    this.state = "active";
    this.startRenewal();
  }

  private cancelRenewal(): void {
    if (this.timer == null) return;
    this.timer.cancel();
    this.timer = undefined;
  }

  private startRenewal(): void {
    if (!this.renewalEnabled || this.state !== "active") return;
    this.timer = setLongInterval(() => this.renew(), this.intervalMs);
    this.timer.unref();
  }

  private renew(): void {
    if (this.state !== "active" || this.error != null || this.inFlight != null) return;
    const renewal = this.client.extendLease(this.job.id, {
      fencingToken: this.job.fencingToken,
      leaseMs: this.leaseMs,
      leaseToken: this.job.leaseToken,
      partitionKey: this.job.partitionKey,
      returnOkOnSuccess: true
    }).then(
      () => undefined,
      (error: unknown) => {
        this.error = new LeaseRenewalError(error);
        this.cancelRenewal();
      }
    );
    this.inFlight = renewal;
    void renewal.finally(() => {
      if (this.inFlight === renewal) this.inFlight = undefined;
    });
  }
}

function snapshotLeasedJob(job: LeasedJob): ClaimedItem {
  const leaseToken = Buffer.from(job.leaseToken);
  const snapshot: ClaimedItem = {
    fencingToken: job.fencingToken,
    id: job.id,
    leaseToken,
    partitionKey: job.partitionKey,
    runState: job.runState,
    state: job.state,
    type: job.type
  };
  const wire = (job as ClaimedItem)[CLAIMED_ITEM_WIRE];
  if (wire != null) {
    Object.defineProperty(snapshot, CLAIMED_ITEM_WIRE, {
      enumerable: false,
      value: { ...wire, leaseToken }
    });
  }
  return snapshot;
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return value == null || !Number.isFinite(value) ? fallback : Math.max(1, Math.trunc(value));
}
