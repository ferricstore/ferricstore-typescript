import { setLongInterval, type LongTimer } from "./internal.js";

export class NativeHeartbeat {
  private generation = 0;
  private inFlightGeneration?: number;
  private lastInboundActivityMs = Date.now();
  private timer?: LongTimer;

  constructor(
    private readonly intervalMs: number | undefined,
    private readonly send: () => Promise<void>,
    private readonly fail: (error: unknown) => void
  ) {}

  recordInbound(): void {
    this.lastInboundActivityMs = Date.now();
  }

  start(): void {
    if (this.intervalMs == null || this.timer != null) return;
    const generation = ++this.generation;
    this.timer = setLongInterval(() => { void this.tick(generation); }, this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    this.generation += 1;
    this.timer?.cancel();
    this.timer = undefined;
  }

  private async tick(generation: number): Promise<void> {
    if (
      generation !== this.generation
      || this.inFlightGeneration === generation
      || this.intervalMs == null
      || Date.now() - this.lastInboundActivityMs < this.intervalMs
    ) return;
    this.inFlightGeneration = generation;
    try {
      await this.send();
    } catch (error) {
      if (generation === this.generation) this.fail(error);
    } finally {
      if (this.inFlightGeneration === generation) this.inFlightGeneration = undefined;
    }
  }
}
