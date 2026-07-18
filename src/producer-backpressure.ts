import { FerricStoreError, OverloadedError } from "./errors.js";
import { sleep } from "./internal.js";
import type { BackpressurePolicy } from "./types.js";

export async function executeProducerWriteWithBackpressure(
  execute: () => Promise<unknown>,
  policy: Required<BackpressurePolicy>,
  signal: AbortSignal
): Promise<unknown> {
  for (let attempt = 0; ; attempt += 1) {
    throwIfClosed(signal);
    try {
      return await execute();
    } catch (error) {
      if (
        !(error instanceof OverloadedError) ||
        error.retryable !== true ||
        error.safeToRetry !== true ||
        attempt >= policy.maxRetries
      ) throw error;
      const exponential = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** attempt);
      const base = Math.min(policy.maxDelayMs, error.retryAfterMs ?? exponential);
      const jitter = base * (policy.jitterPct / 100) * Math.random();
      await sleep(Math.min(policy.maxDelayMs, base + jitter), signal);
    }
  }
}

function throwIfClosed(signal: AbortSignal): void {
  if (!signal.aborted) return;
  if (signal.reason instanceof Error) throw signal.reason;
  throw new FerricStoreError("FerricStore client is closed", { raw: signal.reason });
}
