import { sleep } from "./internal.js";
import type { QueueHandler, QueueWorkerResult } from "./queue.js";
import type { WorkerConfig } from "./types.js";
import {
  nextWorkerIdleSleepMs,
  workerIdleSleepMs,
  workerMaxIdleSleepMs,
  workerSignalAborted
} from "./worker-internal.js";

interface WaveWorker {
  flush(): Promise<number>;
  runOnce(handler: QueueHandler): Promise<QueueWorkerResult>;
}

/** Run claim waves with bounded idle backoff until cancellation. */
export async function runQueueWorkerInWaves(
  worker: WaveWorker,
  handler: QueueHandler,
  options: WorkerConfig
): Promise<void> {
  const idleSleepMs = workerIdleSleepMs(options);
  let currentIdleSleepMs = idleSleepMs;
  const maxIdleSleepMs = workerMaxIdleSleepMs(options, idleSleepMs);

  try {
    while (!workerSignalAborted(options.signal)) {
      const result = await worker.runOnce(handler);
      if (result.claimed === 0) {
        if (workerSignalAborted(options.signal)) break;
        try {
          await sleep(currentIdleSleepMs, options.signal);
        } catch (error) {
          if (!workerSignalAborted(options.signal)) throw error;
          break;
        }
        currentIdleSleepMs = nextWorkerIdleSleepMs(currentIdleSleepMs, maxIdleSleepMs);
      } else {
        currentIdleSleepMs = idleSleepMs;
      }
    }
  } finally {
    await worker.flush();
  }
}
