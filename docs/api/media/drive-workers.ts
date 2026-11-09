import type { FerricStoreClient, FlowRecord } from "../src/index.js";

const TERMINAL_STATES = new Set(["completed", "failed", "cancelled"]);

/**
 * Drives workers in one process until a Flow is terminal, so an example can
 * print its result. Production workers are long-running `worker().run()`
 * loops, usually in separate services.
 */
export async function runUntilTerminal(
  flow: FerricStoreClient,
  id: string,
  workers: readonly { runOnce(): Promise<unknown> }[],
  maxRounds = 100
): Promise<FlowRecord> {
  for (let round = 0; round < maxRounds; round += 1) {
    const record = await flow.get(id, { full: true });
    if (record != null && TERMINAL_STATES.has(record.state)) return record;
    for (const worker of workers) await worker.runOnce();
  }
  throw new Error(`Flow ${id} did not finish after ${maxRounds} worker rounds`);
}
