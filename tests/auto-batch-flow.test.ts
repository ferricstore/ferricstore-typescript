import { describe, expect, it } from "vitest";

import { maybeAutoBatchExecutor } from "../src/auto-batch.js";
import type { Command } from "../src/internal.js";

describe("Flow auto-batch ordering", () => {
  it("runs unrelated extended create-many batches concurrently", async () => {
    const calls: Command[][] = [];
    const releases: (() => void)[] = [];
    const executor = maybeAutoBatchExecutor({
      async executeCommand() {
        throw new Error("unexpected individual command");
      },
      async executePipeline(commands) {
        calls.push([...commands]);
        await new Promise<void>((resolve) => releases.push(resolve));
        return commands.map(() => Buffer.from("OK"));
      }
    }, { enabled: true, maxCommands: 1, maxDelayMs: 0 });

    const first = executor.executeCommandArgs?.(extendedCreateMany("flow-a"));
    if (first == null) throw new TypeError("auto-batch executor requires array dispatch");
    await waitFor(() => calls.length === 1);
    const second = executor.executeCommandArgs?.(extendedCreateMany("flow-b"));
    if (second == null) throw new TypeError("auto-batch executor requires array dispatch");
    await new Promise<void>((resolve) => setImmediate(resolve));
    await new Promise<void>((resolve) => setImmediate(resolve));
    const startedBeforeFirstCompleted = calls.length;

    releases.shift()?.();
    await waitFor(() => calls.length === 2);
    releases.shift()?.();
    await Promise.all([first, second]);

    expect(startedBeforeFirstCompleted).toBe(2);
  });
});

function extendedCreateMany(id: string): Command {
  return [
    "FLOW.CREATE_MANY",
    "AUTO",
    "TYPE",
    "job",
    "STATE",
    "queued",
    "NOW",
    1,
    "ITEMS_EXT",
    1,
    id,
    "-",
    Buffer.from("payload"),
    0,
    0
  ];
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error("condition was not reached");
}
