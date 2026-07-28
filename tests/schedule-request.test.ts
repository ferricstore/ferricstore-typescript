import { describe, expect, it } from "vitest";

import { FerricStoreClient } from "../src/index.js";
import type { CommandExecutor } from "../src/index.js";

function clientThatMustNotExecute(): { client: FerricStoreClient; calls: unknown[][] } {
  const calls: unknown[][] = [];
  const executor: CommandExecutor = {
    async executeCommand(...args): Promise<never> {
      calls.push(args);
      throw new Error("invalid schedule request reached transport");
    }
  };
  return { client: new FerricStoreClient(executor), calls };
}

describe("schedule request validation", () => {
  it.each([
    ["empty id", "id", "", { target: { type: "task" } }],
    ["missing target", "target", "daily", {}],
    ["array target", "target", "daily", { target: [] }],
    ["missing target type", "target type", "daily", { target: { state: "queued" } }],
    ["unknown target field", "unknown", "daily", { target: { type: "task", unknown: true } }],
    [
      "ambiguous target id",
      "id_prefix",
      "daily",
      { target: { id: "fixed", id_prefix: "generated", type: "task" } }
    ],
    [
      "recurring fixed target id",
      "target id",
      "daily",
      { everyMs: 1, target: { id: "fixed", type: "task" } }
    ],
    [
      "unsafe target timestamp",
      "run_at_ms",
      "daily",
      { target: { run_at_ms: Number.MAX_SAFE_INTEGER + 1, type: "task" } }
    ],
    [
      "invalid target priority",
      "priority",
      "daily",
      { target: { priority: 3, type: "task" } }
    ],
    ["invalid kind", "kind", "daily", { kind: "weekly", target: { type: "task" } }],
    ["delay missing delay", "delay_ms", "daily", { kind: "delay", target: { type: "task" } }],
    [
      "delay timestamp overflow",
      "delay_ms",
      "daily",
      { delayMs: 2, nowMs: Number.MAX_SAFE_INTEGER, target: { type: "task" } }
    ],
    [
      "interval missing period",
      "every_ms",
      "daily",
      { kind: "interval", target: { type: "task" } }
    ],
    ["invalid interval period", "every_ms", "daily", { everyMs: 0, target: { type: "task" } }],
    ["cron missing expression", "cron", "daily", { kind: "cron", target: { type: "task" } }],
    [
      "timezone on interval",
      "timezone",
      "daily",
      { everyMs: 1, target: { type: "task" }, timezone: "UTC" }
    ],
    [
      "timing field from another kind",
      "every_ms",
      "daily",
      { everyMs: 1, kind: "one_shot", target: { type: "task" } }
    ],
    [
      "ambiguous absolute start",
      "start_at_ms",
      "daily",
      { atMs: 1, everyMs: 1, startAtMs: 2, target: { type: "task" } }
    ],
    [
      "one-shot catch-up policy",
      "catchup_policy",
      "daily",
      { catchupPolicy: "fire_once", target: { type: "task" } }
    ],
    [
      "one-shot overlap policy",
      "overlap_policy",
      "daily",
      { overlapPolicy: "skip", target: { type: "task" } }
    ],
    [
      "one-shot overlap retry",
      "overlap_retry_ms",
      "daily",
      { overlapRetryMs: 1, target: { type: "task" } }
    ],
    [
      "unused overlap retry",
      "overlap_retry_ms",
      "daily",
      { everyMs: 1, overlapPolicy: "allow", overlapRetryMs: 1, target: { type: "task" } }
    ],
    [
      "end before first run",
      "end_at_ms",
      "daily",
      { endAtMs: 9, everyMs: 1, startAtMs: 10, target: { type: "task" } }
    ],
    [
      "one-shot max fires",
      "max_fires",
      "daily",
      { maxFires: 1, target: { type: "task" } }
    ]
  ])("rejects %s before transport", async (_name, message, id, options) => {
    const { client, calls } = clientThatMustNotExecute();

    await expect(client.scheduleCreate(id, options as never)).rejects.toThrow(message);
    expect(calls).toEqual([]);
  });

  it.each([
    ["manual fire id", "id", (client: FerricStoreClient) => client.scheduleFire("")],
    [
      "manual fire timestamp",
      "fire_at_ms",
      (client: FerricStoreClient) => client.scheduleFire("daily", { fireAtMs: -1 })
    ],
    [
      "pause timestamp",
      "now_ms",
      (client: FerricStoreClient) => client.schedulePause("daily", { nowMs: -1 })
    ],
    [
      "due lease",
      "lease_ms",
      (client: FerricStoreClient) => client.scheduleFireDue({ leaseMs: 0 })
    ],
    [
      "due timestamp overflow",
      "lease_ms",
      (client: FerricStoreClient) =>
        client.scheduleFireDue({ leaseMs: 1, nowMs: Number.MAX_SAFE_INTEGER })
    ],
    [
      "due count",
      "limit",
      (client: FerricStoreClient) => client.scheduleFireDue({ limit: 0 })
    ],
    [
      "list range",
      "from_ms",
      (client: FerricStoreClient) => client.scheduleList({ fromMs: -1 })
    ],
    [
      "list state",
      "state",
      (client: FerricStoreClient) =>
        client.scheduleList({ state: "unknown" as never })
    ],
    [
      "list inverted range",
      "from_ms",
      (client: FerricStoreClient) => client.scheduleList({ fromMs: 2, toMs: 1 })
    ],
    [
      "list count",
      "count",
      (client: FerricStoreClient) => client.scheduleList({ count: 0 })
    ]
  ])("rejects invalid %s before transport", async (_name, message, call) => {
    const { client, calls } = clientThatMustNotExecute();

    await expect(call(client)).rejects.toThrow(message);
    expect(calls).toEqual([]);
  });

  it("does not emit the unsupported NOW option for schedule reads", async () => {
    const calls: unknown[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<null> {
        calls.push(args);
        return null;
      }
    };
    const client = new FerricStoreClient(executor);

    await (client.scheduleGet as (...args: unknown[]) => Promise<unknown>)(
      "daily",
      { nowMs: 123 }
    );

    expect(calls).toEqual([["FLOW.SCHEDULE.GET", "daily"]]);
  });
});
