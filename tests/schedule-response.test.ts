import { describe, expect, expectTypeOf, it } from "vitest";

import {
  FerricStoreClient,
  type CommandExecutor,
  type ScheduleFireDueResult,
  type ScheduleFireResult,
  type ScheduleRecord
} from "../src/index.js";

class ResponseExecutor implements CommandExecutor {
  readonly calls: unknown[][] = [];

  constructor(private readonly response: unknown) {}

  async executeCommand(...args: unknown[]): Promise<unknown> {
    this.calls.push(args);
    return this.response;
  }
}

function intervalSchedule(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    attempts: 0,
    catchup_policy: "fire_once",
    coalesced_count: 0,
    created_at_ms: 500,
    cron: null,
    every_ms: 1_000,
    fire_count: 0,
    id: "daily",
    kind: "interval",
    last_coalesced_count: 0,
    overlap_policy: "allow",
    overlap_retry_ms: null,
    skipped_count: 0,
    state: "active",
    target: { id_prefix: "daily", type: "task" },
    timezone: null,
    ...overrides
  };
}

function cronSchedule(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return intervalSchedule({
    catchup_policy: null,
    cron: "* * * * *",
    every_ms: null,
    kind: "cron",
    timezone: "Etc/UTC",
    ...overrides
  });
}

function oneShotSchedule(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return intervalSchedule({
    catchup_policy: null,
    every_ms: null,
    kind: "one_shot",
    ...overrides
  });
}

function withoutField(record: Record<string, unknown>, field: string): Record<string, unknown> {
  return Object.fromEntries(Object.entries(record).filter(([key]) => key !== field));
}

describe("schedule catch-up responses", () => {
  it("types and preserves catch-up schedule fields", async () => {
    const executor = new ResponseExecutor({
      attempts: 0,
      catchup_policy: "fire_once",
      coalesced_count: 12,
      created_at_ms: 500,
      cron: null,
      every_ms: 1_000,
      fire_count: 1,
      id: "daily",
      kind: "interval",
      last_catchup_at_ms: 1_950,
      last_coalesced_count: 4,
      next_run_at_ms: 2_000,
      overlap_policy: "allow",
      overlap_retry_ms: null,
      skipped_count: 0,
      state: "active",
      target: { id_prefix: "daily", type: "task" },
      timezone: null
    });
    const client = new FerricStoreClient(executor);

    const schedule: ScheduleRecord = await client.scheduleCreate("daily", {
      catchupPolicy: "fire_once",
      everyMs: 1_000,
      kind: "interval",
      target: { id_prefix: "daily", type: "task" }
    });

    expect(schedule.catchup_policy).toBe("fire_once");
    expect(schedule.coalesced_count).toBe(12);
    expectTypeOf(schedule.created_at_ms).toEqualTypeOf<number>();
    expectTypeOf(schedule.every_ms).toEqualTypeOf<number | null>();
    expectTypeOf(schedule.cron).toEqualTypeOf<string | null>();
    expectTypeOf(schedule.overlap_retry_ms).toEqualTypeOf<number | null>();
    expect(schedule.created_at_ms).toBe(500);
    expect(schedule.every_ms).toBe(1_000);
    expect(schedule.last_catchup_at_ms).toBe(1_950);
    expect(schedule.last_coalesced_count).toBe(4);
    expect(executor.calls).toEqual([[
      "FLOW.SCHEDULE.CREATE",
      "daily",
      "KIND",
      "interval",
      "EVERY_MS",
      1_000,
      "TARGET",
      { id_prefix: "daily", type: "task" },
      "CATCHUP_POLICY",
      "fire_once"
    ]]);
  });

  it.each(["created_at_ms", "every_ms", "cron", "timezone", "overlap_retry_ms"])(
    "rejects an incomplete recurrence response missing %s",
    async (field) => {
      const response = withoutField(intervalSchedule(), field);
      const client = new FerricStoreClient(new ResponseExecutor(response));

      await expect(client.scheduleGet("daily")).rejects.toThrow(field);
    }
  );

  it.each(["every_ms", "cron", "timezone", "overlap_retry_ms"])(
    "rejects a required-nullable recurrence response with undefined %s",
    async (field) => {
      const client = new FerricStoreClient(new ResponseExecutor(intervalSchedule({
        [field]: undefined
      })));

      await expect(client.scheduleGet("daily")).rejects.toThrow(field);
    }
  );

  it.each([
    [intervalSchedule({ every_ms: 0 }), "every_ms"],
    [intervalSchedule({ cron: "* * * * *" }), "cron"],
    [intervalSchedule({ timezone: "Etc/UTC" }), "timezone"],
    [cronSchedule({ cron: null }), "cron"],
    [cronSchedule({ timezone: null }), "timezone"],
    [intervalSchedule({ overlap_retry_ms: 5 }), "overlap_retry_ms"],
    [oneShotSchedule({ overlap_policy: "skip" }), "overlap_policy"]
  ])("rejects an inconsistent recurrence response", async (response, field) => {
    const client = new FerricStoreClient(new ResponseExecutor(response));
    await expect(client.scheduleGet("daily")).rejects.toThrow(field);
  });

  it("returns a typed fire-due coalesced total", async () => {
    const client = new FerricStoreClient(new ResponseExecutor({
      claimed: 1,
      coalesced: 1_000_000,
      errors: [],
      fired: 1,
      last_target_id: "daily:1:1",
      skipped: 0
    }));

    const result: ScheduleFireDueResult = await client.scheduleFireDue();
    expect(result.coalesced).toBe(1_000_000);
  });

  it("keeps a later claim failure separate from claimed schedule outcomes", async () => {
    const client = new FerricStoreClient(new ResponseExecutor({
      claim_error: "ERR claim unavailable",
      claimed: 1,
      coalesced: 0,
      errors: [],
      fired: 1,
      last_target_id: "daily:1:1",
      skipped: 0
    }));

    const result = await client.scheduleFireDue();
    expectTypeOf(result.claim_error).toEqualTypeOf<string | null | undefined>();
    expect(result.claim_error).toBe("ERR claim unavailable");
  });

  it("accepts a schedule while its due occurrence is leased", async () => {
    const client = new FerricStoreClient(new ResponseExecutor(intervalSchedule({
      attempts: 1,
      state: "running"
    })));

    const schedule = await client.scheduleGet("daily");
    expect(schedule?.state).toBe("running");
  });

  it("preserves an actionable recurrence planning failure", async () => {
    const client = new FerricStoreClient(new ResponseExecutor(cronSchedule({
      attempts: 1,
      end_reason: "planning_failed",
      last_planning_error: "ERR invalid recurrence",
      state: "failed",
      target: { type: "task" }
    })));

    const schedule = await client.scheduleGet("daily");
    expectTypeOf(schedule?.last_planning_error).toEqualTypeOf<string | null | undefined>();
    expect(schedule?.last_planning_error).toBe("ERR invalid recurrence");
  });

  it.each([
    [{ claimed: 0, errors: [], fired: 0, skipped: 0 }, "missing coalesced"],
    [
      {
        claimed: 0,
        coalesced: Number.MAX_SAFE_INTEGER + 1,
        errors: [],
        fired: 0,
        skipped: 0
      },
      "safe non-negative integer"
    ],
    [{ claimed: 1, coalesced: 0, errors: [], fired: 1, skipped: 0 }, "last_target_id"],
    [{ claimed: 1, coalesced: 0, errors: [], fired: 0, skipped: 1 }, "last_skip_reason"],
    [
      {
        claimed: 0, coalesced: 0, errors: [], fired: 0,
        last_target_id: "stale", skipped: 0
      },
      "last_target_id"
    ],
    [
      {
        claimed: 0, coalesced: 0, errors: [], fired: 0,
        last_skip_reason: "stale", skipped: 0
      },
      "last_skip_reason"
    ],
    [
      {
        claimed: 1, coalesced: 1, errors: [["daily", "failed"]],
        fired: 0, skipped: 0
      },
      "coalesced"
    ],
    [
      {
        claim_error: 7, claimed: 0, coalesced: 0, errors: [], fired: 0, skipped: 0
      },
      "claim_error"
    ]
  ])("rejects malformed fire-due summaries", async (response, message) => {
    const client = new FerricStoreClient(new ResponseExecutor(response));
    await expect(client.scheduleFireDue()).rejects.toThrow(message);
  });

  it.each([
    [withoutField(intervalSchedule(), "kind"), "kind"],
    [intervalSchedule({ catchup_policy: null }), "catchup_policy"],
    [cronSchedule({ catchup_policy: "fire_once" }), "catchup_policy"],
    [withoutField(intervalSchedule(), "fire_count"), "fire_count"],
    [oneShotSchedule({ coalesced_count: 1 }), "coalesced_count"],
    [
      intervalSchedule({
        coalesced_count: 3,
        last_catchup_at_ms: 100,
        last_coalesced_count: 4
      }),
      "last_coalesced_count"
    ],
    [intervalSchedule({ coalesced_count: 1, last_coalesced_count: 1 }), "last_catchup_at_ms"],
    [intervalSchedule({ flow_id: 7 }), "flow_id"],
    [intervalSchedule({ last_target_id: "" }), "last_target_id"],
    [intervalSchedule({ target: {} }), "target type"]
  ])("rejects malformed canonical schedule records", async (response, message) => {
    const client = new FerricStoreClient(new ResponseExecutor(response));
    await expect(client.scheduleCreate("daily", {
      everyMs: 1_000,
      kind: "interval",
      target: { id_prefix: "daily", type: "task" }
    })).rejects.toThrow(message);
  });

  it("sends lease and explicit manual fire timestamps", async () => {
    const executor = new ResponseExecutor({
      fired: 1,
      schedule: intervalSchedule({ fire_count: 1 }),
      target_id: "daily:900:1"
    });
    const client = new FerricStoreClient(executor);

    const fired: ScheduleFireResult =
      await client.scheduleFire("daily", { fireAtMs: 900, nowMs: 1_000 });

    const dueExecutor = new ResponseExecutor({
      claimed: 0,
      coalesced: 0,
      errors: [],
      fired: 0,
      skipped: 0
    });
    await new FerricStoreClient(dueExecutor).scheduleFireDue({ leaseMs: 5_000 });

    expect(executor.calls).toEqual([[
      "FLOW.SCHEDULE.FIRE", "daily", "FIRE_AT_MS", 900, "NOW", 1_000
    ]]);
    expect(fired.schedule.fire_count).toBe(1);
    expect(fired.target_id).toBe("daily:900:1");
    expect(dueExecutor.calls).toEqual([[
      "FLOW.SCHEDULE.FIRE_DUE", "LEASE_MS", 5_000
    ]]);
  });

  it("builds schedule lifecycle commands", async () => {
    const calls: unknown[][] = [];
    const record = cronSchedule({ target: { type: "task" } });
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        calls.push(args);
        if (args[0] === "FLOW.SCHEDULE.GET") return null;
        if (args[0] === "FLOW.SCHEDULE.LIST") return [];
        if (args[0] === "FLOW.SCHEDULE.DELETE") return "OK";
        return record;
      }
    };
    const client = new FerricStoreClient(executor);

    await expect(client.scheduleGet("daily")).resolves.toBeNull();
    await client.schedulePause("daily", { nowMs: 101 });
    await client.scheduleResume("daily", { nowMs: 102 });
    await expect(client.scheduleDelete("daily", { nowMs: 103 })).resolves.toBeUndefined();
    await expect(client.scheduleList({ count: 10, kind: "cron" })).resolves.toEqual([]);

    expect(calls).toEqual([
      ["FLOW.SCHEDULE.GET", "daily"],
      ["FLOW.SCHEDULE.PAUSE", "daily", "NOW", 101],
      ["FLOW.SCHEDULE.RESUME", "daily", "NOW", 102],
      ["FLOW.SCHEDULE.DELETE", "daily", "NOW", 103],
      ["FLOW.SCHEDULE.LIST", "KIND", "cron", "COUNT", 10]
    ]);
  });

  it("rejects a non-OK schedule delete response", async () => {
    const client = new FerricStoreClient(new ResponseExecutor({
      id: "daily",
      state: "deleted"
    }));

    await expect(client.scheduleDelete("daily")).rejects.toThrow("must be OK");
  });

  it.each([
    [{ fired: 1, target_id: "daily:1:1" }, "schedule"],
    [
      {
        fired: 1,
        schedule: intervalSchedule({ fire_count: 1 })
      },
      "target_id"
    ],
    [
      {
        fired: 0,
        reason: "overlap",
        schedule: intervalSchedule({ fire_count: 1 }),
        skipped: 1,
        target_id: "stale"
      },
      "target_id"
    ],
    [
      {
        fired: 1,
        reason: "stale",
        schedule: intervalSchedule({ fire_count: 1 }),
        target_id: "daily:1:1"
      },
      "reason"
    ]
  ])("rejects malformed manual-fire envelopes", async (response, message) => {
    const client = new FerricStoreClient(new ResponseExecutor(response));
    await expect(client.scheduleFire("daily")).rejects.toThrow(message);
  });
});
