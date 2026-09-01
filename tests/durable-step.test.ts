import { describe, expect, it, vi } from "vitest";
import {
  FerricStoreClient,
  JsonCodec,
  RequestNotSentError,
  type AdvanceOptions,
  type ClaimedItem
} from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";
import { durableMutationMayHaveCommitted, runDurableStep } from "../src/client-durable-step.js";

const STEP_NAME = "charge-customer:v1";
const STEP_VALUE_NAME =
  "__ferricstore_step__:sha256:ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7";

describe("durable workflow steps", () => {
  it("makes a normal single-state claim immediately chainable without a larger response", async () => {
    const firstLease = Buffer.from("lease-1");
    const secondLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      [[Buffer.from("flow-1"), Buffer.from("tenant-a"), firstLease, 7]],
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), secondLease, 8]
    ]);
    const client = new FerricStoreClient(executor);

    const [job] = await client.claimJobs("order", {
      partitionKey: "tenant-a",
      state: "created",
      worker: "worker-1"
    });
    if (job == null) throw new Error("expected claim");
    expect(job.runState).toBe("created");
    await expect(client.advance(job, { toState: "charged" })).resolves.toMatchObject({
      runState: "charged"
    });

    expect(executor.calls[0]).toContain("JOBS_COMPACT");
    expect(executor.calls[0]).not.toContain("JOBS_COMPACT_STATE");
    expect(executor.calls[1]?.slice(0, 5)).toEqual([
      "FLOW.STEP_CONTINUE", "flow-1", firstLease, "created", "charged"
    ]);
  });

  it("returns the logical state needed to continue after lease takeover", async () => {
    const reclaimedLease = Buffer.from("lease-b");
    const renewedLease = Buffer.from("lease-c");
    const executor = new FakeExecutor([
      [[
        Buffer.from("flow-1"), Buffer.from("tenant-a"), reclaimedLease, 9,
        Buffer.from("charged")
      ]],
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), renewedLease, 10]
    ]);
    const client = new FerricStoreClient(executor);

    const [job] = await client.reclaim("order", {
      jobOnly: true,
      partitionKey: "tenant-a",
      worker: "worker-b"
    });
    if (job == null || !("leaseToken" in job)) throw new Error("expected compact reclaim");
    expect(job.runState).toBe("charged");
    await expect(client.advance(job, { toState: "finished" })).resolves.toMatchObject({
      fencingToken: 10,
      runState: "finished"
    });

    expect(executor.calls[0]).toEqual(expect.arrayContaining([
      "FLOW.RECLAIM", "RETURN", "JOBS_COMPACT_STATE"
    ]));
  });

  it("advances a claim using its current state and fencing data", async () => {
    const current = claimed();
    const renewedLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([[
      Buffer.from("flow-1"),
      Buffer.from("tenant-a"),
      renewedLease,
      8
    ]]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    const refreshed = await client.advance(current, {
      leaseMs: 45_000,
      nowMs: 101,
      payload: { phase: "charged" },
      toState: "charged"
    });

    expect(refreshed).toMatchObject({
      fencingToken: 8,
      id: "flow-1",
      partitionKey: "tenant-a",
      runState: "charged",
      state: "running",
      type: "order"
    });
    expect(refreshed.leaseToken).toEqual(renewedLease);
    expect(executor.calls).toEqual([[
      "FLOW.STEP_CONTINUE",
      "flow-1",
      current.leaseToken,
      "created",
      "charged",
      "FENCING",
      7,
      "LEASE_MS",
      45_000,
      "NOW",
      101,
      "PARTITION",
      "tenant-a",
      "PAYLOAD",
      Buffer.from('{"phase":"charged"}'),
      "RETURN",
      "JOBS_COMPACT"
    ]]);
    expect(executor.calls[0]).not.toContain("WORKER");
    expect(current).toEqual(claimed());
  });

  it("does not forward low-level ownership controls supplied by untyped callers", async () => {
    const executor = new FakeExecutor([[
      Buffer.from("flow-1"), Buffer.from("tenant-a"), Buffer.from("lease-2"), 8
    ]]);
    const client = new FerricStoreClient(executor);
    const options = {
      returnJob: false,
      toState: "charged",
      worker: "different-worker"
    } as AdvanceOptions & { returnJob: boolean; worker: string };

    await expect(client.advance(claimed(), options)).resolves.toMatchObject({
      runState: "charged"
    });
    expect(executor.calls[0]).not.toContain("WORKER");
    expect(executor.calls[0]).toEqual(expect.arrayContaining(["RETURN", "JOBS_COMPACT"]));
  });

  it("rejects an unstateful claim before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const current = { ...claimed(), runState: undefined };

    await expect(client.advance(current, { toState: "charged" })).rejects.toThrow(
      "job.runState must be a non-empty string"
    );
    expect(executor.calls).toEqual([]);
  });

  it("classifies continuation encoding failures as unsent before dispatch", async () => {
    const executor = new FakeExecutor();
    const encodingFailure = new TypeError("payload cannot be encoded");
    const client = new FerricStoreClient(executor, {
      codec: {
        decode: (value) => value,
        encode: () => { throw encodingFailure; }
      }
    });

    let rejected: unknown;
    try {
      await client.advance(claimed(), { payload: { phase: "charged" }, toState: "charged" });
    } catch (error) {
      rejected = error;
    }

    expect(rejected).toBeInstanceOf(RequestNotSentError);
    expect(rejected).toMatchObject({ requestDisposition: "unsent", safeToRetry: true });
    expect(durableMutationMayHaveCommitted(rejected)).toBe(false);
    expect((rejected as Error).cause).toBe(encodingFailure);
    expect(executor.calls).toEqual([]);
  });

  it("validates the lease before running and atomically journals the closure result", async () => {
    const current = claimed();
    const renewedLease = Buffer.from("lease-2");
    const executor = new FakeExecutor([
      flowRecord(),
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), renewedLease, 8]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(async () => {
      expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.EXTEND_LEASE"]);
      return { receipt: "rcpt-1" };
    });

    const stepped = await client.step(current, {
      leaseMs: 45_000,
      name: STEP_NAME,
      nowMs: 101,
      run,
      stateMeta: { attempt: 1 },
      toState: "charged"
    });

    expect(run).toHaveBeenCalledOnce();
    const receipt: string = stepped.result.receipt;
    expect(receipt).toBe("rcpt-1");
    expect(stepped.result).toEqual({ receipt: "rcpt-1" });
    expect(stepped.job).toMatchObject({
      fencingToken: 8,
      id: "flow-1",
      runState: "charged",
      type: "order"
    });
    expect(executor.calls[0]).toEqual([
      "FLOW.EXTEND_LEASE",
      "flow-1",
      current.leaseToken,
      "FENCING",
      7,
      "LEASE_MS",
      45_000,
      "NOW",
      101,
      "PARTITION",
      "tenant-a"
    ]);
    expect(executor.calls[1]).toEqual([
      "FLOW.STEP_CONTINUE",
      "flow-1",
      current.leaseToken,
      "created",
      "charged",
      "FENCING",
      7,
      "LEASE_MS",
      45_000,
      "NOW",
      101,
      "PARTITION",
      "tenant-a",
      "RETURN",
      "JOBS_COMPACT",
      "STATE_META",
      "attempt",
      1,
      "VALUE",
      STEP_VALUE_NAME,
      Buffer.from('{"receipt":"rcpt-1"}')
    ]);
  });

  it("rejects every preflight claim mismatch before invoking the closure", async () => {
    const mismatches: [string, Parameters<typeof flowRecord>[0]][] = [
      ["id", { id: "other-flow" }],
      ["partition", { partitionKey: "tenant-b" }],
      ["lease", { leaseToken: Buffer.from("other-lease") }],
      ["fence", { fencingToken: 8 }],
      ["physical state", { state: "waiting" }],
      ["logical state", { runState: "other-state" }]
    ];

    for (const [name, overrides] of mismatches) {
      const executor = new FakeExecutor([flowRecord(overrides)]);
      const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
      const run = vi.fn(() => "charged");

      await expect(client.step(claimed(), {
        name: STEP_NAME,
        run,
        toState: "charged"
      }), name).rejects.toThrow(/lease validation/iu);
      expect(run, name).not.toHaveBeenCalled();
      expect(executor.calls, name).toHaveLength(1);
    }
  });

  it("rejects every invalid continuation claim rotation", async () => {
    const lease1 = Buffer.from("lease-1");
    const lease2 = Buffer.from("lease-2");
    const mismatches: [string, unknown][] = [
      ["id", [Buffer.from("other-flow"), Buffer.from("tenant-a"), lease2, 8]],
      ["partition", [Buffer.from("flow-1"), Buffer.from("tenant-b"), lease2, 8]],
      ["unchanged lease", [Buffer.from("flow-1"), Buffer.from("tenant-a"), lease1, 8]],
      ["unchanged fence", [Buffer.from("flow-1"), Buffer.from("tenant-a"), lease2, 7]],
      ["lower fence", [Buffer.from("flow-1"), Buffer.from("tenant-a"), lease2, 6]],
      ["physical state", flowRecord({
        fencingToken: 8,
        leaseToken: lease2,
        runState: "charged",
        state: "waiting"
      })],
      ["logical state", flowRecord({
        fencingToken: 8,
        leaseToken: lease2,
        runState: "other-state"
      })]
    ];

    for (const [name, response] of mismatches) {
      const executor = new FakeExecutor([flowRecord(), response]);
      const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

      await expect(client.step(claimed(), {
        name: STEP_NAME,
        run: () => "charged",
        toState: "charged"
      }), name).rejects.toThrow(/continuation response/iu);
      expect(executor.calls.map((call) => call[0]), name).toEqual([
        "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"
      ]);
    }
  });

  it("rejects zero and negative fencing tokens before dispatch", async () => {
    for (const fencingToken of [0, -1, 0n, -1n]) {
      const executor = new FakeExecutor();
      const client = new FerricStoreClient(executor);

      await expect(client.step(claimed({ fencingToken }), {
        name: STEP_NAME,
        run: () => "never",
        toState: "charged"
      })).rejects.toThrow("positive integer");
      expect(executor.calls).toEqual([]);
    }
  });

  it("snapshots continuation mutations before awaiting the closure", async () => {
    const executor = new FakeExecutor([
      flowRecord(),
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), Buffer.from("lease-2"), 8]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const options = {
      name: STEP_NAME,
      run: async () => {
        options.toState = "mutated";
        options.values.audit = "mutated";
        return "receipt-1";
      },
      toState: "charged",
      values: { audit: "original" }
    };

    const stepped = await client.step(claimed(), options);

    expect(stepped.job.runState).toBe("charged");
    expect(executor.calls[1]).toEqual(expect.arrayContaining([
      "created", "charged", "VALUE", "audit", Buffer.from('"original"')
    ]));
    expect(executor.calls[1]).not.toContain("mutated");
  });

  it("replays a committed result without running or advancing again", async () => {
    const current = claimed({
      leaseToken: Buffer.from("lease-b"),
      fencingToken: 9,
      runState: "charged"
    });
    const executor = new FakeExecutor([
      flowRecord({
        fencingToken: 9,
        leaseToken: Buffer.from("lease-b"),
        runState: "charged",
        valueRefs: { [STEP_VALUE_NAME]: { ref: "result-ref" } }
      }),
      [Buffer.from('{"receipt":"rcpt-1"}')]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => ({ receipt: "should-not-run" }));

    const stepped = await client.step(current, {
      name: STEP_NAME,
      nowMs: 201,
      run,
      toState: "charged"
    });

    expect(run).not.toHaveBeenCalled();
    expect(stepped.result).toEqual({ receipt: "rcpt-1" });
    expect(stepped.job).toMatchObject({
      fencingToken: 9,
      id: "flow-1",
      runState: "charged"
    });
    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.EXTEND_LEASE",
      "FLOW.VALUE.MGET"
    ]);
    expect(executor.calls[1]).toEqual(["FLOW.VALUE.MGET", "result-ref"]);
  });

  it("fails closed when a committed result did not reach the requested target state", async () => {
    const executor = new FakeExecutor([
      flowRecord({ valueRefs: { [STEP_VALUE_NAME]: { ref: "result-ref" } } })
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => ({ receipt: "must-not-run" }));

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("target state");

    expect(run).not.toHaveBeenCalled();
    expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.EXTEND_LEASE"]);
  });

  it("returns the codec-normalized result on first execution and replay", async () => {
    const lease2 = Buffer.from("lease-2");
    const instant = new Date("2026-08-31T12:34:56.000Z");
    const executor = new FakeExecutor([
      flowRecord(),
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), lease2, 8],
      flowRecord({
        fencingToken: 8,
        leaseToken: lease2,
        runState: "charged",
        valueRefs: { [STEP_VALUE_NAME]: "result-ref" }
      }),
      [Buffer.from(JSON.stringify(instant))]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => instant);

    const first = await client.step(claimed(), {
      name: STEP_NAME,
      run,
      toState: "charged"
    });
    const replay = await client.step(first.job, {
      name: STEP_NAME,
      run,
      toState: "charged"
    });

    expect(first.result).toBe(instant.toISOString());
    expect(replay.result).toEqual(first.result);
    expect(run).toHaveBeenCalledOnce();
  });

  it("replays a stored JSON null without treating it as a missing blob", async () => {
    const executor = new FakeExecutor([
      flowRecord({ runState: "charged", valueRefs: { [STEP_VALUE_NAME]: "null-ref" } }),
      [Buffer.from("null")]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => "duplicate");

    await expect(client.step(claimed({ runState: "charged" }), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).resolves.toMatchObject({ result: null });
    expect(run).not.toHaveBeenCalled();
  });

  it("fails closed when the committed blob response omits its item", async () => {
    const omitted = new Array<unknown>(1);
    const executor = new FakeExecutor([
      flowRecord({ runState: "charged", valueRefs: { [STEP_VALUE_NAME]: "omitted-ref" } }),
      omitted
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => "duplicate");

    await expect(client.step(claimed({ runState: "charged" }), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("response item 0 is missing");
    expect(run).not.toHaveBeenCalled();
  });

  it("fails closed when the durable result reference metadata is malformed", async () => {
    const executor = new FakeExecutor([
      flowRecord({
        runState: "charged",
        valueRefs: { [STEP_VALUE_NAME]: { digest: "not-a-reference" } }
      })
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => "duplicate");

    await expect(client.step(claimed({ runState: "charged" }), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("invalid reference metadata");
    expect(run).not.toHaveBeenCalled();
  });

  it("rejects a full lease-validation record with no run state before the closure", async () => {
    const malformed = flowRecord();
    malformed.delete("run_state");
    const executor = new FakeExecutor([malformed]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => "charged");

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("lease validation returned an invalid claim");
    expect(run).not.toHaveBeenCalled();
  });

  it("never infers a missing run state on a full continuation response", async () => {
    const malformed = flowRecord({
      fencingToken: 8,
      leaseToken: Buffer.from("lease-2")
    });
    malformed.delete("run_state");
    const executor = new FakeExecutor([flowRecord(), malformed]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run: () => "charged",
      toState: "charged"
    })).rejects.toThrow("continuation response returned an invalid claim");
  });

  it("never runs the closure when lease validation fails", async () => {
    const executor = new FakeExecutor([new Error("ERR stale lease")]);
    const client = new FerricStoreClient(executor);
    const run = vi.fn(() => "charged");

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("stale lease");
    expect(run).not.toHaveBeenCalled();
    expect(executor.calls).toHaveLength(1);
  });

  it("propagates closure failures without committing a continuation", async () => {
    const executor = new FakeExecutor([flowRecord()]);
    const client = new FerricStoreClient(executor);
    const failure = new Error("provider unavailable");

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run: () => { throw failure; },
      toState: "charged"
    })).rejects.toBe(failure);
    expect(executor.calls.map((call) => call[0])).toEqual(["FLOW.EXTEND_LEASE"]);
  });

  it("fails closed when committed result metadata points to a missing value", async () => {
    const executor = new FakeExecutor([
      flowRecord({ runState: "charged", valueRefs: { [STEP_VALUE_NAME]: "missing-ref" } }),
      [null]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const run = vi.fn(() => ({ duplicate: true }));

    await expect(client.step(claimed({ runState: "charged" }), {
      name: STEP_NAME,
      run,
      toState: "charged"
    })).rejects.toThrow("committed durable step result is missing");
    expect(run).not.toHaveBeenCalled();
    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.EXTEND_LEASE",
      "FLOW.VALUE.MGET"
    ]);
  });

  it("rejects invalid durable step names before validating the lease", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.step(claimed(), {
      name: "",
      run: () => "never",
      toState: "charged"
    })).rejects.toThrow("name must be a non-empty string");
    expect(executor.calls).toEqual([]);
  });

  it("reserves the journal value name from caller mutations", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.step(claimed(), {
      name: STEP_NAME,
      run: () => "never",
      toState: "charged",
      values: { [STEP_VALUE_NAME]: "collision" }
    })).rejects.toThrow("reserved durable step result");
    expect(executor.calls).toEqual([]);
  });

  it("does not let a post-commit callback replace a confirmed durable result", async () => {
    const executor = new FakeExecutor([
      flowRecord(),
      [Buffer.from("flow-1"), Buffer.from("tenant-a"), Buffer.from("lease-2"), 8]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(runDurableStep(client, claimed(), {
      name: STEP_NAME,
      run: () => "receipt-1",
      toState: "charged"
    }, {
      committed: () => { throw new Error("continuation dispatch rejected"); }
    })).resolves.toMatchObject({
      job: { fencingToken: 8, runState: "charged" },
      result: "receipt-1"
    });
  });

  it("does not let a recovery callback replace a replayed durable result", async () => {
    const executor = new FakeExecutor([
      flowRecord({ runState: "charged", valueRefs: { [STEP_VALUE_NAME]: "result-ref" } }),
      [Buffer.from('"receipt-1"')]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(runDurableStep(client, claimed({ runState: "charged" }), {
      name: STEP_NAME,
      run: () => "duplicate",
      toState: "charged"
    }, {
      replayed: () => { throw new Error("replay dispatch rejected"); }
    })).resolves.toMatchObject({ result: "receipt-1" });
  });

  it("preserves the mutation failure when failure notification also throws", async () => {
    const mutationFailure = new Error("mutation outcome failed");
    const executor = new FakeExecutor([flowRecord(), mutationFailure]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(runDurableStep(client, claimed(), {
      name: STEP_NAME,
      run: () => "receipt-1",
      toState: "charged"
    }, {
      commitFailed: () => { throw new Error("failure dispatch rejected"); }
    })).rejects.toBe(mutationFailure);
  });
});

function claimed(overrides: Partial<ClaimedItem> = {}): ClaimedItem {
  return {
    fencingToken: 7,
    id: "flow-1",
    leaseToken: Buffer.from("lease-1"),
    partitionKey: "tenant-a",
    runState: "created",
    state: "running",
    type: "order",
    ...overrides
  };
}

function flowRecord(overrides: {
  fencingToken?: number;
  id?: string;
  leaseToken?: Buffer;
  partitionKey?: string;
  runState?: string;
  state?: string;
  valueRefs?: Record<string, unknown>;
} = {}): Map<unknown, unknown> {
  const entries: [unknown, unknown][] = [
    ["id", overrides.id ?? "flow-1"],
    ["type", "order"],
    ["state", overrides.state ?? "running"],
    ["run_state", overrides.runState ?? "created"],
    ["partition_key", overrides.partitionKey ?? "tenant-a"],
    ["lease_token", overrides.leaseToken ?? Buffer.from("lease-1")],
    ["fencing_token", overrides.fencingToken ?? 7],
    ["version", 3]
  ];
  if (overrides.valueRefs != null) entries.push(["value_refs", overrides.valueRefs]);
  return new Map<unknown, unknown>(entries);
}
