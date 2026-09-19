import { expect, it } from "vitest";
import {
  JsonCodec,
  RawCodec
} from "../../src/index.js";
import {
  createAndClaim,
  eventId,
  field,
  integrationClient,
  suffix,
  text
} from "./live-support.js";

const skipRewindReasonPersistence = process.env.FERRICSTORE_SKIP_REWIND_REASON_PERSISTENCE === "true";

it.skipIf(skipRewindReasonPersistence)("persists rewind reasons across records, values, and history", async () => {
  const flow = await integrationClient({ codec: new JsonCodec() });
  const runId = suffix();
  const type = `ts-sdk:rewind-reason:${runId}`;

  try {
    const rewindJob = await createAndClaim(flow, type, runId, "rewind-reason");
    const historyBefore = await flow.history(rewindJob.id, {
      count: 10,
      partitionKey: rewindJob.partitionKey
    });
    const createdEventId = eventId(historyBefore[0]);
    await flow.complete(rewindJob.id, {
      fencingToken: rewindJob.job.fencingToken,
      leaseToken: rewindJob.job.leaseToken,
      partitionKey: rewindJob.partitionKey
    });
    const completed = await flow.get(rewindJob.id, { partitionKey: rewindJob.partitionKey });
    if (completed == null) throw new Error("expected completed rewind flow");

    const reason = { action: "operator rollback", runId };
    const rewound = await flow.rewind(rewindJob.id, {
      expectState: "completed",
      partitionKey: rewindJob.partitionKey,
      reason,
      returnRecord: true,
      toEvent: createdEventId
    });
    if (!isFlowRecord(rewound)) throw new Error("expected rewound Flow record");
    expect(rewound.state).toBe("queued");
    expect(rewound.version).toBe(completed.version + 1);
    expect(rewound.fencingToken).toBe(nextFencingToken(completed.fencingToken));

    const errorRef = field(rewound.raw, "error_ref");
    expect(Buffer.isBuffer(errorRef) || errorRef instanceof Uint8Array).toBe(true);
    const errorRefText = text(errorRef);
    await expect(flow.valueMGet([errorRefText])).resolves.toEqual([reason]);

    const rawFlow = await integrationClient({ codec: new RawCodec() });
    try {
      await expect(rawFlow.valueMGet([errorRefText])).resolves.toEqual([
        Buffer.from(JSON.stringify(reason))
      ]);
    } finally {
      await rawFlow.close();
    }

    const historyAfter = await flow.history(rewindJob.id, {
      count: 20,
      partitionKey: rewindJob.partitionKey
    });
    const rewoundEvent = historyAfter.find((event) => {
      const fields: unknown = Array.isArray(event) ? event[1] : event;
      return text(field(fields, "event")) === "rewound";
    });
    if (rewoundEvent == null) throw new Error("expected rewind history event");
    const rewoundFields: unknown = Array.isArray(rewoundEvent) ? rewoundEvent[1] : rewoundEvent;
    expect(text(field(rewoundFields, "error_ref"))).toBe(errorRefText);

    const restored = await flow.rewind(rewindJob.id, {
      partitionKey: rewindJob.partitionKey,
      returnRecord: true,
      toEvent: eventId(rewoundEvent)
    });
    if (!isFlowRecord(restored)) throw new Error("expected restored Flow record");
    expect(restored.state).toBe("queued");
    expect(restored.version).toBe(rewound.version + 1);
    expect(restored.fencingToken).toBe(nextFencingToken(rewound.fencingToken));
    expect(text(field(restored.raw, "error_ref"))).toBe(errorRefText);
    await expect(flow.valueMGet([errorRefText])).resolves.toEqual([reason]);
  } finally {
    await flow.close();
  }
}, 60_000);

function isFlowRecord(value: unknown): value is {
  raw?: unknown;
  state: string;
  version: number;
  fencingToken: number | bigint;
} {
  return typeof value === "object" && value != null && "state" in value && "version" in value && "fencingToken" in value;
}

function nextFencingToken(token: number | bigint): number | bigint {
  return typeof token === "bigint" ? token + 1n : token + 1;
}
