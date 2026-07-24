import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

const QUALITY = {
  coverage: "complete",
  exactness: "projected_exact",
  freshness: "projection_watermark",
  pagination: "live_seek",
};
const USAGE = {
  duplicate_entries: 0,
  hydrated_records: 1,
  memory_high_water_bytes: 1_024,
  range_pages: 1,
  range_seeks: 1,
  residual_checks: 0,
  response_bytes: 1_024,
  result_records: 1,
  scanned_bytes: 100,
  scanned_entries: 1,
  wall_time_us: 10,
};
const OPTIONS = {
  count: 2,
  partitionKey: "tenant-a",
  state: "queued",
} as const;

describe("Flow query convenience response decoding", () => {
  it("decodes records without enumerating an intermediate generic map", async () => {
    let recordEnumerations = 0;
    const extension = Buffer.from("future");
    const source = new Proxy(
      {
        ...record("one"),
        future_extension: extension,
        payload: Buffer.from(JSON.stringify({ amount: 42 })),
      },
      {
        ownKeys(target) {
          recordEnumerations += 1;
          return Reflect.ownKeys(target);
        },
      },
    );
    const client = new FerricStoreClient(
      new FakeExecutor([recordsResult([source])]),
      { codec: new JsonCodec() },
    );

    const records = await client.list("invoice", OPTIONS);

    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      id: "one",
      partitionKey: "tenant-a",
      payload: { amount: 42 },
      state: "queued",
      type: "invoice",
    });
    expect((records[0]?.raw as Record<string, unknown>).future_extension).toBe(
      extension,
    );
    expect(recordEnumerations).toBe(0);
  });

  it("retains strict envelope and map-key validation", async () => {
    const sparseRecords = new Array<unknown>(1);
    const duplicateKeys = new Map<unknown, unknown>([
      ["id", "one"],
      [Buffer.from("id"), "other"],
      ["partition_key", "tenant-a"],
      ["state", "queued"],
      ["type", "invoice"],
      ["version", 1],
    ]);
    const client = new FerricStoreClient(
      new FakeExecutor([
        recordsResult([record("one"), record("two")]),
        recordsResult(sparseRecords),
        countResult(),
        recordsResult([duplicateKeys]),
      ]),
    );

    await expect(client.list("invoice", OPTIONS)).rejects.toThrow(
      "result_records does not match records",
    );
    await expect(client.list("invoice", OPTIONS)).rejects.toThrow(
      "records must be a dense array",
    );
    await expect(client.list("invoice", OPTIONS)).rejects.toThrow(
      "convenience query returned a count result",
    );
    await expect(client.list("invoice", OPTIONS)).rejects.toThrow(
      "invalid or duplicate key",
    );
  });
});

function record(id: string): Record<string, unknown> {
  return {
    id: Buffer.from(id),
    partition_key: Buffer.from("tenant-a"),
    state: Buffer.from("queued"),
    type: Buffer.from("invoice"),
    version: 1,
  };
}

function recordsResult(records: unknown[]): Record<string, unknown> {
  return {
    page: { has_more: false },
    quality: QUALITY,
    records,
    usage: USAGE,
    version: "ferric.flow.query.result/v1",
  };
}

function countResult(): Record<string, unknown> {
  return {
    quality: QUALITY,
    result: { kind: "count", value: 1 },
    usage: USAGE,
    version: "ferric.flow.query.result/v1",
  };
}
