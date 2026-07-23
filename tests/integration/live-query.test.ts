import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  FlowQueryError,
  JsonCodec
} from "../../src/index.js";
import { eventually, field, suffix, text, url } from "./live-support.js";

const PAGE_QUERY =
  "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state " +
  "ORDER BY updated_at_ms ASC LIMIT 2 RETURN RECORDS";
const CURSOR_QUERY =
  "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state " +
  "ORDER BY updated_at_ms ASC LIMIT 2 CURSOR @cursor RETURN RECORDS";
const COUNT_QUERY =
  "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state " +
  "RETURN COUNT";

describe("FerricStore 0.10 query planner integration", () => {
  it("covers pagination, count, explain, index status, diagnostics, and conveniences", async () => {
    const client = await FerricStoreClient.fromUrl(url(), { codec: new JsonCodec() });
    const run = suffix();
    const partition = `ts-sdk:query:${run}:partition`;
    const type = `ts-sdk-query-${run}`;
    const state = "query-ready";
    const now = Date.now();
    const ids = Array.from({ length: 3 }, (_, index) => `ts-sdk:query:${run}:${index}`);
    const params = { partition, state, type };

    try {
      for (let index = 0; index < ids.length; index += 1) {
        const id = ids[index];
        if (id == null) throw new Error("query integration ids must be dense");
        await client.create(id, {
          idempotent: true,
          nowMs: now + index,
          partitionKey: partition,
          payload: { secret: `payload-${index}` },
          runAtMs: now + index,
          state,
          type
        });
      }

      const first = await eventually(
        () => client.query(PAGE_QUERY, params),
        (result) =>
          result.kind === "records" && result.records.length === 2 && result.page.hasMore,
        "FLOW.QUERY projection did not become ready"
      );
      expect(first.kind).toBe("records");
      if (first.kind !== "records" || first.page.cursor == null) {
        throw new Error("expected a cursor-bearing records result");
      }
      expect(first.usage.resultRecords).toBe(2);
      expect(first.quality.pagination).not.toBe("");

      const second = await client.query(CURSOR_QUERY, {
        ...params,
        cursor: first.page.cursor
      });
      expect(second.kind).toBe("records");
      if (second.kind !== "records") throw new Error("expected a records result");
      expect(second.records).toHaveLength(1);
      expect(second.page).toEqual({ hasMore: false });

      const pagedIds = [...first.records, ...second.records].map((record) =>
        text(field(record, "id"))
      );
      expect(new Set(pagedIds).size).toBe(pagedIds.length);
      expect(new Set(pagedIds)).toEqual(new Set(ids));

      const counted = await eventually(
        () => client.query(COUNT_QUERY, params),
        (result) => result.kind === "count" && result.count === ids.length,
        "FLOW.QUERY count projection did not become ready"
      );
      expect(counted).toMatchObject({ kind: "count", count: ids.length });
      expect(counted.usage.resultRecords).toBe(1);

      await expect(client.explain(PAGE_QUERY, params)).resolves.toMatchObject({
        actual: undefined,
        status: "planned"
      });
      const analyzed = await client.explainAnalyze(PAGE_QUERY, params);
      expect(analyzed.status).toBe("executed");
      expect(analyzed.actual?.resultRecords).toBe(2);

      const indexes = await client.queryIndexes();
      expect(greaterThanZero(indexes.registry.catalogVersion)).toBe(true);
      expect(indexes.indexes.length).toBeGreaterThan(0);

      const error = await client.query(
        "FROM runs WHERE partition_key = @partition AND unsupported = 1 " +
          "ORDER BY updated_at_ms ASC LIMIT 2 RETURN RECORDS",
        { partition }
      ).catch((reason: unknown) => reason);
      expect(error).toBeInstanceOf(FlowQueryError);
      expect(error).toMatchObject({ code: "unsupported_field" });
      expect((error as FlowQueryError).position).toBeDefined();
      expect((error as FlowQueryError).hint).not.toBe("");

      const listed = await eventually(
        () => client.query(PAGE_QUERY.replace("LIMIT 2", "LIMIT 3"), params),
        (result) => result.kind === "records" && result.records.length === ids.length,
        "FLOW.QUERY list projection did not become ready"
      );
      if (listed.kind !== "records") throw new Error("expected a records result");
      await expect(client.list(type, {
        count: 3,
        partitionKey: partition,
        state
      })).resolves.toHaveLength(listed.records.length);
    } finally {
      await client.close();
    }
  }, 40_000);
});

function greaterThanZero(value: number | bigint): boolean {
  return typeof value === "bigint" ? value > 0n : value > 0;
}
