import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  FlowQueryError,
  type FlowQueryResult,
} from "../src/index.js";
import { nativeNegotiation } from "../src/native-negotiation.js";
import * as flowProtocol from "../src/protocol-flow.js";
import {
  buildProtocolCommand,
  COMMAND_OPCODES,
  OPCODES,
} from "../src/protocol.js";
import { flowRoutingData } from "../src/topology-routing.js";
import { FakeExecutor } from "./fake-executor.js";

const QUERY =
  "FROM runs WHERE partition_key = @partition AND type = @type " +
  "ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS";

const USAGE = {
  range_seeks: 1,
  range_pages: 1,
  scanned_entries: 2,
  scanned_bytes: 100,
  hydrated_records: 2,
  residual_checks: 0,
  duplicate_entries: 0,
  result_records: 2,
  response_bytes: 100,
  memory_high_water_bytes: 1_024,
  wall_time_us: 10,
};

const QUALITY = {
  exactness: "projected_exact",
  freshness: "projection_watermark",
  coverage: "complete",
  pagination: "live_seek",
};

function record(id: string): Record<string, unknown> {
  return {
    id: Buffer.from(id),
    type: Buffer.from("invoice"),
    state: Buffer.from("queued"),
    partition_key: Buffer.from("tenant-a"),
    version: 1,
  };
}

function recordsResult(): Record<string, unknown> {
  return {
    version: "ferric.flow.query.result/v1",
    records: [record("one"), record("two")],
    page: { has_more: true, cursor: "fqc1_next" },
    quality: QUALITY,
    usage: USAGE,
  };
}

function explainResult(
  status: "executed" | "planned",
): Record<string, unknown> {
  return {
    version: "ferric.flow.explain/v1",
    query_fingerprint: "a".repeat(64),
    status,
    plan: { path: "ordered_range" },
    estimate: { scanned_entries: 2 },
    bounds: { scanned_entries: 50_000 },
    ...(status === "executed" ? { actual: USAGE } : {}),
  };
}

function countResult(value: number | bigint): Record<string, unknown> {
  return {
    version: "ferric.flow.query.result/v1",
    result: { kind: "count", value },
    quality: QUALITY,
    usage: { ...USAGE, result_records: 1 },
  };
}

function indexResult(): Record<string, unknown> {
  return {
    contract_version: "ferric.flow.query.indexes/v1",
    observed_at_ms: 100,
    statistics_max_age_ms: 30_000,
    registry: { epoch: 2, catalog_version: 3 },
    services: { backfill: { status: "idle" } },
    indexes: [
      {
        id: "flow_runs_tenant_updated",
        version: 1,
        build_id: "build-1",
        state: "active",
        queryable: true,
      },
    ],
  };
}

function queryCapabilities(): Record<string, unknown> {
  return {
    limits: { max_response_bytes: 4_096 },
    response_codecs: { compact_response_opcodes: {} },
    schemas: {
      "FLOW.QUERY": {
        required: ["version", "query"],
        fields: ["version", "query", "params", "deadline_ms"],
      },
    },
    flow_query: {
      request_contract: "ferric.flow.query.request/v1",
      result_contract: "ferric.flow.query.result/v1",
      explain_contract: "ferric.flow.explain/v1",
      index_status_contract: "ferric.flow.query.indexes/v1",
      language_versions: ["FQL1"],
      capabilities: [
        "flow_query_v1",
        "flow_explain_v1",
        "flow_explain_analyze_v1",
        "flow_composite_index_v1",
        "flow_query_index_status_v1",
      ],
      shapes: [
        "runs_by_run_id_record",
        "runs_by_partition_and_run_id_record",
        "runs_by_partition_predicates_ordered_records",
        "runs_by_partition_type_state_ordered_records",
        "runs_by_partition_type_terminals_ordered_records",
        "runs_by_partition_metadata_ordered_records",
        "runs_by_partition_type_running_lease_deadline_ordered_records",
        "runs_by_partition_parent_ordered_records",
        "runs_by_partition_root_ordered_records",
        "runs_by_partition_correlation_ordered_records",
        "runs_by_partition_predicates_count",
        "events_by_run_id_ordered_records",
      ],
    },
  };
}

describe("FerricStore 0.10 Flow query contract", () => {
  it("uses one opaque non-mutating collection opcode without the removed compact request", () => {
    expect(COMMAND_OPCODES["FLOW.QUERY"]).toBe(0x0231);
    const table = COMMAND_OPCODES as Readonly<
      Record<string, number | undefined>
    >;
    for (const removed of [
      "FLOW.LIST",
      "FLOW.SEARCH",
      "FLOW.TERMINALS",
      "FLOW.FAILURES",
      "FLOW.BY_PARENT",
      "FLOW.BY_ROOT",
      "FLOW.BY_CORRELATION",
      "FLOW.STUCK",
    ]) {
      expect(table[removed]).toBeUndefined();
    }
    expect("compactFlowListPayload" in flowProtocol).toBe(false);
  });

  it("builds a strict native query payload and leaves prepared routing opaque", () => {
    const args = [
      "FLOW.QUERY",
      "FQL1",
      QUERY,
      "type",
      "invoice",
      "partition",
      "tenant-a",
    ] as const;
    expect(buildProtocolCommand(args)).toEqual({
      opcode: 0x0231,
      payload: {
        version: "FQL1",
        query: QUERY,
        params: { type: "invoice", partition: "tenant-a" },
      },
    });
    expect(flowRoutingData("FLOW.QUERY", args)).toBeUndefined();
    expect(OPCODES.flowQuery).toBe(0x0231);
  });

  it("decodes query, explain, analyze, index status, and actionable errors", async () => {
    const diagnostic = {
      code: "unsupported_field",
      message: "unsupported query field",
      detail: "Use a supported field.",
      hint: "See context.supported_fields.",
      retryable: false,
      safe_to_retry: false,
      retry_after_ms: 0,
      position: { byte: 18, line: 1, column: 19 },
      context: { supported_fields: ["partition_key", "run_id", "type"] },
    };
    const executor = new FakeExecutor([
      recordsResult(),
      explainResult("planned"),
      explainResult("executed"),
      indexResult(),
      new FerricStoreError("unsupported query field", { raw: diagnostic }),
    ]);
    const client = new FerricStoreClient(executor);

    const result: FlowQueryResult = await client.query(QUERY, {
      partition: "tenant-a",
      type: "invoice",
    });
    expect(result.kind).toBe("records");
    if (result.kind !== "records") throw new Error("expected records result");
    expect(result.records).toHaveLength(2);
    expect(result.page).toEqual({ hasMore: true, cursor: "fqc1_next" });
    expect(result.usage.resultRecords).toBe(2);
    expect(result.quality.pagination).toBe("live_seek");

    await expect(
      client.explain(QUERY, { partition: "tenant-a", type: "invoice" }),
    ).resolves.toMatchObject({ actual: undefined, status: "planned" });
    await expect(
      client.explainAnalyze(QUERY, { partition: "tenant-a", type: "invoice" }),
    ).resolves.toMatchObject({
      actual: { resultRecords: 2 },
      status: "executed",
    });
    await expect(client.queryIndexes()).resolves.toMatchObject({
      registry: { catalogVersion: 3, epoch: 2 },
      indexes: [{ id: "flow_runs_tenant_updated", queryable: true }],
    });

    const error = await client
      .query(QUERY, { partition: "tenant-a" })
      .catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(FlowQueryError);
    expect(error).toMatchObject({
      code: "unsupported_field",
      hint: "See context.supported_fields.",
      position: { byte: 18, column: 19, line: 1 },
      retryable: false,
      safeToRetry: false,
    });
  });

  it("compiles collection conveniences to bounded partition-scoped FQL", async () => {
    const executor = new FakeExecutor(
      Array.from({ length: 8 }, () => recordsResult()),
    );
    const client = new FerricStoreClient(executor);

    await client.list("invoice", {
      partitionKey: "tenant-a",
      state: "queued",
      count: 2,
    });
    await client.search("invoice", {
      partitionKey: "tenant-a",
      state: "queued",
      attributes: { tenant: "acme" },
    });
    await client.byParent("parent-1", { partitionKey: "tenant-a", count: 2 });

    expect(executor.calls.map((call) => call[0])).toEqual([
      "FLOW.QUERY",
      "FLOW.QUERY",
      "FLOW.QUERY",
    ]);
    expect(executor.calls[0]?.[2]).toContain("LIMIT 2 RETURN RECORDS");
    expect(executor.calls[1]?.[2]).toContain(
      "attribute['tenant'] = @attribute_0",
    );
    expect(executor.calls[2]?.[2]).toContain("parent_flow_id = @lineage_id");
    await expect(
      client.list("invoice", { partitionKey: "", count: 2 }),
    ).rejects.toThrow("partition key");
    await expect(
      client.stuck("any", { partitionKey: "tenant-a", nowMs: 1_000 }),
    ).rejects.toThrow("concrete flow type");
    await expect(
      client.list("any", { partitionKey: "tenant-a" }),
    ).rejects.toThrow(/concrete flow type|attribute predicate/u);
    await expect(
      client.list("invoice", {
        partitionKey: "tenant-a",
        state: "any",
      }),
    ).rejects.toThrow("attribute predicate");
    await expect(
      client.search("any", {
        partitionKey: "tenant-a",
        stateMeta: { queued: { risk: 3 } },
      }),
    ).rejects.toThrow("concrete flow type");
    await expect(
      client.search("invoice", {
        partitionKey: "tenant-a",
        state: "any",
        stateMeta: { risk: 3 },
      }),
    ).rejects.toThrow("concrete state");
    await expect(
      client.terminals("any", { partitionKey: "tenant-a" }),
    ).rejects.toThrow("concrete flow type");
    await expect(
      client.terminals("invoice", {
        partitionKey: "tenant-a",
        attributes: { tenant: "acme" },
      }),
    ).rejects.toThrow("attribute");
    await expect(
      client.failures("any", { partitionKey: "tenant-a" }),
    ).rejects.toThrow(/concrete flow type|attribute predicate/u);
    await expect(
      client.byParent("parent-1", {
        partitionKey: "tenant-a",
        attributes: { tenant: "acme" },
      }),
    ).rejects.toThrow("attribute");
    await expect(client.queryIndexes("")).rejects.toThrow("query index id");
    expect(executor.calls).toHaveLength(3);
  });

  it("rejects metadata outside the normalized server domain before IO", async () => {
    const executor = new FakeExecutor([recordsResult(), recordsResult()]);
    const client = new FerricStoreClient(executor);

    await expect(
      client.search("invoice", {
        partitionKey: "tenant-a",
        attributes: { ["x".repeat(65)]: "one" },
      }),
    ).rejects.toThrow("attribute key");
    await expect(
      client.search("invoice", {
        partitionKey: "tenant-a",
        stateMeta: {
          queued: { risk: 1 },
          " queued ": { risk: 2 },
        },
      }),
    ).rejects.toThrow("stateMeta state");
    await expect(
      client.search("invoice", {
        partitionKey: "tenant-a",
        attributes: { tenant: "acme" },
        stateMeta: [] as never,
      }),
    ).rejects.toThrow("stateMeta");
    expect(executor.calls).toHaveLength(0);
  });

  it("orders metadata predicates independently of the process locale", async () => {
    const executor = new FakeExecutor([recordsResult()]);
    const client = new FerricStoreClient(executor);

    await client.search("invoice", {
      partitionKey: "tenant-a",
      attributes: { ä: 1, z: 2 },
    });

    const query = executor.calls[0]?.[2];
    expect(query).toBeTypeOf("string");
    expect((query as string).indexOf("attribute['z']")).toBeLessThan(
      (query as string).indexOf("attribute['ä']"),
    );
  });

  it("preserves full-width count and catalog generation integers", async () => {
    const maxSigned = 9_223_372_036_854_775_807n;
    const maxUnsigned = 18_446_744_073_709_551_615n;
    const indexes = indexResult();
    indexes.registry = { epoch: maxUnsigned, catalog_version: maxUnsigned };
    indexes.indexes = [
      {
        ...(indexes.indexes as Record<string, unknown>[])[0],
        version: maxUnsigned,
      },
    ];
    const client = new FerricStoreClient(
      new FakeExecutor([countResult(maxSigned), indexes]),
    );

    await expect(
      client.query("FROM runs WHERE partition_key = @partition RETURN COUNT", {
        partition: "tenant-a",
      }),
    ).resolves.toMatchObject({ kind: "count", count: maxSigned });
    await expect(client.queryIndexes()).resolves.toMatchObject({
      registry: { catalogVersion: maxUnsigned, epoch: maxUnsigned },
      indexes: [{ version: maxUnsigned }],
    });
  });

  it("rejects zero bigint catalog and index versions", async () => {
    const zeroCatalogVersion = indexResult();
    zeroCatalogVersion.registry = { epoch: 0n, catalog_version: 0n };
    const zeroIndexVersion = indexResult();
    zeroIndexVersion.indexes = [
      {
        ...(zeroIndexVersion.indexes as Record<string, unknown>[])[0],
        version: 0n,
      },
    ];
    const client = new FerricStoreClient(
      new FakeExecutor([zeroCatalogVersion, zeroIndexVersion]),
    );

    await expect(client.queryIndexes()).rejects.toThrow(
      "catalog_version must be positive",
    );
    await expect(client.queryIndexes()).rejects.toThrow(
      "index version must be positive",
    );
  });

  it("rejects quality values outside the server contract", async () => {
    const client = new FerricStoreClient(
      new FakeExecutor([
        {
          ...recordsResult(),
          quality: { ...QUALITY, exactness: "x".repeat(65) },
        },
      ]),
    );
    await expect(
      client.query(QUERY, { partition: "tenant-a" }),
    ).rejects.toThrow("quality exactness");

    const malformedText = new FerricStoreClient(
      new FakeExecutor([
        {
          ...recordsResult(),
          quality: { ...QUALITY, exactness: "\ud800" },
        },
      ]),
    );
    await expect(
      malformedText.query(QUERY, { partition: "tenant-a" }),
    ).rejects.toThrow("quality exactness");
  });

  it("rejects unbounded requests and malformed response envelopes", async () => {
    const client = new FerricStoreClient(
      new FakeExecutor([
        {
          ...recordsResult(),
          records: Array.from({ length: 101 }, (_, index) =>
            record(String(index)),
          ),
        },
      ]),
    );
    await expect(
      client.query(QUERY, { partition: "tenant-a" }),
    ).rejects.toThrow("at most 100");
    await expect(client.query("x".repeat(16 * 1_024 + 1))).rejects.toThrow(
      "16384 bytes",
    );
    await expect(
      client.query(QUERY, { value: Number.POSITIVE_INFINITY }),
    ).rejects.toThrow("finite");
  });

  it("requires the complete query manifest during HELLO negotiation", () => {
    const negotiated = nativeNegotiation({ capabilities: queryCapabilities() });
    expect(negotiated.flowQuery.languageVersions.has("FQL1")).toBe(true);
    const incompatible = queryCapabilities();
    (incompatible.flow_query as Record<string, unknown>).index_status_contract =
      "future/v2";
    expect(() => nativeNegotiation({ capabilities: incompatible })).toThrow(
      "index_status_contract",
    );
    expect(() => nativeNegotiation({ capabilities: { limits: {} } })).toThrow(
      "incompatible FerricStore server",
    );
  });
});
