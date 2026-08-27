import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  FlowQueryError,
  type FlowQueryResult,
} from "../src/index.js";
import { nativeNegotiation } from "../src/native-negotiation.js";
import { hasFlowExplainPrefix } from "../src/flow-query-request.js";
import { tryDecodeFlowQueryError } from "../src/flow-query-response.js";
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
    page: { has_more: true, cursor: "fqc1_next-page-token" },
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
    plan: {
      order: Buffer.from("native"),
      path: Buffer.from("ordered_range"),
      requested_order: [
        { direction: Buffer.from("desc"), field: Buffer.from("updated_at_ms") },
      ],
    },
    estimate: { scanned_entries: 2 },
    stats: { source: "fresh" },
    quality: QUALITY,
    bounds: { scanned_entries: 50_000 },
    pressure: { resources: [] },
    decision: { reason: "only_bounded_candidate" },
    alternatives: [],
    actual: status === "executed" ? USAGE : null,
    diagnostic: null,
  };
}

function specializedExplainResult(): Record<string, unknown> {
  return {
    version: "ferric.flow.explain/v1",
    query_fingerprint: "a".repeat(64),
    status: "planned",
    capabilities: {
      requested: [],
      available: ["flow_query_point_v1"],
      missing: [],
    },
    plan: { path: "primary_key", record_source: "authoritative_log" },
    estimate: { scan_records: 1, result_records: 1 },
    bounds: { scan_records: 1, result_records: 1, groups: 0 },
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
    observed_at_ms: 1_000_000,
    statistics_max_age_ms: 300_000,
    registry: { epoch: 2, catalog_version: 3 },
    services: {
      registry: "ready",
      lifecycle_worker: "ready",
      statistics_store: "ready",
      statistics_worker: "unavailable",
    },
    indexes: [
      {
        id: "flow_runs_tenant_updated",
        version: 1,
        build_id: "build-1",
        source: "runs",
        state: "active",
        queryable: true,
        fields: [
          { name: "partition_key", direction: "asc", encoding: "hashed" },
          { name: "updated_at_ms", direction: "desc", encoding: "ordered" },
        ],
        workloads: ["tenant_updated"],
        count_prefixes: [1],
        covering_fields: [
          "partition_key",
          "run_id",
          "updated_at_ms",
          "version",
        ],
        format: {
          query_row: "ferric.flow.query.row/v1",
          key: "ferric.flow.query.composite.key/v1",
          entry: "ferric.flow.query.composite.entry/v2",
          reverse: "ferric.flow.query.composite.reverse/v1",
          counter: "ferric.flow.query.composite.counter/v1",
        },
        coverage: {
          complete_shards: 2,
          total_shards: 2,
          validation: "passed",
        },
        build: {
          scope: "catalog_build",
          phase_counts: { done: 2 },
          current_phases: ["done"],
          completed_shards: 2,
          total_shards: 2,
          scanned_records: 10,
          written_entries: 10,
          written_bytes: 900,
        },
        validation: {
          scope: "catalog_build",
          status: "passed",
          phase_counts: { done: 2 },
          current_phases: ["done"],
          completed_shards: 2,
          total_shards: 2,
          checked_records: 10,
          checked_entries: 10,
          mismatches: 0,
          failure_reason: null,
          validated_at_ms: 999_000,
        },
        retirement: { status: "not_applicable" },
        statistics: {
          status: "fresh",
          samples: 2,
          fresh_samples: 2,
          stale_samples: 0,
          future_samples: 0,
          oldest_collected_at_ms: 998_000,
          newest_collected_at_ms: 999_000,
          oldest_age_ms: 2_000,
          newest_age_ms: 1_000,
        },
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
        "flow_query_result_projection_v1",
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

describe("FerricStore 0.11 Flow query contract", () => {
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
    expect(result.page).toEqual({
      hasMore: true,
      cursor: "fqc1_next-page-token",
    });
    expect(result.usage.resultRecords).toBe(2);
    expect(result.quality.pagination).toBe("live_seek");

    const explained = await client.explain(QUERY, {
      partition: "tenant-a",
      type: "invoice",
    });
    expect(explained).toMatchObject({
      actual: undefined,
      stats: { source: "fresh" },
      quality: QUALITY,
      pressure: { resources: [] },
      decision: { reason: "only_bounded_candidate" },
      alternatives: [],
      plan: {
        order: "native",
        path: "ordered_range",
        requested_order: [{ direction: "desc", field: "updated_at_ms" }],
      },
      status: "planned",
    });
    await expect(
      client.explainAnalyze(QUERY, { partition: "tenant-a", type: "invoice" }),
    ).resolves.toMatchObject({
      actual: { resultRecords: 2 },
      status: "executed",
    });
    const indexStatus = await client.queryIndexes();
    expect(indexStatus).toMatchObject({
      registry: { catalogVersion: 3, epoch: 2 },
      indexes: [
        {
          id: "flow_runs_tenant_updated",
          source: "runs",
          queryable: true,
          fields: [
            { name: "partition_key", direction: "asc", encoding: "hashed" },
            { name: "updated_at_ms", direction: "desc", encoding: "ordered" },
          ],
          workloads: ["tenant_updated"],
          countPrefixes: [1],
          coveringFields: [
            "partition_key",
            "run_id",
            "updated_at_ms",
            "version",
          ],
          format: {
            entry: "ferric.flow.query.composite.entry/v2",
            counter: "ferric.flow.query.composite.counter/v1",
          },
          coverage: { completeShards: 2, totalShards: 2, validation: "passed" },
          build: { completedShards: 2, scannedRecords: 10 },
          validation: { status: "passed", validatedAtMs: 999_000 },
          retirement: { status: "not_applicable" },
          statistics: { status: "fresh", samples: 2 },
        },
      ],
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

  it("requires the complete actionable EXPLAIN v1 envelope", async () => {
    for (const field of [
      "stats",
      "quality",
      "pressure",
      "decision",
      "alternatives",
      "actual",
      "diagnostic",
    ]) {
      const response = Object.fromEntries(
        Object.entries(explainResult("planned")).filter(([name]) => name !== field),
      );
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).explain(QUERY, {
          partition: "tenant-a",
          type: "invoice",
        }),
      ).rejects.toThrow(/explain/u);
    }
  });

  it("rejects unsupported full-result quality values", async () => {
    const response = recordsResult();
    response.quality = { ...QUALITY, exactness: "future_exactness" };

    await expect(
      new FerricStoreClient(new FakeExecutor([response])).query(QUERY, {
        partition: "tenant-a",
        type: "invoice",
      }),
    ).rejects.toThrow(/quality/u);
  });

  it("rejects diagnostics outside the server's bounded wire contract", () => {
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
    const cases: Record<string, unknown>[] = [
      { ...diagnostic, detail: "x".repeat(1_025) },
      {
        ...diagnostic,
        context: Object.fromEntries(
          Array.from({ length: 17 }, (_, index) => [`field_${index}`, index]),
        ),
      },
      { ...diagnostic, context: { "": "invalid" } },
      { ...diagnostic, context: { fields: Array.from({ length: 33 }, () => 0) } },
      { ...diagnostic, context: { estimate: 1.5 } },
      { ...diagnostic, context: { estimate: 1n << 63n } },
      {
        ...diagnostic,
        context: { a: { b: { c: { d: { e: { f: { value: 1 } } } } } } },
      },
    ];

    expect(tryDecodeFlowQueryError(diagnostic)).toBeInstanceOf(FlowQueryError);
    for (const malformed of cases) {
      expect(tryDecodeFlowQueryError(malformed)).toBeUndefined();
    }
  });

  it("decodes bounded specialized EXPLAIN capabilities", async () => {
    const result = await new FerricStoreClient(
      new FakeExecutor([specializedExplainResult()]),
    ).explain("FROM runs WHERE run_id = @run RETURN RECORD", { run: "run-1" });

    expect(result).toMatchObject({
      status: "planned",
      capabilities: {
        requested: [],
        available: ["flow_query_point_v1"],
        missing: [],
      },
      stats: undefined,
      quality: undefined,
      alternatives: [],
    });
  });

  it("rejects malformed specialized EXPLAIN envelopes", async () => {
    const cases: ((response: Record<string, unknown>) => void)[] = [
      (response) => {
        delete response.capabilities;
      },
      (response) => {
        delete (response.capabilities as Record<string, unknown>).requested;
      },
      (response) => {
        (response.capabilities as Record<string, unknown>).available = [
          "flow_query_point_v1",
          "flow_query_point_v1",
        ];
      },
      (response) => {
        (response.capabilities as Record<string, unknown>).missing = Array.from(
          { length: 65 },
          (_, index) => `missing_${index}`,
        );
      },
      (response) => {
        response.stats = {};
      },
      (response) => {
        response.status = "executed";
      },
      (response) => {
        response.actual = null;
      },
    ];

    for (const mutate of cases) {
      const response = specializedExplainResult();
      mutate(response);
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).explain(
          "FROM runs WHERE run_id = @run RETURN RECORD",
          { run: "run-1" },
        ),
      ).rejects.toThrow(/explain/u);
    }
  });

  it("requires every typed index lifecycle section and service", async () => {
    const indexFields = [
      "source",
      "fields",
      "workloads",
      "count_prefixes",
      "coverage",
      "build",
      "validation",
      "retirement",
      "statistics",
    ];
    for (const field of indexFields) {
      const response = indexResult();
      const index = (response.indexes as Record<string, unknown>[])[0];
      if (index == null) throw new Error("index fixture must contain one index");
      response.indexes = [
        Object.fromEntries(Object.entries(index).filter(([name]) => name !== field)),
      ];
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).queryIndexes(),
      ).rejects.toThrow(/FLOW\.QUERY\.INDEXES/u);
    }
    for (const service of [
      "registry",
      "lifecycle_worker",
      "statistics_store",
      "statistics_worker",
    ]) {
      const response = indexResult();
      response.services = Object.fromEntries(
        Object.entries(response.services as Record<string, unknown>).filter(
          ([name]) => name !== service,
        ),
      );
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).queryIndexes(),
      ).rejects.toThrow(/services/u);
    }
  });

  it("decodes retirement progress without build-only scope metadata", async () => {
    const response = indexResult();
    const index = (response.indexes as Record<string, unknown>[])[0];
    if (index == null) throw new Error("index fixture must contain one index");
    index.state = "retiring";
    index.queryable = false;
    index.retirement = {
      status: "pending",
      phase_counts: { pending: 2 },
      current_phases: ["pending"],
      completed_shards: 0,
      total_shards: 2,
      deleted_entries: 0,
      deleted_bytes: 0,
      rewritten_reverse_rows: 0,
    };

    await expect(
      new FerricStoreClient(new FakeExecutor([response])).queryIndexes(),
    ).resolves.toMatchObject({
      indexes: [{ retirement: { status: "pending", completedShards: 0 } }],
    });
  });

  it("preserves unsigned 64-bit index counters and timestamps", async () => {
    const maximum = (1n << 64n) - 1n;
    const response = indexResult();
    const registry = response.registry as Record<string, unknown>;
    const index = (response.indexes as Record<string, unknown>[])[0];
    if (index == null) throw new Error("index fixture must contain one index");
    const coverage = index.coverage as Record<string, unknown>;
    const build = index.build as Record<string, unknown>;
    const validation = index.validation as Record<string, unknown>;
    const statistics = index.statistics as Record<string, unknown>;

    response.observed_at_ms = maximum;
    response.statistics_max_age_ms = maximum;
    registry.epoch = maximum;
    registry.catalog_version = maximum;
    index.version = maximum;
    index.state = "retiring";
    index.queryable = false;
    Object.assign(coverage, { complete_shards: maximum, total_shards: maximum });
    Object.assign(build, {
      phase_counts: { done: maximum },
      completed_shards: maximum,
      total_shards: maximum,
      scanned_records: maximum,
      written_entries: maximum,
      written_bytes: maximum,
    });
    Object.assign(validation, {
      phase_counts: { done: maximum },
      completed_shards: maximum,
      total_shards: maximum,
      checked_records: maximum,
      checked_entries: maximum,
      validated_at_ms: maximum,
    });
    index.retirement = {
      status: "complete",
      phase_counts: { done: maximum },
      current_phases: ["done"],
      completed_shards: maximum,
      total_shards: maximum,
      deleted_entries: maximum,
      deleted_bytes: maximum,
      rewritten_reverse_rows: maximum,
    };
    Object.assign(statistics, {
      samples: maximum,
      fresh_samples: maximum,
      stale_samples: 0,
      future_samples: 0,
      oldest_collected_at_ms: 0,
      newest_collected_at_ms: 0,
      oldest_age_ms: maximum,
      newest_age_ms: maximum,
    });

    const status = await new FerricStoreClient(
      new FakeExecutor([response]),
    ).queryIndexes();

    expect(status.observedAtMs).toBe(maximum);
    expect(status.indexes[0]?.build.scannedRecords).toBe(maximum);
    expect(status.indexes[0]?.validation.validatedAtMs).toBe(maximum);
    expect(status.indexes[0]?.retirement.deletedEntries).toBe(maximum);
    expect(status.indexes[0]?.statistics.samples).toBe(maximum);
  });

  it("rejects inconsistent bounded result accounting and short cursors", async () => {
    const cases: Record<string, unknown>[] = [
      { ...recordsResult(), usage: { ...USAGE, hydrated_records: 3 } },
      { ...recordsResult(), usage: { ...USAGE, duplicate_entries: 3 } },
      { ...recordsResult(), usage: { ...USAGE, range_pages: 4 } },
      { ...recordsResult(), usage: { ...USAGE, residual_checks: 25 } },
      {
        ...recordsResult(),
        usage: { ...USAGE, scanned_entries: 1, hydrated_records: 1 },
      },
      { ...recordsResult(), page: { has_more: true, cursor: "fqc1_short" } },
    ];
    for (const response of cases) {
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).query(QUERY, {
          partition: "tenant-a",
          type: "invoice",
        }),
      ).rejects.toThrow(/usage|cursor/u);
    }
  });

  it("preserves non-grammar leading whitespace when building EXPLAIN", async () => {
    const executor = new FakeExecutor([explainResult("planned")]);
    const query = "\u00a0FROM runs WHERE run_id = @id RETURN RECORD";

    await new FerricStoreClient(executor).explain(query, { id: "run-1" });

    expect(executor.calls[0]?.[2]).toBe(`EXPLAIN ${query}`);
  });

  it("does not case-fold Unicode confusables into FQL keywords", () => {
    expect(hasFlowExplainPrefix("EXPLA\u0131N FROM runs")).toBe(false);
  });

  it("rejects invalid parameter identifiers and values above 65535 bytes before IO", async () => {
    const executor = new FakeExecutor([]);
    const client = new FerricStoreClient(executor);
    for (const name of ["space name", "unicode_ä", "colon:name", "slash/name"]) {
      await expect(client.query(QUERY, { [name]: "value" })).rejects.toThrow(
        /parameter name/u,
      );
    }
    await expect(
      client.query(QUERY, { value: "x".repeat(65_536) }),
    ).rejects.toThrow(/65535 bytes/u);
    await expect(
      client.query(QUERY, { value: Buffer.alloc(65_536) }),
    ).rejects.toThrow(/65535 bytes/u);
    expect(executor.calls).toHaveLength(0);
  });

  it("requires bounded covering and format metadata in 0.11 index status", async () => {
    const cases: ((index: Record<string, unknown>) => void)[] = [
      (index) => { delete index.covering_fields; },
      (index) => { index.covering_fields = ["run_id", "run_id"]; },
      (index) => {
        index.covering_fields = Array.from(
          { length: 33 },
          (_, position) => `attribute.field_${position}`,
        );
      },
      (index) => { delete index.format; },
      (index) => {
        (index.format as Record<string, unknown>).counter = false;
      },
      (index) => {
        (index.format as Record<string, unknown>).counter = undefined;
      },
    ];

    for (const mutate of cases) {
      const response = indexResult();
      const index = (response.indexes as Record<string, unknown>[])[0];
      if (index == null) throw new Error("index fixture must contain one index");
      mutate(index);
      await expect(
        new FerricStoreClient(new FakeExecutor([response])).queryIndexes(),
      ).rejects.toThrow(/covering_fields|format/u);
    }
  });

  it("preserves sparse maps returned by a projected query", async () => {
    const projectedRecord = {
      id: Buffer.from("one"),
      state: Buffer.from("queued"),
      attributes: { customer: Buffer.from("acme") },
    };
    const executor = new FakeExecutor([
      {
        ...recordsResult(),
        records: [projectedRecord],
        page: { has_more: false, cursor: null },
        usage: { ...USAGE, result_records: 1 },
      },
    ]);
    const client = new FerricStoreClient(executor);
    const query =
      "FROM runs WHERE run_id = @run RETURN RECORD " +
      "(run_id, state, attribute['customer'])";

    const result = await client.query(query, { run: "one" });
    if (result.kind !== "records") throw new Error("expected records result");

    expect(result.records).toEqual([projectedRecord]);
    expect(Object.keys(result.records[0] ?? {}).sort()).toEqual([
      "attributes",
      "id",
      "state",
    ]);
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
    const missingProjection = queryCapabilities();
    const manifest = missingProjection.flow_query as Record<string, unknown>;
    manifest.capabilities = (manifest.capabilities as string[]).filter(
      (capability) => capability !== "flow_query_result_projection_v1",
    );
    expect(() =>
      nativeNegotiation({ capabilities: missingProjection }),
    ).toThrow("flow_query_result_projection_v1");
    expect(() => nativeNegotiation({ capabilities: { limits: {} } })).toThrow(
      "incompatible FerricStore server",
    );
  });
});
