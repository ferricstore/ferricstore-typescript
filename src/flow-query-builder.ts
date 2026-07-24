import { Buffer } from "node:buffer";
import type { ReadOptions, SearchOptions } from "./client-options.js";
import type {
  FlowQueryParameter,
  FlowQueryParameters,
} from "./flow-query-types.js";
import {
  compareUnicodeScalars,
  isPlainRecord,
  metadataEntries,
  metadataSelector,
  normalizeStateMeta,
  objectEntries,
} from "./flow-query-metadata.js";
import { validateFlowQueryText } from "./flow-query-request.js";

export const MAX_FLOW_QUERY_RESULTS = 100;
const MAX_FLOW_QUERY_PARTITION_BYTES = 65_535;
const MAX_FLOW_QUERY_TIME = Number.MAX_SAFE_INTEGER;
const MAX_FLOW_QUERY_STATE_BYTES = 64;

interface BuiltFlowQuery {
  readonly query: string;
  readonly params: FlowQueryParameters;
}

class FlowCollectionQuery {
  readonly params = Object.create(null) as Record<string, FlowQueryParameter>;
  readonly predicates = ["partition_key = @partition_key"];
  orderField = "updated_at_ms";

  constructor(
    partitionKey: string,
    private readonly limit: number,
    private readonly reverse: boolean,
  ) {
    this.params.partition_key = partitionKey;
  }

  equality(selector: string, parameter: string, value: unknown): void {
    this.params[parameter] = queryParameter(value, parameter);
    this.predicates.push(`${selector} = @${parameter}`);
  }

  window(fromMs: number | undefined, toMs: number | undefined): void {
    if (fromMs == null && toMs == null) return;
    const lower = fromMs == null ? 0 : boundedTime(fromMs, "fromMs");
    const upper =
      toMs == null ? MAX_FLOW_QUERY_TIME : boundedTime(toMs, "toMs");
    if (lower > upper) throw new TypeError("fromMs must not exceed toMs");
    this.predicates.push("updated_at_ms BETWEEN @from_ms AND @to_ms");
    this.params.from_ms = lower;
    this.params.to_ms = upper;
  }

  metadata(
    root: string,
    values: Readonly<Record<string, unknown>> | undefined,
  ): void {
    const entries = metadataEntries(values, root);
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index];
      if (entry == null) throw new TypeError("metadata entries must be dense");
      this.equality(
        metadataSelector(root, entry[0]),
        `${root}_${index}`,
        entry[1],
      );
    }
  }

  stateMetadata(
    values: Readonly<Record<string, Readonly<Record<string, unknown>>>>,
  ): void {
    const entries: [string, string, unknown][] = [];
    const states = new Set<string>();
    for (const [rawState, metadata] of objectEntries(values, "stateMeta")) {
      const state = requiredText(rawState, "stateMeta state").trim();
      const stateBytes = Buffer.byteLength(state, "utf8");
      if (stateBytes === 0 || stateBytes > MAX_FLOW_QUERY_STATE_BYTES) {
        throw new TypeError(
          `stateMeta state names must be 1..${MAX_FLOW_QUERY_STATE_BYTES} bytes`,
        );
      }
      if (states.has(state)) {
        throw new TypeError(
          "stateMeta state is duplicated after normalization",
        );
      }
      states.add(state);
      if (!isPlainRecord(metadata))
        throw new TypeError("stateMeta values must be metadata maps");
      for (const [name, value] of metadataEntries(metadata, "stateMeta")) {
        entries.push([state, name, value]);
      }
    }
    entries.sort(
      ([leftState, leftName], [rightState, rightName]) =>
        compareUnicodeScalars(leftState, rightState) ||
        compareUnicodeScalars(leftName, rightName),
    );
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index];
      if (entry == null) throw new TypeError("stateMeta entries must be dense");
      this.equality(
        metadataSelector("state_meta", entry[0], entry[1]),
        `state_meta_${index}`,
        entry[2],
      );
    }
  }

  build(): BuiltFlowQuery {
    if (this.predicates.length > 12) {
      throw new TypeError("FLOW.QUERY accepts at most 12 predicates");
    }
    const direction = this.reverse ? "DESC" : "ASC";
    const query =
      `FROM runs WHERE ${this.predicates.join(" AND ")} ` +
      `ORDER BY ${this.orderField} ${direction} LIMIT ${this.limit} RETURN RECORDS`;
    validateFlowQueryText(query);
    return { query, params: this.params };
  }
}

export function buildFlowListQuery(
  type: string,
  options: ReadOptions,
): BuiltFlowQuery {
  const attributeCount = Object.keys(options.attributes ?? {}).length;
  if (type === "any" && attributeCount === 0) {
    throw new TypeError(
      "FLOW.QUERY list requires a concrete flow type or an attribute predicate",
    );
  }
  if (
    options.terminalOnly !== true &&
    options.state === "any" &&
    attributeCount === 0
  ) {
    throw new TypeError(
      "FLOW.QUERY list state any requires an attribute predicate",
    );
  }
  const builder = queryBuilder(options);
  addType(builder, type);
  if (options.terminalOnly === true) {
    addTerminalState(builder, options.state);
  } else if (options.state == null || options.state === "") {
    builder.equality("state", "state", "queued");
  } else if (options.state !== "any") {
    builder.equality("state", "state", requiredText(options.state, "state"));
  }
  builder.metadata("attribute", options.attributes);
  builder.window(options.fromMs, options.toMs);
  return builder.build();
}

export function buildFlowSearchQuery(
  type: string,
  options: SearchOptions,
): BuiltFlowQuery {
  const stateMeta = normalizeStateMeta(options.stateMeta, options.state);
  if (
    Object.keys(options.attributes ?? {}).length === 0 &&
    Object.keys(stateMeta).length === 0
  ) {
    throw new TypeError(
      "FLOW.QUERY search requires an attribute or stateMeta predicate",
    );
  }
  if ((type === "" || type === "any") && Object.keys(stateMeta).length > 0) {
    throw new TypeError(
      "FLOW.QUERY stateMeta predicates require a concrete flow type",
    );
  }
  const builder = queryBuilder(options);
  if (type !== "" && type !== "any")
    builder.equality("type", "type", requiredText(type, "type"));
  if (options.terminalOnly === true) {
    addTerminalState(builder, options.state);
  } else if (
    options.state != null &&
    options.state !== "" &&
    options.state !== "any"
  ) {
    builder.equality("state", "state", requiredText(options.state, "state"));
  }
  builder.metadata("attribute", options.attributes);
  builder.stateMetadata(stateMeta);
  builder.window(options.fromMs, options.toMs);
  return builder.build();
}

export function buildFlowTerminalQuery(
  type: string,
  options: ReadOptions,
): BuiltFlowQuery {
  if (requiredText(type, "type") === "any") {
    throw new TypeError("FLOW.QUERY terminals require a concrete flow type");
  }
  if (metadataEntries(options.attributes, "attribute").length > 0) {
    throw new TypeError(
      "FLOW.QUERY terminals do not support attribute predicates",
    );
  }
  const builder = queryBuilder(options);
  addType(builder, type);
  addTerminalState(builder, options.state);
  builder.window(options.fromMs, options.toMs);
  return builder.build();
}

export function buildFlowFailureQuery(
  type: string,
  options: ReadOptions,
): BuiltFlowQuery {
  if (
    options.state != null &&
    options.state !== "" &&
    options.state !== "any" &&
    options.state !== "failed"
  ) {
    throw new TypeError("FLOW failures state must be failed or any");
  }
  return buildFlowListQuery(type, {
    ...options,
    state: "failed",
    terminalOnly: false,
  });
}

export function buildFlowLineageQuery(
  selector: "correlation_id" | "parent_flow_id" | "root_flow_id",
  id: string,
  options: ReadOptions,
): BuiltFlowQuery {
  if (options.terminalOnly === true) {
    throw new TypeError("terminalOnly cannot be combined with a lineage query");
  }
  if (metadataEntries(options.attributes, "attribute").length > 0) {
    throw new TypeError(
      "FLOW.QUERY lineage does not support attribute predicates",
    );
  }
  const builder = queryBuilder(options);
  builder.equality(selector, "lineage_id", requiredText(id, "lineage id"));
  if (
    options.state != null &&
    options.state !== "" &&
    options.state !== "any"
  ) {
    builder.equality("state", "state", requiredText(options.state, "state"));
  }
  builder.metadata("attribute", options.attributes);
  builder.window(options.fromMs, options.toMs);
  return builder.build();
}

export function buildFlowStuckQuery(
  type: string,
  options: {
    readonly partitionKey: string;
    readonly count?: number;
    readonly olderThanMs?: number;
    readonly nowMs?: number;
  },
): BuiltFlowQuery {
  const concreteType = requiredText(type, "type");
  if (concreteType === "any") {
    throw new TypeError("FLOW.QUERY stuck requires a concrete flow type");
  }
  const now =
    options.nowMs == null ? Date.now() : boundedTime(options.nowMs, "nowMs");
  const older =
    options.olderThanMs == null
      ? 0
      : boundedTime(options.olderThanMs, "olderThanMs");
  const cutoff = now - older;
  if (cutoff < 0) throw new TypeError("olderThanMs must not exceed nowMs");
  const builder = new FlowCollectionQuery(
    requiredPartition(options.partitionKey),
    boundedCount(options.count),
    false,
  );
  builder.orderField = "lease_deadline_ms";
  builder.equality("type", "type", concreteType);
  builder.equality("state", "state", "running");
  builder.predicates.push(
    "lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms",
  );
  builder.params.lease_from_ms = 0;
  builder.params.lease_to_ms = cutoff;
  return builder.build();
}

function queryBuilder(options: ReadOptions): FlowCollectionQuery {
  if (options.includeCold === true)
    throw new TypeError("FLOW.QUERY does not expose includeCold");
  if (options.consistentProjection === true) {
    throw new TypeError("FLOW.QUERY does not expose consistentProjection");
  }
  if (options.rev != null && typeof options.rev !== "boolean") {
    throw new TypeError("rev must be boolean");
  }
  return new FlowCollectionQuery(
    requiredPartition(options.partitionKey),
    boundedCount(options.count),
    options.rev !== false,
  );
}

function requiredPartition(value: string): string {
  if (typeof value !== "string") {
    throw new TypeError(
      "FLOW.QUERY convenience methods require a partition key",
    );
  }
  const size = Buffer.byteLength(value, "utf8");
  if (size === 0 || size > MAX_FLOW_QUERY_PARTITION_BYTES) {
    throw new TypeError(
      `FLOW.QUERY partition key must be 1..${MAX_FLOW_QUERY_PARTITION_BYTES} bytes`,
    );
  }
  return value;
}

function boundedCount(value: number | undefined): number {
  if (value == null) return MAX_FLOW_QUERY_RESULTS;
  if (
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > MAX_FLOW_QUERY_RESULTS
  ) {
    throw new TypeError(
      `FLOW.QUERY limit must be between 1 and ${MAX_FLOW_QUERY_RESULTS}`,
    );
  }
  return value;
}

function boundedTime(value: number, name: string): number {
  if (
    !Number.isSafeInteger(value) ||
    value < 0 ||
    value > MAX_FLOW_QUERY_TIME
  ) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

function addType(builder: FlowCollectionQuery, type: string): void {
  const normalized = requiredText(type, "type");
  if (normalized !== "any") builder.equality("type", "type", normalized);
}

function addTerminalState(
  builder: FlowCollectionQuery,
  state: string | undefined,
): void {
  if (state == null || state === "" || state === "any") {
    builder.predicates.push("state IN (@terminal_0, @terminal_1, @terminal_2)");
    builder.params.terminal_0 = "completed";
    builder.params.terminal_1 = "failed";
    builder.params.terminal_2 = "cancelled";
  } else if (
    state === "completed" ||
    state === "failed" ||
    state === "cancelled"
  ) {
    builder.equality("state", "state", state);
  } else {
    throw new TypeError(
      "terminal state must be completed, failed, cancelled, or any",
    );
  }
}

function requiredText(value: unknown, context: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${context} must be non-empty text`);
  }
  return value;
}

function queryParameter(value: unknown, context: string): FlowQueryParameter {
  if (
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    typeof value === "number" ||
    Buffer.isBuffer(value)
  ) {
    return value;
  }
  throw new TypeError(`${context} must be a scalar FLOW.QUERY parameter`);
}
