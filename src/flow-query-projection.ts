import { Buffer } from "node:buffer";
import {
  FLOW_QUERY_MAX_BYTES,
  validateFlowQueryText
} from "./flow-query-request.js";

const MAX_PROJECTION_FIELDS = 32;
const MAX_DYNAMIC_NAME_BYTES = 64;
const FIELD_BRAND = Symbol("FerricStoreFlowProjectionField");

export type FlowProjectionShape = "record" | "records";

export interface FlowRunProjectionField {
  readonly source: "runs";
  readonly selector: string;
  readonly [FIELD_BRAND]: true;
}

export interface FlowEventProjectionField {
  readonly source: "events";
  readonly selector: string;
  readonly [FIELD_BRAND]: true;
}

export type FlowProjectionField = FlowRunProjectionField | FlowEventProjectionField;

function field<Source extends "runs" | "events">(
  source: Source,
  selector: string
): Source extends "runs" ? FlowRunProjectionField : FlowEventProjectionField {
  return Object.freeze({ source, selector, [FIELD_BRAND]: true }) as unknown as
    Source extends "runs" ? FlowRunProjectionField : FlowEventProjectionField;
}

const run = Object.freeze({
  id: field("runs", "run_id"),
  type: field("runs", "type"),
  state: field("runs", "state"),
  version: field("runs", "version"),
  priority: field("runs", "priority"),
  partitionKey: field("runs", "partition_key"),
  createdAtMs: field("runs", "created_at_ms"),
  updatedAtMs: field("runs", "updated_at_ms"),
  nextRunAtMs: field("runs", "next_run_at_ms"),
  leaseDeadlineMs: field("runs", "lease_deadline_ms"),
  attempts: field("runs", "attempts"),
  runState: field("runs", "run_state"),
  maxActiveMs: field("runs", "max_active_ms"),
  parentFlowId: field("runs", "parent_flow_id"),
  rootFlowId: field("runs", "root_flow_id"),
  correlationId: field("runs", "correlation_id"),
  attributes: field("runs", "attributes"),
  stateMetadata: field("runs", "state_meta"),
  attribute(name: string): FlowRunProjectionField {
    return field("runs", `attribute[${quoteName(name, false)}]`);
  },
  stateMeta(state: string, name: string): FlowRunProjectionField {
    return field(
      "runs",
      `state_meta[${quoteName(state, true)}][${quoteName(name, false)}]`
    );
  }
});

const event = Object.freeze({
  id: field("events", "event_id"),
  fields: field("events", "fields"),
  field(name: string): FlowEventProjectionField {
    return field("events", `fields[${quoteName(name, false)}]`);
  }
});

export const FlowProjection = Object.freeze({ run, event });

export function projectFlowQuery(
  query: string,
  shape: FlowProjectionShape,
  ...fields: readonly FlowProjectionField[]
): string {
  validateFlowQueryText(query);
  if (shape !== "record" && shape !== "records") {
    throw new TypeError("Flow query projection shape must be record or records");
  }
  if (fields.length === 0 || fields.length > MAX_PROJECTION_FIELDS) {
    throw new TypeError(
      `Flow query projection must contain 1..${MAX_PROJECTION_FIELDS} fields`
    );
  }
  if (!fields.every((item) => item?.[FIELD_BRAND])) {
    throw new TypeError("Flow query projection accepts only FlowProjection fields");
  }

  const source = querySource(query);
  if (fields.some((item) => item.source !== source)) {
    throw new TypeError(`Flow query projection fields must belong to ${source}`);
  }
  const selectors = fields.map((item) => item.selector);
  if (new Set(selectors).size !== selectors.length) {
    throw new TypeError("Flow query projection contains a duplicate field");
  }
  if (containsReturnKeyword(query)) {
    throw new TypeError("Flow query already contains a RETURN clause");
  }

  const trimmed = query.trim();
  const base = trimmed.endsWith(";") ? trimmed.slice(0, -1).trimEnd() : trimmed;
  const result = `${base} RETURN ${shape.toUpperCase()} (${selectors.join(", ")})`;
  if (Buffer.byteLength(result, "utf8") > FLOW_QUERY_MAX_BYTES) {
    throw new TypeError(`FLOW.QUERY query exceeds ${FLOW_QUERY_MAX_BYTES} bytes`);
  }
  return result;
}

function querySource(query: string): "runs" | "events" {
  const match = /^\s*FROM\s+(runs|events)\b/iu.exec(query);
  if (match?.[1] == null) {
    throw new TypeError("Projected Flow query must start with FROM runs or FROM events");
  }
  return match[1].toLowerCase() as "runs" | "events";
}

function containsReturnKeyword(query: string): boolean {
  let quoted = false;
  for (let index = 0; index < query.length;) {
    const character = query.charAt(index);
    if (character === "'") {
      if (quoted && query[index + 1] === "'") {
        index += 2;
        continue;
      }
      quoted = !quoted;
      index += 1;
      continue;
    }
    if (!quoted && /[A-Za-z_]/u.test(character)) {
      let end = index + 1;
      while (end < query.length && /[A-Za-z0-9_]/u.test(query.charAt(end))) end += 1;
      if (query.slice(index, end).toUpperCase() === "RETURN") return true;
      index = end;
      continue;
    }
    index += 1;
  }
  return false;
}

function quoteName(value: string, allowPrivate: boolean): string {
  if (typeof value !== "string") {
    throw new TypeError("Flow query projection metadata name must be text");
  }
  validateUnicodeScalarText(value);
  const size = Buffer.byteLength(value, "utf8");
  if (
    size === 0 ||
    size > MAX_DYNAMIC_NAME_BYTES ||
    (!allowPrivate && value.startsWith("__"))
  ) {
    throw new TypeError(
      `Flow query projection metadata names must be 1..${MAX_DYNAMIC_NAME_BYTES} UTF-8 bytes`
    );
  }
  return `'${value.replaceAll("'", "''")}'`;
}

function validateUnicodeScalarText(value: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (!(low >= 0xdc00 && low <= 0xdfff)) {
        throw new TypeError("Flow query projection metadata name must be valid UTF-8");
      }
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new TypeError("Flow query projection metadata name must be valid UTF-8");
    }
  }
}
