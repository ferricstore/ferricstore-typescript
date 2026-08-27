import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";

import type { Command } from "../src/internal.js";
import { nativeNegotiation } from "../src/native-negotiation.js";
import { compactPipelinePayload } from "../src/protocol-compact-request.js";

function binary(value: string): Buffer {
  const encoded = Buffer.from(value);
  const out = Buffer.allocUnsafe(4 + encoded.byteLength);
  out.writeUInt32BE(encoded.byteLength, 0);
  encoded.copy(out, 4);
  return out;
}

describe("compact Stream XADD pipeline", () => {
  it("keeps the exact mode-34 wire shape", () => {
    const payload = compactPipelinePayload(
      [
        ["XADD", "stream-a", "*", "field", "one"],
        [
          "xadd",
          Buffer.from("stream-b"),
          Buffer.from("*"),
          "first",
          "two",
          "second",
          "three",
        ],
      ],
      Number.MAX_SAFE_INTEGER,
    );

    expect(payload).toEqual(
      Buffer.concat([
        Buffer.from([0x94, 0x80 | 34, 0, 0, 0, 2]),
        binary("stream-a"),
        Buffer.from([0, 1]),
        binary("field"),
        binary("one"),
        binary("stream-b"),
        Buffer.from([0, 2]),
        binary("first"),
        binary("two"),
        binary("second"),
        binary("three"),
      ]),
    );
  });

  it.each<{ command: Command }>([
    { command: ["XADD", "stream", "1-0", "field", "value"] },
    { command: ["XADD", "stream", "NOMKSTREAM", "*", "field", "value"] },
    { command: ["XADD", "stream", "MAXLEN", "~", 100, "*", "field", "value"] },
    { command: ["XADD", "stream", {}, "field", "value"] },
    { command: ["XADD", "stream", "*"] },
    { command: ["XADD", "stream", "*", "field"] },
    { command: ["XADD", "stream", "*", "field", {}] },
  ])("declines non-fast-path grammar $command", ({ command }) => {
    expect(
      compactPipelinePayload([command], Number.MAX_SAFE_INTEGER),
    ).toBeUndefined();
  });

  it("honors the exact request-body limit", () => {
    const commands = [["XADD", "stream", "*", "field", "välue"]] as const;
    const payload = compactPipelinePayload(commands, Number.MAX_SAFE_INTEGER);

    expect(payload).toBeDefined();
    expect(compactPipelinePayload(commands, payload?.byteLength ?? 0)).toEqual(
      payload,
    );
    expect(() =>
      compactPipelinePayload(commands, (payload?.byteLength ?? 0) - 1),
    ).toThrow("frame limit");
  });

  it("rejects sparse command arguments before encoding", () => {
    const command = new Array(5);
    command[0] = "XADD";
    command[1] = "stream";
    command[2] = "*";
    command[3] = "field";

    expect(() =>
      compactPipelinePayload([command], Number.MAX_SAFE_INTEGER),
    ).toThrow("pipeline command arguments must be dense");
  });

  it("requires the server to advertise mode 34 at submission time", () => {
    const commands = [["XADD", "stream", "*", "field", "value"]] as const;

    expect(
      compactPipelinePayload(commands, Number.MAX_SAFE_INTEGER, false),
    ).toBeUndefined();
    expect(
      compactPipelinePayload(commands, Number.MAX_SAFE_INTEGER, true),
    ).toBeDefined();
  });

  it("negotiates mode 34 as an optional HELLO capability", () => {
    const capabilities = {
      limits: { max_response_bytes: 1024 },
      response_codecs: { compact_response_opcodes: {} },
      schemas: {
        "FLOW.QUERY": {
          fields: ["version", "query", "params", "deadline_ms"],
          required: ["version", "query"],
        },
      },
      flow_query: {
        request_contract: "ferric.flow.query.request/v1",
        result_contract: "ferric.flow.query.result/v1",
        explain_contract: "ferric.flow.explain/v1",
        index_status_contract: "ferric.flow.query.indexes/v1",
        capabilities: [
          "flow_query_v1",
          "flow_query_result_projection_v1",
          "flow_explain_v1",
          "flow_explain_analyze_v1",
          "flow_composite_index_v1",
          "flow_query_index_status_v1",
        ],
        language_versions: ["FQL1"],
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

    expect(nativeNegotiation({ capabilities }).compactStreamXAdd).toBe(false);
    expect(
      nativeNegotiation({
        capabilities: {
          ...capabilities,
          pipeline: { modes: { stream_xadd_auto: 34 } },
        },
      }).compactStreamXAdd,
    ).toBe(true);
  });
});
