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

describe("compact Pub/Sub PUBLISH pipeline", () => {
  it("keeps the exact mode-35 wire shape", () => {
    const payload = compactPipelinePayload(
      [
        ["PUBLISH", "channel-a", "one"],
        ["publish", Buffer.from("channel-b"), Buffer.from("two")],
      ],
      Number.MAX_SAFE_INTEGER,
      true,
      true,
    );

    expect(payload).toEqual(
      Buffer.concat([
        Buffer.from([0x94, 0x80 | 35, 0, 0, 0, 2]),
        binary("channel-a"),
        binary("one"),
        binary("channel-b"),
        binary("two"),
      ]),
    );
  });

  it.each<{ command: Command }>([
    { command: ["PUBLISH", "channel"] },
    { command: ["PUBLISH", "channel", "message", "extra"] },
    { command: ["PUBLISH", {}, "message"] },
    { command: ["PUBLISH", "channel", {}] },
  ])("declines non-fast-path grammar $command", ({ command }) => {
    expect(
      compactPipelinePayload([command], Number.MAX_SAFE_INTEGER, true, true),
    ).toBeUndefined();
  });

  it("honors the exact request-body limit", () => {
    const commands = [["PUBLISH", "channel", "välue"]] as const;
    const payload = compactPipelinePayload(
      commands,
      Number.MAX_SAFE_INTEGER,
      true,
      true,
    );

    expect(payload).toBeDefined();
    expect(
      compactPipelinePayload(commands, payload?.byteLength ?? 0, true, true),
    ).toEqual(payload);
    expect(() =>
      compactPipelinePayload(
        commands,
        (payload?.byteLength ?? 0) - 1,
        true,
        true,
      ),
    ).toThrow("frame limit");
  });

  it("rejects sparse command arguments before encoding", () => {
    const command = new Array(3);
    command[0] = "PUBLISH";
    command[1] = "channel";

    expect(() =>
      compactPipelinePayload([command], Number.MAX_SAFE_INTEGER, true, true),
    ).toThrow("pipeline command arguments must be dense");
  });

  it("requires the server to advertise mode 35 at submission time", () => {
    const commands = [["PUBLISH", "channel", "message"]] as const;

    expect(
      compactPipelinePayload(commands, Number.MAX_SAFE_INTEGER, true, false),
    ).toBeUndefined();
    expect(
      compactPipelinePayload(commands, Number.MAX_SAFE_INTEGER, true, true),
    ).toBeDefined();
  });

  it("negotiates mode 35 as an optional HELLO capability", () => {
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

    expect(nativeNegotiation({ capabilities }).compactPubSubPublish).toBe(false);
    expect(
      nativeNegotiation({
        capabilities: {
          ...capabilities,
          pipeline: { modes: { pubsub_publish: 35 } },
        },
      }).compactPubSubPublish,
    ).toBe(true);
  });
});
