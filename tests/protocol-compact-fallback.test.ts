import { describe, expect, it } from "vitest";

import {
  COMMAND_OPCODES,
  buildProtocolCommand,
  tryPipelineCommand
} from "../src/protocol.js";

describe("compact request fallback", () => {
  it("uses the typed MSET body when a value cannot use the compact binary body", () => {
    const value = { nested: [1, true] };

    expect(buildProtocolCommand(["MSET", "key", value])).toMatchObject({
      opcode: COMMAND_OPCODES.MSET,
      payload: { pairs: [["key", value]] }
    });
  });

  it("uses the typed pipeline body when a SET value cannot use the compact body", () => {
    const value = { nested: [1, true] };

    expect(tryPipelineCommand([["SET", "key", value]])).toMatchObject({
      opcode: COMMAND_OPCODES.PIPELINE,
      payload: {
        commands: [{ body: { key: "key", value }, opcode: COMMAND_OPCODES.SET }]
      }
    });
  });

  it("uses the typed FLOW.CREATE_MANY body for structured payloads", () => {
    const payload = { customer: "acme", items: [1, 2] };

    expect(buildProtocolCommand([
      "FLOW.CREATE_MANY", "AUTO", "TYPE", "order", "STATE", "queued", "NOW", 1,
      "ITEMS", "flow-1", payload
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.CREATE_MANY"],
      payload: {
        items: [["flow-1", payload]],
        now_ms: 1,
        state: "queued",
        type: "order"
      }
    });
  });

  it("rejects non-integer Flow fields before compact or typed encoding", () => {
    const lease = Buffer.from("lease");
    const commands = [
      [
        "FLOW.CREATE_MANY", "AUTO", "TYPE", "order", "STATE", "queued", "NOW", 1.5,
        "ITEMS", "flow-1", Buffer.alloc(0)
      ],
      [
        "FLOW.CLAIM_DUE", "order", "WORKER", "worker-1", "LEASE_MS", 1.5, "LIMIT", 1,
        "RETURN", "JOBS_COMPACT"
      ],
      [
        "FLOW.COMPLETE_MANY", "AUTO", "NOW", 1.5,
        "ITEMS", "flow-1", lease, 1
      ],
      [
        "FLOW.RETRY_MANY", "AUTO", "NOW", 1, "RUN_AT", 1.5,
        "ITEMS", "flow-1", lease, 1
      ],
      [
        "FLOW.FAIL_MANY", "AUTO", "NOW", 1.5,
        "ITEMS", "flow-1", lease, 1
      ]
    ] as const;

    for (const command of commands) {
      expect(() => buildProtocolCommand(command)).toThrow(
        "integer command argument must be an integer"
      );
      expect(() => buildProtocolCommand(command, Number.MAX_SAFE_INTEGER, false)).toThrow(
        "integer command argument must be an integer"
      );
    }
  });
});
