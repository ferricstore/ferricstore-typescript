import { describe, expect, it } from "vitest";

import {
  COMMAND_OPCODES,
  OPCODES,
  buildProtocolCommand,
  tryPipelineCommand
} from "../src/protocol.js";

const BLOCK_MS_ERROR = "blockMs must be a safe non-negative integer no greater than 4294967295";
const INVALID_BLOCK_MS_VALUES = [
  0.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
  Number.NEGATIVE_INFINITY,
  4_294_967_296,
  Number.MAX_SAFE_INTEGER + 1,
  BigInt(Number.MAX_SAFE_INTEGER) + 1n,
  "0.5",
  "NaN",
  "Infinity",
  "4294967296",
  "9007199254740992"
] as const;
const MALFORMED_FALLBACK_BLOCK_VALUES = [
  false,
  null,
  { malformed: true },
  Buffer.from("not-a-number"),
  "0x10",
  " 10 "
] as const;

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

  it.each([...INVALID_BLOCK_MS_VALUES, -1])("rejects FLOW.CLAIM_DUE BLOCK %s before native encoding", (blockMs) => {
    const base = [
      "FLOW.CLAIM_DUE", "email", "STATE", "queued", "WORKER", "worker-1",
      "LEASE_MS", 30_000, "LIMIT", 1, "BLOCK", blockMs
    ] as const;
    const compact = () => buildProtocolCommand([...base, "RETURN", "JOBS_COMPACT"]);
    const records = () => buildProtocolCommand([...base, "RETURN", "RECORDS"]);
    expect(compact).toThrow(TypeError);
    expect(compact).toThrow(BLOCK_MS_ERROR);
    expect(records).toThrow(TypeError);
    expect(records).toThrow(BLOCK_MS_ERROR);
  });

  it.each([0.5, Number.NaN, 4_294_967_296, Number.MAX_SAFE_INTEGER + 1, "0.5"])(
    "rejects invalid FLOW.CLAIM_DUE BLOCK %s when an unsupported option forces fallback",
    (blockMs) => expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "UNSUPPORTED_OPTION", "value", "BLOCK", blockMs
    ])).toThrow(BLOCK_MS_ERROR)
  );

  it.each(["BLOCK", "BLOCK_MS"])("rejects odd-position %s after an unsupported option", (blockOption) => {
    expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "UNSUPPORTED_OPTION", blockOption, 0.5
    ])).toThrow(BLOCK_MS_ERROR);
  });

  it.each(["BLOCK", "BLOCK_MS"])("rejects missing %s successors after an unsupported option", (blockOption) => {
    expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "UNSUPPORTED_OPTION", blockOption
    ])).toThrow(BLOCK_MS_ERROR);
  });

  it.each(["BLOCK", "BLOCK_MS"])("rejects arbitrary %s successors after an unsupported option", (blockOption) => {
    for (const blockMs of MALFORMED_FALLBACK_BLOCK_VALUES) {
      expect(() => buildProtocolCommand([
        "FLOW.CLAIM_DUE", "email", "UNSUPPORTED_OPTION", blockOption, blockMs
      ])).toThrow(BLOCK_MS_ERROR);
    }
  });

  it("rejects truncated counted options before skipping their declared span", () => {
    for (const countOption of ["STATES", "PARTITIONS"] as const) {
      for (const blockOption of ["BLOCK", "BLOCK_MS"] as const) {
        for (const args of [
          [countOption, 3, blockOption],
          [countOption, 3, blockOption, false]
        ] as const) {
          expect(() => buildProtocolCommand([
            "FLOW.CLAIM_DUE", "email", ...args
          ])).toThrow(BLOCK_MS_ERROR);
        }
      }
    }
  });

  it("accepts numeric string BLOCK and skips required or structured BLOCK values", () => {
    expect(buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "WORKER", "worker-1", "BLOCK", "250", "RETURN", "JOBS_COMPACT"
    ])).toMatchObject({ serverBlockMs: 250 });
    expect(buildProtocolCommand([
      "FLOW.CLAIM_DUE", "BLOCK", "WORKER", "worker-1", "RETURN", "JOBS_COMPACT"
    ])).toMatchObject({ opcode: OPCODES.flowClaimDue });
    expect(buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "STATE", "BLOCK", "WORKER", "worker-1", "RETURN", "JOBS_COMPACT"
    ])).toMatchObject({ opcode: OPCODES.flowClaimDue });
    expect(() => buildProtocolCommand([
      "FLOW.CLAIM_DUE", "email", "UNSUPPORTED_OPTION", "BLOCK", "ordinary-value"
    ])).toThrow(BLOCK_MS_ERROR);
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
