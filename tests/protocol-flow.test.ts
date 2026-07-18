import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  COMMAND_OPCODES,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  buildProtocolCommand,
  decodeResponse,
  encodeValue,
  pipelineCommand,
  tryPipelineCommand,
  unwrapPipelineResponse,
  type ResponseFrame
} from "../src/protocol.js";
import { InvalidCommandError, OverloadedError } from "../src/errors.js";
import type { CommandArgument } from "../src/internal.js";
import { compactResponseHints } from "./compact-response-test-support.js";

describe("native Flow protocol codec", () => {
  it("decodes compact GET responses", () => {
    const value = Buffer.from("value");
    const body = Buffer.concat([Buffer.from([0, 0, 0x82, 1]), u32(value.byteLength), value]);
    const decoded = decodeResponse(responseFrame(OPCODES.get, body), OPCODES.get, compactResponseHints);

    expect(Buffer.isBuffer(decoded)).toBe(true);
    expect((decoded as Buffer).toString("utf8")).toBe("value");
  });

  it("decodes compact OK lists for every direct server opcode", () => {
    const body = Buffer.concat([Buffer.from([0, 0, 0x81]), u32(2)]);

    for (const opcode of [
      OPCODES.flowRetryMany,
      OPCODES.flowFailMany,
      COMMAND_OPCODES["FLOW.CANCEL_MANY"]
    ]) {
      const decoded = decodeResponse(responseFrame(opcode, body), opcode, compactResponseHints);
      expect((decoded as Buffer[]).map((item) => item.toString("utf8"))).toEqual(["OK", "OK"]);
    }
  });

  it("builds compact homogeneous pipelines and unwraps response pairs", () => {
    const pipeline = pipelineCommand([
      ["GET", "a"],
      ["GET", "b"]
    ]);

    expect(pipeline.opcode).toBe(OPCODES.pipeline);
    expect(pipeline.flags).toBe(0x02);
    expect(unwrapPipelineResponse([["ok", Buffer.from("a")], ["ok", null]])).toEqual([Buffer.from("a"), null]);
  });

  it("writes compact GET string keys without allocating a Buffer per key", () => {
    const keys = ["plain", "naïve", "🙂"];
    const from = vi.spyOn(Buffer, "from");

    try {
      const pipeline = pipelineCommand(keys.map((key) => ["GET", key]));

      expect(pipeline.flags).toBe(FLAG_CUSTOM_PAYLOAD);
      expect(from.mock.calls.filter(([value]) =>
        typeof value === "string" && keys.includes(value)
      )).toHaveLength(0);
    } finally {
      from.mockRestore();
    }
  });

  it("writes compact SET string keys without allocating a Buffer per key", () => {
    const keys = ["plain", "naïve", "🙂"];
    const value = Buffer.from("value");
    const from = vi.spyOn(Buffer, "from");

    try {
      const pipeline = pipelineCommand(keys.map((key) => ["SET", key, value]));

      expect(pipeline.flags).toBe(FLAG_CUSTOM_PAYLOAD);
      expect(from.mock.calls.filter(([item]) =>
        typeof item === "string" && keys.includes(item)
      )).toHaveLength(0);
    } finally {
      from.mockRestore();
    }
  });

  it("uses typed bodies without constructing custom payloads in generic pipelines", () => {
    const pipeline = pipelineCommand([
      ["MGET", "a", "b"],
      ["MGET", "c", "d"]
    ]);

    expect(pipeline.flags).toBeUndefined();
    expect(pipeline.opcode).toBe(OPCODES.pipeline);
    expect(pipeline.payload).toMatchObject({
      commands: [
        { body: { keys: ["a", "b"] }, opcode: OPCODES.mget },
        { body: { keys: ["c", "d"] }, opcode: OPCODES.mget }
      ]
    });
  });

  it("decodes compact pipeline successes without status tuple allocation", () => {
    const value = Buffer.from("a");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x95]),
      u32(2),
      Buffer.from([0, 1]),
      binary(value),
      Buffer.from([0, 0])
    ]);
    const decoded = decodeResponse(
      responseFrame(OPCODES.pipeline, body), OPCODES.pipeline, compactResponseHints
    );

    expect(decoded).toEqual([value, null]);
    expect(unwrapPipelineResponse(decoded)).toBe(decoded);
    expect(unwrapPipelineResponse(decoded)).toEqual([value, null]);
  });

  it("does not confuse a compact pipeline list value with a status tuple", () => {
    const first = Buffer.from("ok");
    const second = Buffer.from("payload");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x95]),
      u32(1),
      Buffer.from([0, 6]),
      u32(2),
      binary(first),
      binary(second)
    ]);
    const decoded = decodeResponse(
      responseFrame(OPCODES.pipeline, body), OPCODES.pipeline, compactResponseHints
    );

    expect(unwrapPipelineResponse(decoded)).toEqual([[first, second]]);
  });

  it("surfaces typed errors from already-decoded compact pipeline responses", () => {
    const raw = Buffer.from("server is busy");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x95]),
      u32(2),
      Buffer.from([0, 0]),
      Buffer.from([1]),
      binary(raw)
    ]);
    const decoded = decodeResponse(
      responseFrame(OPCODES.pipeline, body), OPCODES.pipeline, compactResponseHints
    );
    const collected = unwrapPipelineResponse(decoded, { throwOnItemError: false });

    expect(collected).toBe(decoded);
    expect(collected[0]).toBeNull();
    expect(collected[1]).toBeInstanceOf(OverloadedError);
    expect(() => unwrapPipelineResponse(decoded)).toThrow(OverloadedError);
  });

  it("rejects malformed pipeline response shapes and cardinality", () => {
    expect(() => unwrapPipelineResponse(Buffer.from("OK"))).toThrow("invalid response");
    expect(() => unwrapPipelineResponse([Buffer.from("only-one")], {}, 2)).toThrow("expected 2 items");
  });

  it("builds direct native FLOW.CREATE_MANY for mixed partition batches", () => {
    const payload = Buffer.from("payload");
    const command = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "MIXED",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "INDEPENDENT",
      true,
      "ITEMS",
      "flow-1",
      "p1",
      payload,
      "flow-2",
      "p2",
      payload
    ]);

    expect(command.opcode).toBe(OPCODES.flowCreateMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x9e);
  });

  it("builds direct native FLOW.CLAIM_DUE with compact job return options", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      100,
      "PARTITIONS",
      2,
      "p1",
      "p2",
      "RETURN",
      "JOBS_COMPACT",
      "NOPAYLOAD"
    ]);

    expect(command.opcode).toBe(OPCODES.flowClaimDue);
    expect(command.flags).toBe(0x02);
    expect(command).toMatchObject({ compactClaimMode: "base" });
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x91);
  });

  it("preserves the server reclaim default in compact FLOW.CLAIM_DUE", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      100,
      "RETURN",
      "JOBS_COMPACT"
    ]);

    expect(command.flags).toBe(0x02);
    expect(compactClaimReclaimExpired(command.payload as Buffer)).toBe(true);
  });

  it("preserves repeated FLOW.CLAIM_DUE state filters in a native states payload", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "created",
      "STATE",
      "charged",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      100,
      "RETURN",
      "JOBS_COMPACT"
    ]);

    expect(command.opcode).toBe(OPCODES.flowClaimDue);
    expect(command.flags).toBeUndefined();
    expect(command.payload).toMatchObject({ states: ["created", "charged"] });
  });

  it("uses generic command execution for full-record multi-state claims", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATES",
      2,
      "created",
      "charged",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      10
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.commandExec,
      payload: {
        args: [
          "email",
          "STATE",
          "created",
          "STATE",
          "charged",
          "WORKER",
          "worker-1",
          "LEASE_MS",
          30000,
          "LIMIT",
          10
        ],
        command: "FLOW.CLAIM_DUE"
      }
    });
  });

  it("keeps full-record FLOW.CLAIM_DUE on generic execution", () => {
    const command = buildProtocolCommand([
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "WORKER",
      "worker-1",
      "LEASE_MS",
      30000,
      "LIMIT",
      1
    ]);

    expect(command.opcode).toBe(OPCODES.commandExec);
  });

  it("builds direct native FLOW.CREATE for simple queue creates", () => {
    const command = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "PARTITION",
      "p1",
      "ATTRIBUTE",
      "region",
      "us-east",
      "RUN_AT",
      1000,
      "PRIORITY",
      1
    ]);

    expect(command.opcode).toBe(OPCODES.flowCreate);
    expect(command.payload).toMatchObject({
      id: "flow-1",
      type: "email",
      state: "queued",
      now_ms: 1000,
      partition_key: "p1",
      attributes: { region: "us-east" },
      run_at_ms: 1000,
      priority: 1
    });
  });

  it("keeps shared attributes on direct native FLOW.CREATE_MANY", () => {
    const command = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "tenant-a",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "ATTRIBUTE",
      "region",
      "us-east",
      "ITEMS",
      "flow-1",
      Buffer.alloc(0)
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowCreateMany,
      payload: {
        attributes: { region: "us-east" },
        items: [["flow-1", Buffer.alloc(0)]],
        partition_key: "tenant-a"
      }
    });
    expect(command.flags).toBeUndefined();
  });

  it("builds direct native FLOW.CREATE with a payload", () => {
    const payload = Buffer.from("payload");
    const command = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "PAYLOAD",
      payload
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowCreate,
      payload: {
        id: "flow-1",
        now_ms: 1000,
        payload,
        state: "queued",
        type: "email"
      }
    });
  });

  it("uses typed state metadata for supported Flow commands", () => {
    const create = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "email",
      "STATE",
      "queued",
      "NOW",
      1000,
      "STATE_META",
      "version",
      "1"
    ]);
    const complete = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      Buffer.from("lease-token"),
      "FENCING",
      7,
      "NOW",
      2000,
      "STATE_META",
      "version",
      "2"
    ]);
    const policy = buildProtocolCommand([
      "FLOW.POLICY.SET",
      "email",
      "INDEXED_STATE_META",
      "version"
    ]);

    expect(create).toMatchObject({
      opcode: OPCODES.flowCreate,
      payload: {
        id: "flow-1",
        now_ms: 1000,
        state: "queued",
        state_meta: { version: "1" },
        type: "email"
      }
    });
    expect(complete).toMatchObject({
      opcode: OPCODES.flowComplete,
      payload: {
        fencing_token: 7,
        id: "flow-1",
        lease_token: Buffer.from("lease-token"),
        now_ms: 2000,
        state_meta: { version: "2" }
      }
    });
    expect(policy).toMatchObject({
      opcode: OPCODES.commandExec,
      payload: {
        args: ["email", "INDEXED_STATE_META", "version"],
        command: "FLOW.POLICY.SET"
      }
    });
  });

  it("moves request context out of generic command-exec args", () => {
    const command = buildProtocolCommand([
      "INVOCATION.CREATE",
      "send-email",
      "{}",
      "REQUEST_CONTEXT",
      {
        scopes: "invocation:create:* tenant:acme",
        subject: "proxy",
        tenant: "acme"
      }
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.commandExec,
      payload: {
        args: ["send-email", "{}"],
        command: "INVOCATION.CREATE",
        request_context: {
          scopes: ["invocation:create:*", "tenant:acme"],
          subject: "proxy",
          tenant: "acme"
        }
      }
    });
  });

  it("moves request context out of explicit COMMAND_EXEC args", () => {
    const command = buildProtocolCommand([
      "COMMAND_EXEC",
      "INVOCATION.CREATE",
      "send-email",
      "{}",
      "REQUEST_CONTEXT",
      {
        scopes: ["invocation:create:*", "invocation:create:*"],
        subject: "proxy"
      }
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.commandExec,
      payload: {
        args: ["send-email", "{}"],
        command: "INVOCATION.CREATE",
        request_context: {
          scopes: ["invocation:create:*"],
          subject: "proxy"
        }
      }
    });
  });

  it("fails closed instead of stripping malformed request contexts", () => {
    const malformedContexts: readonly unknown[] = [
      "not-an-object",
      { subject: 42 },
      { scopes: ["invocation:create:*", 42] },
      { scope: "invocation:create:*" }
    ];

    for (const context of malformedContexts) {
      expect(() => buildProtocolCommand([
        "INVOCATION.CREATE",
        "send-email",
        "{}",
        "REQUEST_CONTEXT",
        context as CommandArgument
      ])).toThrow(InvalidCommandError);
      expect(() => buildProtocolCommand([
        "COMMAND_EXEC",
        "INVOCATION.CREATE",
        "send-email",
        "{}",
        "REQUEST_CONTEXT",
        context as CommandArgument
      ])).toThrow(/request context/i);
    }
  });

  it("builds direct native FLOW.SEARCH with attributes and state metadata", () => {
    const command = buildProtocolCommand([
      "FLOW.SEARCH",
      "email",
      "STATE",
      "queued",
      "COUNT",
      10,
      "ATTRIBUTE",
      "tenant",
      "INDEXED_STATE_META",
      "STATE_META",
      "queued",
      { version: 3 },
      "TERMINAL_ONLY",
      "true"
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowSearch,
      payload: {
        attributes: { tenant: "INDEXED_STATE_META" },
        count: 10,
        state: "queued",
        state_meta: { queued: { version: 3 } },
        terminal_only: true,
        type: "email"
      }
    });

    expect(buildProtocolCommand([
      "FLOW.SEARCH",
      "email",
      "STATE",
      "queued",
      "STATE_META",
      "version",
      3
    ])).toMatchObject({
      opcode: OPCODES.flowSearch,
      payload: {
        state_meta: { queued: { version: 3 } }
      }
    });
  });

  it("keeps invalid FLOW.SEARCH boolean tokens on the server-validated command path", () => {
    const args: readonly CommandArgument[] = ["FLOW.SEARCH", "email", "REV", "definitely"];

    expect(buildProtocolCommand(args)).toMatchObject({
      opcode: OPCODES.commandExec,
      payload: {
        args: args.slice(1),
        command: "FLOW.SEARCH"
      }
    });
  });

  it("does not validate command-only FLOW.SEARCH grammar on the client", () => {
    const args: readonly CommandArgument[] = [
      "FLOW.SEARCH",
      "email",
      "INDEXED_STATE_META",
      "version",
      "STATE_META",
      "version",
      3
    ];

    expect(buildProtocolCommand(args)).toEqual({
      opcode: OPCODES.commandExec,
      payload: {
        args: args.slice(1),
        command: "FLOW.SEARCH"
      }
    });
  });

  it("validates integer FLOW.SIGNAL options before native dispatch", () => {
    const args: readonly CommandArgument[] = [
      "FLOW.SIGNAL",
      "flow-1",
      "SIGNAL",
      "wake",
      "PARTITION",
      "tenant-a",
      "NOW",
      "not-a-number"
    ];

    expect(() => buildProtocolCommand(args)).toThrow("integer command argument must be an integer");
  });

  it("preserves prototype-shaped attribute names in native command maps", () => {
    const command = buildProtocolCommand([
      "FLOW.SEARCH",
      "email",
      "ATTRIBUTE",
      "__proto__",
      "safe"
    ]);
    const attributes = (command.payload as { attributes: Record<string, unknown> }).attributes;

    expect(Object.getPrototypeOf(attributes)).toBe(Object.prototype);
    expect(Object.hasOwn(attributes, "__proto__")).toBe(true);
    expect(attributes.__proto__).toBe("safe");
  });

  it("builds direct native FLOW.COMPLETE for simple claimed completions", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      lease,
      "FENCING",
      7,
      "NOW",
      2000,
      "PARTITION",
      "p1"
    ]);

    expect(command.opcode).toBe(OPCODES.flowComplete);
    expect(command.payload).toMatchObject({
      id: "flow-1",
      lease_token: lease,
      fencing_token: 7,
      now_ms: 2000,
      partition_key: "p1"
    });
  });

  it("builds direct native FLOW.COMPLETE with a result", () => {
    const lease = Buffer.from("lease-token");
    const result = Buffer.from("result");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE",
      "flow-1",
      lease,
      "FENCING",
      7,
      "NOW",
      2000,
      "RESULT",
      result
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowComplete,
      payload: {
        fencing_token: 7,
        id: "flow-1",
        lease_token: lease,
        now_ms: 2000,
        result
      }
    });
  });

  it("builds direct native FLOW.COMPLETE_MANY for claimed mixed batches", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "INDEPENDENT",
      true,
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x92);
  });

  it("keeps FLOW.COMPLETE_MANY OK-on-success on the direct native opcode", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBe(0x02);
    expect(Buffer.isBuffer(command.payload)).toBe(true);
    expect((command.payload as Buffer).readUInt8(0)).toBe(0x93);
  });

  it("uses compact OK-on-success requests for representable retry and fail batches", () => {
    const lease = Buffer.from("lease-token");
    const retry = buildProtocolCommand([
      "FLOW.RETRY_MANY",
      "MIXED",
      "NOW",
      2_000,
      "RUN_AT",
      3_000,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);
    const fail = buildProtocolCommand([
      "FLOW.FAIL_MANY",
      "MIXED",
      "NOW",
      2_000,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(retry).toMatchObject({ flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.flowRetryMany });
    expect((retry.payload as Buffer).readUInt8(0)).toBe(0x98);
    expect(fail).toMatchObject({ flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.flowFailMany });
    expect((fail.payload as Buffer).readUInt8(0)).toBe(0x93);
  });

  it("uses compact OK-on-success requests for representable cancel batches", () => {
    const command = buildProtocolCommand([
      "FLOW.CANCEL_MANY",
      "MIXED",
      "NOW",
      123,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      7
    ]);

    expect(command).toMatchObject({
      flags: FLAG_CUSTOM_PAYLOAD,
      opcode: COMMAND_OPCODES["FLOW.CANCEL_MANY"]
    });
    expect(command.payload).toEqual(Buffer.concat([
      Buffer.from([0x9a]),
      u32(0xffff_ffff),
      i64(123n),
      Buffer.from([2]),
      u32(1),
      binary(Buffer.from("flow-1")),
      binary(Buffer.from("p1")),
      i64(7n)
    ]));
  });

  it("preserves literal AUTO partitions in compact cancel batches", () => {
    const command = buildProtocolCommand([
      "FLOW.CANCEL_MANY",
      "AUTO",
      "NOW",
      123,
      "ITEMS",
      "flow-1",
      7
    ]);

    expect(command.payload).toEqual(Buffer.concat([
      Buffer.from([0x99]),
      binary(Buffer.from("AUTO")),
      i64(123n),
      Buffer.from([0]),
      u32(1),
      binary(Buffer.from("flow-1")),
      u32(0xffff_ffff),
      i64(7n)
    ]));
  });

  it("uses compact OK-on-success requests for representable transition batches", () => {
    const command = buildProtocolCommand([
      "FLOW.TRANSITION_MANY",
      "MIXED",
      "queued",
      "next",
      "NOW",
      123,
      "RUN_AT",
      456,
      "INDEPENDENT",
      true,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      7,
      "-"
    ]);

    expect(command).toMatchObject({
      flags: FLAG_CUSTOM_PAYLOAD,
      opcode: COMMAND_OPCODES["FLOW.TRANSITION_MANY"]
    });
    expect(command.payload).toEqual(Buffer.concat([
      Buffer.from([0x9c]),
      binary(Buffer.from("queued")),
      binary(Buffer.from("next")),
      u32(0xffff_ffff),
      i64(123n),
      i64(456n),
      Buffer.from([2]),
      u32(1),
      binary(Buffer.from("flow-1")),
      binary(Buffer.from("p1")),
      i64(7n),
      u32(0xffff_ffff)
    ]));
  });

  it("keeps bigint fencing tokens on compact many-item hot paths", () => {
    const fencingToken = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const lease = Buffer.from("lease");
    const commands = [
      ["FLOW.COMPLETE_MANY", "MIXED", "NOW", 123, "ITEMS", "flow-1", "p1", lease, fencingToken],
      ["FLOW.RETRY_MANY", "MIXED", "NOW", 123, "RUN_AT", 456, "ITEMS", "flow-1", "p1", lease, fencingToken],
      ["FLOW.FAIL_MANY", "MIXED", "NOW", 123, "ITEMS", "flow-1", "p1", lease, fencingToken],
      ["FLOW.CANCEL_MANY", "MIXED", "NOW", 123, "ITEMS", "flow-1", "p1", fencingToken],
      ["FLOW.TRANSITION_MANY", "MIXED", "queued", "next", "NOW", 123, "ITEMS", "flow-1", "p1", fencingToken, "-"]
    ] as const;

    for (const args of commands) {
      const command = buildProtocolCommand(args);
      expect(command.flags).toBe(FLAG_CUSTOM_PAYLOAD);
      expect((command.payload as Buffer).includes(i64(fencingToken))).toBe(true);
    }
  });

  it("preserves binary fixed partitions in compact and typed transition batches", () => {
    const partition = Buffer.from([0xff, 0x00, 0x61]);
    const args = [
      "FLOW.TRANSITION_MANY",
      partition,
      "queued",
      "next",
      "NOW",
      123,
      "ITEMS",
      "flow-1",
      7,
      "-"
    ] as const;
    const compact = buildProtocolCommand(args);
    const typed = buildProtocolCommand(args, Number.MAX_SAFE_INTEGER, false);

    expect(compact.payload).toEqual(Buffer.concat([
      Buffer.from([0x9b]),
      binary(Buffer.from("queued")),
      binary(Buffer.from("next")),
      binary(partition),
      i64(123n),
      i64(123n),
      Buffer.from([0]),
      u32(1),
      binary(Buffer.from("flow-1")),
      u32(0xffff_ffff),
      i64(7n),
      u32(0xffff_ffff)
    ]));
    expect(typed).toMatchObject({ payload: { partition_key: partition } });
  });

  it("keeps rich retry and fail batches generic while retaining OK-on-success", () => {
    const lease = Buffer.from("lease-token");
    for (const name of ["FLOW.RETRY_MANY", "FLOW.FAIL_MANY"] as const) {
      const command = buildProtocolCommand([
        name,
        "tenant-a",
        "NOW",
        2_000,
        "ERROR",
        Buffer.from("failure"),
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        lease,
        7
      ]);

      expect(command).toMatchObject({
        opcode: COMMAND_OPCODES[name],
        payload: { return: "OK_ON_SUCCESS" }
      });
      expect(command.flags).toBeUndefined();
    }
  });

  it("keeps TTL-bearing FLOW.COMPLETE_MANY on generic native encoding", () => {
    const lease = Buffer.from("lease-token");
    const command = buildProtocolCommand([
      "FLOW.COMPLETE_MANY",
      "MIXED",
      "NOW",
      2000,
      "TTL",
      5000,
      "RETURN",
      "OK_ON_SUCCESS",
      "ITEMS",
      "flow-1",
      "p1",
      lease,
      7
    ]);

    expect(command.opcode).toBe(OPCODES.flowCompleteMany);
    expect(command.flags).toBeUndefined();
    expect(command.payload).toMatchObject({
      return: "OK_ON_SUCCESS",
      ttl_ms: 5000
    });
  });

  it("decodes compact claim jobs with attributes", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const attrs = { tenant: Buffer.from("acme") };
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x80]),
      u32(1),
      binary(id),
      binary(partition),
      binary(lease),
      i64(9n),
      encodeValue(attrs)
    ]);

    const decoded = decodeResponse(
      responseFrame(OPCODES.flowClaimDue, body), OPCODES.flowClaimDue, compactResponseHints
    );

    expect(decoded).toEqual([[id, partition, lease, 9, null, attrs]]);
  });

  it("decodes compact claim jobs with state but no attributes", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const runState = Buffer.from("running:step");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x80]),
      u32(1),
      binary(id),
      binary(partition),
      binary(lease),
      i64(9n),
      binary(runState)
    ]);

    expect(decodeResponse(
      responseFrame(OPCODES.flowClaimDue, body), OPCODES.flowClaimDue, compactResponseHints
    )).toEqual([
      [id, partition, lease, 9, runState]
    ]);
  });

  it("uses the correlated compact claim mode instead of guessing another valid shape", () => {
    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const runState = Buffer.from("running:step");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x80]),
      u32(1),
      binary(id),
      binary(partition),
      binary(lease),
      i64(9n),
      binary(runState)
    ]);
    const frame = responseFrame(OPCODES.flowClaimDue, body);
    expect(() => decodeResponse(frame, OPCODES.flowClaimDue, {
      ...compactResponseHints,
      compactClaimMode: "base"
    })).toThrow(
      "expected base"
    );
    expect(decodeResponse(frame, OPCODES.flowClaimDue, {
      ...compactResponseHints,
      compactClaimMode: "state"
    })).toEqual([
      [id, partition, lease, 9, runState]
    ]);
  });

  it("correlates compact claim modes inside native pipeline responses", () => {
    const claim = [
      "FLOW.CLAIM_DUE",
      "email",
      "STATE",
      "queued",
      "RETURN",
      "JOBS_COMPACT_STATE"
    ] as const;
    const pipeline = tryPipelineCommand([claim, ["GET", "key"]]);
    expect(pipeline?.pipelineClaimModes).toEqual(["state", undefined]);

    const id = Buffer.from("flow-1");
    const partition = Buffer.from("p1");
    const lease = Buffer.from("lease-token");
    const runState = Buffer.from("running:step");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x95]),
      u32(1),
      Buffer.from([0, 4]),
      binary(id),
      binary(partition),
      binary(lease),
      i64(9n),
      binary(runState)
    ]);
    const frame = responseFrame(OPCODES.pipeline, body);

    expect(() => decodeResponse(frame, OPCODES.pipeline, {
      ...compactResponseHints,
      pipelineClaimModes: ["base"]
    })).toThrow("trailing compact pipeline bytes");
    expect(decodeResponse(frame, OPCODES.pipeline, {
      ...compactResponseHints,
      pipelineClaimModes: ["state"]
    })).toEqual([[id, partition, lease, 9, runState]]);
  });

});

function responseFrame(opcode: number, body: Buffer): ResponseFrame {
  return { body, bodyLength: body.byteLength, flags: 0, laneId: opcode < 0x0100 ? 0 : 1, opcode, requestId: 1n };
}

function compactClaimReclaimExpired(body: Buffer): boolean {
  let offset = 1;
  for (let index = 0; index < 3; index += 1) {
    const size = body.readUInt32BE(offset);
    offset += 4;
    if (size !== 0xffff_ffff) {
      offset += size;
    }
  }
  offset += 8 * 3;
  return body.readUInt8(offset) !== 0;
}

function u32(value: number): Buffer {
  const buffer = Buffer.allocUnsafe(4);
  buffer.writeUInt32BE(value, 0);
  return buffer;
}

function i64(value: bigint): Buffer {
  const buffer = Buffer.allocUnsafe(8);
  buffer.writeBigInt64BE(value, 0);
  return buffer;
}

function binary(value: Buffer): Buffer {
  return Buffer.concat([u32(value.byteLength), value]);
}
