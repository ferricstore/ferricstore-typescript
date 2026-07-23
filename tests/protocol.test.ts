import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  COMMAND_OPCODES,
  FLAG_CUSTOM_PAYLOAD,
  OPCODES,
  RESPONSE_VERSION,
  buildProtocolCommand,
  decodeValue,
  decodeResponse,
  encodeRequest,
  encodeValue,
  pipelineCommand,
  tryPipelineCommand,
  tryDecodeFrame,
  unwrapPipelineResponse,
  type ResponseFrame
} from "../src/protocol.js";
import { OverloadedError } from "../src/errors.js";
import { compactResponseHints } from "./compact-response-test-support.js";
import type { Command, CommandArgument } from "../src/internal.js";

describe("native protocol codec", () => {
  it("rejects sparse native pipelines before encoding", () => {
    const commands = new Array<Command>(2);
    commands[1] = ["PING"];

    expect(() => pipelineCommand(commands)).toThrow("pipeline commands must be a dense array");
  });

  it("exports the latest native command opcode table", () => {
    expect(COMMAND_OPCODES.OPTIONS).toBe(0x000b);
    expect(COMMAND_OPCODES["FLOW.QUERY"]).toBe(0x0231);
    expect(COMMAND_OPCODES["FLOW.BUDGET.RELEASE"]).toBe(0x0258);
    expect(OPCODES.flowQuery).toBe(COMMAND_OPCODES["FLOW.QUERY"]);
  });

  it("encodes request frames with FerricStore native header", () => {
    const frame = encodeRequest({ laneId: 0, opcode: OPCODES.ping, payload: { message: "hello" } }, 7n);

    expect(frame.toString("ascii", 0, 4)).toBe("FSNP");
    expect(frame.readUInt8(4)).toBe(1);
    expect(frame.readUInt32BE(6)).toBe(0);
    expect(frame.readUInt16BE(10)).toBe(OPCODES.ping);
    expect(frame.readBigUInt64BE(12)).toBe(7n);
    expect(frame.readUInt32BE(20)).toBe(frame.byteLength - 24);
  });

  it("rejects oversized request bodies before copying their large values", () => {
    const concat = vi.spyOn(Buffer, "concat");
    const value = Buffer.alloc(1024 * 1024, 0x61);

    try {
      expect(() => encodeRequest({ opcode: OPCODES.set, payload: { key: "key", value } }, 8n, 64)).toThrow(
        "server-advertised 64-byte frame limit"
      );
      expect(concat.mock.calls.every(([chunks]) =>
        chunks.reduce((total, chunk) => total + chunk.byteLength, 0) < value.byteLength
      )).toBe(true);
    } finally {
      concat.mockRestore();
    }
  });

  it("preflights oversized custom Uint8Array bodies before copying them", () => {
    const from = vi.spyOn(Buffer, "from");
    const value = new Uint8Array(1024 * 1024);

    try {
      expect(() => encodeRequest(
        { flags: FLAG_CUSTOM_PAYLOAD, opcode: OPCODES.pipeline, payload: value },
        9n,
        64
      )).toThrow("server-advertised 64-byte frame limit");
      expect(from.mock.calls.some(([source]) => source === value)).toBe(false);
    } finally {
      from.mockRestore();
    }
  });

  it("writes large native request values directly into the final frame", () => {
    const concat = vi.spyOn(Buffer, "concat");
    const value = Buffer.alloc(1024 * 1024, 0x62);

    try {
      const frame = encodeRequest(
        { opcode: OPCODES.set, payload: { key: "large-key", value } },
        9n,
        2 * 1024 * 1024
      );
      const decoded = decodeValue(frame.subarray(24)).value as Record<string, unknown>;

      expect(decoded.key).toEqual(Buffer.from("large-key"));
      expect(Buffer.isBuffer(decoded.value) && decoded.value.equals(value)).toBe(true);
      expect(concat).not.toHaveBeenCalled();
    } finally {
      concat.mockRestore();
    }
  });

  it("rejects oversized compact bodies before constructing full payload copies", () => {
    const concat = vi.spyOn(Buffer, "concat");
    const allocate = vi.spyOn(Buffer, "allocUnsafe");
    const value = Buffer.alloc(1024 * 1024, 0x61);

    try {
      expect(() => buildProtocolCommand(["MSET", "key", value], 64)).toThrow(
        "server-advertised 64-byte frame limit"
      );
      expect(concat.mock.calls.every(([chunks]) =>
        chunks.reduce((total, chunk) => total + chunk.byteLength, 0) < value.byteLength
      )).toBe(true);
      expect(allocate.mock.calls.every(([size]) => size < value.byteLength)).toBe(true);
    } finally {
      concat.mockRestore();
      allocate.mockRestore();
    }
  });

  it("preflights compact Flow batch bodies before allocating their payloads", () => {
    const value = Buffer.alloc(1024 * 1024, 0x63);
    const allocate = vi.spyOn(Buffer, "allocUnsafe");
    const commands: readonly Command[] = [
      [
        "FLOW.CREATE_MANY", "MIXED", "TYPE", "email", "STATE", "queued", "NOW", 1,
        "ITEMS", "flow-1", "partition-1", value
      ],
      [
        "FLOW.CLAIM_DUE", "email", "WORKER", value, "LEASE_MS", 1, "LIMIT", 1,
        "RETURN", "JOBS_COMPACT", "NOPAYLOAD"
      ],
      [
        "FLOW.COMPLETE_MANY", "MIXED", "NOW", 1,
        "ITEMS", "flow-1", "partition-1", value, 1
      ]
    ];

    try {
      for (const command of commands) {
        expect(() => buildProtocolCommand(command, 64)).toThrow(
          "server-advertised 64-byte frame limit"
        );
      }
      expect(allocate.mock.calls.every(([size]) => size < value.byteLength)).toBe(true);
    } finally {
      allocate.mockRestore();
    }
  });

  it("does not construct discarded compact payloads inside generic pipelines", () => {
    const concat = vi.spyOn(Buffer, "concat");
    const value = Buffer.alloc(1024 * 1024, 0x62);

    try {
      const pipeline = tryPipelineCommand([["MSET", "key", value]], 2 * 1024 * 1024);
      expect(pipeline).toBeDefined();
      expect(concat.mock.calls.every(([chunks]) =>
        chunks.reduce((total, chunk) => total + chunk.byteLength, 0) < value.byteLength
      )).toBe(true);
    } finally {
      concat.mockRestore();
    }
  });

  it("rejects oversized frames before buffering their declared bodies", () => {
    const header = Buffer.alloc(24);
    header.write("FSNP", 0, "ascii");
    header.writeUInt8(RESPONSE_VERSION, 4);
    header.writeUInt32BE(33, 20);

    expect(() => tryDecodeFrame(header, 32)).toThrow("native protocol frame exceeded 32 bytes");
  });

  it("uses the safe default when a frame limit is non-finite", () => {
    const header = Buffer.alloc(24);
    header.write("FSNP", 0, "ascii");
    header.writeUInt8(RESPONSE_VERSION, 4);
    header.writeUInt32BE(33, 20);

    expect(tryDecodeFrame(header, Number.NaN)).toBeNull();
  });

  it("builds direct compact-capable KV commands", () => {
    const get = buildProtocolCommand(["GET", "user:1"]);
    const set = buildProtocolCommand(["SET", "user:1", Buffer.from("value"), "PX", 1000]);

    expect(get).toMatchObject({ opcode: OPCODES.get, payload: { key: "user:1" } });
    expect(set.opcode).toBe(OPCODES.set);
    expect(set.payload).toMatchObject({ key: "user:1", ttl: 1000 });
  });

  it("uses dedicated native opcodes for valid atomic KV commands", () => {
    const expected = Buffer.from("expected");
    const value = Buffer.from("value");
    const token = Buffer.from("token");

    expect(buildProtocolCommand(["CAS", "key", expected, value])).toEqual({
      opcode: COMMAND_OPCODES.CAS,
      payload: { expected, key: "key", value }
    });
    expect(buildProtocolCommand(["CAS", "key", expected, value, "EX", 2])).toEqual({
      opcode: COMMAND_OPCODES.CAS,
      payload: { expected, key: "key", ttl: 2_000, value }
    });
    expect(buildProtocolCommand(["LOCK", "key", "owner", 5_000])).toEqual({
      opcode: COMMAND_OPCODES.LOCK,
      payload: { key: "key", owner: "owner", ttl_ms: 5_000 }
    });
    expect(buildProtocolCommand(["UNLOCK", "key", "owner"])).toEqual({
      opcode: COMMAND_OPCODES.UNLOCK,
      payload: { key: "key", owner: "owner" }
    });
    expect(buildProtocolCommand(["EXTEND", "key", "owner", 5_000])).toEqual({
      opcode: COMMAND_OPCODES.EXTEND,
      payload: { key: "key", owner: "owner", ttl_ms: 5_000 }
    });
    expect(buildProtocolCommand(["RATELIMIT.ADD", "key", 1_000, 10])).toEqual({
      opcode: COMMAND_OPCODES["RATELIMIT.ADD"],
      payload: { key: "key", max: 10, window_ms: 1_000 }
    });
    expect(buildProtocolCommand(["RATELIMIT.ADD", "key", 1_000, 10, 2])).toEqual({
      opcode: COMMAND_OPCODES["RATELIMIT.ADD"],
      payload: { count: 2, key: "key", max: 10, window_ms: 1_000 }
    });
    expect(buildProtocolCommand(["FETCH_OR_COMPUTE", "key", 30_000, "hint"])).toEqual({
      opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE,
      payload: { hint: "hint", key: "key", ttl_ms: 30_000 },
      serverBlockMs: 30_000
    });
    expect(buildProtocolCommand(["FETCH_OR_COMPUTE_RESULT", "key", token, value, 30_000])).toEqual({
      opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE_RESULT,
      payload: { key: "key", token, ttl_ms: 30_000, value }
    });
    expect(buildProtocolCommand(["FETCH_OR_COMPUTE_ERROR", "key", token, "failed"])).toEqual({
      opcode: COMMAND_OPCODES.FETCH_OR_COMPUTE_ERROR,
      payload: { key: "key", message: "failed", token }
    });
  });

  it("keeps incompatible atomic KV forms on the generic server-validated path", () => {
    const expected = Buffer.from("expected");
    const value = Buffer.from("value");
    const token = Buffer.from("token");
    const commands: readonly Command[] = [
      ["CAS", "key", expected, value, "EX", 0],
      ["CAS", "key", expected, value, "PX", 1],
      ["LOCK", "key", "owner", 0],
      ["RATELIMIT.ADD", "key", 0, 10],
      ["FETCH_OR_COMPUTE", "key", 0]
    ];

    for (const command of commands) {
      expect(buildProtocolCommand(command), command[0] as string).toMatchObject({
        opcode: OPCODES.commandExec,
        payload: { args: command.slice(1), command: command[0] }
      });
    }

    expect(() => buildProtocolCommand(["FETCH_OR_COMPUTE_RESULT", "key", value, 30_000]))
      .toThrow(/ownership token/u);
    expect(() => buildProtocolCommand(["FETCH_OR_COMPUTE_RESULT", "key", token, value, 0]))
      .toThrow(/ttl_ms must be positive/u);
    expect(() => buildProtocolCommand(["FETCH_OR_COMPUTE_ERROR", "key", "failed"]))
      .toThrow(/ownership token/u);
  });

  it("preserves every SET expiry option in the direct native payload", () => {
    expect(buildProtocolCommand(["SET", "relative-seconds", "value", "EX", 2])).toMatchObject({
      opcode: OPCODES.set,
      payload: { key: "relative-seconds", value: "value", ttl: 2_000 }
    });
    expect(buildProtocolCommand(["SET", "absolute-seconds", "value", "EXAT", 1_800_000_000])).toMatchObject({
      opcode: OPCODES.set,
      payload: { exat: 1_800_000_000, key: "absolute-seconds", value: "value" }
    });
    expect(buildProtocolCommand(["SET", "absolute-ms", "value", "PXAT", 1_800_000_000_123])).toMatchObject({
      opcode: OPCODES.set,
      payload: { key: "absolute-ms", pxat: 1_800_000_000_123, value: "value" }
    });
    expect(buildProtocolCommand(["SET", "preserve", "value", "KEEPTTL", "XX", "GET"])).toMatchObject({
      opcode: OPCODES.set,
      payload: { get: true, keepttl: true, key: "preserve", value: "value", xx: true }
    });
  });

  it("keeps invalid SET option combinations on the server-validated command path", () => {
    const malformed: readonly CommandArgument[][] = [
      ["SET", "key", "value", "EX"],
      ["SET", "key", "value", "EX", 0],
      ["SET", "key", "value", "EX", "1.0"],
      ["SET", "key", "value", "PX", 1, "KEEPTTL"],
      ["SET", "key", "value", "EX", 1, "PX", 1],
      ["SET", "key", "value", "NX", "XX"],
      ["SET", "key", "value", "UNKNOWN"]
    ];

    for (const args of malformed) {
      expect(buildProtocolCommand(args)).toMatchObject({
        opcode: OPCODES.commandExec,
        payload: { args: args.slice(1), command: "SET" }
      });
    }
  });

  it("keeps malformed optimized commands on the server-validated command path", () => {
    const malformed: readonly CommandArgument[][] = [
      ["AUTH"],
      ["AUTH", "user", "password", "extra"],
      ["PING", "one", "two"],
      ["OPTIONS", "extra"],
      ["BACKPRESSURE", "extra"],
      ["QUIT", "extra"],
      ["ROUTE"],
      ["ROUTE", "key", "extra"],
      ["SHARDS", "extra"],
      ["GET"],
      ["GET", "key", "extra"],
      ["MGET"],
      ["MSET"],
      ["MSET", "key"],
      ["DEL"]
    ];

    for (const args of malformed) {
      expect(buildProtocolCommand(args), args[0] as string).toMatchObject({
        opcode: OPCODES.commandExec,
        payload: {
          args: args.slice(1),
          command: args[0]
        }
      });
    }

    expect(buildProtocolCommand(["AUTH", "password"]).opcode).toBe(OPCODES.auth);
    expect(buildProtocolCommand(["PING", "hello"]).opcode).toBe(OPCODES.ping);
    expect(buildProtocolCommand(["OPTIONS"]).opcode).toBe(OPCODES.options);
    expect(buildProtocolCommand(["BACKPRESSURE"]).opcode).toBe(OPCODES.backpressure);
    expect(buildProtocolCommand(["QUIT"]).opcode).toBe(OPCODES.quit);
    expect(buildProtocolCommand(["ROUTE", "key"]).opcode).toBe(OPCODES.route);
    expect(buildProtocolCommand(["SHARDS"]).opcode).toBe(OPCODES.shards);
    expect(buildProtocolCommand(["GET", "key"]).opcode).toBe(OPCODES.get);
    expect(buildProtocolCommand(["MGET", "key"]).opcode).toBe(OPCODES.mget);
    expect(buildProtocolCommand(["MSET", "key", "value"]).opcode).toBe(OPCODES.mset);
    expect(buildProtocolCommand(["DEL", "key"]).opcode).toBe(OPCODES.del);
  });

  it("carries each server-side blocking interval into the transport deadline", () => {
    expect(buildProtocolCommand(["BLPOP", "queue", 0.1]).serverBlockMs).toBe(100);
    expect(buildProtocolCommand(["BRPOP", "queue", 2]).serverBlockMs).toBe(2_000);
    expect(buildProtocolCommand(["BLMOVE", "source", "target", "LEFT", "RIGHT", 3]).serverBlockMs).toBe(3_000);
    expect(buildProtocolCommand(["BRPOPLPUSH", "source", "target", 4]).serverBlockMs).toBe(4_000);
    expect(buildProtocolCommand(["BLMPOP", 0.25, 1, "queue", "LEFT"]).serverBlockMs).toBe(250);
    expect(buildProtocolCommand(["XREAD", "COUNT", 1, "BLOCK", 400, "STREAMS", "events", "$"]).serverBlockMs).toBe(400);
    expect(buildProtocolCommand([
      "XREADGROUP",
      "GROUP",
      "workers",
      "worker-1",
      "BLOCK",
      500,
      "STREAMS",
      "events",
      ">"
    ]).serverBlockMs).toBe(500);
    expect(buildProtocolCommand(["WAIT", 1, 600]).serverBlockMs).toBe(600);
    expect(buildProtocolCommand(["WAITAOF", 1, 1, 700]).serverBlockMs).toBe(700);
    expect(buildProtocolCommand(["FETCH_OR_COMPUTE", "cache", 30_000]).serverBlockMs).toBe(30_000);
    expect(buildProtocolCommand(["BLPOP", "queue", 0]).serverBlockMs).toBe(0);
    expect(buildProtocolCommand(["XREAD", "STREAMS", "events", "$"]).serverBlockMs).toBeUndefined();
  });

  it("budgets every sequential blocking interval in a native pipeline", () => {
    const first = [
      "FLOW.CLAIM_DUE", "orders", "STATE", "queued", "WORKER", "worker-1",
      "LEASE_MS", 30_000, "LIMIT", 1, "RETURN", "JOBS_COMPACT", "BLOCK", 100
    ] as const;
    const second = [
      "FLOW.CLAIM_DUE", "orders", "STATE", "retry", "WORKER", "worker-1",
      "LEASE_MS", 30_000, "LIMIT", 1, "RETURN", "JOBS_COMPACT", "BLOCK", 250
    ] as const;

    expect(pipelineCommand([first])).toMatchObject({ serverBlockMs: 100 });
    expect(pipelineCommand([["GET", "probe"], first, second])).toMatchObject({ serverBlockMs: 350 });
    expect(pipelineCommand([
      first,
      [
        "FLOW.CLAIM_DUE", "orders", "STATE", "running", "WORKER", "worker-1",
        "LEASE_MS", 30_000, "LIMIT", 1, "RETURN", "JOBS_COMPACT", "BLOCK", 0
      ]
    ])).toMatchObject({ serverBlockMs: 0 });
    expect(pipelineCommand([["GET", "probe"], ["GET", "key"]]).serverBlockMs).toBeUndefined();
  });

  it("leaves control commands out of native pipeline frames", () => {
    expect(tryPipelineCommand([["PING", "one"], ["GET", "key"]])).toBeUndefined();
    expect(tryPipelineCommand([["OPTIONS"], ["GET", "key"]])).toBeUndefined();
  });

  it("leaves connection-blocking commands out of native pipeline frames", () => {
    const blockingCommands = [
      ["BLPOP", "queue", 1],
      ["BRPOP", "queue", 1],
      ["BRPOPLPUSH", "source", "target", 1],
      ["BLMOVE", "source", "target", "LEFT", "RIGHT", 1],
      ["BLMPOP", 1, 1, "queue", "LEFT"],
      ["XREAD", "BLOCK", 100, "STREAMS", "events", "$"],
      ["XREADGROUP", "GROUP", "workers", "worker-1", "BLOCK", 100, "STREAMS", "events", ">"]
    ] as const;

    for (const command of blockingCommands) {
      expect(tryPipelineCommand([["GET", "probe"], command])).toBeUndefined();
      expect(tryPipelineCommand([["GET", "probe"], ["COMMAND_EXEC", ...command]])).toBeUndefined();
    }
  });

  it("leaves normalized connection-state mutations out of native pipeline frames", () => {
    const stateCommands = [
      ["AUTH", "secret"],
      ["CLIENT", "SETNAME", "worker-1"],
      ["CLIENT.SETNAME", "worker-1"],
      ["HELLO", "driver_name", "test"],
      ["QUIT"],
      ["RESET"],
      ["SANDBOX", "tenant-a"],
      ["STARTUP", "driver_name", "test"],
      ["SUBSCRIBE_EVENTS", "TOPOLOGY_CHANGED"],
      ["UNSUBSCRIBE_EVENTS", "TOPOLOGY_CHANGED"],
      ["WINDOW_UPDATE", "MAX_INFLIGHT_PER_CONNECTION", 10]
    ] as const;

    for (const command of stateCommands) {
      expect(tryPipelineCommand([command, ["GET", "key"]])).toBeUndefined();
      expect(tryPipelineCommand([["COMMAND_EXEC", ...command], ["GET", "key"]])).toBeUndefined();
    }
  });

  it("builds typed FLOW.CREATE payload, named values, refs, metadata, and lineage", () => {
    const payload = Buffer.from("payload");
    const namedValue = Buffer.from("named-value");
    const command = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "order",
      "STATE",
      "created",
      "NOW",
      100,
      "PARTITION",
      "tenant-a",
      "PAYLOAD",
      payload,
      "PARENT_FLOW_ID",
      "parent-1",
      "ROOT_FLOW_ID",
      "root-1",
      "CORRELATION_ID",
      "correlation-1",
      "VALUE",
      "customer",
      namedValue,
      "VALUE_REF",
      "nullable",
      "ref-null",
      "STATE_META",
      "version",
      1
    ]);

    expect(command).toMatchObject({
      opcode: OPCODES.flowCreate,
      payload: {
        correlation_id: "correlation-1",
        id: "flow-1",
        now_ms: 100,
        parent_flow_id: "parent-1",
        partition_key: "tenant-a",
        payload,
        root_flow_id: "root-1",
        state: "created",
        state_meta: { version: 1 },
        type: "order",
        value_refs: { nullable: "ref-null" },
        values: { customer: namedValue }
      }
    });
  });

  it("keeps max-active create options on direct native Flow opcodes", () => {
    const create = buildProtocolCommand([
      "FLOW.CREATE",
      "flow-1",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "MAX_ACTIVE_MS",
      30_000
    ]);
    const createMany = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "tenant-a",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "MAX_ACTIVE_MS",
      "infinity",
      "ITEMS",
      "flow-2",
      Buffer.alloc(0)
    ]);

    expect(create).toMatchObject({
      opcode: OPCODES.flowCreate,
      payload: { max_active_ms: 30_000 }
    });
    expect(createMany).toMatchObject({
      opcode: OPCODES.flowCreateMany,
      payload: { max_active_ms: "infinity" }
    });
  });

  it("builds compact FLOW.VALUE.MGET requests", () => {
    const command = buildProtocolCommand([
      "FLOW.VALUE.MGET",
      "ref-a",
      "ref-b",
      "MAX_BYTES",
      4_096
    ]);

    expect(command).toMatchObject({
      flags: FLAG_CUSTOM_PAYLOAD,
      opcode: COMMAND_OPCODES["FLOW.VALUE.MGET"]
    });
    expect(command.payload).toEqual(Buffer.concat([
      Buffer.from([0x9d]),
      i64(4_096n),
      u32(2),
      binary(Buffer.from("ref-a")),
      binary(Buffer.from("ref-b"))
    ]));
  });

  it("keeps FLOW.VALUE.MGET typed when custom payloads are unavailable", () => {
    expect(buildProtocolCommand([
      "FLOW.VALUE.MGET",
      "ref-a",
      "ref-b",
      "MAX_BYTES",
      4_096
    ], Number.MAX_SAFE_INTEGER, false)).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.VALUE.MGET"],
      payload: {
        max_bytes: 4_096,
        refs: ["ref-a", "ref-b"]
      }
    });
  });

  it("supports the MAXBYTES FLOW.VALUE.MGET alias in compact and typed requests", () => {
    const args = ["FLOW.VALUE.MGET", "ref-a", "ref-b", "MAXBYTES", 4_096] as const;
    const compact = buildProtocolCommand(args);

    expect(compact).toMatchObject({
      flags: FLAG_CUSTOM_PAYLOAD,
      opcode: COMMAND_OPCODES["FLOW.VALUE.MGET"]
    });
    expect(compact.payload).toEqual(Buffer.concat([
      Buffer.from([0x9d]),
      i64(4_096n),
      u32(2),
      binary(Buffer.from("ref-a")),
      binary(Buffer.from("ref-b"))
    ]));
    expect(buildProtocolCommand(args, Number.MAX_SAFE_INTEGER, false)).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.VALUE.MGET"],
      payload: { max_bytes: 4_096, refs: ["ref-a", "ref-b"] }
    });
  });

  it("builds typed FLOW.QUERY requests for the bounded FQL contract", () => {
    const command = buildProtocolCommand([
      "FLOW.QUERY",
      "FQL1",
      "FROM runs WHERE partition_key = @partition RETURN COUNT",
      "partition",
      "tenant-a"
    ]);

    expect(command).toEqual({
      opcode: COMMAND_OPCODES["FLOW.QUERY"],
      payload: {
        version: "FQL1",
        query: "FROM runs WHERE partition_key = @partition RETURN COUNT",
        params: { partition: "tenant-a" }
      }
    });
  });

  it("keeps unsupported compact Flow request shapes on their generic paths", () => {
    const cancel = buildProtocolCommand([
      "FLOW.CANCEL_MANY",
      "tenant-a",
      "NOW",
      123,
      "REASON",
      Buffer.from("cancelled"),
      "ITEMS",
      "flow-1",
      7
    ]);
    const transition = buildProtocolCommand([
      "FLOW.TRANSITION_MANY",
      "tenant-a",
      "queued",
      "next",
      "NOW",
      123,
      "PAYLOAD",
      Buffer.from("payload"),
      "ITEMS",
      "flow-1",
      7,
      Buffer.from("lease")
    ]);

    expect(cancel).toMatchObject({ opcode: OPCODES.commandExec });
    expect(transition).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.TRANSITION_MANY"],
      payload: { payload: Buffer.from("payload") }
    });
    expect(cancel.flags).toBeUndefined();
    expect(transition.flags).toBeUndefined();
  });

  it("writes compact Flow string items without allocating a Buffer per item", () => {
    const values = ["ref-a", "ref-b", "flow-1", "tenant-a"];
    const from = vi.spyOn(Buffer, "from");

    try {
      buildProtocolCommand(["FLOW.VALUE.MGET", values[0], values[1]]);
      buildProtocolCommand([
        "FLOW.CANCEL_MANY",
        "MIXED",
        "NOW",
        123,
        "ITEMS",
        values[2],
        values[3],
        7
      ]);

      expect(from.mock.calls.filter(([value]) =>
        typeof value === "string" && values.includes(value)
      )).toHaveLength(0);
    } finally {
      from.mockRestore();
    }
  });

  it("writes direct compact MGET and MSET strings without allocating a Buffer per item", () => {
    const values = ["{keys}:a", "{keys}:b", "value:a", "value:b"];
    const from = vi.spyOn(Buffer, "from");

    try {
      buildProtocolCommand(["MGET", values[0], values[1]]);
      buildProtocolCommand(["MSET", values[0], values[2], values[1], values[3]]);

      expect(from.mock.calls.filter(([value]) =>
        typeof value === "string" && values.includes(value)
      )).toHaveLength(0);
    } finally {
      from.mockRestore();
    }
  });

  it("keeps compact Flow bulk request frames smaller than typed equivalents", () => {
    const refs = Array.from({ length: 128 }, (_, index) => `ref-${index}`);
    const args = ["FLOW.VALUE.MGET", ...refs, "MAX_BYTES", 4_096] as const;
    const compact = encodeRequest(buildProtocolCommand(args), 1n);
    const typed = encodeRequest(
      buildProtocolCommand(args, Number.MAX_SAFE_INTEGER, false),
      1n
    );

    expect(compact.byteLength).toBeLessThan(typed.byteLength);
  });

  it("builds typed FLOW.VALUE.PUT requests with ownership and overwrite controls", () => {
    const value = Buffer.from("INDEXED_STATE_META");

    expect(buildProtocolCommand([
      "FLOW.VALUE.PUT",
      value,
      "NOW",
      100,
      "OWNER_FLOW_ID",
      "flow-1",
      "NAME",
      "customer",
      "OVERRIDE",
      "true",
      "TTL",
      60_000
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.VALUE.PUT"],
      payload: {
        name: "customer",
        now_ms: 100,
        override: true,
        owner_flow_id: "flow-1",
        ttl_ms: 60_000,
        value
      }
    });
  });

  it("builds typed extended FLOW.CREATE_MANY items and shared named values", () => {
    const payload = Buffer.from("payload");
    const customer = Buffer.from("ITEMS");
    const extended = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "tenant-a",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "ITEMS_EXT",
      1,
      "flow-1",
      "-",
      payload,
      1,
      "customer",
      customer,
      1,
      "invoice",
      "ref-invoice"
    ]);
    const shared = buildProtocolCommand([
      "FLOW.CREATE_MANY",
      "tenant-a",
      "TYPE",
      "order",
      "STATE",
      "queued",
      "NOW",
      100,
      "VALUE",
      "customer",
      customer,
      "VALUE_REF",
      "invoice",
      "ref-invoice",
      "ITEMS",
      "flow-1",
      payload
    ]);

    expect(extended).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.CREATE_MANY"],
      payload: {
        items: [{
          id: "flow-1",
          payload,
          value_refs: { invoice: "ref-invoice" },
          values: { customer }
        }],
        now_ms: 100,
        partition_key: "tenant-a",
        state: "queued",
        type: "order"
      }
    });
    expect(shared).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.CREATE_MANY"],
      payload: {
        items: [["flow-1", payload]],
        value_refs: { invoice: "ref-invoice" },
        values: { customer }
      }
    });
  });

  it("builds typed rich Flow many-item mutations", () => {
    const lease = Buffer.from("lease");
    const value = Buffer.from("ITEMS");
    const commonOptions = [
      "NOW",
      100,
      "VALUE",
      "customer",
      value,
      "VALUE_REF",
      "invoice",
      "ref-invoice",
      "DROP_VALUE",
      "obsolete",
      "OVERRIDE_VALUE",
      "customer",
      "ATTRIBUTE_MERGE",
      "phases",
      ["validated", "charged"],
      "ATTRIBUTE_DELETE",
      "obsolete_attribute",
      "STATE_META",
      "version",
      2
    ] as const;

    for (const name of ["FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"] as const) {
      const command = buildProtocolCommand([
        name,
        "tenant-a",
        ...commonOptions,
        "ITEMS",
        "flow-1",
        lease,
        7
      ]);
      expect(command).toMatchObject({
        opcode: COMMAND_OPCODES[name],
        payload: {
          attributes_delete: ["obsolete_attribute"],
          attributes_merge: { phases: ["validated", "charged"] },
          drop_values: ["obsolete"],
          items: [["flow-1", lease, 7]],
          now_ms: 100,
          override_values: ["customer"],
          partition_key: "tenant-a",
          state_meta: { version: 2 },
          value_refs: { invoice: "ref-invoice" },
          values: { customer: value }
        }
      });
    }

    expect(buildProtocolCommand([
      "FLOW.TRANSITION_MANY",
      "tenant-a",
      "running",
      "validated",
      ...commonOptions,
      "ITEMS",
      "flow-1",
      7,
      lease
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.TRANSITION_MANY"],
      payload: {
        attributes_delete: ["obsolete_attribute"],
        attributes_merge: { phases: ["validated", "charged"] },
        from_state: "running",
        items: [{ fencing_token: 7, id: "flow-1", lease_token: lease }],
        partition_key: "tenant-a",
        to_state: "validated",
        values: { customer: value }
      }
    });
  });

  it("builds typed Flow reads with payload and named-value byte limits", () => {
    expect(buildProtocolCommand([
      "FLOW.GET",
      "flow-1",
      "PARTITION",
      "tenant-a",
      "FULL",
      "true",
      "PAYLOAD",
      "MAXBYTES",
      1_024,
      "VALUE",
      "customer",
      "VALUE_MAX_BYTES",
      2_048
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.GET"],
      payload: {
        full: true,
        id: "flow-1",
        partition_key: "tenant-a",
        payload: true,
        payload_max_bytes: 1_024,
        value_max_bytes: 2_048,
        values: ["customer"]
      }
    });
  });

  it("builds typed FLOW.SPAWN_CHILDREN with parent and child named values", () => {
    const parentValue = Buffer.from("parent-value");
    const childPayload = Buffer.from("child-payload");
    const childValue = Buffer.from("child-value");

    expect(buildProtocolCommand([
      "FLOW.SPAWN_CHILDREN",
      "parent-1",
      "GROUP",
      "children",
      "WAIT",
      "all",
      "NOW",
      100,
      "VALUE",
      "parent-data",
      parentValue,
      "ITEMS_EXT",
      1,
      "child-1",
      "-",
      "child-type",
      childPayload,
      1,
      "child-data",
      childValue,
      1,
      "shared",
      "ref-shared"
    ])).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.SPAWN_CHILDREN"],
      payload: {
        children: [{
          id: "child-1",
          payload: childPayload,
          type: "child-type",
          value_refs: { shared: "ref-shared" },
          values: { "child-data": childValue }
        }],
        group_id: "children",
        id: "parent-1",
        now_ms: 100,
        values: { "parent-data": parentValue },
        wait: "all"
      }
    });
  });

  it("decodes compact FLOW.VALUE.MGET responses with presence intact", () => {
    const storedNull = Buffer.from("null");
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x83]),
      u32(2),
      Buffer.from([1]),
      binary(storedNull),
      Buffer.from([0])
    ]);

    expect(decodeResponse(
      responseFrame(COMMAND_OPCODES["FLOW.VALUE.MGET"], body),
      COMMAND_OPCODES["FLOW.VALUE.MGET"],
      compactResponseHints
    )).toEqual([storedNull, null]);
  });

  it("preserves the item count for fixed-width compact MGET responses with empty values", () => {
    const body = Buffer.concat([
      Buffer.from([0, 0, 0x89]),
      u32(3),
      u32(0)
    ]);

    expect(decodeResponse(
      responseFrame(OPCODES.mget, body), OPCODES.mget, compactResponseHints
    )).toEqual([
      Buffer.alloc(0),
      Buffer.alloc(0),
      Buffer.alloc(0)
    ]);
  });

  it("classifies structured native busy responses and preserves retry metadata", () => {
    const raw = {
      code: "lane_queue_full",
      message: "ERR native lane queue is full",
      retry_after_ms: 10,
      scope: "lane"
    };
    const body = Buffer.concat([Buffer.from([0, 4]), encodeValue(raw)]);

    let thrown: unknown;
    try {
      decodeResponse(responseFrame(OPCODES.flowCreate, body), OPCODES.flowCreate);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(OverloadedError);
    expect(thrown).toMatchObject({
      reason: "lane_queue_full",
      retryAfterMs: 10
    });
    expect((thrown as OverloadedError).raw).toMatchObject({ retry_after_ms: 10 });
  });

  it("preserves retry metadata for structured busy pipeline items", () => {
    const raw = {
      code: "flow_control_window_exhausted",
      message: "ERR native lane inflight window exhausted",
      retry_after_ms: 1
    };

    expect(() => unwrapPipelineResponse([["busy", raw]])).toThrow(OverloadedError);
    try {
      unwrapPipelineResponse([["busy", raw]]);
    } catch (error) {
      expect(error).toMatchObject({
        reason: "flow_control_window_exhausted",
        retryAfterMs: 1
      });
    }
  });

  it("builds typed single-item Flow mutations with named-value changes", () => {
    const lease = Buffer.from("lease");
    const value = Buffer.from("value");
    const transition = buildProtocolCommand([
      "FLOW.TRANSITION",
      "flow-1",
      "created",
      "validated",
      "LEASE_TOKEN",
      lease,
      "FENCING",
      7,
      "NOW",
      100,
      "VALUE",
      "customer",
      value,
      "VALUE_REF",
      "shared",
      "ref-shared",
      "DROP_VALUE",
      "obsolete",
      "OVERRIDE_VALUE",
      "customer",
      "ATTRIBUTE_MERGE",
      "phases",
      ["validated", "charged"],
      "ATTRIBUTE_DELETE",
      "obsolete_attribute",
      "STATE_META",
      "version",
      2
    ]);

    expect(transition).toMatchObject({
      opcode: COMMAND_OPCODES["FLOW.TRANSITION"],
      payload: {
        attributes_delete: ["obsolete_attribute"],
        attributes_merge: { phases: ["validated", "charged"] },
        drop_values: ["obsolete"],
        fencing_token: 7,
        from_state: "created",
        id: "flow-1",
        lease_token: lease,
        now_ms: 100,
        override_values: ["customer"],
        state_meta: { version: 2 },
        to_state: "validated",
        value_refs: { shared: "ref-shared" },
        values: { customer: value }
      }
    });

    for (const name of ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL"] as const) {
      const command = buildProtocolCommand([
        name,
        "flow-1",
        lease,
        "FENCING",
        7,
        "NOW",
        100,
        "VALUE",
        "customer",
        value,
        "VALUE_REF",
        "shared",
        "ref-shared",
        "DROP_VALUE",
        "obsolete",
        "OVERRIDE_VALUE",
        "customer",
        "ATTRIBUTE_MERGE",
        "phases",
        ["validated", "charged"],
        "ATTRIBUTE_DELETE",
        "obsolete_attribute",
        "STATE_META",
        "version",
        2
      ]);
      expect(command).toMatchObject({
        opcode: COMMAND_OPCODES[name],
        payload: {
          attributes_delete: ["obsolete_attribute"],
          attributes_merge: { phases: ["validated", "charged"] },
          drop_values: ["obsolete"],
          id: "flow-1",
          lease_token: lease,
          override_values: ["customer"],
          state_meta: { version: 2 },
          value_refs: { shared: "ref-shared" },
          values: { customer: value }
        }
      });
    }
  });

});

function responseFrame(opcode: number, body: Buffer): ResponseFrame {
  return { body, bodyLength: body.byteLength, flags: 0, laneId: opcode < 0x0100 ? 0 : 1, opcode, requestId: 1n };
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
