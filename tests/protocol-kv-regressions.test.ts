import { describe, expect, it } from "vitest";

import {
  buildProtocolCommand,
  COMMAND_OPCODES,
  encodeRequest,
  tryPipelineCommand
} from "../src/protocol.js";

describe("native KV protocol regressions", () => {
  it("keeps scalar-coercing KV commands on COMMAND_EXEC when custom encoding is unavailable", () => {
    const commands = [
      ["GET", 42],
      ["SET", "key", 42],
      ["SET", 42, "value"],
      ["DEL", 42],
      ["MGET", 42],
      ["MSET", "key", 42]
    ] as const;

    for (const args of commands) {
      expect(buildProtocolCommand(args, Number.MAX_SAFE_INTEGER, false)).toMatchObject({
        opcode: COMMAND_OPCODES.COMMAND_EXEC,
        payload: { args: args.slice(1), command: args[0] }
      });
    }
  });

  it("preserves REQUEST_CONTEXT when it is ordinary KV data", () => {
    expect(buildProtocolCommand(["HSET", "hash", "REQUEST_CONTEXT", Buffer.from("value")])).toMatchObject({
      opcode: COMMAND_OPCODES.HSET,
      payload: { fields: { REQUEST_CONTEXT: Buffer.from("value") }, key: "hash" }
    });
    expect(buildProtocolCommand([
      "RPUSH", "list", Buffer.from("REQUEST_CONTEXT"), Buffer.from("value")
    ])).toMatchObject({
      opcode: COMMAND_OPCODES.RPUSH,
      payload: { key: "list", values: [Buffer.from("REQUEST_CONTEXT"), Buffer.from("value")] }
    });
    expect(buildProtocolCommand(["HGET", "REQUEST_CONTEXT", "field"])).toMatchObject({
      opcode: COMMAND_OPCODES.HGET,
      payload: { field: "field", key: "REQUEST_CONTEXT" }
    });

    const generic = buildProtocolCommand(["HSET", "hash", "REQUEST_CONTEXT", { subject: "ordinary-data" }]);
    expect(generic).toMatchObject({
      opcode: COMMAND_OPCODES.COMMAND_EXEC,
      payload: {
        args: ["hash", "REQUEST_CONTEXT", { subject: "ordinary-data" }],
        command: "HSET"
      }
    });
    expect(generic.payload).not.toHaveProperty("request_context");
  });

  it("uses all collection opcodes for wire-equivalent command forms", () => {
    const value = Buffer.from("value");
    const cases = [
      [["HSET", "hash", "field", value], COMMAND_OPCODES.HSET, { fields: { field: value }, key: "hash" }],
      [["HGET", "hash", "field"], COMMAND_OPCODES.HGET, { field: "field", key: "hash" }],
      [["HMGET", "hash", "one", "two"], COMMAND_OPCODES.HMGET, { fields: ["one", "two"], key: "hash" }],
      [["HGETALL", "hash"], COMMAND_OPCODES.HGETALL, { key: "hash" }],
      [["LPUSH", "list", value], COMMAND_OPCODES.LPUSH, { key: "list", values: [value] }],
      [["RPUSH", "list", value], COMMAND_OPCODES.RPUSH, { key: "list", values: [value] }],
      [["LPOP", "list"], COMMAND_OPCODES.LPOP, { key: "list" }],
      [["RPOP", "list", 2], COMMAND_OPCODES.RPOP, { count: 2, key: "list" }],
      [["LRANGE", "list", 0, -1], COMMAND_OPCODES.LRANGE, { key: "list", start: 0, stop: -1 }],
      [["SADD", "set", value], COMMAND_OPCODES.SADD, { key: "set", members: [value] }],
      [["SREM", "set", value], COMMAND_OPCODES.SREM, { key: "set", members: [value] }],
      [["SMEMBERS", "set"], COMMAND_OPCODES.SMEMBERS, { key: "set" }],
      [["SISMEMBER", "set", value], COMMAND_OPCODES.SISMEMBER, { key: "set", member: value }],
      [["ZADD", "zset", 1.5, value], COMMAND_OPCODES.ZADD, { items: [[1.5, value]], key: "zset" }],
      [["ZREM", "zset", value], COMMAND_OPCODES.ZREM, { key: "zset", members: [value] }],
      [["ZRANGE", "zset", 0, -1, "WITHSCORES"], COMMAND_OPCODES.ZRANGE, {
        key: "zset", start: 0, stop: -1, withscores: true
      }],
      [["ZSCORE", "zset", value], COMMAND_OPCODES.ZSCORE, { key: "zset", member: value }]
    ] as const;

    for (const [args, opcode, payload] of cases) {
      expect(buildProtocolCommand(args)).toMatchObject({ opcode, payload });
    }
  });

  it("keeps non-equivalent and unsupported collection grammars on COMMAND_EXEC", () => {
    const value = Buffer.from("value");
    const commands = [
      ["HSET", "hash", Buffer.from("field"), value],
      ["HSET", "hash", "field", { unsupported: true }],
      ["HMGET", "hash"],
      ["LPUSH", "list"],
      ["LPOP", "list", 1],
      ["RPOP", "list", 0],
      ["SADD", "set"],
      ["SISMEMBER", "set", { unsupported: true }],
      ["ZADD", "zset", "NX", 1, value],
      ["ZADD", "zset", "not-a-score", value],
      ["ZRANGE", "zset", 0, -1, "BYSCORE"],
      ["ZSCORE", "zset"]
    ] as const;

    for (const args of commands) {
      expect(buildProtocolCommand(args)).toMatchObject({
        opcode: COMMAND_OPCODES.COMMAND_EXEC,
        payload: { args: args.slice(1), command: args[0] }
      });
    }
  });

  it("preserves prototype-named hash fields in direct payload maps", () => {
    const command = buildProtocolCommand([
      "HSET",
      "hash",
      "__proto__",
      Buffer.from("first"),
      "constructor",
      Buffer.from("second")
    ]);
    const fields = (command.payload as { fields: Record<string, unknown> }).fields;

    expect(command.opcode).toBe(COMMAND_OPCODES.HSET);
    expect(Object.hasOwn(fields, "__proto__")).toBe(true);
    expect(fields.__proto__).toEqual(Buffer.from("first"));
    expect(Object.hasOwn(fields, "constructor")).toBe(true);
    expect(fields.constructor).toEqual(Buffer.from("second"));
  });

  it("fuses direct collection commands and makes their encoded frame smaller", () => {
    const direct = buildProtocolCommand(["HGET", "hash", "field"]);
    const generic = {
      opcode: COMMAND_OPCODES.COMMAND_EXEC,
      payload: { args: ["hash", "field"], command: "HGET" }
    };
    const pipeline = tryPipelineCommand([
      ["HGET", "hash", "field"],
      ["SADD", "set", Buffer.from("member")]
    ]);

    expect(encodeRequest(direct, 1n).byteLength).toBeLessThan(encodeRequest(generic, 2n).byteLength);
    expect(pipeline).toMatchObject({ opcode: COMMAND_OPCODES.PIPELINE });
    expect(pipeline?.payload).toMatchObject({
      commands: [
        { opcode: COMMAND_OPCODES.HGET },
        { opcode: COMMAND_OPCODES.SADD }
      ]
    });
  });
});
