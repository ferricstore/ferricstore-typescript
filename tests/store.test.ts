import { describe, expect, expectTypeOf, it } from "vitest";
import {
  FerricStoreClient,
  JsonCodec,
  type Codec,
  type ExpiryCondition,
  type GeoAddOptions,
  type GetExOptions,
  type HashScanResult,
  type IntegerReply,
  type SetScanResult,
  type SetOptions,
  type SortedSetScanResult,
  type TopKReserveOptions,
  type ZAddOptions
} from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("typed data stores", () => {
  it("exposes typed store helpers from FerricStoreClient", () => {
    const client = new FerricStoreClient(new FakeExecutor());

    expect(client.kv).toBeDefined();
    expect(client.bloom).toBeDefined();
    expect(client.cms).toBeDefined();
    expect(client.cuckoo).toBeDefined();
    expect(client.hash).toBeDefined();
    expect(client.lists).toBeDefined();
    expect(client.sets).toBeDefined();
    expect(client.zset).toBeDefined();
    expect(client.stream).toBeDefined();
    expect(client.bitmap).toBeDefined();
    expect(client.hyperloglog).toBeDefined();
    expect(client.geo).toBeDefined();
    expect("json" in client).toBe(false);
    expect(client.tdigest).toBeDefined();
    expect(client.topk).toBeDefined();
  });

  it("builds string KV commands with SET options and codec encoding", async () => {
    const executor = new FakeExecutor([null, Buffer.from('{"ok":true}')]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.kv.set("k1", { ok: true }, { get: true, nx: true, px: 1000 });
    await expect(client.kv.get("k1")).resolves.toEqual({ ok: true });

    expect(executor.calls[0]).toEqual([
      "SET",
      "k1",
      Buffer.from('{"ok":true}'),
      "PX",
      1000,
      "NX",
      "GET"
    ]);
    expect(executor.calls[1]).toEqual(["GET", "k1"]);
  });

  it("decodes the previous value returned by SET GET", async () => {
    const executor = new FakeExecutor([Buffer.from('{"version":1}')]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.kv.set("config", { version: 2 }, { get: true })).resolves.toEqual({
      version: 1
    });
  });

  it("does not mistake a codec-encoded OK value for the SET acknowledgement", async () => {
    const codec: Codec<string> = {
      decode: (value) => value == null ? null : `decoded:${value.toString("utf8")}`,
      encode: (value) => Buffer.from(value ?? "")
    };
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, { codec });

    await expect(client.kv.set("status", "new", { get: true })).resolves.toBe("decoded:OK");
  });

  it("rejects conflicting GETEX and HGETEX expiry modes before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.kv.getex("key", { persist: true, ex: 1 } as unknown as GetExOptions)).rejects.toThrow(
      "GETEX expiry options are mutually exclusive"
    );
    await expect(client.hash.hgetex("hash", ["field"], { px: 1, exat: 2 } as unknown as GetExOptions)).rejects.toThrow(
      "GETEX expiry options are mutually exclusive"
    );
    expect(executor.calls).toEqual([]);

    const assertInvalidOptionsAreRejectedByTypeScript = (): void => {
      // @ts-expect-error GETEX accepts at most one expiry mutation.
      void client.kv.getex("key", { persist: true, ex: 1 });
      // @ts-expect-error HGETEX accepts at most one expiry mutation.
      void client.hash.hgetex("hash", ["field"], { px: 1, exat: 2 });
    };
    void assertInvalidOptionsAreRejectedByTypeScript;
  });

  it("rejects contradictory SET, ZADD, and GEOADD options before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const member = [{ member: "one", score: 1 }];
    const place = [{ latitude: 32, longitude: 34, member: "one" }];

    await expect(client.kv.set("key", "value", { nx: true, xx: true } as unknown as SetOptions)).rejects.toThrow(
      "SET NX and XX options are mutually exclusive"
    );
    await expect(client.kv.set("key", "value", { ex: 1, px: 1 } as unknown as SetOptions)).rejects.toThrow(
      "SET expiry options are mutually exclusive"
    );
    await expect(client.kv.set("key", "value", { ex: 1, keepTtl: true } as unknown as SetOptions)).rejects.toThrow(
      "SET expiry options are mutually exclusive"
    );
    await expect(client.zset.zadd("rank", member, { nx: true, xx: true } as unknown as ZAddOptions)).rejects.toThrow(
      "ZADD NX and XX options are mutually exclusive"
    );
    await expect(client.zset.zadd("rank", member, { gt: true, lt: true } as unknown as ZAddOptions)).rejects.toThrow(
      "ZADD GT and LT options are mutually exclusive"
    );
    await expect(client.zset.zadd("rank", member, { nx: true, gt: true } as unknown as ZAddOptions)).rejects.toThrow(
      "ZADD NX cannot be combined with GT or LT"
    );
    await expect(client.geo.geoadd("places", place, { nx: true, xx: true } as unknown as GeoAddOptions)).rejects.toThrow(
      "GEOADD NX and XX options are mutually exclusive"
    );
    expect(executor.calls).toEqual([]);

    const assertInvalidOptionsAreRejectedByTypeScript = (): void => {
      // @ts-expect-error SET NX and XX are mutually exclusive.
      void client.kv.set("key", "value", { nx: true, xx: true });
      // @ts-expect-error SET accepts at most one expiry mode.
      void client.kv.set("key", "value", { ex: 1, px: 1 });
      // @ts-expect-error SET KEEPTTL cannot be combined with another expiry mode.
      void client.kv.set("key", "value", { ex: 1, keepTtl: true });
      // @ts-expect-error ZADD NX and XX are mutually exclusive.
      void client.zset.zadd("rank", member, { nx: true, xx: true });
      // @ts-expect-error ZADD GT and LT are mutually exclusive.
      void client.zset.zadd("rank", member, { gt: true, lt: true });
      // @ts-expect-error ZADD NX cannot be combined with GT or LT.
      void client.zset.zadd("rank", member, { nx: true, gt: true });
      // @ts-expect-error GEOADD NX and XX are mutually exclusive.
      void client.geo.geoadd("places", place, { nx: true, xx: true });
    };
    void assertInvalidOptionsAreRejectedByTypeScript;
  });

  it("preserves exact int64 counter replies beyond Number safe range", async () => {
    const high = 9_007_199_254_740_993n;
    const low = -9_007_199_254_740_993n;
    const executor = new FakeExecutor([high, low, high, high]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.incr("counter")).resolves.toBe(high);
    await expect(client.kv.decrby("counter", 2n)).resolves.toBe(low);
    await expect(client.hash.hincrby("hash", "counter", 2n)).resolves.toBe(high);
    await expect(client.kv.exists("counter")).rejects.toThrow("safe range");

    expect(executor.calls).toEqual([
      ["INCR", "counter"],
      ["DECRBY", "counter", 2n],
      ["HINCRBY", "hash", "counter", 2n],
      ["EXISTS", "counter"]
    ]);
  });

  it("preserves exact scalar expiry replies beyond Number safe range", async () => {
    const high = 9_007_199_254_740_993n;
    const executor = new FakeExecutor([
      high,
      Buffer.from("9007199254740993"),
      high,
      Buffer.from("9007199254740993")
    ]);
    const client = new FerricStoreClient(executor);

    expectTypeOf<ReturnType<FerricStoreClient["kv"]["ttl"]>>()
      .toEqualTypeOf<Promise<IntegerReply>>();
    await expect(client.kv.ttl("key")).resolves.toBe(high);
    await expect(client.kv.pttl("key")).resolves.toBe(high);
    await expect(client.kv.expiretime("key")).resolves.toBe(high);
    await expect(client.kv.pexpiretime("key")).resolves.toBe(high);

    expect(executor.calls).toEqual([
      ["TTL", "key"],
      ["PTTL", "key"],
      ["EXPIRETIME", "key"],
      ["PEXPIRETIME", "key"]
    ]);
  });

  it("forwards core-supported conditional expiry and SCAN type filters", async () => {
    const executor = new FakeExecutor([1, 1, 1, 1, [Buffer.from("0"), []]]);
    const client = new FerricStoreClient(executor);
    await expect(client.kv.expire("key", 10, "NX")).resolves.toBe(true);
    await expect(client.kv.pexpire("key", 20, "XX")).resolves.toBe(true);
    await expect(client.kv.expireat("key", 30, "GT")).resolves.toBe(true);
    await expect(client.kv.pexpireat("key", 40, "LT")).resolves.toBe(true);
    await expect(client.kv.scan(0, { type: "hash" })).resolves.toEqual(["0", []]);

    expect(executor.calls).toEqual([
      ["EXPIRE", "key", 10, "NX"],
      ["PEXPIRE", "key", 20, "XX"],
      ["EXPIREAT", "key", 30, "GT"],
      ["PEXPIREAT", "key", 40, "LT"],
      ["SCAN", 0, "TYPE", "hash"]
    ]);
  });

  it("preserves exact HyperLogLog cardinality replies beyond Number safe range", async () => {
    const high = 9_007_199_254_740_993n;
    const client = new FerricStoreClient(new FakeExecutor([high]));

    expectTypeOf<ReturnType<FerricStoreClient["hyperloglog"]["pfcount"]>>()
      .toEqualTypeOf<Promise<number | bigint>>();
    await expect(client.hyperloglog.pfcount("hll")).resolves.toBe(high);
  });

  it("dispatches a large atomic MSET without overflowing the JavaScript call stack", async () => {
    const entryCount = 70_000;
    const entries: [string, string][] = Array.from(
      { length: entryCount },
      (_, index) => [`{bulk}:key:${index}`, `value:${index}`]
    );
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.mset(entries)).resolves.toBe(true);
    expect(executor.calls).toHaveLength(1);
    expect(executor.calls[0]).toHaveLength(1 + entryCount * 2);
    expect(executor.calls[0]?.[0]).toBe("MSET");
  });

  it("accepts large bulk arrays without requiring JavaScript argument spreading", async () => {
    const itemCount = 200_000;
    const keys = Array.from({ length: itemCount }, (_, index) => `key:${index}`);
    const values = Array.from({ length: itemCount }, (_, index) => `value:${index}`);
    const numbers = Array.from({ length: itemCount }, (_, index) => index);
    const executor = new FakeExecutor([0, itemCount, Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.del(keys)).resolves.toBe(0);
    await expect(client.lists.lpushMany("list", values)).resolves.toBe(itemCount);
    await expect(client.tdigest.add("digest", numbers)).resolves.toBe(true);

    expect(executor.calls[0]).toHaveLength(itemCount + 1);
    expect(executor.calls[1]).toHaveLength(itemCount + 2);
    expect(executor.calls[2]).toHaveLength(itemCount + 2);
  });

  it("keeps array-valued members as one scalar in existing variadic APIs", async () => {
    const executor = new FakeExecutor([1]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.sets.sadd("set", ["nested", "value"])).resolves.toBe(1);
    expect(executor.calls[0]).toEqual([
      "SADD",
      "set",
      Buffer.from('["nested","value"]')
    ]);
  });

  it("rejects sparse or malformed tuple inputs before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const sparseKeyValues = new Array<[string, unknown]>(1);
    const sparseEntries = new Array<[unknown, number | bigint]>(1);
    const malformedKeyValues = [["key"]] as unknown as [string, unknown][];
    const malformedEntries = [["item"]] as unknown as [unknown, number | bigint][];

    const operations = [
      async () => await client.kv.mset(sparseKeyValues),
      async () => await client.kv.msetnx(sparseKeyValues),
      async () => await client.hash.hset("hash", sparseKeyValues),
      async () => await client.hash.hsetex("hash", 10, sparseKeyValues),
      async () => await client.stream.xadd("stream", "*", sparseKeyValues),
      async () => await client.cms.incrBy("cms", sparseEntries),
      async () => await client.topk.incrBy("topk", sparseEntries),
      async () => await client.kv.mset(malformedKeyValues),
      async () => await client.cms.incrBy("cms", malformedEntries)
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow("dense two-item tuples");
    }
    expect(executor.calls).toEqual([]);
  });

  it("rejects sparse bulk argument arrays before encoding or dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const sparseStrings = new Array<string>(1);
    const sparseValues = new Array<unknown>(1);

    const operations = [
      async () => await client.kv.mget(sparseStrings),
      async () => await client.hash.hmget("hash", sparseStrings),
      async () => await client.hash.hexpire("hash", 10, sparseStrings),
      async () => await client.lists.blpop(sparseStrings, 1),
      async () => await client.sets.sinter(sparseStrings),
      async () => await client.sets.smismember("set", sparseValues),
      async () => await client.zset.zmscore("zset", sparseValues),
      async () => await client.cms.merge("cms", sparseStrings),
      async () => await client.tdigest.merge("tdigest", sparseStrings)
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow("argument arrays must be dense");
    }
    expect(executor.calls).toEqual([]);
  });

  it("dispatches large array-based hash writes without variadic calls", async () => {
    const entryCount = 70_000;
    const entries: [string, string][] = Array.from(
      { length: entryCount },
      (_, index) => [`field:${index}`, `value:${index}`]
    );
    const executor = new FakeExecutor([entryCount]);
    const client = new FerricStoreClient(executor);

    await expect(client.hash.hset("hash", entries)).resolves.toBe(entryCount);
    expect(executor.calls[0]).toHaveLength(2 + entryCount * 2);
  });

  it("dispatches large array-based module writes without variadic calls", async () => {
    const entryCount = 70_000;
    const entries: [string, number][] = Array.from(
      { length: entryCount },
      (_, index) => [`item:${index}`, 1]
    );
    const executor = new FakeExecutor([new Array(entryCount).fill(1)]);
    const client = new FerricStoreClient(executor);

    const result = await client.cms.incrBy("cms", entries);
    expect(result).toHaveLength(entryCount);
    expect(executor.calls[0]).toHaveLength(2 + entryCount * 2);
  });

  it("dispatches large stream arrays without overflowing the JavaScript call stack", async () => {
    const streamCount = 70_000;
    const streams = Array.from(
      { length: streamCount },
      (_, index) => ({ key: `stream:${index}`, id: `${index}-0` })
    );
    const executor = new FakeExecutor([[], []]);
    const client = new FerricStoreClient(executor);

    await expect(client.stream.xread(streams)).resolves.toEqual([]);
    await expect(client.stream.xreadgroup("workers", "worker-1", streams)).resolves.toEqual([]);

    expect(executor.calls[0]).toHaveLength(2 + streamCount * 2);
    expect(executor.calls[0]?.[1]).toBe("STREAMS");
    expect(executor.calls[0]?.[1 + streamCount]).toBe("stream:69999");
    expect(executor.calls[0]?.[2 + streamCount]).toBe("0-0");
    expect(executor.calls[1]).toHaveLength(5 + streamCount * 2);
  });

  it("dispatches large FLOW.VALUE.MGET arrays through the array-native path", async () => {
    const refCount = 150_000;
    const refs = Array.from({ length: refCount }, (_, index) => `ref:${index}`);
    const executor = new FakeExecutor([new Array(refCount).fill(null)]);
    const client = new FerricStoreClient(executor);

    await expect(client.valueMGet(refs)).resolves.toHaveLength(refCount);
    expect(executor.calls[0]).toHaveLength(1 + refCount);
    expect(executor.calls[0]?.[refCount]).toBe("ref:149999");
  });

  it("dispatches large CMS.MERGE weight arrays without variadic calls", async () => {
    const sourceCount = 150_000;
    const sources = new Array<string>(sourceCount).fill("cms:source");
    const weights = new Array<number>(sourceCount).fill(1);
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.cms.merge("cms:destination", sources, { weights })).resolves.toBe(true);
    expect(executor.calls[0]).toHaveLength(4 + sourceCount * 2);
    expect(executor.calls[0]?.[3 + sourceCount]).toBe("WEIGHTS");
  });

  it("fails closed on a non-binary boolean reply", async () => {
    const client = new FerricStoreClient(new FakeExecutor([2]));

    await expect(client.kv.setnx("key", "value")).rejects.toThrow(
      "invalid binary boolean response"
    );
  });

  it("accepts typed native booleans from SISMEMBER", async () => {
    const client = new FerricStoreClient(new FakeExecutor([true, false]));

    await expect(client.sets.sismember("set", "present")).resolves.toBe(true);
    await expect(client.sets.sismember("set", "missing")).resolves.toBe(false);
  });

  it("fails closed on non-text typed replies and decodes Uint8Array text", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      { unexpected: true },
      new Uint8Array(Buffer.from("string"))
    ]));

    await expect(client.kv.type("key")).rejects.toThrow("invalid text response");
    await expect(client.kv.type("key")).resolves.toBe("string");
  });

  it("returns validated text from KEYS and SCAN with stable tuple types", async () => {
    const encoded = (value: string): Uint8Array => new Uint8Array(Buffer.from(value));
    const client = new FerricStoreClient(new FakeExecutor([
      [Buffer.from("key:a"), encoded("key:b")],
      [Buffer.from("7"), [Buffer.from("key:a"), encoded("key:b")]]
    ]));

    expectTypeOf<ReturnType<FerricStoreClient["kv"]["keys"]>>()
      .toEqualTypeOf<Promise<string[]>>();
    expectTypeOf<ReturnType<FerricStoreClient["kv"]["scan"]>>()
      .toEqualTypeOf<Promise<[string, string[]]>>();
    expectTypeOf<Parameters<FerricStoreClient["kv"]["expire"]>[2]>()
      .toEqualTypeOf<ExpiryCondition | undefined>();
    await expect(client.kv.keys("key:*")).resolves.toEqual(["key:a", "key:b"]);
    await expect(client.kv.scan(0)).resolves.toEqual(["7", ["key:a", "key:b"]]);
  });

  it("rejects malformed KEYS and SCAN key responses", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      [{ invalid: true }],
      [Buffer.from("0"), [Buffer.from("key")], Buffer.from("ignored")],
      [Buffer.from("0"), [{ invalid: true }]]
    ]));

    await expect(client.kv.keys("*")).rejects.toThrow("KEYS returned an invalid text response");
    await expect(client.kv.scan(0)).rejects.toThrow("invalid scan response");
    await expect(client.kv.scan(0)).rejects.toThrow("SCAN returned an invalid text response");
  });

  it("builds hash commands from object entries", async () => {
    const executor = new FakeExecutor([2, [Buffer.from('{"id":1}'), null]]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.hash.hset("user:1", { email: "a@example.com", profile: { id: 1 } })).resolves.toBe(2);
    await expect(client.hash.hmget("user:1", ["profile", "missing"])).resolves.toEqual([{ id: 1 }, null]);

    expect(executor.calls[0]).toEqual([
      "HSET",
      "user:1",
      "email",
      Buffer.from('"a@example.com"'),
      "profile",
      Buffer.from('{"id":1}')
    ]);
    expect(executor.calls[1]).toEqual(["HMGET", "user:1", "profile", "missing"]);
  });

  it("decodes hash values in maps, random pairs, and scan tuples", async () => {
    const executor = new FakeExecutor([
      [
        Buffer.from("profile"), Buffer.from('{"id":1}'),
        Buffer.from("__proto__"), Buffer.from('{"safe":true}')
      ],
      [Buffer.from("profile"), Buffer.from('{"id":2}')],
      [Buffer.from("0"), [Buffer.from("profile"), Buffer.from('{"id":3}')]]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    const all = await client.hash.hgetall("users");
    await expect(client.hash.hrandfield("users", 1, true)).resolves.toEqual([
      "profile", { id: 2 }
    ]);
    await expect(client.hash.hscan("users", 0)).resolves.toEqual([
      "0", ["profile", { id: 3 }]
    ]);

    expect(all.profile).toEqual({ id: 1 });
    expect(Object.hasOwn(all, "__proto__")).toBe(true);
    expect(all.__proto__).toEqual({ safe: true });
    expect(({} as { safe?: boolean }).safe).toBeUndefined();
  });

  it("publishes precise scan and positional multi-result shapes", async () => {
    const executor = new FakeExecutor([
      [Buffer.from("7"), [Buffer.from("profile"), Buffer.from('{"id":1}')]],
      [Buffer.from("8"), [Buffer.from('{"id":2}')]],
      [Buffer.from("9"), [Buffer.from('{"id":3}'), Buffer.from("1.5")]],
      [Buffer.from("9007199254740993"), Buffer.from("-2")],
      [Buffer.from("1"), 0n],
      [Buffer.from("1.25"), null]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    const hashScan = client.hash.hscan<{ id: number }>("users", 0);
    const setScan = client.sets.sscan<{ id: number }>("users", 0);
    const sortedSetScan = client.zset.zscan<{ id: number }>("rank", 0);
    const fieldTtls = client.hash.hpttl("users", ["profile", "missing"]);
    const memberships = client.sets.smismember("users", [{ id: 1 }, { id: 2 }]);
    const scores = client.zset.zmscore("rank", [{ id: 1 }, { id: 2 }]);

    expectTypeOf(hashScan).toEqualTypeOf<Promise<HashScanResult<{ id: number }>>>();
    expectTypeOf(setScan).toEqualTypeOf<Promise<SetScanResult<{ id: number }>>>();
    expectTypeOf(sortedSetScan).toEqualTypeOf<Promise<SortedSetScanResult<{ id: number }>>>();
    expectTypeOf(fieldTtls).toEqualTypeOf<Promise<IntegerReply[]>>();
    expectTypeOf(memberships).toEqualTypeOf<Promise<number[]>>();
    expectTypeOf(scores).toEqualTypeOf<Promise<(string | null)[]>>();

    await expect(hashScan).resolves.toEqual(["7", ["profile", { id: 1 }]]);
    await expect(setScan).resolves.toEqual(["8", [{ id: 2 }]]);
    await expect(sortedSetScan).resolves.toEqual(["9", [{ id: 3 }, "1.5"]]);
    await expect(fieldTtls).resolves.toEqual([9_007_199_254_740_993n, -2]);
    await expect(memberships).resolves.toEqual([1, 0]);
    await expect(scores).resolves.toEqual(["1.25", null]);
  });

  it("rejects random-member WITHVALUES/WITHSCORES without a count", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const hash = client.hash as unknown as {
      hrandfield(key: string, count?: number, withValues?: boolean): Promise<unknown>;
    };
    const zset = client.zset as unknown as {
      zrandmember(key: string, count?: number, withScores?: boolean): Promise<unknown>;
    };

    await expect(hash.hrandfield("hash", undefined, true)).rejects.toThrow(
      "HRANDFIELD WITHVALUES requires a count"
    );
    await expect(zset.zrandmember("zset", undefined, true)).rejects.toThrow(
      "ZRANDMEMBER WITHSCORES requires a count"
    );
    expect(executor.calls).toEqual([]);

    const assertInvalidGrammarIsRejectedByTypeScript = (): void => {
      // @ts-expect-error WITHVALUES requires the count form.
      void client.hash.hrandfield("hash", undefined, true);
      // @ts-expect-error WITHSCORES requires the count form.
      void client.zset.zrandmember("zset", undefined, true);
    };
    void assertInvalidGrammarIsRejectedByTypeScript;
  });

  it("decodes HGETALL map responses without assigning through object prototypes", async () => {
    const executor = new FakeExecutor([
      new Map<unknown, unknown>([
        [Buffer.from("profile"), Buffer.from('{"id":4}')],
        [Buffer.from("constructor"), Buffer.from('{"safe":true}')]
      ])
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.hash.hgetall("users")).resolves.toEqual({
      constructor: { safe: true },
      profile: { id: 4 }
    });
  });

  it("builds list and set commands with encoded members", async () => {
    const executor = new FakeExecutor([2, 1]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.lists.lpush("jobs", { id: 1 }, { id: 2 })).resolves.toBe(2);
    await expect(client.sets.sadd("seen", { id: 1 })).resolves.toBe(1);

    expect(executor.calls[0]).toEqual([
      "LPUSH",
      "jobs",
      Buffer.from('{"id":1}'),
      Buffer.from('{"id":2}')
    ]);
    expect(executor.calls[1]).toEqual(["SADD", "seen", Buffer.from('{"id":1}')]);
  });

  it("decodes nested list, set, and sorted-set member replies", async () => {
    const executor = new FakeExecutor([
      Buffer.from('{"id":1}'),
      [Buffer.from("jobs"), Buffer.from('{"id":2}')],
      [Buffer.from("jobs"), [Buffer.from('{"id":3}'), Buffer.from('{"id":4}')]],
      [Buffer.from("0"), [Buffer.from('{"id":5}')]],
      [Buffer.from('{"id":6}'), Buffer.from("1.5")],
      Buffer.from('{"id":7}'),
      [Buffer.from("0"), [Buffer.from('{"id":8}'), Buffer.from("2")]]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.lists.lmove("a", "b", "LEFT", "RIGHT")).resolves.toEqual({ id: 1 });
    await expect(client.lists.blpop(["jobs"], 1)).resolves.toEqual(["jobs", { id: 2 }]);
    await expect(client.lists.blmpop(1, ["jobs"], "LEFT", { count: 2 })).resolves.toEqual([
      "jobs", [{ id: 3 }, { id: 4 }]
    ]);
    await expect(client.sets.sscan("seen", 0)).resolves.toEqual(["0", [{ id: 5 }]]);
    await expect(client.zset.zrange("rank", 0, -1, { withScores: true })).resolves.toEqual([
      { id: 6 }, "1.5"
    ]);
    await expect(client.zset.zrandmember("rank")).resolves.toEqual({ id: 7 });
    await expect(client.zset.zscan("rank", 0)).resolves.toEqual([
      "0", [{ id: 8 }, "2"]
    ]);
  });

  it("rejects over-wide hash, sorted-set, and stream-read tuples", async () => {
    const executor = new FakeExecutor([
      [[Buffer.from("field"), Buffer.from("value"), Buffer.from("ignored")]],
      [[Buffer.from("member"), Buffer.from("1"), Buffer.from("ignored")]],
      [Buffer.from("0"), [[Buffer.from("member"), Buffer.from("1"), Buffer.from("ignored")]]],
      [[Buffer.from("events"), [], Buffer.from("ignored")]]
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.hash.hgetall("hash")).rejects.toThrow("invalid hash pair response");
    await expect(client.zset.zrange("rank", 0, -1, { withScores: true })).rejects.toThrow(
      "invalid sorted-set member/score response"
    );
    await expect(client.zset.zscan("rank", 0)).rejects.toThrow(
      "invalid sorted-set member/score response"
    );
    await expect(client.stream.xread([{ key: "events", id: "0-0" }])).rejects.toThrow(
      "invalid stream read response"
    );
  });

  it("normalizes nested sorted-set pairs to the same flat public shape", async () => {
    const executor = new FakeExecutor([
      [
        [Buffer.from('{"id":1}'), Buffer.from("1.5")],
        [Buffer.from('{"id":2}'), Buffer.from("2.5")]
      ],
      [Buffer.from("0"), [[Buffer.from('{"id":3}'), Buffer.from("3.5")]]]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.zset.zrange("rank", 0, -1, { withScores: true })).resolves.toEqual([
      { id: 1 }, "1.5", { id: 2 }, "2.5"
    ]);
    await expect(client.zset.zscan("rank", 0)).resolves.toEqual([
      "0", [{ id: 3 }, "3.5"]
    ]);
  });

  it("builds sorted set and stream commands", async () => {
    const executor = new FakeExecutor([1, "0-1"]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.zset.zadd("rank", [{ member: { id: "a" }, score: 10 }], { ch: true, nx: true });
    await client.stream.xadd("events", "*", { type: "created", payload: { id: 1 } });

    expect(executor.calls[0]).toEqual([
      "ZADD",
      "rank",
      "NX",
      "CH",
      10,
      Buffer.from('{"id":"a"}')
    ]);
    expect(executor.calls[1]).toEqual([
      "XADD",
      "events",
      "*",
      "type",
      Buffer.from('"created"'),
      "payload",
      Buffer.from('{"id":1}')
    ]);
  });

  it("decodes stream field values and geosearch members", async () => {
    const executor = new FakeExecutor([
      [[Buffer.from("1-0"), [Buffer.from("payload"), Buffer.from('{"id":1}')]]],
      [[Buffer.from("2-0"), Buffer.from("payload"), Buffer.from('{"id":9}')]],
      [[
        Buffer.from("events"),
        [[Buffer.from("1-0"), [Buffer.from("payload"), Buffer.from('{"id":2}')]]]
      ]],
      [[Buffer.from('{"name":"place"}'), Buffer.from("1.25")]],
      1
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.stream.xrange("events")).resolves.toEqual([
      ["1-0", ["payload", { id: 1 }]]
    ]);
    await expect(client.stream.xrevrange("events")).resolves.toEqual([
      ["2-0", ["payload", { id: 9 }]]
    ]);
    await expect(client.stream.xread([{ key: "events", id: "0-0" }])).resolves.toEqual([
      ["events", [["1-0", ["payload", { id: 2 }]]]]
    ]);
    await expect(client.geo.geosearch("places", [
      "FROMMEMBER", "origin",
      "BYRADIUS", 10, "km", "WITHDIST"
    ])).resolves.toEqual([[{ name: "place" }, "1.25"]]);
    await expect(client.geo.geosearchstore("nearby", "places", [
      "FROMMEMBER", "origin", "BYRADIUS", 10, "km"
    ])).resolves.toBe(1);
    expect(executor.calls.at(-2)?.[3]).toEqual(Buffer.from('"origin"'));
    expect(executor.calls.at(-1)?.[4]).toEqual(Buffer.from('"origin"'));
  });

  it("does not treat a GEOSEARCH member named like a response option as metadata", async () => {
    const executor = new FakeExecutor([[Buffer.from("candidate")]]);
    const client = new FerricStoreClient(executor);

    await expect(client.geo.geosearch("places", [
      "FROMMEMBER", "WITHDIST", "BYRADIUS", 10, "km"
    ])).resolves.toEqual([Buffer.from("candidate")]);

    expect(executor.calls[0]).toEqual([
      "GEOSEARCH",
      "places",
      "FROMMEMBER",
      Buffer.from("WITHDIST"),
      "BYRADIUS",
      10,
      "km"
    ]);
  });

  it("rejects GEOSEARCH result tuples whose metadata arity does not match the request", async () => {
    const client = new FerricStoreClient(new FakeExecutor([
      [[Buffer.from("candidate")]],
      [[Buffer.from("candidate"), Buffer.from("1.25"), Buffer.from("ignored")]]
    ]));
    const args = ["FROMMEMBER", "origin", "BYRADIUS", 10, "km", "WITHDIST"] as const;

    await expect(client.geo.geosearch("places", [...args])).rejects.toThrow("invalid GEOSEARCH result");
    await expect(client.geo.geosearch("places", [...args])).rejects.toThrow("invalid GEOSEARCH result");
  });

  it("builds bitmap, hyperloglog, and geo commands", async () => {
    const executor = new FakeExecutor([0, 1, 1]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.bitmap.bitcount("bits", 0, 10, "BIT");
    await client.hyperloglog.pfadd("hll", { id: 1 });
    await client.geo.geoadd("places", [{ latitude: 32.0853, longitude: 34.7818, member: "tlv" }], { ch: true, nx: true });

    expect(executor.calls[0]).toEqual(["BITCOUNT", "bits", 0, 10, "BIT"]);
    expect(executor.calls[1]).toEqual(["PFADD", "hll", Buffer.from('{"id":1}')]);
    expect(executor.calls[2]).toEqual([
      "GEOADD",
      "places",
      "NX",
      "CH",
      34.7818,
      32.0853,
      Buffer.from('"tlv"')
    ]);
  });

  it("enforces BITCOUNT and BITPOS positional grammar before dispatch", async () => {
    const executor = new FakeExecutor([4, 6]);
    const client = new FerricStoreClient(executor);
    const callBitcount = Reflect.get(client.bitmap, "bitcount") as (...args: unknown[]) => Promise<number>;
    const callBitpos = Reflect.get(client.bitmap, "bitpos") as (...args: unknown[]) => Promise<number>;

    await expect(client.bitmap.bitcount("bits")).resolves.toBe(4);
    await expect(client.bitmap.bitpos("bits", 1, 3)).resolves.toBe(6);

    await expect(callBitcount.call(client.bitmap, "bits", 0)).rejects.toThrow(
      "BITCOUNT start requires end"
    );
    await expect(callBitcount.call(client.bitmap, "bits", undefined, 7)).rejects.toThrow(
      "BITCOUNT end requires start"
    );
    await expect(callBitcount.call(client.bitmap, "bits", 0, undefined, "BIT")).rejects.toThrow(
      "BITCOUNT unit requires start and end"
    );
    await expect(callBitpos.call(client.bitmap, "bits", 1, undefined, 7)).rejects.toThrow(
      "BITPOS end requires start"
    );
    await expect(callBitpos.call(client.bitmap, "bits", 1, 0, undefined, "BIT")).rejects.toThrow(
      "BITPOS unit requires start and end"
    );

    expect(executor.calls).toEqual([
      ["BITCOUNT", "bits"],
      ["BITPOS", "bits", 1, 3]
    ]);

    const assertInvalidRangesAreRejectedByTypeScript = (): void => {
      // @ts-expect-error BITCOUNT requires both start and end when a range is supplied.
      void client.bitmap.bitcount("bits", 0);
      // @ts-expect-error BITPOS unit requires both start and end.
      void client.bitmap.bitpos("bits", 1, 0, undefined, "BIT");
    };
    void assertInvalidRangesAreRejectedByTypeScript;
  });

  it("builds object and blocking list command variants", async () => {
    const executor = new FakeExecutor(["raw", ["list", "value"]]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.objectEncoding("k1")).resolves.toBe("raw");
    await client.lists.blpop(["q1", "q2"], 5);

    expect(executor.calls[0]).toEqual(["OBJECT", "ENCODING", "k1"]);
    expect(executor.calls[1]).toEqual(["BLPOP", "q1", "q2", 5]);
  });

  it("preserves the nullable MEMORY USAGE contract for missing keys", async () => {
    const executor = new FakeExecutor([null, 123]);
    const client = new FerricStoreClient(executor);

    expectTypeOf<ReturnType<FerricStoreClient["kv"]["memoryUsage"]>>()
      .toEqualTypeOf<Promise<number | null>>();
    await expect(client.kv.memoryUsage("missing")).resolves.toBeNull();
    await expect(client.kv.memoryUsage("present")).resolves.toBe(123);
    expect(executor.calls).toEqual([
      ["MEMORY", "USAGE", "missing"],
      ["MEMORY", "USAGE", "present"]
    ]);
  });

  it("builds probabilistic data-structure commands", async () => {
    const executor = new FakeExecutor([1, 1, [2], [null], Buffer.from("OK")]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await client.bloom.add("bf:users", { id: 1 });
    await client.cuckoo.addnx("cf:users", { id: 1 });
    await client.cms.incrBy("cms:views", [[{ id: 1 }, 2]]);
    await client.topk.add("topk:urls", "/docs");
    await client.tdigest.create("td:latency", { compression: 200 });

    expect(executor.calls[0]).toEqual(["BF.ADD", "bf:users", Buffer.from('{"id":1}')]);
    expect(executor.calls[1]).toEqual(["CF.ADDNX", "cf:users", Buffer.from('{"id":1}')]);
    expect(executor.calls[2]).toEqual(["CMS.INCRBY", "cms:views", Buffer.from('{"id":1}'), 2]);
    expect(executor.calls[3]).toEqual(["TOPK.ADD", "topk:urls", Buffer.from('"/docs"')]);
    expect(executor.calls[4]).toEqual(["TDIGEST.CREATE", "td:latency", "COMPRESSION", 200]);
  });

  it("requires all TOPK.RESERVE tuning parameters together", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.topk.reserve(
      "topk:urls",
      10,
      { depth: 7 } as unknown as TopKReserveOptions
    )).rejects.toThrow("width and depth must be provided together");
    expect(executor.calls).toEqual([]);

    await expect(client.topk.reserve("topk:urls", 10, {
      depth: 7,
      width: 8
    })).resolves.toBe(true);
    expect(executor.calls).toEqual([["TOPK.RESERVE", "topk:urls", 10, 8, 7]]);

    const assertPartialTuningIsRejectedByTypeScript = (): void => {
      // @ts-expect-error TOPK.RESERVE tuning parameters are all-or-none.
      void client.topk.reserve("topk:urls", 10, { depth: 7 });
      // @ts-expect-error FerricStore 0.8 removed TOPK.RESERVE decay.
      void client.topk.reserve("topk:urls", 10, { width: 8, depth: 7, decay: 0.9 });
    };
    void assertPartialTuningIsRejectedByTypeScript;
  });

  it("preserves exact int64 module count and rank replies", async () => {
    const high = 9_007_199_254_740_993n;
    const executor = new FakeExecutor([
      high,
      high,
      [high],
      [high],
      [high],
      [high],
      [Buffer.from('"/docs"'), high]
    ]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.bloom.card("bf:users")).resolves.toBe(high);
    await expect(client.cuckoo.count("cf:users", { id: 1 })).resolves.toBe(high);
    await expect(client.cms.incrBy("cms:views", [[{ id: 1 }, 2n]])).resolves.toEqual([high]);
    await expect(client.cms.query("cms:views", { id: 1 })).resolves.toEqual([high]);
    await expect(client.tdigest.rank("td:latency", 100)).resolves.toEqual([high]);
    await expect(client.tdigest.revrank("td:latency", 100)).resolves.toEqual([high]);
    await expect(client.topk.list<string>("topk:urls", { withCount: true })).resolves.toEqual(["/docs", high]);

    expect(executor.calls[2]).toEqual(["CMS.INCRBY", "cms:views", Buffer.from('{"id":1}'), 2n]);
  });

  it("exposes TOPK.COUNT with codec encoding and exact integer replies", async () => {
    const high = 9_007_199_254_740_993n;
    const executor = new FakeExecutor([[3, high]]);
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });

    await expect(client.topk.count("topk:urls", "/docs", { path: "/api" })).resolves.toEqual([
      3,
      high
    ]);
    expect(executor.calls[0]).toEqual([
      "TOPK.COUNT",
      "topk:urls",
      Buffer.from('"/docs"'),
      Buffer.from('{"path":"/api"}')
    ]);
  });

  it("rejects cardinality mismatches from every positional multi-result helper", async () => {
    const cases: [string, (client: FerricStoreClient) => Promise<unknown>][] = [
      ["MGET", async (client) => await client.kv.mget(["a", "b"])],
      ["HMGET", async (client) => await client.hash.hmget("hash", ["a", "b"])],
      ["HEXPIRE", async (client) => await client.hash.hexpire("hash", 10, ["a", "b"])],
      ["HTTL", async (client) => await client.hash.httl("hash", ["a", "b"])],
      ["HPERSIST", async (client) => await client.hash.hpersist("hash", ["a", "b"])],
      ["HPEXPIRE", async (client) => await client.hash.hpexpire("hash", 10, ["a", "b"])],
      ["HPTTL", async (client) => await client.hash.hpttl("hash", ["a", "b"])],
      ["HEXPIRETIME", async (client) => await client.hash.hexpiretime("hash", ["a", "b"])],
      ["HGETDEL", async (client) => await client.hash.hgetdel("hash", ["a", "b"])],
      ["HGETEX", async (client) => await client.hash.hgetex("hash", ["a", "b"])],
      ["SMISMEMBER", async (client) => await client.sets.smismember("set", ["a", "b"])],
      ["ZMSCORE", async (client) => await client.zset.zmscore("zset", ["a", "b"])],
      ["GEOPOS", async (client) => await client.geo.geopos("geo", "a", "b")],
      ["GEOHASH", async (client) => await client.geo.geohash("geo", "a", "b")],
      ["BF.MADD", async (client) => await client.bloom.madd("bf", "a", "b")],
      ["BF.MEXISTS", async (client) => await client.bloom.mexists("bf", "a", "b")],
      ["CF.MEXISTS", async (client) => await client.cuckoo.mexists("cf", "a", "b")],
      ["CMS.INCRBY", async (client) => await client.cms.incrBy("cms", [["a", 1], ["b", 1]])],
      ["CMS.QUERY", async (client) => await client.cms.query("cms", "a", "b")],
      ["TOPK.ADD", async (client) => await client.topk.add("topk", "a", "b")],
      ["TOPK.INCRBY", async (client) => await client.topk.incrBy("topk", [["a", 1], ["b", 1]])],
      ["TOPK.QUERY", async (client) => await client.topk.query("topk", "a", "b")],
      ["TDIGEST.QUANTILE", async (client) => await client.tdigest.quantile("td", 0.1, 0.9)],
      ["TDIGEST.CDF", async (client) => await client.tdigest.cdf("td", 1, 2)],
      ["TDIGEST.RANK", async (client) => await client.tdigest.rank("td", 1, 2)],
      ["TDIGEST.REVRANK", async (client) => await client.tdigest.revrank("td", 1, 2)],
      ["TDIGEST.BYRANK", async (client) => await client.tdigest.byrank("td", 1, 2)],
      ["TDIGEST.BYREVRANK", async (client) => await client.tdigest.byrevrank("td", 1, 2)]
    ];

    for (const [command, invoke] of cases) {
      const client = new FerricStoreClient(new FakeExecutor([[null]]));
      await expect(invoke(client), command).rejects.toThrow(`${command} returned 1 items; expected 2`);
    }
  });
});
