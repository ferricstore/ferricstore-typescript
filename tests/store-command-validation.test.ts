import { describe, expect, it } from "vitest";

import { FerricStoreClient } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("store command-shape validation", () => {
  it("rejects empty required collections before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);
    const callWithoutItems = (receiver: unknown, methodName: string, ...args: unknown[]): Promise<unknown> => {
      const method = Reflect.get(Object(receiver), methodName) as unknown;
      if (typeof method !== "function") throw new TypeError(`${methodName} is not callable`);
      return Reflect.apply(method, receiver, args) as Promise<unknown>;
    };
    const operations: [command: string, operation: () => Promise<unknown>][] = [
      ["DEL", async () => await callWithoutItems(client.kv, "del")],
      ["EXISTS", async () => await callWithoutItems(client.kv, "exists")],
      ["MGET", async () => await client.kv.mget([])],
      ["MSET", async () => await client.kv.mset({})],
      ["MSETNX", async () => await client.kv.msetnx([])],
      ["UNLINK", async () => await callWithoutItems(client.kv, "unlink")],
      ["HSET", async () => await client.hash.hset("hash", {})],
      ["HDEL", async () => await callWithoutItems(client.hash, "hdel", "hash")],
      ["HMGET", async () => await client.hash.hmget("hash", [])],
      ["HEXPIRE", async () => await client.hash.hexpire("hash", 1, [])],
      ["HTTL", async () => await client.hash.httl("hash", [])],
      ["HPERSIST", async () => await client.hash.hpersist("hash", [])],
      ["HPEXPIRE", async () => await client.hash.hpexpire("hash", 1, [])],
      ["HPTTL", async () => await client.hash.hpttl("hash", [])],
      ["HEXPIRETIME", async () => await client.hash.hexpiretime("hash", [])],
      ["HGETDEL", async () => await client.hash.hgetdel("hash", [])],
      ["HGETEX", async () => await client.hash.hgetex("hash", [])],
      ["HSETEX", async () => await client.hash.hsetex("hash", 1, {})],
      ["LPUSH", async () => await callWithoutItems(client.lists, "lpush", "list")],
      ["RPUSH", async () => await client.lists.rpushMany("list", [])],
      ["LPUSHX", async () => await client.lists.lpushxMany("list", [])],
      ["RPUSHX", async () => await callWithoutItems(client.lists, "rpushx", "list")],
      ["BLPOP", async () => await client.lists.blpop([], 1)],
      ["BRPOP", async () => await client.lists.brpop([], 1)],
      ["BLMPOP", async () => await client.lists.blmpop(1, [], "LEFT")],
      ["SADD", async () => await callWithoutItems(client.sets, "sadd", "set")],
      ["SREM", async () => await client.sets.sremMany("set", [])],
      ["SMISMEMBER", async () => await client.sets.smismember("set", [])],
      ["SDIFF", async () => await client.sets.sdiff([])],
      ["SINTER", async () => await client.sets.sinter([])],
      ["SUNION", async () => await client.sets.sunion([])],
      ["SDIFFSTORE", async () => await client.sets.sdiffstore("out", [])],
      ["SINTERSTORE", async () => await client.sets.sinterstore("out", [])],
      ["SUNIONSTORE", async () => await client.sets.sunionstore("out", [])],
      ["SINTERCARD", async () => await client.sets.sintercard([])],
      ["ZADD", async () => await client.zset.zadd("zset", [])],
      ["ZREM", async () => await client.zset.zremMany("zset", [])],
      ["ZMSCORE", async () => await client.zset.zmscore("zset", [])],
      ["XADD", async () => await client.stream.xadd("stream", "*", {})],
      ["XREAD", async () => await client.stream.xread([])],
      ["XDEL", async () => await callWithoutItems(client.stream, "xdel", "stream")],
      ["XREADGROUP", async () => await client.stream.xreadgroup("group", "consumer", [])],
      ["XACK", async () => await callWithoutItems(client.stream, "xack", "stream", "group")],
      ["PFCOUNT", async () => await callWithoutItems(client.hyperloglog, "pfcount")],
      ["PFMERGE", async () => await callWithoutItems(client.hyperloglog, "pfmerge", "out")],
      ["GEOADD", async () => await client.geo.geoadd("geo", [])],
      ["GEOPOS", async () => await client.geo.geoposMany("geo", [])],
      ["GEOHASH", async () => await callWithoutItems(client.geo, "geohash", "geo")],
      ["GEOSEARCH", async () => await client.geo.geosearch("geo", [])],
      ["GEOSEARCHSTORE", async () => await client.geo.geosearchstore("out", "geo", [])],
      ["BF.MADD", async () => await client.bloom.maddMany("bf", [])],
      ["BF.MEXISTS", async () => await callWithoutItems(client.bloom, "mexists", "bf")],
      ["CF.MEXISTS", async () => await client.cuckoo.mexistsMany("cf", [])],
      ["CMS.INCRBY", async () => await client.cms.incrBy("cms", [])],
      ["CMS.QUERY", async () => await client.cms.queryMany("cms", [])],
      ["TOPK.ADD", async () => await client.topk.addMany("topk", [])],
      ["TOPK.INCRBY", async () => await client.topk.incrBy("topk", [])],
      ["TOPK.QUERY", async () => await client.topk.queryMany("topk", [])],
      ["TOPK.COUNT", async () => await client.topk.countMany("topk", [])],
      ["TDIGEST.ADD", async () => await callWithoutItems(client.tdigest, "add", "td")],
      ["TDIGEST.QUANTILE", async () => await callWithoutItems(client.tdigest, "quantile", "td")],
      ["TDIGEST.CDF", async () => await callWithoutItems(client.tdigest, "cdf", "td")],
      ["TDIGEST.RANK", async () => await callWithoutItems(client.tdigest, "rank", "td")],
      ["TDIGEST.REVRANK", async () => await callWithoutItems(client.tdigest, "revrank", "td")],
      ["TDIGEST.BYRANK", async () => await callWithoutItems(client.tdigest, "byrank", "td")],
      ["TDIGEST.BYREVRANK", async () => await callWithoutItems(client.tdigest, "byrevrank", "td")],
      ["TDIGEST.MERGE", async () => await client.tdigest.merge("out", [])]
    ];

    for (const [command, operation] of operations) {
      await expect(operation(), command).rejects.toThrow(`${command} requires at least one`);
    }
    expect(executor.calls).toEqual([]);

    const assertEmptyVariadicsAreRejectedByTypeScript = (): void => {
      // @ts-expect-error DEL requires a key.
      void client.kv.del();
      // @ts-expect-error LPUSH requires an element.
      void client.lists.lpush("list");
      // @ts-expect-error TDIGEST.ADD requires a value.
      void client.tdigest.add("td");
    };
    void assertEmptyVariadicsAreRejectedByTypeScript;
  });

  it("preserves FerricStore's supported key-only PFADD form", async () => {
    const executor = new FakeExecutor([1]);
    const client = new FerricStoreClient(executor);

    await expect(client.hyperloglog.pfadd("hll")).resolves.toBe(1);
    expect(executor.calls).toEqual([["PFADD", "hll"]]);
  });

  it("rejects BITOP without a source key before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.bitmap.bitop("OR", "destination", [])).rejects.toThrow(
      "BITOP requires at least one source key"
    );
    expect(executor.calls).toEqual([]);
  });

  it("requires exactly one BITOP NOT source before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.bitmap.bitop("NOT", "destination", ["one", "two"])).rejects.toThrow(
      "BITOP NOT requires exactly one source key"
    );
    expect(executor.calls).toEqual([]);
  });

  it("rejects CMS.MERGE without a source before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.cms.merge("destination", [])).rejects.toThrow(
      "CMS.MERGE requires at least one source"
    );
    expect(executor.calls).toEqual([]);
  });

  it("rejects a CMS.MERGE weight-count mismatch before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(
      client.cms.merge("destination", ["one", "two"], { weights: [1] })
    ).rejects.toThrow("CMS.MERGE weights must match the source count");
    expect(executor.calls).toEqual([]);
  });
});
