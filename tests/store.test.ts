import { describe, expect, it } from "vitest";
import { FerricStoreClient, JsonCodec } from "../src/index.js";
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
    expect(client.json).toBeDefined();
    expect(client.tdigest).toBeDefined();
    expect(client.topk).toBeDefined();
  });

  it("builds string KV commands with SET options and codec encoding", async () => {
    const executor = new FakeExecutor([Buffer.from("OK"), Buffer.from('{"ok":true}')]);
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

  it("builds object and blocking list command variants", async () => {
    const executor = new FakeExecutor(["raw", ["list", "value"]]);
    const client = new FerricStoreClient(executor);

    await expect(client.kv.objectEncoding("k1")).resolves.toBe("raw");
    await client.lists.blpop(["q1", "q2"], 5);

    expect(executor.calls[0]).toEqual(["OBJECT", "ENCODING", "k1"]);
    expect(executor.calls[1]).toEqual(["BLPOP", "q1", "q2", 5]);
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

  it("builds JSON commands and parses JSON results", async () => {
    const executor = new FakeExecutor([Buffer.from("OK"), Buffer.from('{"id":1}')]);
    const client = new FerricStoreClient(executor);

    await client.json.set("json:user:1", "$", { id: 1 }, { nx: true });
    await expect(client.json.get("json:user:1")).resolves.toEqual({ id: 1 });

    expect(executor.calls[0]).toEqual(["JSON.SET", "json:user:1", "$", '{"id":1}', "NX"]);
    expect(executor.calls[1]).toEqual(["JSON.GET", "json:user:1", "$"]);
  });
});
