import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  FlowClient,
  JsonCodec,
  QueueClient,
  RawCodec,
  WorkflowClient,
  complete,
  transition,
  type ClaimedItem,
  type FencedItem
} from "../../src/index.js";

const runIntegration = process.env.FERRICSTORE_INTEGRATION === "1";

function url(): string {
  return process.env.FERRICSTORE_URL ?? "redis://127.0.0.1:6379/0";
}

function suffix(): string {
  return randomUUID();
}

function text(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  throw new Error(`expected text-compatible value, got ${typeof value}`);
}

function ok(value: unknown): boolean {
  return value === true || value === 1 || value === "OK" || (Buffer.isBuffer(value) && value.toString("utf8") === "OK");
}

function field(source: unknown, name: string): unknown {
  if (source instanceof Map) {
    return source.get(name) ?? source.get(Buffer.from(name));
  }
  if (typeof source === "object" && source != null) {
    const record = source as Record<string, unknown>;
    return record[name];
  }
  return undefined;
}

function eventId(event: unknown): string {
  if (Array.isArray(event) && event.length > 0) {
    return text(event[0]);
  }
  const value = field(event, "event_id") ?? field(event, "id");
  if (value == null) {
    throw new Error(`history event did not contain an event id: ${JSON.stringify(event)}`);
  }
  return text(value);
}

function fenced(job: ClaimedItem): FencedItem {
  return {
    fencingToken: job.fencingToken,
    id: job.id,
    leaseToken: job.leaseToken,
    partitionKey: job.partitionKey
  };
}

async function deletePrefixedKeys(flow: FlowClient, prefix: string): Promise<void> {
  const keys = (await flow.kv.keys(`${prefix}*`)).map(text);
  if (keys.length > 0) {
    await flow.kv.del(...keys);
  }
}

async function claimOne(
  flow: FlowClient,
  type: string,
  state: string,
  partitionKey: string,
  options: {
    leaseMs?: number;
    nowMs?: number;
    worker?: string;
  } = {}
): Promise<ClaimedItem> {
  const jobs = await flow.claimJobs(type, {
    leaseMs: options.leaseMs ?? 30_000,
    limit: 1,
    nowMs: options.nowMs,
    partitionKey,
    state,
    worker: options.worker ?? "ts-sdk-integration-worker"
  });
  expect(jobs).toHaveLength(1);
  const job = jobs[0];
  if (job == null) {
    throw new Error("expected a claimed job");
  }
  return job;
}

async function createAndClaim(
  flow: FlowClient,
  type: string,
  runId: string,
  name: string,
  options: { nowMs?: number; leaseMs?: number; state?: string } = {}
): Promise<{ readonly id: string; readonly partitionKey: string; readonly job: ClaimedItem }> {
  const id = `ts-sdk:${name}:${runId}`;
  const partitionKey = `${id}:partition`;
  const state = options.state ?? "queued";
  await flow.create(id, {
    idempotent: true,
    nowMs: options.nowMs,
    partitionKey,
    payload: { name },
    runAtMs: options.nowMs,
    state,
    type
  });
  const job = await claimOne(flow, type, state, partitionKey, {
    leaseMs: options.leaseMs,
    nowMs: options.nowMs
  });
  return { id, job, partitionKey };
}

describe.runIf(runIntegration)("FerricStore integration", () => {
  it("uses KV helpers and a full Flow claim/complete cycle", async () => {
    const flow = await FlowClient.fromUrl(url(), {
      codec: new JsonCodec()
    });

    const runId = suffix();
    const key = `ts-sdk:kv:${runId}`;
    const id = `ts-sdk:flow:${runId}`;
    const type = "ts-sdk-integration";

    try {
      await flow.kv.set(key, { ok: true }, { px: 60_000 });
      await expect(flow.kv.get(key)).resolves.toEqual({ ok: true });

      await flow.create(id, {
        idempotent: true,
        partitionKey: id,
        payload: { hello: "world" },
        state: "queued",
        type
      });

      const jobs = await flow.claimDue(type, {
        leaseMs: 30_000,
        limit: 1,
        partitionKey: id,
        payload: true,
        state: "queued",
        worker: "ts-sdk-integration-worker"
      });

      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      if (job == null || !("leaseToken" in job)) {
        throw new Error("expected an integration job");
      }
      expect(job).toMatchObject({
        id,
        partitionKey: id,
        payload: { hello: "world" },
        state: "running",
        type
      });

      await flow.complete(job.id, {
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        result: { ok: true }
      });

      const record = await flow.get(id, { partitionKey: id });
      expect(record?.state).toBe("completed");
    } finally {
      await flow.kv.del(key);
      await flow.close();
    }
  });

  it("covers native helpers and read-only diagnostics", async () => {
    const flow = await FlowClient.fromUrl(url(), { codec: new JsonCodec() });
    const runId = suffix();
    const prefix = `ts-sdk:native:${runId}:`;
    const key = `${prefix}cas`;
    const lockKey = `${prefix}lock`;
    const rateKey = `${prefix}rate`;
    const cacheKey = `${prefix}cache`;

    try {
      expect(await flow.ping()).toSatisfy((value: unknown) => value === true || text(value) === "PONG");
      expect(text(await flow.echo("hello"))).toBe("hello");
      const pipeline = await flow.pipeline([
        ["SET", key, flow.codec.encode("old")],
        ["GET", key]
      ]);
      expect(flow.codec.decode(pipeline[1] as Buffer)).toBe("old");

      await expect(flow.cas(key, "old", "new")).resolves.toBe(true);
      expect(flow.codec.decode((await flow.command("GET", key)) as Buffer)).toBe("new");

      await expect(flow.lock(lockKey, "owner-a", 30_000)).resolves.toBe(true);
      await expect(flow.extendLock(lockKey, "owner-a", 30_000)).resolves.toBe(1);
      await expect(flow.unlock(lockKey, "owner-a")).resolves.toBe(1);

      const rate = await flow.rateLimitAdd(rateKey, { count: 2, max: 5, windowMs: 60_000 });
      expect(rate.count).toBeGreaterThanOrEqual(1);
      expect(rate.remaining).toBeGreaterThanOrEqual(0);

      const keyInfo = await flow.keyInfo(key);
      expect(keyInfo.raw).toBeTypeOf("object");

      const first = await flow.fetchOrCompute(cacheKey, { hint: "integration", ttlMs: 60_000 });
      expect(first.shouldCompute).toBe(true);
      await expect(flow.fetchOrComputeResult(cacheKey, { computed: true }, { ttlMs: 60_000 })).resolves.toBe(true);
      const cached = await flow.fetchOrCompute<{ computed: boolean }>(cacheKey, { ttlMs: 60_000 });
      expect(cached.hit).toBe(true);
      expect(cached.value).toEqual({ computed: true });

      const errorKey = `${prefix}cache-error`;
      const firstError = await flow.fetchOrCompute(errorKey, { ttlMs: 60_000 });
      expect(firstError.shouldCompute).toBe(true);
      await expect(flow.fetchOrComputeError(errorKey, "boom")).resolves.toBe(true);

      await expect(flow.serverInfo("server")).resolves.toContain("#");
      await expect(flow.commandCount()).resolves.toBeGreaterThan(0);
      await expect(flow.commandList()).resolves.not.toHaveLength(0);
      await expect(flow.commandInfo("get")).resolves.toHaveLength(1);
      await expect(flow.commandDocs("get")).resolves.toBeDefined();
      expect((await flow.commandGetKeys(["GET", key])).map(text)).toContain(key);
      await expect(flow.clientId()).resolves.toBeGreaterThan(0);
      await expect(flow.clientSetName(`ts-sdk-${runId}`)).resolves.toBe(true);
      await expect(flow.clientGetName()).resolves.toBe(`ts-sdk-${runId}`);
      await expect(flow.clientInfo()).resolves.toContain("id=");
      await expect(flow.clientList()).resolves.toContain("id=");
      await expect(flow.clientTracking("ON", { optin: true })).resolves.toBe(true);
      await expect(flow.clientTrackingInfo()).resolves.toBeDefined();
      await expect(flow.clientGetRedir()).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.clientCaching("NO")).resolves.toBe(true);
      await expect(flow.clientTracking("OFF")).resolves.toBe(true);
      await expect(flow.lastsave()).resolves.toBeGreaterThan(0);
      await expect(flow.moduleList()).resolves.toEqual([]);
      await expect(flow.publish(`${prefix}channel`, "hello")).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.pubsubChannels()).resolves.toBeDefined();
      await expect(flow.pubsubNumSub(`${prefix}channel`)).resolves.toBeDefined();
      await expect(flow.pubsubNumPat()).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.aclWhoami()).resolves.toBe("default");
      await expect(flow.clusterHealth()).resolves.toBeTypeOf("object");
      await expect(flow.clusterStats()).resolves.toBeTypeOf("object");
      await expect(flow.clusterKeyslot(key)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.clusterSlots()).resolves.toBeDefined();
      await expect(flow.clusterStatus()).resolves.toBeTypeOf("object");
      await expect(flow.clusterRole()).resolves.toBeDefined();
      await expect(flow.ferricstoreConfig("GET", "*")).resolves.toBeDefined();
      await expect(flow.ferricstoreMetrics()).resolves.toBeTypeOf("object");
      await expect(flow.ferricstoreHotness()).resolves.toBeTypeOf("object");
      await expect(flow.ferricstoreDoctor("CHECK", "SCOPE", "BITCASK")).resolves.toBeDefined();
    } finally {
      await deletePrefixedKeys(flow, prefix);
      await flow.close();
    }
  });

  it("covers typed Redis-compatible store families", async () => {
    const flow = await FlowClient.fromUrl(url(), { codec: new RawCodec() });
    const runId = suffix();
    const prefix = `ts-sdk:store:${runId}:`;

    try {
      const stringKey = `${prefix}string`;
      const secondKey = `${prefix}string2`;
      const thirdKey = `${prefix}string3`;
      await expect(flow.kv.set(stringKey, "abc", { px: 60_000 })).resolves.toSatisfy(ok);
      expect(text(await flow.kv.get(stringKey))).toBe("abc");
      await expect(flow.kv.exists(stringKey)).resolves.toBe(1);
      expect((await flow.kv.mget<Buffer>([stringKey, `${prefix}missing`])).map((item) => (item == null ? null : text(item)))).toEqual(["abc", null]);
      await expect(flow.kv.mset({ [secondKey]: "2", [thirdKey]: "3" })).resolves.toBe(true);
      await expect(flow.kv.msetnx({ [`${prefix}nx1`]: "1", [`${prefix}nx2`]: "2" })).resolves.toBe(true);
      await expect(flow.kv.incr(`${prefix}counter`)).resolves.toBe(1);
      await expect(flow.kv.incrby(`${prefix}counter`, 4)).resolves.toBe(5);
      await expect(flow.kv.decr(`${prefix}counter`)).resolves.toBe(4);
      await expect(flow.kv.decrby(`${prefix}counter`, 2)).resolves.toBe(2);
      expect(Number(await flow.kv.incrbyfloat(`${prefix}float`, 1.5))).toBeGreaterThanOrEqual(1.5);
      await expect(flow.kv.append(`${prefix}append`, "abc")).resolves.toBe(3);
      await expect(flow.kv.strlen(`${prefix}append`)).resolves.toBe(3);
      expect(text(await flow.kv.getset(`${prefix}append`, "xyz"))).toBe("abc");
      expect(text(await flow.kv.getrange(`${prefix}append`, 0, 1))).toBe("xy");
      await expect(flow.kv.setrange(`${prefix}append`, 1, "Q")).resolves.toBe(3);
      expect(text(await flow.kv.getex(`${prefix}append`, { px: 60_000 }))).toBe("xQz");
      await expect(flow.kv.ttl(`${prefix}append`)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.pttl(`${prefix}append`)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.persist(`${prefix}append`)).resolves.toBe(true);
      await expect(flow.kv.expire(`${prefix}append`, 60)).resolves.toBe(true);
      await expect(flow.kv.pexpire(`${prefix}append`, 60_000)).resolves.toBe(true);
      await expect(flow.kv.expireat(`${prefix}append`, Math.floor(Date.now() / 1000) + 60)).resolves.toBe(true);
      await expect(flow.kv.pexpireat(`${prefix}append`, Date.now() + 60_000)).resolves.toBe(true);
      await expect(flow.kv.expiretime(`${prefix}append`)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.pexpiretime(`${prefix}append`)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.type(`${prefix}append`)).resolves.toBe("string");
      await expect(flow.kv.setnx(`${prefix}setnx`, "1")).resolves.toBe(true);
      await expect(flow.kv.setex(`${prefix}setex`, 60, "1")).resolves.toBe(true);
      await expect(flow.kv.psetex(`${prefix}psetex`, 60_000, "1")).resolves.toBe(true);
      await expect(flow.kv.copy(stringKey, `${prefix}copy`, { replace: true })).resolves.toBe(true);
      await expect(flow.kv.rename(`${prefix}copy`, `${prefix}renamed`)).resolves.toBe(true);
      await expect(flow.kv.renamenx(`${prefix}renamed`, `${prefix}renamed-nx`)).resolves.toBe(true);
      await expect(flow.kv.randomkey()).resolves.toBeTypeOf("string");
      await expect(flow.kv.keys(`${prefix}*`)).resolves.not.toHaveLength(0);
      await expect(flow.kv.scan(0, { count: 10, match: `${prefix}*` })).resolves.toBeDefined();
      await expect(flow.kv.dbsize()).resolves.toBeGreaterThan(0);
      await expect(flow.kv.objectEncoding(stringKey)).resolves.toBeTypeOf("string");
      await expect(flow.kv.objectHelp()).resolves.not.toHaveLength(0);
      await expect(flow.kv.objectFreq(stringKey)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.objectIdleTime(stringKey)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.kv.objectRefcount(stringKey)).resolves.toBe(1);
      await expect(flow.kv.wait(0, 1)).resolves.toBe(0);
      await expect(flow.kv.waitAof(0, 0, 1)).resolves.toBeDefined();
      await expect(flow.kv.memoryUsage(stringKey)).resolves.toBeGreaterThanOrEqual(0);
      expect(text(await flow.kv.getdel(`${prefix}setnx`))).toBe("1");
      await expect(flow.kv.unlink(`${prefix}nx1`)).resolves.toBeGreaterThanOrEqual(0);

      const hashKey = `${prefix}hash`;
      await expect(flow.hash.hset(hashKey, { count: "1", field: "value" })).resolves.toBeGreaterThanOrEqual(1);
      expect(text(await flow.hash.hget(hashKey, "field"))).toBe("value");
      expect((await flow.hash.hmget<Buffer>(hashKey, ["field", "none"])).map((item) => (item == null ? null : text(item)))).toEqual(["value", null]);
      await expect(flow.hash.hgetall(hashKey)).resolves.toBeDefined();
      await expect(flow.hash.hexists(hashKey, "field")).resolves.toBe(true);
      expect((await flow.hash.hkeys(hashKey)).map(text)).toContain("field");
      expect((await flow.hash.hvals<Buffer>(hashKey)).map(text)).toContain("value");
      await expect(flow.hash.hlen(hashKey)).resolves.toBeGreaterThanOrEqual(2);
      await expect(flow.hash.hincrby(hashKey, "count", 2)).resolves.toBe(3);
      expect(Number(await flow.hash.hincrbyfloat(hashKey, "float", 1.25))).toBeGreaterThanOrEqual(1.25);
      await expect(flow.hash.hsetnx(hashKey, "new", "item")).resolves.toBe(true);
      await expect(flow.hash.hstrlen(hashKey, "field")).resolves.toBe(5);
      await expect(flow.hash.hrandfield(hashKey, 1, true)).resolves.toBeDefined();
      await expect(flow.hash.hscan(hashKey, 0, { count: 10 })).resolves.toBeDefined();
      await expect(flow.hash.hexpire(hashKey, 60, ["field"])).resolves.toBeDefined();
      await expect(flow.hash.httl(hashKey, ["field"])).resolves.toBeDefined();
      await expect(flow.hash.hpersist(hashKey, ["field"])).resolves.toBeDefined();
      await expect(flow.hash.hpexpire(hashKey, 60_000, ["field"])).resolves.toBeDefined();
      await expect(flow.hash.hpttl(hashKey, ["field"])).resolves.toBeDefined();
      await expect(flow.hash.hexpiretime(hashKey, ["field"])).resolves.toBeDefined();
      expect(text((await flow.hash.hgetex<Buffer>(hashKey, ["field"], { px: 60_000 }))[0])).toBe("value");
      await expect(flow.hash.hsetex(hashKey, 60, { temp: "1" })).resolves.toBeGreaterThanOrEqual(0);
      expect(text((await flow.hash.hgetdel<Buffer>(hashKey, ["temp"]))[0])).toBe("1");
      await expect(flow.hash.hdel(hashKey, "new")).resolves.toBe(1);

      const listKey = `${prefix}list`;
      const listDst = `${prefix}list-dst`;
      await expect(flow.lists.lpush(listKey, "b", "a")).resolves.toBe(2);
      await expect(flow.lists.rpush(listKey, "c")).resolves.toBe(3);
      expect((await flow.lists.lrange<Buffer>(listKey, 0, -1)).map(text)).toContain("a");
      await expect(flow.lists.llen(listKey)).resolves.toBe(3);
      expect(text(await flow.lists.lindex(listKey, 0))).toBe("a");
      await expect(flow.lists.lset(listKey, 1, "bb")).resolves.toBe(true);
      await expect(flow.lists.lrem(listKey, 0, "bb")).resolves.toBe(1);
      await expect(flow.lists.ltrim(listKey, 0, 1)).resolves.toBe(true);
      await expect(flow.lists.lpos(listKey, "a")).resolves.toBe(0);
      await expect(flow.lists.linsert(listKey, "AFTER", "a", "aa")).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.lists.lmove(listKey, listDst, "LEFT", "RIGHT")).resolves.toBeDefined();
      await expect(flow.lists.rpoplpush(listDst, listKey)).resolves.toBeDefined();
      await expect(flow.lists.lpushx(listKey, "left")).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.lists.rpushx(listKey, "right")).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.lists.blpop([listKey], 1)).resolves.toBeDefined();
      await flow.lists.rpush(listKey, "block");
      await expect(flow.lists.brpop([listKey], 1)).resolves.toBeDefined();
      await flow.lists.rpush(listKey, "move");
      await expect(flow.lists.blmove(listKey, listDst, "LEFT", "RIGHT", 1)).resolves.toBeDefined();
      await flow.lists.rpush(listKey, "mpop");
      await expect(flow.lists.blmpop(1, [listKey], "LEFT", { count: 1 })).resolves.toBeDefined();

      const setA = `${prefix}set-a`;
      const setB = `${prefix}set-b`;
      await expect(flow.sets.sadd(setA, "a", "b")).resolves.toBe(2);
      await expect(flow.sets.sadd(setB, "b", "c")).resolves.toBe(2);
      expect((await flow.sets.smembers<Buffer>(setA)).map(text)).toContain("a");
      await expect(flow.sets.sismember(setA, "a")).resolves.toBe(true);
      await expect(flow.sets.smismember(setA, ["a", "z"])).resolves.toEqual([1, 0]);
      await expect(flow.sets.scard(setA)).resolves.toBe(2);
      await expect(flow.sets.srandmember(setA, 1)).resolves.toBeDefined();
      await expect(flow.sets.sdiff<Buffer>([setA, setB])).resolves.toHaveLength(1);
      await expect(flow.sets.sinter<Buffer>([setA, setB])).resolves.toHaveLength(1);
      await expect(flow.sets.sunion<Buffer>([setA, setB])).resolves.not.toHaveLength(0);
      await expect(flow.sets.sdiffstore(`${prefix}sdiff`, [setA, setB])).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.sets.sinterstore(`${prefix}sinter`, [setA, setB])).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.sets.sunionstore(`${prefix}sunion`, [setA, setB])).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.sets.sintercard([setA, setB], 10)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.sets.smove(setA, setB, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.sets.sscan(setB, 0, { count: 10 })).resolves.toBeDefined();
      await expect(flow.sets.spop(setB, 1)).resolves.toBeDefined();
      await expect(flow.sets.srem(setA, "b")).resolves.toBeGreaterThanOrEqual(0);

      const zset = `${prefix}zset`;
      await expect(flow.zset.zadd(zset, [{ member: "a", score: 1 }, { member: "b", score: 2 }, { member: "c", score: 3 }])).resolves.toBe(3);
      await expect(flow.zset.zscore(zset, "a")).resolves.toBeTypeOf("string");
      await expect(flow.zset.zrank(zset, "a")).resolves.toBe(0);
      await expect(flow.zset.zrevrank(zset, "c")).resolves.toBe(0);
      await expect(flow.zset.zrange(zset, 0, -1)).resolves.not.toHaveLength(0);
      await expect(flow.zset.zrevrange(zset, 0, -1)).resolves.not.toHaveLength(0);
      await expect(flow.zset.zcard(zset)).resolves.toBe(3);
      await expect(flow.zset.zincrby(zset, 1, "a")).resolves.toBeTypeOf("string");
      await expect(flow.zset.zcount(zset, "-inf", "+inf")).resolves.toBeGreaterThanOrEqual(3);
      await expect(flow.zset.zrandmember(zset, 1, true)).resolves.toBeDefined();
      await expect(flow.zset.zmscore(zset, ["a", "none"])).resolves.toHaveLength(2);
      await expect(flow.zset.zrangebyscore(zset, "-inf", "+inf")).resolves.not.toHaveLength(0);
      await expect(flow.zset.zrevrangebyscore(zset, "+inf", "-inf")).resolves.not.toHaveLength(0);
      await expect(flow.zset.zscan(zset, 0, { count: 10 })).resolves.toBeDefined();
      await expect(flow.zset.zrem(zset, "b")).resolves.toBe(1);
      await expect(flow.zset.zpopmin(zset, 1)).resolves.not.toHaveLength(0);
      await expect(flow.zset.zpopmax(zset, 1)).resolves.not.toHaveLength(0);

      const stream = `${prefix}stream`;
      const streamId = text(await flow.stream.xadd(stream, "*", { field: "value" }));
      await expect(flow.stream.xlen(stream)).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.stream.xrange(stream)).resolves.not.toHaveLength(0);
      await expect(flow.stream.xrevrange(stream)).resolves.not.toHaveLength(0);
      await expect(flow.stream.xread([{ id: "0-0", key: stream }], { count: 1 })).resolves.toBeDefined();
      await expect(flow.stream.xinfoStream(stream)).resolves.toBeDefined();
      const group = `group-${runId}`;
      await expect(flow.stream.xgroupCreate(stream, group, "0")).resolves.toBe(true);
      await expect(flow.stream.xreadgroup(group, "consumer", [{ id: ">", key: stream }], { count: 1 })).resolves.toBeDefined();
      await expect(flow.stream.xack(stream, group, streamId)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.stream.xtrim(stream, "MAXLEN", 10, true)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.stream.xdel(stream, streamId)).resolves.toBeGreaterThanOrEqual(0);

      const bitmap = `${prefix}bitmap`;
      await expect(flow.bitmap.setbit(bitmap, 7, 1)).resolves.toBe(0);
      await expect(flow.bitmap.getbit(bitmap, 7)).resolves.toBe(1);
      await expect(flow.bitmap.bitcount(bitmap)).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.bitmap.bitpos(bitmap, 1)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.bitmap.bitop("OR", `${prefix}bitmap-out`, bitmap)).resolves.toBeGreaterThanOrEqual(1);

      const hll = `${prefix}hll`;
      await expect(flow.hyperloglog.pfadd(hll, "a", "b")).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.hyperloglog.pfcount(hll)).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.hyperloglog.pfmerge(`${prefix}hll-dst`, hll)).resolves.toBe(true);

      const geo = `${prefix}geo`;
      await expect(flow.geo.geoadd(geo, [{ latitude: 38.115556, longitude: 13.361389, member: "palermo" }])).resolves.toBe(1);
      await expect(flow.geo.geoadd(geo, [{ latitude: 37.502669, longitude: 15.087269, member: "catania" }])).resolves.toBe(1);
      await expect(flow.geo.geopos(geo, "palermo")).resolves.toBeDefined();
      await expect(flow.geo.geodist(geo, "palermo", "catania", "km")).resolves.toBeTypeOf("string");
      await expect(flow.geo.geohash(geo, "palermo")).resolves.toBeDefined();
      await expect(flow.geo.geosearch(geo, ["FROMMEMBER", "palermo", "BYRADIUS", 200, "km"])).resolves.toBeDefined();
      await expect(
        flow.geo.geosearchstore(`${prefix}geo-dst`, geo, [
          "FROMMEMBER",
          "palermo",
          "BYRADIUS",
          200,
          "km"
        ])
      ).resolves.toBeGreaterThanOrEqual(0);
    } finally {
      await deletePrefixedKeys(flow, prefix);
      await flow.close();
    }
  });

  it("covers native probabilistic helpers except JSON", async () => {
    const flow = await FlowClient.fromUrl(url(), { codec: new RawCodec() });
    const runId = suffix();
    const prefix = `ts-sdk:prob:${runId}:`;

    try {
      const bloom = `${prefix}bf`;
      await expect(flow.bloom.reserve(bloom, 0.01, 100)).resolves.toBe(true);
      await expect(flow.bloom.add(bloom, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.bloom.madd(bloom, "b", "c")).resolves.toHaveLength(2);
      await expect(flow.bloom.exists(bloom, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.bloom.mexists(bloom, "a", "z")).resolves.toEqual(expect.arrayContaining([expect.any(Number)]));
      await expect(flow.bloom.card(bloom)).resolves.toBeGreaterThanOrEqual(1);
      await expect(flow.bloom.info(bloom)).resolves.not.toHaveLength(0);

      const cuckoo = `${prefix}cf`;
      await expect(flow.cuckoo.reserve(cuckoo, 100)).resolves.toBe(true);
      await expect(flow.cuckoo.add(cuckoo, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.cuckoo.addnx(cuckoo, "b")).resolves.toBeTypeOf("boolean");
      await expect(flow.cuckoo.exists(cuckoo, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.cuckoo.mexists(cuckoo, "a", "z")).resolves.toHaveLength(2);
      await expect(flow.cuckoo.count(cuckoo, "a")).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.cuckoo.del(cuckoo, "a")).resolves.toBeTypeOf("boolean");
      await expect(flow.cuckoo.info(cuckoo)).resolves.not.toHaveLength(0);

      const cmsA = `${prefix}cms-a`;
      const cmsB = `${prefix}cms-b`;
      const cmsProb = `${prefix}cms-prob`;
      const cmsDst = `${prefix}cms-dst`;
      await expect(flow.cms.initByDim(cmsA, 20, 4)).resolves.toBe(true);
      await expect(flow.cms.initByDim(cmsB, 20, 4)).resolves.toBe(true);
      await expect(flow.cms.initByProb(cmsProb, 0.01, 0.01)).resolves.toBe(true);
      await expect(flow.cms.incrBy(cmsA, [["a", 2], ["b", 3]])).resolves.toHaveLength(2);
      await expect(flow.cms.incrBy(cmsB, [["a", 1]])).resolves.toHaveLength(1);
      await expect(flow.cms.query(cmsA, "a", "b")).resolves.toHaveLength(2);
      await expect(flow.cms.merge(cmsDst, [cmsA, cmsB])).resolves.toBe(true);
      await expect(flow.cms.info(cmsDst)).resolves.not.toHaveLength(0);

      const topk = `${prefix}topk`;
      await expect(flow.topk.reserve(topk, 3)).resolves.toBe(true);
      await expect(flow.topk.add<Buffer>(topk, "a", "b", "a")).resolves.toHaveLength(3);
      await expect(flow.topk.incrBy<Buffer>(topk, [["c", 2]])).resolves.toHaveLength(1);
      await expect(flow.topk.query(topk, "a", "z")).resolves.toHaveLength(2);
      await expect(flow.topk.list<Buffer>(topk, { withCount: true })).resolves.not.toHaveLength(0);
      await expect(flow.topk.info(topk)).resolves.not.toHaveLength(0);

      const tdigest = `${prefix}tdigest`;
      const tdigestSrc = `${prefix}tdigest-src`;
      await expect(flow.tdigest.create(tdigest)).resolves.toBe(true);
      await expect(flow.tdigest.add(tdigest, 1, 2, 3, 4)).resolves.toBe(true);
      await expect(flow.tdigest.quantile(tdigest, 0.5)).resolves.toHaveLength(1);
      await expect(flow.tdigest.cdf(tdigest, 2)).resolves.toHaveLength(1);
      await expect(flow.tdigest.rank(tdigest, 2)).resolves.toHaveLength(1);
      await expect(flow.tdigest.revrank(tdigest, 2)).resolves.toHaveLength(1);
      await expect(flow.tdigest.byrank(tdigest, 1)).resolves.toHaveLength(1);
      await expect(flow.tdigest.byrevrank(tdigest, 1)).resolves.toHaveLength(1);
      await expect(flow.tdigest.trimmedMean(tdigest, 0.1, 0.9)).resolves.toBeTypeOf("string");
      await expect(flow.tdigest.min(tdigest)).resolves.toBeTypeOf("string");
      await expect(flow.tdigest.max(tdigest)).resolves.toBeTypeOf("string");
      await expect(flow.tdigest.info(tdigest)).resolves.not.toHaveLength(0);
      await expect(flow.tdigest.create(tdigestSrc)).resolves.toBe(true);
      await expect(flow.tdigest.add(tdigestSrc, 5, 6)).resolves.toBe(true);
      await expect(flow.tdigest.merge(`${prefix}tdigest-dst`, [tdigest, tdigestSrc], { override: true })).resolves.toBe(true);
      await expect(flow.tdigest.reset(tdigest)).resolves.toBe(true);
    } finally {
      await deletePrefixedKeys(flow, prefix);
      await flow.close();
    }
  });

  it("covers Flow state-machine repair and index commands", async () => {
    const flow = await FlowClient.fromUrl(url(), { codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-flow-${runId}`;
    const now = Date.now();

    try {
      const valueResponse = await flow.valuePut({ shared: true }, { partitionKey: `ts-sdk:value:${runId}`, ttlMs: 60_000 });
      const valueRef = field(valueResponse, "ref");
      if (valueRef == null) {
        throw new Error("FLOW.VALUE.PUT did not return a ref");
      }
      await expect(flow.valueMGet([text(valueRef)])).resolves.toEqual([{ shared: true }]);

      const signalId = `ts-sdk:signal:${runId}`;
      const signalPartition = `${signalId}:partition`;
      await flow.create(signalId, {
        idempotent: true,
        partitionKey: signalPartition,
        payload: { step: "created" },
        state: "created",
        type
      });
      await expect(flow.signal(signalId, {
        ifState: "created",
        partitionKey: signalPartition,
        signal: "approve",
        transitionTo: "approved"
      })).resolves.toBeDefined();
      await expect(flow.flowSignal(signalId, {
        ifState: "approved",
        partitionKey: signalPartition,
        signal: "ship",
        transitionTo: "shipped"
      })).resolves.toBeDefined();
      await expect(flow.get(signalId, { partitionKey: signalPartition })).resolves.toMatchObject({ state: "shipped" });

      const batchPartition = `ts-sdk:batch:${runId}:partition`;
      await expect(flow.createMany(batchPartition, [
        { id: `ts-sdk:batch:${runId}:a`, payload: { n: 1 } },
        { id: `ts-sdk:batch:${runId}:b`, payload: { n: 2 } }
      ], { idempotent: true, nowMs: now, runAtMs: now, state: "batch", type })).resolves.toBeDefined();
      const batchJobs = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now,
        partitionKey: batchPartition,
        state: "batch",
        worker: "ts-sdk-batch-worker"
      });
      expect(batchJobs).toHaveLength(2);
      await expect(flow.completeJobs(batchJobs, { result: { batch: true } })).resolves.toBeDefined();

      const transitionJob = await createAndClaim(flow, type, runId, "transition");
      await expect(flow.extendLease(transitionJob.id, {
        fencingToken: transitionJob.job.fencingToken,
        leaseMs: 30_000,
        leaseToken: transitionJob.job.leaseToken,
        partitionKey: transitionJob.partitionKey
      })).resolves.toMatchObject({ id: transitionJob.id });
      await expect(flow.transition(transitionJob.id, {
        fencingToken: transitionJob.job.fencingToken,
        fromState: "queued",
        leaseToken: transitionJob.job.leaseToken,
        partitionKey: transitionJob.partitionKey,
        payload: { step: "ready" },
        toState: "ready"
      })).resolves.toBeDefined();
      const readyJob = await claimOne(flow, type, "ready", transitionJob.partitionKey);
      await expect(flow.complete(readyJob.id, {
        fencingToken: readyJob.fencingToken,
        leaseToken: readyJob.leaseToken,
        partitionKey: readyJob.partitionKey,
        result: { ok: true }
      })).resolves.toBeDefined();

      const retryJob = await createAndClaim(flow, type, runId, "retry", { nowMs: now });
      await expect(flow.retry(retryJob.id, {
        error: { retry: true },
        fencingToken: retryJob.job.fencingToken,
        leaseToken: retryJob.job.leaseToken,
        nowMs: now,
        partitionKey: retryJob.partitionKey,
        runAtMs: now
      })).resolves.toBeDefined();
      const retriedJob = await claimOne(flow, type, "queued", retryJob.partitionKey, { nowMs: now + 1 });
      await expect(flow.complete(retriedJob.id, {
        fencingToken: retriedJob.fencingToken,
        leaseToken: retriedJob.leaseToken,
        partitionKey: retriedJob.partitionKey
      })).resolves.toBeDefined();

      const failedJob = await createAndClaim(flow, type, runId, "fail");
      await expect(flow.fail(failedJob.id, {
        error: { failed: true },
        fencingToken: failedJob.job.fencingToken,
        leaseToken: failedJob.job.leaseToken,
        partitionKey: failedJob.partitionKey
      })).resolves.toBeDefined();
      await expect(flow.get(failedJob.id, { partitionKey: failedJob.partitionKey })).resolves.toMatchObject({ state: "failed" });
      expect((await flow.failures(type, { count: 20 })).some((record) => record.id === failedJob.id)).toBe(true);

      const cancelJob = await createAndClaim(flow, type, runId, "cancel");
      await expect(flow.cancel(cancelJob.id, {
        fencingToken: cancelJob.job.fencingToken,
        leaseToken: cancelJob.job.leaseToken,
        partitionKey: cancelJob.partitionKey,
        reason: { cancelled: true }
      })).resolves.toBeDefined();
      await expect(flow.get(cancelJob.id, { partitionKey: cancelJob.partitionKey })).resolves.toMatchObject({ state: "cancelled" });
      expect((await flow.terminals(type, { count: 50 })).some((record) => record.id === cancelJob.id)).toBe(true);

      const manyPartition = `ts-sdk:many:${runId}:partition`;
      await flow.createMany(manyPartition, [
        { id: `ts-sdk:many:${runId}:a` },
        { id: `ts-sdk:many:${runId}:b` }
      ], { nowMs: now, runAtMs: now, state: "many-transition", type });
      const manyJobs = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now,
        partitionKey: manyPartition,
        state: "many-transition",
        worker: "ts-sdk-many-worker"
      });
      expect(manyJobs).toHaveLength(2);
      await expect(flow.transitionMany(manyPartition, {
        fromState: "many-transition",
        items: manyJobs.map(fenced),
        nowMs: now,
        toState: "many-complete"
      })).resolves.toBeDefined();
      const manyCompleteJobs = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now + 1,
        partitionKey: manyPartition,
        state: "many-complete",
        worker: "ts-sdk-many-worker"
      });
      expect(manyCompleteJobs).toHaveLength(2);
      await expect(flow.completeMany(manyPartition, manyCompleteJobs, { result: { ok: true } })).resolves.toBeDefined();

      const retryManyPartition = `ts-sdk:retry-many:${runId}:partition`;
      await flow.createMany(retryManyPartition, [
        { id: `ts-sdk:retry-many:${runId}:a` },
        { id: `ts-sdk:retry-many:${runId}:b` }
      ], { nowMs: now, runAtMs: now, state: "retry-many", type });
      const retryManyJobs = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now,
        partitionKey: retryManyPartition,
        state: "retry-many",
        worker: "ts-sdk-retry-many-worker"
      });
      expect(retryManyJobs).toHaveLength(2);
      await expect(flow.retryMany(retryManyPartition, retryManyJobs, {
        error: { retry: "many" },
        nowMs: now,
        runAtMs: now
      })).resolves.toBeDefined();
      const retryManyAgain = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now + 1,
        partitionKey: retryManyPartition,
        state: "retry-many",
        worker: "ts-sdk-retry-many-worker"
      });
      expect(retryManyAgain).toHaveLength(2);
      await expect(flow.failMany(retryManyPartition, retryManyAgain, { error: { done: true } })).resolves.toBeDefined();

      const cancelManyPartition = `ts-sdk:cancel-many:${runId}:partition`;
      await flow.createMany(cancelManyPartition, [
        { id: `ts-sdk:cancel-many:${runId}:a` },
        { id: `ts-sdk:cancel-many:${runId}:b` }
      ], { nowMs: now, runAtMs: now, state: "cancel-many", type });
      const cancelManyJobs = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now,
        partitionKey: cancelManyPartition,
        state: "cancel-many",
        worker: "ts-sdk-cancel-many-worker"
      });
      expect(cancelManyJobs).toHaveLength(2);
      await expect(flow.cancelMany(cancelManyPartition, cancelManyJobs.map(fenced), { reason: { cancel: "many" } })).resolves.toBeDefined();

      const reclaimId = `ts-sdk:reclaim:${runId}`;
      const reclaimPartition = `${reclaimId}:partition`;
      await flow.create(reclaimId, {
        nowMs: 1_000,
        partitionKey: reclaimPartition,
        runAtMs: 1_000,
        state: "reclaim",
        type
      });
      await claimOne(flow, type, "reclaim", reclaimPartition, {
        leaseMs: 10,
        nowMs: 1_000,
        worker: "ts-sdk-reclaim-initial"
      });
      const reclaimed = await flow.reclaim(type, {
        jobOnly: true,
        leaseMs: 30_000,
        limit: 1,
        nowMs: 2_000,
        partitionKey: reclaimPartition,
        worker: "ts-sdk-reclaim-worker"
      });
      expect(reclaimed).toHaveLength(1);
      const reclaimedJob = reclaimed[0];
      if (reclaimedJob == null || !("leaseToken" in reclaimedJob)) {
        throw new Error("expected reclaimed compact job");
      }
      await expect(flow.complete(reclaimedJob.id, {
        fencingToken: reclaimedJob.fencingToken,
        leaseToken: reclaimedJob.leaseToken,
        partitionKey: reclaimedJob.partitionKey
      })).resolves.toBeDefined();

      const stuckId = `ts-sdk:stuck:${runId}`;
      const stuckPartition = `${stuckId}:partition`;
      await flow.create(stuckId, {
        nowMs: 1_000,
        partitionKey: stuckPartition,
        runAtMs: 1_000,
        state: "stuck",
        type
      });
      const stuckJob = await claimOne(flow, type, "stuck", stuckPartition, { leaseMs: 60_000, nowMs: 1_000 });
      expect(
        (
          await flow.stuck(type, {
            count: 10,
            nowMs: 120_000,
            olderThanMs: 1,
            partitionKey: stuckPartition
          })
        ).some((record) => record.id === stuckId)
      ).toBe(true);
      await expect(flow.complete(stuckJob.id, {
        fencingToken: stuckJob.fencingToken,
        leaseToken: stuckJob.leaseToken,
        partitionKey: stuckJob.partitionKey
      })).resolves.toBeDefined();

      const parentId = `ts-sdk:parent:${runId}`;
      const parentPartition = `${parentId}:partition`;
      await flow.create(parentId, {
        correlationId: `corr:${runId}`,
        idempotent: true,
        partitionKey: parentPartition,
        rootFlowId: `root:${runId}`,
        state: "dispatch",
        type
      });
      const parent = await flow.get(parentId, { partitionKey: parentPartition });
      if (parent == null) {
        throw new Error("expected parent flow");
      }
      await expect(flow.spawnChildren(parentId, [
        { id: `ts-sdk:child:${runId}:a`, payload: { child: "a" }, type },
        { id: `ts-sdk:child:${runId}:b`, payload: { child: "b" }, type }
      ], {
        failure: "children_failed",
        fencingToken: parent.fencingToken,
        fromState: "dispatch",
        groupId: "fanout",
        partitionKey: parentPartition,
        success: "children_done",
        wait: "any",
        waitState: "waiting_children"
      })).resolves.toBeDefined();
      expect((await flow.byParent(parentId)).some((record) => record.id.startsWith(`ts-sdk:child:${runId}:`))).toBe(true);
      expect((await flow.byRoot(`root:${runId}`)).some((record) => record.id === parentId)).toBe(true);
      expect((await flow.byCorrelation(`corr:${runId}`)).some((record) => record.id === parentId)).toBe(true);

      const rewindJob = await createAndClaim(flow, type, runId, "rewind");
      const historyBefore = await flow.history(rewindJob.id, { count: 10, partitionKey: rewindJob.partitionKey });
      const createdEventId = eventId(historyBefore[0]);
      await flow.complete(rewindJob.id, {
        fencingToken: rewindJob.job.fencingToken,
        leaseToken: rewindJob.job.leaseToken,
        partitionKey: rewindJob.partitionKey
      });
      await expect(flow.rewind(rewindJob.id, {
        expectState: "completed",
        partitionKey: rewindJob.partitionKey,
        returnRecord: true,
        toEvent: createdEventId
      })).resolves.toMatchObject({ state: "queued" });

      await expect(flow.list(type, { count: 100 })).resolves.not.toHaveLength(0);
      await expect(flow.info(type)).resolves.toBeTypeOf("object");
      await expect(flow.history(signalId, { count: 5, partitionKey: signalPartition })).resolves.toBeDefined();
      await expect(flow.retentionCleanup()).resolves.toBeTypeOf("object");
    } finally {
      await flow.close();
    }
  }, 20_000);

  it("covers queue and workflow wrappers against the live server", async () => {
    const flow = await FlowClient.fromUrl(url(), { codec: new JsonCodec() });
    const runId = suffix();
    const now = Date.now();

    try {
      const queueType = `ts-sdk-queue-${runId}`;
      const queuePartition = `ts-sdk:queue:${runId}:partition`;
      const queue = new QueueClient(flow).queue({ type: queueType, worker: "ts-sdk-queue-worker" });
      await queue.enqueue(`ts-sdk:queue:${runId}`, {
        idempotent: true,
        nowMs: now,
        partitionKey: queuePartition,
        payload: { step: "queued" },
        runAtMs: now
      });
      const queueResult = await queue.worker({ batchSize: 1, nowMs: now + 1, partitionKey: queuePartition, worker: "ts-sdk-queue-worker" }).runOnce((job) => {
        expect(job.payload).toEqual({ step: "queued" });
        return { ok: true };
      });
      expect(queueResult).toEqual({ claimed: 1, completed: 1, failed: 0, retried: 0 });

      const workflowType = `ts-sdk-workflow-${runId}`;
      const workflow = new WorkflowClient(flow).workflow({
        initialState: "received",
        type: workflowType,
        worker: "ts-sdk-workflow-worker"
      });
      workflow
        .state("received", () => transition("validated", { payload: { validated: true } }))
        .state("validated", (ctx) => complete({ result: { id: ctx.id, done: true } }));

      const workflowId = `ts-sdk:workflow:${runId}`;
      const workflowPartition = `${workflowId}:partition`;
      await workflow.start(workflowId, {
        idempotent: true,
        nowMs: now,
        partitionKey: workflowPartition,
        payload: { order: runId },
        runAtMs: now
      });
      await expect(workflow.worker({ batchSize: 1, nowMs: now + 1, partitionKey: workflowPartition, states: ["received"], worker: "ts-sdk-workflow-worker" }).runOnce()).resolves.toMatchObject({
        claimed: 1,
        transitioned: 1
      });
      await expect(workflow.worker({ batchSize: 1, nowMs: now + 2, partitionKey: workflowPartition, states: ["validated"], worker: "ts-sdk-workflow-worker" }).runOnce()).resolves.toMatchObject({
        claimed: 1,
        completed: 1
      });
      await expect(workflow.get(workflowId, { partitionKey: workflowPartition })).resolves.toMatchObject({ state: "completed" });
    } finally {
      await flow.close();
    }
  }, 20_000);
});
