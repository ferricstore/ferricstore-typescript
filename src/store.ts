import type { Codec } from "./codecs.js";
import { okResponse, type CommandArgument } from "./internal.js";

export interface StoreCommandClient {
  readonly codec: Codec<unknown>;
  command(...args: CommandArgument[]): Promise<unknown>;
}

export interface SetOptions {
  ex?: number;
  px?: number;
  exat?: number;
  pxat?: number;
  nx?: boolean;
  xx?: boolean;
  get?: boolean;
  keepTtl?: boolean;
}

export interface GetExOptions {
  ex?: number;
  px?: number;
  exat?: number;
  pxat?: number;
  persist?: boolean;
}

export interface ScanOptions {
  match?: string;
  count?: number;
}

export interface ZAddMember {
  score: number;
  member: unknown;
}

export interface ZAddOptions {
  nx?: boolean;
  xx?: boolean;
  gt?: boolean;
  lt?: boolean;
  ch?: boolean;
}

export interface RangeLimit {
  offset: number;
  count: number;
}

export interface XReadStream {
  key: string;
  id: string;
}

export interface GeoMember {
  longitude: number;
  latitude: number;
  member: unknown;
}

export interface GeoAddOptions {
  nx?: boolean;
  xx?: boolean;
  ch?: boolean;
}

export class KeyValueStore {
  constructor(private readonly client: StoreCommandClient) {}

  async get<T = unknown>(key: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("GET", key));
  }

  async set(key: string, value: unknown, options: SetOptions = {}): Promise<unknown> {
    return await this.client.command("SET", key, encode(this.client.codec, value), ...setOptions(options));
  }

  async del(...keys: string[]): Promise<number> {
    return number(await this.client.command("DEL", ...keys));
  }

  async exists(...keys: string[]): Promise<number> {
    return number(await this.client.command("EXISTS", ...keys));
  }

  async mget<T = unknown>(keys: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("MGET", ...keys)).map((item) => decode<T>(this.client.codec, item));
  }

  async mset(entries: Record<string, unknown> | Array<[string, unknown]>): Promise<boolean> {
    return okResponse(await this.client.command("MSET", ...flattenKeyValues(this.client.codec, entries)));
  }

  async msetnx(entries: Record<string, unknown> | Array<[string, unknown]>): Promise<boolean> {
    return number(await this.client.command("MSETNX", ...flattenKeyValues(this.client.codec, entries))) === 1;
  }

  async incr(key: string): Promise<number> {
    return number(await this.client.command("INCR", key));
  }

  async decr(key: string): Promise<number> {
    return number(await this.client.command("DECR", key));
  }

  async incrby(key: string, increment: number): Promise<number> {
    return number(await this.client.command("INCRBY", key, increment));
  }

  async decrby(key: string, decrement: number): Promise<number> {
    return number(await this.client.command("DECRBY", key, decrement));
  }

  async incrbyfloat(key: string, increment: number): Promise<string> {
    return string(await this.client.command("INCRBYFLOAT", key, increment));
  }

  async append(key: string, value: unknown): Promise<number> {
    return number(await this.client.command("APPEND", key, encode(this.client.codec, value)));
  }

  async strlen(key: string): Promise<number> {
    return number(await this.client.command("STRLEN", key));
  }

  async getset<T = unknown>(key: string, value: unknown): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("GETSET", key, encode(this.client.codec, value)));
  }

  async getdel<T = unknown>(key: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("GETDEL", key));
  }

  async getex<T = unknown>(key: string, options: GetExOptions = {}): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("GETEX", key, ...getexOptions(options)));
  }

  async setnx(key: string, value: unknown): Promise<boolean> {
    return number(await this.client.command("SETNX", key, encode(this.client.codec, value))) === 1;
  }

  async setex(key: string, seconds: number, value: unknown): Promise<boolean> {
    return okResponse(await this.client.command("SETEX", key, seconds, encode(this.client.codec, value)));
  }

  async psetex(key: string, milliseconds: number, value: unknown): Promise<boolean> {
    return okResponse(await this.client.command("PSETEX", key, milliseconds, encode(this.client.codec, value)));
  }

  async getrange(key: string, start: number, end: number): Promise<Buffer | string | unknown> {
    return await this.client.command("GETRANGE", key, start, end);
  }

  async setrange(key: string, offset: number, value: unknown): Promise<number> {
    return number(await this.client.command("SETRANGE", key, offset, encode(this.client.codec, value)));
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    return number(await this.client.command("EXPIRE", key, seconds)) === 1;
  }

  async pexpire(key: string, milliseconds: number): Promise<boolean> {
    return number(await this.client.command("PEXPIRE", key, milliseconds)) === 1;
  }

  async expireat(key: string, unixSeconds: number): Promise<boolean> {
    return number(await this.client.command("EXPIREAT", key, unixSeconds)) === 1;
  }

  async pexpireat(key: string, unixMilliseconds: number): Promise<boolean> {
    return number(await this.client.command("PEXPIREAT", key, unixMilliseconds)) === 1;
  }

  async ttl(key: string): Promise<number> {
    return number(await this.client.command("TTL", key));
  }

  async pttl(key: string): Promise<number> {
    return number(await this.client.command("PTTL", key));
  }

  async persist(key: string): Promise<boolean> {
    return number(await this.client.command("PERSIST", key)) === 1;
  }

  async expiretime(key: string): Promise<number> {
    return number(await this.client.command("EXPIRETIME", key));
  }

  async pexpiretime(key: string): Promise<number> {
    return number(await this.client.command("PEXPIRETIME", key));
  }

  async type(key: string): Promise<string> {
    return string(await this.client.command("TYPE", key));
  }

  async unlink(...keys: string[]): Promise<number> {
    return number(await this.client.command("UNLINK", ...keys));
  }

  async rename(key: string, newKey: string): Promise<boolean> {
    return okResponse(await this.client.command("RENAME", key, newKey));
  }

  async renamenx(key: string, newKey: string): Promise<boolean> {
    return number(await this.client.command("RENAMENX", key, newKey)) === 1;
  }

  async copy(source: string, destination: string, options: { replace?: boolean } = {}): Promise<boolean> {
    return number(await this.client.command("COPY", source, destination, ...(options.replace === true ? ["REPLACE"] : []))) === 1;
  }

  async randomkey(): Promise<string | null> {
    const response = await this.client.command("RANDOMKEY");
    return response == null ? null : string(response);
  }

  async keys(pattern: string): Promise<unknown[]> {
    return array(await this.client.command("KEYS", pattern));
  }

  async scan(cursor: string | number, options: ScanOptions = {}): Promise<unknown> {
    return await this.client.command("SCAN", cursor, ...scanOptions(options));
  }

  async dbsize(): Promise<number> {
    return number(await this.client.command("DBSIZE"));
  }

  async flushdb(mode?: "ASYNC" | "SYNC"): Promise<boolean> {
    return okResponse(await this.client.command("FLUSHDB", ...(mode == null ? [] : [mode])));
  }

  async flushall(mode?: "ASYNC" | "SYNC"): Promise<boolean> {
    return okResponse(await this.client.command("FLUSHALL", ...(mode == null ? [] : [mode])));
  }

  async objectEncoding(key: string): Promise<string | null> {
    const response = await this.client.command("OBJECT", "ENCODING", key);
    return response == null ? null : string(response);
  }

  async objectHelp(): Promise<unknown[]> {
    return array(await this.client.command("OBJECT", "HELP"));
  }

  async objectFreq(key: string): Promise<number> {
    return number(await this.client.command("OBJECT", "FREQ", key));
  }

  async objectIdleTime(key: string): Promise<number> {
    return number(await this.client.command("OBJECT", "IDLETIME", key));
  }

  async objectRefcount(key: string): Promise<number> {
    return number(await this.client.command("OBJECT", "REFCOUNT", key));
  }

  async wait(numReplicas: number, timeoutMs: number): Promise<number> {
    return number(await this.client.command("WAIT", numReplicas, timeoutMs));
  }

  async waitAof(numLocal: number, numReplicas: number, timeoutMs: number): Promise<unknown[]> {
    return array(await this.client.command("WAITAOF", numLocal, numReplicas, timeoutMs));
  }

  async memoryUsage(key: string): Promise<number> {
    return number(await this.client.command("MEMORY", "USAGE", key));
  }
}

export class HashStore {
  constructor(private readonly client: StoreCommandClient) {}

  async hset(key: string, entries: Record<string, unknown> | Array<[string, unknown]>): Promise<number> {
    return number(await this.client.command("HSET", key, ...flattenKeyValues(this.client.codec, entries)));
  }

  async hget<T = unknown>(key: string, field: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("HGET", key, field));
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return number(await this.client.command("HDEL", key, ...fields));
  }

  async hmget<T = unknown>(key: string, fields: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("HMGET", key, ...fields)).map((item) => decode<T>(this.client.codec, item));
  }

  async hgetall(key: string): Promise<unknown> {
    return await this.client.command("HGETALL", key);
  }

  async hexists(key: string, field: string): Promise<boolean> {
    return number(await this.client.command("HEXISTS", key, field)) === 1;
  }

  async hkeys(key: string): Promise<unknown[]> {
    return array(await this.client.command("HKEYS", key));
  }

  async hvals<T = unknown>(key: string): Promise<Array<T | null>> {
    return array(await this.client.command("HVALS", key)).map((item) => decode<T>(this.client.codec, item));
  }

  async hlen(key: string): Promise<number> {
    return number(await this.client.command("HLEN", key));
  }

  async hincrby(key: string, field: string, increment: number): Promise<number> {
    return number(await this.client.command("HINCRBY", key, field, increment));
  }

  async hincrbyfloat(key: string, field: string, increment: number): Promise<string> {
    return string(await this.client.command("HINCRBYFLOAT", key, field, increment));
  }

  async hsetnx(key: string, field: string, value: unknown): Promise<boolean> {
    return number(await this.client.command("HSETNX", key, field, encode(this.client.codec, value))) === 1;
  }

  async hstrlen(key: string, field: string): Promise<number> {
    return number(await this.client.command("HSTRLEN", key, field));
  }

  async hrandfield(key: string, count?: number, withValues = false): Promise<unknown> {
    const args: CommandArgument[] = ["HRANDFIELD", key];
    if (count != null) args.push(count);
    if (withValues) args.push("WITHVALUES");
    return await this.client.command(...args);
  }

  async hscan(key: string, cursor: string | number, options: ScanOptions = {}): Promise<unknown> {
    return await this.client.command("HSCAN", key, cursor, ...scanOptions(options));
  }

  async hexpire(key: string, seconds: number, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HEXPIRE", key, seconds, "FIELDS", fields.length, ...fields));
  }

  async httl(key: string, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HTTL", key, "FIELDS", fields.length, ...fields));
  }

  async hpersist(key: string, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HPERSIST", key, "FIELDS", fields.length, ...fields));
  }

  async hpexpire(key: string, milliseconds: number, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HPEXPIRE", key, milliseconds, "FIELDS", fields.length, ...fields));
  }

  async hpttl(key: string, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HPTTL", key, "FIELDS", fields.length, ...fields));
  }

  async hexpiretime(key: string, fields: string[]): Promise<unknown[]> {
    return array(await this.client.command("HEXPIRETIME", key, "FIELDS", fields.length, ...fields));
  }

  async hgetdel<T = unknown>(key: string, fields: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("HGETDEL", key, "FIELDS", fields.length, ...fields)).map((item) =>
      decode<T>(this.client.codec, item)
    );
  }

  async hgetex<T = unknown>(key: string, fields: string[], options: GetExOptions = {}): Promise<Array<T | null>> {
    return array(await this.client.command("HGETEX", key, ...getexOptions(options), "FIELDS", fields.length, ...fields)).map((item) =>
      decode<T>(this.client.codec, item)
    );
  }

  async hsetex(key: string, seconds: number, entries: Record<string, unknown> | Array<[string, unknown]>): Promise<number> {
    return number(await this.client.command("HSETEX", key, seconds, ...flattenKeyValues(this.client.codec, entries)));
  }
}

export class ListStore {
  constructor(private readonly client: StoreCommandClient) {}

  async lpush(key: string, ...elements: unknown[]): Promise<number> {
    return number(await this.client.command("LPUSH", key, ...elements.map((item) => encode(this.client.codec, item))));
  }

  async rpush(key: string, ...elements: unknown[]): Promise<number> {
    return number(await this.client.command("RPUSH", key, ...elements.map((item) => encode(this.client.codec, item))));
  }

  async lpop<T = unknown>(key: string, count?: number): Promise<T | Array<T | null> | null> {
    const response = await this.client.command("LPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response) ? response.map((item) => decode<T>(this.client.codec, item)) : decode<T>(this.client.codec, response);
  }

  async rpop<T = unknown>(key: string, count?: number): Promise<T | Array<T | null> | null> {
    const response = await this.client.command("RPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response) ? response.map((item) => decode<T>(this.client.codec, item)) : decode<T>(this.client.codec, response);
  }

  async lrange<T = unknown>(key: string, start: number, stop: number): Promise<Array<T | null>> {
    return array(await this.client.command("LRANGE", key, start, stop)).map((item) => decode<T>(this.client.codec, item));
  }

  async llen(key: string): Promise<number> {
    return number(await this.client.command("LLEN", key));
  }

  async lindex<T = unknown>(key: string, index: number): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("LINDEX", key, index));
  }

  async lset(key: string, index: number, element: unknown): Promise<boolean> {
    return okResponse(await this.client.command("LSET", key, index, encode(this.client.codec, element)));
  }

  async lrem(key: string, count: number, element: unknown): Promise<number> {
    return number(await this.client.command("LREM", key, count, encode(this.client.codec, element)));
  }

  async ltrim(key: string, start: number, stop: number): Promise<boolean> {
    return okResponse(await this.client.command("LTRIM", key, start, stop));
  }

  async lpos(key: string, element: unknown, options: { rank?: number; count?: number; maxlen?: number } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["LPOS", key, encode(this.client.codec, element)];
    if (options.rank != null) args.push("RANK", options.rank);
    if (options.count != null) args.push("COUNT", options.count);
    if (options.maxlen != null) args.push("MAXLEN", options.maxlen);
    return await this.client.command(...args);
  }

  async linsert(key: string, where: "BEFORE" | "AFTER", pivot: unknown, element: unknown): Promise<number> {
    return number(await this.client.command("LINSERT", key, where, encode(this.client.codec, pivot), encode(this.client.codec, element)));
  }

  async lmove(source: string, destination: string, from: "LEFT" | "RIGHT", to: "LEFT" | "RIGHT"): Promise<unknown> {
    return await this.client.command("LMOVE", source, destination, from, to);
  }

  async rpoplpush(source: string, destination: string): Promise<unknown> {
    return await this.client.command("RPOPLPUSH", source, destination);
  }

  async lpushx(key: string, ...elements: unknown[]): Promise<number> {
    return number(await this.client.command("LPUSHX", key, ...elements.map((item) => encode(this.client.codec, item))));
  }

  async rpushx(key: string, ...elements: unknown[]): Promise<number> {
    return number(await this.client.command("RPUSHX", key, ...elements.map((item) => encode(this.client.codec, item))));
  }

  async blpop(keys: string[], timeoutSeconds: number): Promise<unknown> {
    return await this.client.command("BLPOP", ...keys, timeoutSeconds);
  }

  async brpop(keys: string[], timeoutSeconds: number): Promise<unknown> {
    return await this.client.command("BRPOP", ...keys, timeoutSeconds);
  }

  async blmove(source: string, destination: string, from: "LEFT" | "RIGHT", to: "LEFT" | "RIGHT", timeoutSeconds: number): Promise<unknown> {
    return await this.client.command("BLMOVE", source, destination, from, to, timeoutSeconds);
  }

  async blmpop(timeoutSeconds: number, keys: string[], from: "LEFT" | "RIGHT", options: { count?: number } = {}): Promise<unknown> {
    return await this.client.command("BLMPOP", timeoutSeconds, keys.length, ...keys, from, ...(options.count == null ? [] : ["COUNT", options.count]));
  }
}

export class SetStore {
  constructor(private readonly client: StoreCommandClient) {}

  async sadd(key: string, ...members: unknown[]): Promise<number> {
    return number(await this.client.command("SADD", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async srem(key: string, ...members: unknown[]): Promise<number> {
    return number(await this.client.command("SREM", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async smembers<T = unknown>(key: string): Promise<Array<T | null>> {
    return array(await this.client.command("SMEMBERS", key)).map((item) => decode<T>(this.client.codec, item));
  }

  async sismember(key: string, member: unknown): Promise<boolean> {
    return number(await this.client.command("SISMEMBER", key, encode(this.client.codec, member))) === 1;
  }

  async smismember(key: string, members: unknown[]): Promise<unknown[]> {
    return array(await this.client.command("SMISMEMBER", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async scard(key: string): Promise<number> {
    return number(await this.client.command("SCARD", key));
  }

  async srandmember<T = unknown>(key: string, count?: number): Promise<T | Array<T | null> | null> {
    const response = await this.client.command("SRANDMEMBER", key, ...(count == null ? [] : [count]));
    return Array.isArray(response) ? response.map((item) => decode<T>(this.client.codec, item)) : decode<T>(this.client.codec, response);
  }

  async spop<T = unknown>(key: string, count?: number): Promise<T | Array<T | null> | null> {
    const response = await this.client.command("SPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response) ? response.map((item) => decode<T>(this.client.codec, item)) : decode<T>(this.client.codec, response);
  }

  async sdiff<T = unknown>(keys: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("SDIFF", ...keys)).map((item) => decode<T>(this.client.codec, item));
  }

  async sinter<T = unknown>(keys: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("SINTER", ...keys)).map((item) => decode<T>(this.client.codec, item));
  }

  async sunion<T = unknown>(keys: string[]): Promise<Array<T | null>> {
    return array(await this.client.command("SUNION", ...keys)).map((item) => decode<T>(this.client.codec, item));
  }

  async sdiffstore(destination: string, keys: string[]): Promise<number> {
    return number(await this.client.command("SDIFFSTORE", destination, ...keys));
  }

  async sinterstore(destination: string, keys: string[]): Promise<number> {
    return number(await this.client.command("SINTERSTORE", destination, ...keys));
  }

  async sunionstore(destination: string, keys: string[]): Promise<number> {
    return number(await this.client.command("SUNIONSTORE", destination, ...keys));
  }

  async sintercard(keys: string[], limit?: number): Promise<number> {
    return number(await this.client.command("SINTERCARD", keys.length, ...keys, ...(limit == null ? [] : ["LIMIT", limit])));
  }

  async smove(source: string, destination: string, member: unknown): Promise<boolean> {
    return number(await this.client.command("SMOVE", source, destination, encode(this.client.codec, member))) === 1;
  }

  async sscan(key: string, cursor: string | number, options: ScanOptions = {}): Promise<unknown> {
    return await this.client.command("SSCAN", key, cursor, ...scanOptions(options));
  }
}

export class SortedSetStore {
  constructor(private readonly client: StoreCommandClient) {}

  async zadd(key: string, members: ZAddMember[], options: ZAddOptions = {}): Promise<number> {
    const args: CommandArgument[] = ["ZADD", key, ...zaddOptions(options)];
    for (const member of members) {
      args.push(member.score, encode(this.client.codec, member.member));
    }
    return number(await this.client.command(...args));
  }

  async zscore(key: string, member: unknown): Promise<string | null> {
    const response = await this.client.command("ZSCORE", key, encode(this.client.codec, member));
    return response == null ? null : string(response);
  }

  async zrank(key: string, member: unknown): Promise<number | null> {
    const response = await this.client.command("ZRANK", key, encode(this.client.codec, member));
    return response == null ? null : number(response);
  }

  async zrevrank(key: string, member: unknown): Promise<number | null> {
    const response = await this.client.command("ZREVRANK", key, encode(this.client.codec, member));
    return response == null ? null : number(response);
  }

  async zrange(key: string, start: number, stop: number, options: { withScores?: boolean } = {}): Promise<unknown[]> {
    return array(await this.client.command("ZRANGE", key, start, stop, ...(options.withScores === true ? ["WITHSCORES"] : [])));
  }

  async zrevrange(key: string, start: number, stop: number, options: { withScores?: boolean } = {}): Promise<unknown[]> {
    return array(await this.client.command("ZREVRANGE", key, start, stop, ...(options.withScores === true ? ["WITHSCORES"] : [])));
  }

  async zcard(key: string): Promise<number> {
    return number(await this.client.command("ZCARD", key));
  }

  async zrem(key: string, ...members: unknown[]): Promise<number> {
    return number(await this.client.command("ZREM", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async zincrby(key: string, increment: number, member: unknown): Promise<string> {
    return string(await this.client.command("ZINCRBY", key, increment, encode(this.client.codec, member)));
  }

  async zcount(key: string, min: string | number, max: string | number): Promise<number> {
    return number(await this.client.command("ZCOUNT", key, min, max));
  }

  async zpopmin(key: string, count?: number): Promise<unknown[]> {
    return array(await this.client.command("ZPOPMIN", key, ...(count == null ? [] : [count])));
  }

  async zpopmax(key: string, count?: number): Promise<unknown[]> {
    return array(await this.client.command("ZPOPMAX", key, ...(count == null ? [] : [count])));
  }

  async zrandmember(key: string, count?: number, withScores = false): Promise<unknown> {
    const args: CommandArgument[] = ["ZRANDMEMBER", key];
    if (count != null) args.push(count);
    if (withScores) args.push("WITHSCORES");
    return await this.client.command(...args);
  }

  async zmscore(key: string, members: unknown[]): Promise<unknown[]> {
    return array(await this.client.command("ZMSCORE", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async zrangebyscore(key: string, min: string | number, max: string | number, options: { withScores?: boolean; limit?: RangeLimit } = {}): Promise<unknown[]> {
    return array(await this.client.command("ZRANGEBYSCORE", key, min, max, ...rangeScoreOptions(options)));
  }

  async zrevrangebyscore(key: string, max: string | number, min: string | number, options: { withScores?: boolean; limit?: RangeLimit } = {}): Promise<unknown[]> {
    return array(await this.client.command("ZREVRANGEBYSCORE", key, max, min, ...rangeScoreOptions(options)));
  }

  async zscan(key: string, cursor: string | number, options: ScanOptions = {}): Promise<unknown> {
    return await this.client.command("ZSCAN", key, cursor, ...scanOptions(options));
  }
}

export class StreamStore {
  constructor(private readonly client: StoreCommandClient) {}

  async xadd(key: string, id: string, fields: Record<string, unknown> | Array<[string, unknown]>): Promise<unknown> {
    return await this.client.command("XADD", key, id, ...flattenKeyValues(this.client.codec, fields));
  }

  async xlen(key: string): Promise<number> {
    return number(await this.client.command("XLEN", key));
  }

  async xrange(key: string, start = "-", end = "+", count?: number): Promise<unknown[]> {
    return array(await this.client.command("XRANGE", key, start, end, ...(count == null ? [] : ["COUNT", count])));
  }

  async xrevrange(key: string, end = "+", start = "-", count?: number): Promise<unknown[]> {
    return array(await this.client.command("XREVRANGE", key, end, start, ...(count == null ? [] : ["COUNT", count])));
  }

  async xread(streams: XReadStream[], options: { count?: number; blockMs?: number } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["XREAD"];
    if (options.count != null) args.push("COUNT", options.count);
    if (options.blockMs != null) args.push("BLOCK", options.blockMs);
    args.push("STREAMS", ...streams.map((stream) => stream.key), ...streams.map((stream) => stream.id));
    return await this.client.command(...args);
  }

  async xtrim(key: string, strategy: "MAXLEN" | "MINID", threshold: string | number, approximate = false): Promise<number> {
    return number(await this.client.command("XTRIM", key, strategy, ...(approximate ? ["~"] : []), threshold));
  }

  async xdel(key: string, ...ids: string[]): Promise<number> {
    return number(await this.client.command("XDEL", key, ...ids));
  }

  async xinfoStream(key: string): Promise<unknown> {
    return await this.client.command("XINFO", "STREAM", key);
  }

  async xgroupCreate(key: string, group: string, id: string, mkstream = false): Promise<boolean> {
    return okResponse(await this.client.command("XGROUP", "CREATE", key, group, id, ...(mkstream ? ["MKSTREAM"] : [])));
  }

  async xreadgroup(group: string, consumer: string, streams: XReadStream[], options: { count?: number; blockMs?: number; noack?: boolean } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["XREADGROUP", "GROUP", group, consumer];
    if (options.count != null) args.push("COUNT", options.count);
    if (options.blockMs != null) args.push("BLOCK", options.blockMs);
    if (options.noack === true) args.push("NOACK");
    args.push("STREAMS", ...streams.map((stream) => stream.key), ...streams.map((stream) => stream.id));
    return await this.client.command(...args);
  }

  async xack(key: string, group: string, ...ids: string[]): Promise<number> {
    return number(await this.client.command("XACK", key, group, ...ids));
  }
}

export class BitmapStore {
  constructor(private readonly client: StoreCommandClient) {}

  async setbit(key: string, offset: number, value: 0 | 1): Promise<number> {
    return number(await this.client.command("SETBIT", key, offset, value));
  }

  async getbit(key: string, offset: number): Promise<number> {
    return number(await this.client.command("GETBIT", key, offset));
  }

  async bitcount(key: string, start?: number, end?: number, unit?: "BYTE" | "BIT"): Promise<number> {
    return number(await this.client.command("BITCOUNT", key, ...(start == null ? [] : end == null ? [start] : [start, end, ...(unit == null ? [] : [unit])])));
  }

  async bitpos(key: string, bit: 0 | 1, start?: number, end?: number, unit?: "BYTE" | "BIT"): Promise<number> {
    return number(await this.client.command("BITPOS", key, bit, ...(start == null ? [] : end == null ? [start] : [start, end, ...(unit == null ? [] : [unit])])));
  }

  async bitop(operation: "AND" | "OR" | "XOR" | "NOT", destination: string, ...keys: string[]): Promise<number> {
    return number(await this.client.command("BITOP", operation, destination, ...keys));
  }
}

export class HyperLogLogStore {
  constructor(private readonly client: StoreCommandClient) {}

  async pfadd(key: string, ...elements: unknown[]): Promise<number> {
    return number(await this.client.command("PFADD", key, ...elements.map((item) => encode(this.client.codec, item))));
  }

  async pfcount(...keys: string[]): Promise<number> {
    return number(await this.client.command("PFCOUNT", ...keys));
  }

  async pfmerge(destination: string, ...sources: string[]): Promise<boolean> {
    return okResponse(await this.client.command("PFMERGE", destination, ...sources));
  }
}

export class GeoStore {
  constructor(private readonly client: StoreCommandClient) {}

  async geoadd(key: string, members: GeoMember[], options: GeoAddOptions = {}): Promise<number> {
    const args: CommandArgument[] = ["GEOADD", key, ...geoAddOptions(options)];
    for (const item of members) {
      args.push(item.longitude, item.latitude, encode(this.client.codec, item.member));
    }
    return number(await this.client.command(...args));
  }

  async geopos(key: string, ...members: unknown[]): Promise<unknown[]> {
    return array(await this.client.command("GEOPOS", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async geodist(key: string, member1: unknown, member2: unknown, unit?: "m" | "km" | "mi" | "ft"): Promise<string | null> {
    const response = await this.client.command("GEODIST", key, encode(this.client.codec, member1), encode(this.client.codec, member2), ...(unit == null ? [] : [unit]));
    return response == null ? null : string(response);
  }

  async geohash(key: string, ...members: unknown[]): Promise<unknown[]> {
    return array(await this.client.command("GEOHASH", key, ...members.map((item) => encode(this.client.codec, item))));
  }

  async geosearch(key: string, args: CommandArgument[]): Promise<unknown[]> {
    return array(await this.client.command("GEOSEARCH", key, ...args));
  }

  async geosearchstore(destination: string, source: string, args: CommandArgument[]): Promise<number> {
    return number(await this.client.command("GEOSEARCHSTORE", destination, source, ...args));
  }
}

function encode(codec: Codec<unknown>, value: unknown): Buffer {
  return codec.encode(value);
}

function decode<T>(codec: Codec<unknown>, value: unknown): T | null {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return codec.decode(value) as T | null;
  if (value instanceof Uint8Array) return codec.decode(Buffer.from(value)) as T | null;
  return value as T;
}

function number(value: unknown): number {
  return Number(value);
}

function string(value: unknown): string {
  return Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function flattenKeyValues(codec: Codec<unknown>, entries: Record<string, unknown> | Array<[string, unknown]>): CommandArgument[] {
  const pairs = Array.isArray(entries) ? entries : Object.entries(entries);
  return pairs.flatMap(([key, value]) => [key, codec.encode(value)]);
}

function setOptions(options: SetOptions): CommandArgument[] {
  const args: CommandArgument[] = [];
  if (options.ex != null) args.push("EX", options.ex);
  if (options.px != null) args.push("PX", options.px);
  if (options.exat != null) args.push("EXAT", options.exat);
  if (options.pxat != null) args.push("PXAT", options.pxat);
  if (options.nx === true) args.push("NX");
  if (options.xx === true) args.push("XX");
  if (options.get === true) args.push("GET");
  if (options.keepTtl === true) args.push("KEEPTTL");
  return args;
}

function getexOptions(options: GetExOptions): CommandArgument[] {
  if (options.persist === true) return ["PERSIST"];
  if (options.ex != null) return ["EX", options.ex];
  if (options.px != null) return ["PX", options.px];
  if (options.exat != null) return ["EXAT", options.exat];
  if (options.pxat != null) return ["PXAT", options.pxat];
  return [];
}

function scanOptions(options: ScanOptions): CommandArgument[] {
  const args: CommandArgument[] = [];
  if (options.match != null) args.push("MATCH", options.match);
  if (options.count != null) args.push("COUNT", options.count);
  return args;
}

function zaddOptions(options: ZAddOptions): CommandArgument[] {
  const args: CommandArgument[] = [];
  if (options.nx === true) args.push("NX");
  if (options.xx === true) args.push("XX");
  if (options.gt === true) args.push("GT");
  if (options.lt === true) args.push("LT");
  if (options.ch === true) args.push("CH");
  return args;
}

function geoAddOptions(options: GeoAddOptions): CommandArgument[] {
  const args: CommandArgument[] = [];
  if (options.nx === true) args.push("NX");
  if (options.xx === true) args.push("XX");
  if (options.ch === true) args.push("CH");
  return args;
}

function rangeScoreOptions(options: { withScores?: boolean; limit?: RangeLimit }): CommandArgument[] {
  const args: CommandArgument[] = [];
  if (options.withScores === true) args.push("WITHSCORES");
  if (options.limit != null) args.push("LIMIT", options.limit.offset, options.limit.count);
  return args;
}
