import { integerReply, okResponse, type CommandArgument } from "./internal.js";
import {
  decodeScanTuple,
  decodeSortedSetMembers,
  decodeStreamEntries,
  decodeStreamReads
} from "./store-decoders.js";
import {
  appendXReadStreams,
  arrayOrRest,
  commandArgs,
  concatArgs,
  decode,
  encode,
  encodedArgs,
  keyValueArgs,
  mapArray,
  mapArrayResponse,
  number,
  ownOption,
  rangeScoreOptions,
  requireNonEmpty,
  scanOptions,
  string,
  zaddOptions
} from "./store-utilities.js";
import type {
  IntegerReply,
  RangeLimit,
  CollectionScanOptions,
  SortedSetScanResult,
  StoreCommandClient,
  XReadStream,
  ZAddMember,
  ZAddOptions
} from "./store.js";

export class SortedSetStore {
  constructor(private readonly client: StoreCommandClient) {}

  async zadd(key: string, members: ZAddMember[], options: ZAddOptions = {}): Promise<number> {
    requireNonEmpty(members, "ZADD", "member");
    const args: CommandArgument[] = ["ZADD", key, ...zaddOptions(options)];
    for (let index = 0; index < members.length; index += 1) {
      if (!Object.hasOwn(members, index)) throw new TypeError("ZADD members must be dense");
      const member = members[index];
      if (member == null) throw new TypeError("ZADD members must contain member objects");
      if (!Object.hasOwn(member, "score") || !Object.hasOwn(member, "member")) {
        throw new TypeError("ZADD members require own score and member fields");
      }
      args.push(member.score, encode(this.client.codec, member.member));
    }
    return number(await commandArgs(this.client, args));
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
    const withScores = ownOption(options, "withScores") === true;
    return decodeSortedSetMembers(
      this.client.codec,
      await this.client.command("ZRANGE", key, start, stop, ...(withScores ? ["WITHSCORES"] : [])),
      withScores
    );
  }

  async zrevrange(key: string, start: number, stop: number, options: { withScores?: boolean } = {}): Promise<unknown[]> {
    const withScores = ownOption(options, "withScores") === true;
    return decodeSortedSetMembers(
      this.client.codec,
      await this.client.command("ZREVRANGE", key, start, stop, ...(withScores ? ["WITHSCORES"] : [])),
      withScores
    );
  }

  async zcard(key: string): Promise<number> {
    return number(await this.client.command("ZCARD", key));
  }

  zrem(key: string, member: unknown, ...members: unknown[]): Promise<number>;
  zrem(key: string, ...members: unknown[]): Promise<number> {
    return this.zremMany(key, members);
  }

  async zremMany(key: string, members: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["ZREM", key], this.client.codec, members)));
  }

  async zincrby(key: string, increment: number, member: unknown): Promise<string> {
    return string(await this.client.command("ZINCRBY", key, increment, encode(this.client.codec, member)));
  }

  async zcount(key: string, min: string | number, max: string | number): Promise<number> {
    return number(await this.client.command("ZCOUNT", key, min, max));
  }

  async zpopmin(key: string, count?: number): Promise<unknown[]> {
    return decodeSortedSetMembers(this.client.codec, await this.client.command("ZPOPMIN", key, ...(count == null ? [] : [count])), true);
  }

  async zpopmax(key: string, count?: number): Promise<unknown[]> {
    return decodeSortedSetMembers(this.client.codec, await this.client.command("ZPOPMAX", key, ...(count == null ? [] : [count])), true);
  }

  zrandmember<T = unknown>(key: string): Promise<T | null>;
  zrandmember<T = unknown>(key: string, count: number, withScores?: false): Promise<(T | null)[]>;
  zrandmember(key: string, count: number, withScores: boolean): Promise<unknown[]>;
  async zrandmember(key: string, count?: number, withScores = false): Promise<unknown> {
    if (withScores && count == null) throw new TypeError("ZRANDMEMBER WITHSCORES requires a count");
    const args: CommandArgument[] = ["ZRANDMEMBER", key];
    if (count != null) args.push(count);
    if (withScores) args.push("WITHSCORES");
    const response = await commandArgs(this.client, args);
    if (count == null) return decode(this.client.codec, response);
    if (withScores) return decodeSortedSetMembers(this.client.codec, response, true);
    return mapArray(response, "ZRANDMEMBER", (item) => decode(this.client.codec, item));
  }

  async zmscore(key: string, members: unknown[]): Promise<(string | null)[]> {
    const memberCount = members.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["ZMSCORE", key], this.client.codec, members)),
      memberCount,
      "ZMSCORE",
      (score) => score == null ? null : string(score)
    );
  }

  async zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    options: { withScores?: boolean; limit?: RangeLimit } = {}
  ): Promise<unknown[]> {
    const withScores = ownOption(options, "withScores") === true;
    return decodeSortedSetMembers(
      this.client.codec,
      await this.client.command("ZRANGEBYSCORE", key, min, max, ...rangeScoreOptions(options, withScores)),
      withScores
    );
  }

  async zrevrangebyscore(
    key: string,
    max: string | number,
    min: string | number,
    options: { withScores?: boolean; limit?: RangeLimit } = {}
  ): Promise<unknown[]> {
    const withScores = ownOption(options, "withScores") === true;
    return decodeSortedSetMembers(
      this.client.codec,
      await this.client.command("ZREVRANGEBYSCORE", key, max, min, ...rangeScoreOptions(options, withScores)),
      withScores
    );
  }

  async zscan<T = unknown>(
    key: string,
    cursor: string | number,
    options: CollectionScanOptions = {}
  ): Promise<SortedSetScanResult<T>> {
    return decodeScanTuple(
      await this.client.command("ZSCAN", key, cursor, ...scanOptions(options, "ZSCAN")),
      (items) => decodeSortedSetMembers<T>(this.client.codec, items, true)
    );
  }
}

export class StreamStore {
  constructor(private readonly client: StoreCommandClient) {}

  async xadd(key: string, id: string, fields: Record<string, unknown> | [string, unknown][]): Promise<unknown> {
    return await commandArgs(this.client, keyValueArgs(["XADD", key, id], this.client.codec, fields));
  }

  async xlen(key: string): Promise<number> {
    return number(await this.client.command("XLEN", key));
  }

  async xrange(key: string, start = "-", end = "+", count?: number): Promise<unknown[]> {
    return decodeStreamEntries(this.client.codec, await this.client.command("XRANGE", key, start, end, ...(count == null ? [] : ["COUNT", count])));
  }

  async xrevrange(key: string, end = "+", start = "-", count?: number): Promise<unknown[]> {
    return decodeStreamEntries(this.client.codec, await this.client.command("XREVRANGE", key, end, start, ...(count == null ? [] : ["COUNT", count])));
  }

  async xread(streams: XReadStream[], options: { count?: number; blockMs?: number } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["XREAD"];
    const count = ownOption(options, "count");
    const blockMs = ownOption(options, "blockMs");
    if (count != null) args.push("COUNT", count);
    if (blockMs != null) args.push("BLOCK", blockMs);
    appendXReadStreams(args, streams);
    return decodeStreamReads(this.client.codec, await commandArgs(this.client, args));
  }

  async xtrim(key: string, strategy: "MAXLEN" | "MINID", threshold: string | number, approximate = false): Promise<number> {
    return number(await this.client.command("XTRIM", key, strategy, ...(approximate ? ["~"] : []), threshold));
  }

  xdel(key: string, ids: readonly string[]): Promise<number>;
  xdel(key: string, id: string, ...ids: string[]): Promise<number>;
  async xdel(key: string, ...idsOrArray: (string | readonly string[])[]): Promise<number> {
    const ids = requireNonEmpty(arrayOrRest<string>(idsOrArray), "XDEL", "id");
    return number(await commandArgs(this.client, concatArgs(["XDEL", key], ids)));
  }

  async xinfoStream(key: string): Promise<unknown> {
    return await this.client.command("XINFO", "STREAM", key);
  }

  async xgroupCreate(key: string, group: string, id: string, mkstream = false): Promise<boolean> {
    return okResponse(await this.client.command("XGROUP", "CREATE", key, group, id, ...(mkstream ? ["MKSTREAM"] : [])));
  }

  async xreadgroup(group: string, consumer: string, streams: XReadStream[], options: { count?: number; blockMs?: number; noack?: boolean } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["XREADGROUP", "GROUP", group, consumer];
    const count = ownOption(options, "count");
    const blockMs = ownOption(options, "blockMs");
    const noack = ownOption(options, "noack");
    if (count != null) args.push("COUNT", count);
    if (blockMs != null) args.push("BLOCK", blockMs);
    if (noack === true) args.push("NOACK");
    appendXReadStreams(args, streams);
    return decodeStreamReads(this.client.codec, await commandArgs(this.client, args));
  }

  xack(key: string, group: string, ids: readonly string[]): Promise<number>;
  xack(key: string, group: string, id: string, ...ids: string[]): Promise<number>;
  async xack(key: string, group: string, ...idsOrArray: (string | readonly string[])[]): Promise<number> {
    const ids = requireNonEmpty(arrayOrRest<string>(idsOrArray), "XACK", "id");
    return number(await commandArgs(this.client, concatArgs(["XACK", key, group], ids)));
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

  bitcount(key: string): Promise<number>;
  bitcount(key: string, start: number, end: number, unit?: "BYTE" | "BIT"): Promise<number>;
  async bitcount(key: string, start?: number, end?: number, unit?: "BYTE" | "BIT"): Promise<number> {
    if (start == null) {
      if (unit != null) throw new TypeError("BITCOUNT unit requires start and end");
      if (end != null) throw new TypeError("BITCOUNT end requires start");
      return number(await this.client.command("BITCOUNT", key));
    }
    if (end == null) {
      if (unit != null) throw new TypeError("BITCOUNT unit requires start and end");
      throw new TypeError("BITCOUNT start requires end");
    }
    return number(await this.client.command("BITCOUNT", key, start, end, ...(unit == null ? [] : [unit])));
  }

  bitpos(key: string, bit: 0 | 1): Promise<number>;
  // Kept separate so callers cannot supply positional holes through optional parameters.
  // eslint-disable-next-line @typescript-eslint/unified-signatures
  bitpos(key: string, bit: 0 | 1, start: number): Promise<number>;
  // eslint-disable-next-line @typescript-eslint/unified-signatures
  bitpos(key: string, bit: 0 | 1, start: number, end: number, unit?: "BYTE" | "BIT"): Promise<number>;
  async bitpos(key: string, bit: 0 | 1, start?: number, end?: number, unit?: "BYTE" | "BIT"): Promise<number> {
    if (start == null) {
      if (unit != null) throw new TypeError("BITPOS unit requires start and end");
      if (end != null) throw new TypeError("BITPOS end requires start");
      return number(await this.client.command("BITPOS", key, bit));
    }
    if (end == null) {
      if (unit != null) throw new TypeError("BITPOS unit requires start and end");
      return number(await this.client.command("BITPOS", key, bit, start));
    }
    return number(await this.client.command("BITPOS", key, bit, start, end, ...(unit == null ? [] : [unit])));
  }

  bitop(operation: "AND" | "OR" | "XOR" | "NOT", destination: string, keys: readonly string[]): Promise<number>;
  bitop(operation: "AND" | "OR" | "XOR" | "NOT", destination: string, ...keys: string[]): Promise<number>;
  async bitop(operation: "AND" | "OR" | "XOR" | "NOT", destination: string, ...keysOrArray: (string | readonly string[])[]): Promise<number> {
    const keys = arrayOrRest<string>(keysOrArray);
    if (keys.length === 0) throw new TypeError("BITOP requires at least one source key");
    if (operation === "NOT" && keys.length !== 1) {
      throw new TypeError("BITOP NOT requires exactly one source key");
    }
    return number(await commandArgs(this.client, concatArgs(
      ["BITOP", operation, destination],
      keys
    )));
  }
}

export class HyperLogLogStore {
  constructor(private readonly client: StoreCommandClient) {}

  pfadd(key: string, ...elements: unknown[]): Promise<number> {
    return this.pfaddMany(key, elements);
  }

  async pfaddMany(key: string, elements: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["PFADD", key], this.client.codec, elements, true)));
  }

  pfcount(keys: readonly string[]): Promise<IntegerReply>;
  pfcount(key: string, ...keys: string[]): Promise<IntegerReply>;
  async pfcount(...keysOrArray: (string | readonly string[])[]): Promise<IntegerReply> {
    const keys = requireNonEmpty(arrayOrRest<string>(keysOrArray), "PFCOUNT", "key");
    return integerReply(await commandArgs(this.client, concatArgs(["PFCOUNT"], keys)));
  }

  pfmerge(destination: string, sources: readonly string[]): Promise<boolean>;
  pfmerge(destination: string, source: string, ...sources: string[]): Promise<boolean>;
  async pfmerge(destination: string, ...sourcesOrArray: (string | readonly string[])[]): Promise<boolean> {
    const sources = requireNonEmpty(arrayOrRest<string>(sourcesOrArray), "PFMERGE", "source");
    return okResponse(await commandArgs(this.client, concatArgs(
      ["PFMERGE", destination],
      sources
    )));
  }
}
