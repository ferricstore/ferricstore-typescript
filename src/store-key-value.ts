import { binaryBooleanResponse, integerReply, okResponse, textResponse } from "./internal.js";
import { decodeScanTuple } from "./store-decoders.js";
import {
  arrayOrRest,
  commandArgs,
  concatArgs,
  decode,
  denseArray,
  denseArrayResponse,
  encode,
  getexOptions,
  keyValueArgs,
  mapArray,
  mapArrayResponse,
  number,
  requireNonEmpty,
  scanOptions,
  setOptions,
  string
} from "./store-utilities.js";
import type {
  ExpiryCondition,
  GetExOptions,
  IntegerReply,
  ScanOptions,
  SetOptions,
  StoreCommandClient
} from "./store.js";
import { assertKeyValueCommandSharesSlot } from "./key-slot-validation.js";

export class KeyValueStore {
  constructor(private readonly client: StoreCommandClient) {}

  async get<T = unknown>(key: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("GET", key));
  }

  async set(key: string, value: unknown, options: SetOptions = {}): Promise<unknown> {
    const serializedOptions = setOptions(options);
    const get = serializedOptions.includes("GET");
    const response = await this.client.command("SET", key, encode(this.client.codec, value), ...serializedOptions);
    return get ? decode(this.client.codec, response) : response;
  }

  del(keys: readonly string[]): Promise<number>;
  del(key: string, ...keys: string[]): Promise<number>;
  async del(...keysOrArray: (string | readonly string[])[]): Promise<number> {
    const keys = requireNonEmpty(arrayOrRest<string>(keysOrArray), "DEL", "key");
    return number(await commandArgs(this.client, concatArgs(["DEL"], keys)));
  }

  exists(keys: readonly string[]): Promise<number>;
  exists(key: string, ...keys: string[]): Promise<number>;
  async exists(...keysOrArray: (string | readonly string[])[]): Promise<number> {
    const keys = requireNonEmpty(arrayOrRest<string>(keysOrArray), "EXISTS", "key");
    return number(await commandArgs(this.client, concatArgs(["EXISTS"], keys)));
  }

  async mget<T = unknown>(keys: string[]): Promise<(T | null)[]> {
    requireNonEmpty(keys, "MGET", "key");
    const keyCount = keys.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["MGET"], keys)),
      keyCount,
      "MGET",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async mset(entries: Record<string, unknown> | [string, unknown][]): Promise<boolean> {
    const args = keyValueArgs(["MSET"], this.client.codec, entries);
    assertKeyValueCommandSharesSlot(args, "MSET");
    return okResponse(await commandArgs(this.client, args));
  }

  async msetnx(entries: Record<string, unknown> | [string, unknown][]): Promise<boolean> {
    const args = keyValueArgs(["MSETNX"], this.client.codec, entries);
    assertKeyValueCommandSharesSlot(args, "MSETNX");
    return binaryBooleanResponse(await commandArgs(this.client, args));
  }

  async incr(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("INCR", key));
  }

  async decr(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("DECR", key));
  }

  async incrby(key: string, increment: number | bigint): Promise<IntegerReply> {
    return integerReply(await this.client.command("INCRBY", key, increment));
  }

  async decrby(key: string, decrement: number | bigint): Promise<IntegerReply> {
    return integerReply(await this.client.command("DECRBY", key, decrement));
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
    return binaryBooleanResponse(await this.client.command("SETNX", key, encode(this.client.codec, value)));
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

  async expire(key: string, seconds: number, condition?: ExpiryCondition): Promise<boolean> {
    return binaryBooleanResponse(
      await this.client.command("EXPIRE", key, seconds, ...(condition == null ? [] : [condition]))
    );
  }

  async pexpire(key: string, milliseconds: number, condition?: ExpiryCondition): Promise<boolean> {
    return binaryBooleanResponse(
      await this.client.command("PEXPIRE", key, milliseconds, ...(condition == null ? [] : [condition]))
    );
  }

  async expireat(key: string, unixSeconds: number, condition?: ExpiryCondition): Promise<boolean> {
    return binaryBooleanResponse(
      await this.client.command("EXPIREAT", key, unixSeconds, ...(condition == null ? [] : [condition]))
    );
  }

  async pexpireat(key: string, unixMilliseconds: number, condition?: ExpiryCondition): Promise<boolean> {
    return binaryBooleanResponse(
      await this.client.command("PEXPIREAT", key, unixMilliseconds, ...(condition == null ? [] : [condition]))
    );
  }

  async ttl(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("TTL", key));
  }

  async pttl(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("PTTL", key));
  }

  async persist(key: string): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("PERSIST", key));
  }

  async expiretime(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("EXPIRETIME", key));
  }

  async pexpiretime(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("PEXPIRETIME", key));
  }

  async type(key: string): Promise<string> {
    return string(await this.client.command("TYPE", key));
  }

  unlink(keys: readonly string[]): Promise<number>;
  unlink(key: string, ...keys: string[]): Promise<number>;
  async unlink(...keysOrArray: (string | readonly string[])[]): Promise<number> {
    const keys = requireNonEmpty(arrayOrRest<string>(keysOrArray), "UNLINK", "key");
    return number(await commandArgs(this.client, concatArgs(["UNLINK"], keys)));
  }

  async rename(key: string, newKey: string): Promise<boolean> {
    return okResponse(await this.client.command("RENAME", key, newKey));
  }

  async renamenx(key: string, newKey: string): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("RENAMENX", key, newKey));
  }

  async copy(source: string, destination: string, options: { replace?: boolean } = {}): Promise<boolean> {
    const replace = Object.hasOwn(options, "replace") && options.replace === true;
    return binaryBooleanResponse(
      await this.client.command("COPY", source, destination, ...(replace ? ["REPLACE"] : []))
    );
  }

  async randomkey(): Promise<string | null> {
    const response = await this.client.command("RANDOMKEY");
    return response == null ? null : string(response);
  }

  async keys(pattern: string): Promise<string[]> {
    return mapArray(
      await this.client.command("KEYS", pattern),
      "KEYS",
      (key) => textResponse(key, "KEYS")
    );
  }

  async scan(cursor: string | number, options: ScanOptions = {}): Promise<[string, string[]]> {
    return decodeScanTuple(
      await this.client.command("SCAN", cursor, ...scanOptions(options)),
      (items) => mapArray(items, "SCAN", (key) => textResponse(key, "SCAN"))
    );
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
    return denseArray(await this.client.command("OBJECT", "HELP"), "OBJECT HELP");
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

  async waitAof(
    numLocal: number,
    numReplicas: number,
    timeoutMs: number
  ): Promise<[IntegerReply, IntegerReply]> {
    const response = denseArrayResponse(
      await this.client.command("WAITAOF", numLocal, numReplicas, timeoutMs),
      2,
      "WAITAOF"
    );
    return [integerReply(response[0]), integerReply(response[1])];
  }

  async memoryUsage(key: string): Promise<number | null> {
    const response = await this.client.command("MEMORY", "USAGE", key);
    return response == null ? null : number(response);
  }
}
