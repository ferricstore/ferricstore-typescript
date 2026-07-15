import { binaryBooleanResponse, booleanResponse } from "./internal.js";
import { decodeScanTuple } from "./store-decoders.js";
import type { CollectionScanOptions, SetScanResult, StoreCommandClient } from "./store.js";
import {
  binaryInteger,
  commandArgs,
  concatArgs,
  decode,
  encode,
  encodedArgs,
  mapArray,
  mapArrayResponse,
  number,
  requireNonEmpty,
  scanOptions
} from "./store-utilities.js";

export class SetStore {
  constructor(private readonly client: StoreCommandClient) {}

  sadd(key: string, member: unknown, ...members: unknown[]): Promise<number>;
  sadd(key: string, ...members: unknown[]): Promise<number> {
    return this.saddMany(key, members);
  }

  async saddMany(key: string, members: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["SADD", key], this.client.codec, members)));
  }

  srem(key: string, member: unknown, ...members: unknown[]): Promise<number>;
  srem(key: string, ...members: unknown[]): Promise<number> {
    return this.sremMany(key, members);
  }

  async sremMany(key: string, members: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["SREM", key], this.client.codec, members)));
  }

  async smembers<T = unknown>(key: string): Promise<(T | null)[]> {
    return mapArray(
      await this.client.command("SMEMBERS", key),
      "SMEMBERS",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async sismember(key: string, member: unknown): Promise<boolean> {
    const response = await this.client.command("SISMEMBER", key, encode(this.client.codec, member));
    if (response == null) throw new TypeError("SISMEMBER returned an invalid boolean response");
    return booleanResponse(response);
  }

  async smismember(key: string, members: unknown[]): Promise<number[]> {
    const memberCount = members.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["SMISMEMBER", key], this.client.codec, members)),
      memberCount,
      "SMISMEMBER",
      binaryInteger
    );
  }

  async scard(key: string): Promise<number> {
    return number(await this.client.command("SCARD", key));
  }

  async srandmember<T = unknown>(key: string, count?: number): Promise<T | (T | null)[] | null> {
    const response = await this.client.command("SRANDMEMBER", key, ...(count == null ? [] : [count]));
    return Array.isArray(response)
      ? mapArray(response, "SRANDMEMBER", (item) => decode<T>(this.client.codec, item))
      : decode<T>(this.client.codec, response);
  }

  async spop<T = unknown>(key: string, count?: number): Promise<T | (T | null)[] | null> {
    const response = await this.client.command("SPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response)
      ? mapArray(response, "SPOP", (item) => decode<T>(this.client.codec, item))
      : decode<T>(this.client.codec, response);
  }

  async sdiff<T = unknown>(keys: string[]): Promise<(T | null)[]> {
    requireNonEmpty(keys, "SDIFF", "key");
    return mapArray(
      await commandArgs(this.client, concatArgs(["SDIFF"], keys)),
      "SDIFF",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async sinter<T = unknown>(keys: string[]): Promise<(T | null)[]> {
    requireNonEmpty(keys, "SINTER", "key");
    return mapArray(
      await commandArgs(this.client, concatArgs(["SINTER"], keys)),
      "SINTER",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async sunion<T = unknown>(keys: string[]): Promise<(T | null)[]> {
    requireNonEmpty(keys, "SUNION", "key");
    return mapArray(
      await commandArgs(this.client, concatArgs(["SUNION"], keys)),
      "SUNION",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async sdiffstore(destination: string, keys: string[]): Promise<number> {
    requireNonEmpty(keys, "SDIFFSTORE", "key");
    return number(await commandArgs(this.client, concatArgs(["SDIFFSTORE", destination], keys)));
  }

  async sinterstore(destination: string, keys: string[]): Promise<number> {
    requireNonEmpty(keys, "SINTERSTORE", "key");
    return number(await commandArgs(this.client, concatArgs(["SINTERSTORE", destination], keys)));
  }

  async sunionstore(destination: string, keys: string[]): Promise<number> {
    requireNonEmpty(keys, "SUNIONSTORE", "key");
    return number(await commandArgs(this.client, concatArgs(["SUNIONSTORE", destination], keys)));
  }

  async sintercard(keys: string[], limit?: number): Promise<number> {
    requireNonEmpty(keys, "SINTERCARD", "key");
    return number(await commandArgs(this.client, concatArgs(
      ["SINTERCARD", keys.length],
      keys,
      limit == null ? [] : ["LIMIT", limit]
    )));
  }

  async smove(source: string, destination: string, member: unknown): Promise<boolean> {
    return binaryBooleanResponse(
      await this.client.command("SMOVE", source, destination, encode(this.client.codec, member))
    );
  }

  async sscan<T = unknown>(
    key: string,
    cursor: string | number,
    options: CollectionScanOptions = {}
  ): Promise<SetScanResult<T>> {
    return decodeScanTuple(
      await this.client.command("SSCAN", key, cursor, ...scanOptions(options, "SSCAN")),
      (items) => mapArray(items, "SSCAN", (item) => decode<T>(this.client.codec, item))
    );
  }
}
