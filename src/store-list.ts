import { okResponse, type CommandArgument } from "./internal.js";
import { decodeBlockingListMPop, decodeBlockingListPop } from "./store-decoders.js";
import type { StoreCommandClient } from "./store.js";
import {
  commandArgs,
  concatArgs,
  decode,
  encode,
  encodedArgs,
  mapArray,
  number,
  ownOption,
  requireNonEmpty
} from "./store-utilities.js";

export class ListStore {
  constructor(private readonly client: StoreCommandClient) {}

  lpush(key: string, element: unknown, ...elements: unknown[]): Promise<number>;
  lpush(key: string, ...elements: unknown[]): Promise<number> {
    return this.lpushMany(key, elements);
  }

  async lpushMany(key: string, elements: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["LPUSH", key], this.client.codec, elements)));
  }

  rpush(key: string, element: unknown, ...elements: unknown[]): Promise<number>;
  rpush(key: string, ...elements: unknown[]): Promise<number> {
    return this.rpushMany(key, elements);
  }

  async rpushMany(key: string, elements: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["RPUSH", key], this.client.codec, elements)));
  }

  async lpop<T = unknown>(key: string, count?: number): Promise<T | (T | null)[] | null> {
    const response = await this.client.command("LPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response)
      ? mapArray(response, "LPOP", (item) => decode<T>(this.client.codec, item))
      : decode<T>(this.client.codec, response);
  }

  async rpop<T = unknown>(key: string, count?: number): Promise<T | (T | null)[] | null> {
    const response = await this.client.command("RPOP", key, ...(count == null ? [] : [count]));
    return Array.isArray(response)
      ? mapArray(response, "RPOP", (item) => decode<T>(this.client.codec, item))
      : decode<T>(this.client.codec, response);
  }

  async lrange<T = unknown>(key: string, start: number, stop: number): Promise<(T | null)[]> {
    return mapArray(
      await this.client.command("LRANGE", key, start, stop),
      "LRANGE",
      (item) => decode<T>(this.client.codec, item)
    );
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

  async lpos(
    key: string,
    element: unknown,
    options: { rank?: number; count?: number; maxlen?: number } = {}
  ): Promise<number | number[] | null> {
    const args: CommandArgument[] = ["LPOS", key, encode(this.client.codec, element)];
    const rank = ownOption(options, "rank");
    const count = ownOption(options, "count");
    const maxlen = ownOption(options, "maxlen");
    if (rank != null) args.push("RANK", rank);
    if (count != null) args.push("COUNT", count);
    if (maxlen != null) args.push("MAXLEN", maxlen);
    const response = await commandArgs(this.client, args);
    return count == null
      ? response == null ? null : number(response)
      : mapArray(response, "LPOS", number);
  }

  async linsert(
    key: string,
    where: "BEFORE" | "AFTER",
    pivot: unknown,
    element: unknown
  ): Promise<number> {
    return number(await this.client.command(
      "LINSERT",
      key,
      where,
      encode(this.client.codec, pivot),
      encode(this.client.codec, element)
    ));
  }

  async lmove<T = unknown>(
    source: string,
    destination: string,
    from: "LEFT" | "RIGHT",
    to: "LEFT" | "RIGHT"
  ): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("LMOVE", source, destination, from, to));
  }

  async rpoplpush<T = unknown>(source: string, destination: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("RPOPLPUSH", source, destination));
  }

  lpushx(key: string, element: unknown, ...elements: unknown[]): Promise<number>;
  lpushx(key: string, ...elements: unknown[]): Promise<number> {
    return this.lpushxMany(key, elements);
  }

  async lpushxMany(key: string, elements: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["LPUSHX", key], this.client.codec, elements)));
  }

  rpushx(key: string, element: unknown, ...elements: unknown[]): Promise<number>;
  rpushx(key: string, ...elements: unknown[]): Promise<number> {
    return this.rpushxMany(key, elements);
  }

  async rpushxMany(key: string, elements: readonly unknown[]): Promise<number> {
    return number(await commandArgs(this.client, encodedArgs(["RPUSHX", key], this.client.codec, elements)));
  }

  async blpop(keys: string[], timeoutSeconds: number): Promise<unknown> {
    requireNonEmpty(keys, "BLPOP", "key");
    return decodeBlockingListPop(
      this.client.codec,
      await commandArgs(this.client, concatArgs(["BLPOP"], keys, [timeoutSeconds]))
    );
  }

  async brpop(keys: string[], timeoutSeconds: number): Promise<unknown> {
    requireNonEmpty(keys, "BRPOP", "key");
    return decodeBlockingListPop(
      this.client.codec,
      await commandArgs(this.client, concatArgs(["BRPOP"], keys, [timeoutSeconds]))
    );
  }

  async blmove<T = unknown>(
    source: string,
    destination: string,
    from: "LEFT" | "RIGHT",
    to: "LEFT" | "RIGHT",
    timeoutSeconds: number
  ): Promise<T | null> {
    return decode<T>(
      this.client.codec,
      await this.client.command("BLMOVE", source, destination, from, to, timeoutSeconds)
    );
  }

  async blmpop(
    timeoutSeconds: number,
    keys: string[],
    from: "LEFT" | "RIGHT",
    options: { count?: number } = {}
  ): Promise<unknown> {
    requireNonEmpty(keys, "BLMPOP", "key");
    const count = ownOption(options, "count");
    return decodeBlockingListMPop(
      this.client.codec,
      await commandArgs(this.client, concatArgs(
        ["BLMPOP", timeoutSeconds, keys.length],
        keys,
        [from],
        count == null ? [] : ["COUNT", count]
      ))
    );
  }
}
