import type { Codec } from "./codecs.js";
import { binaryBooleanResponse, integerReply, type CommandArgument } from "./internal.js";
import {
  decodeHashFlatPairs,
  decodeHashRecord,
  decodeScanTuple,
  textToken
} from "./store-decoders.js";
import {
  arrayOrRest,
  commandArgs,
  concatArgs,
  decode,
  encode,
  getexOptions,
  keyValueArgs,
  mapArray,
  mapArrayResponse,
  number,
  requireNonEmpty,
  scanOptions,
  string
} from "./store-utilities.js";

export { KeyValueStore } from "./store-key-value.js";
export { GeoStore } from "./store-geo.js";
export { ListStore } from "./store-list.js";
export { BitmapStore, HyperLogLogStore, SortedSetStore, StreamStore } from "./store-extra-collections.js";
export { SetStore } from "./store-set.js";

export interface StoreCommandClient {
  readonly codec: Codec;
  command(...args: CommandArgument[]): Promise<unknown>;
  commandArgs?(args: readonly CommandArgument[]): Promise<unknown>;
}

/** An exact integer reply: safe values are numbers and larger int64 values are bigints. */
export type IntegerReply = number | bigint;

/** Flat field/value pairs returned with a scan cursor. */
export type HashScanResult<T = unknown> = [cursor: string, entries: (string | T | null)[]];

/** Decoded set members returned with a scan cursor. */
export type SetScanResult<T = unknown> = [cursor: string, members: (T | null)[]];

/** Flat member/score pairs returned with a scan cursor. */
export type SortedSetScanResult<T = unknown> = [cursor: string, entries: (T | null | string)[]];

/** SET accepts one condition and at most one expiry mode. */
export type SetOptions = {
  get?: boolean;
} & (
  | { nx?: boolean; xx?: false }
  | { nx?: false; xx?: boolean }
) & (
  | { ex?: never; px?: never; exat?: never; pxat?: never; keepTtl?: boolean }
  | { ex?: number; px?: never; exat?: never; pxat?: never; keepTtl?: false }
  | { ex?: never; px?: number; exat?: never; pxat?: never; keepTtl?: false }
  | { ex?: never; px?: never; exat?: number; pxat?: never; keepTtl?: false }
  | { ex?: never; px?: never; exat?: never; pxat?: number; keepTtl?: false }
);

/** GETEX/HGETEX accept at most one expiry mutation. */
export type GetExOptions =
  | { ex?: never; px?: never; exat?: never; pxat?: never; persist?: false }
  | { ex: number; px?: never; exat?: never; pxat?: never; persist?: false }
  | { ex?: never; px: number; exat?: never; pxat?: never; persist?: false }
  | { ex?: never; px?: never; exat: number; pxat?: never; persist?: false }
  | { ex?: never; px?: never; exat?: never; pxat: number; persist?: false }
  | { ex?: never; px?: never; exat?: never; pxat?: never; persist: true };

export interface ScanOptions {
  match?: string;
  count?: number;
  /** Filter keys by the server-reported data type. */
  type?: string;
}

/** MATCH/COUNT options supported by collection-specific scan commands. */
export type CollectionScanOptions = Omit<ScanOptions, "type">;

export type ExpiryCondition = "NX" | "XX" | "GT" | "LT";

export interface ZAddMember {
  score: number;
  member: unknown;
}

/** ZADD condition and comparison flags must form a valid server grammar. */
export type ZAddOptions = {
  ch?: boolean;
} & (
  | { nx?: boolean; xx?: false; gt?: false; lt?: false }
  | ({ nx?: false; xx?: boolean } & (
    | { gt?: boolean; lt?: false }
    | { gt?: false; lt?: boolean }
  ))
);

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

/** GEOADD NX and XX are mutually exclusive. */
export type GeoAddOptions = {
  ch?: boolean;
} & (
  | { nx?: boolean; xx?: false }
  | { nx?: false; xx?: boolean }
);

export class HashStore {
  constructor(private readonly client: StoreCommandClient) {}

  async hset(key: string, entries: Record<string, unknown> | [string, unknown][]): Promise<number> {
    return number(await commandArgs(this.client, keyValueArgs(["HSET", key], this.client.codec, entries)));
  }

  async hget<T = unknown>(key: string, field: string): Promise<T | null> {
    return decode<T>(this.client.codec, await this.client.command("HGET", key, field));
  }

  hdel(key: string, fields: readonly string[]): Promise<number>;
  hdel(key: string, field: string, ...fields: string[]): Promise<number>;
  async hdel(key: string, ...fieldsOrArray: (string | readonly string[])[]): Promise<number> {
    const fields = requireNonEmpty(arrayOrRest<string>(fieldsOrArray), "HDEL", "field");
    return number(await commandArgs(this.client, concatArgs(["HDEL", key], fields)));
  }

  async hmget<T = unknown>(key: string, fields: string[]): Promise<(T | null)[]> {
    requireNonEmpty(fields, "HMGET", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HMGET", key], fields)),
      fieldCount,
      "HMGET",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async hgetall<T = unknown>(key: string): Promise<Record<string, T | null>> {
    return decodeHashRecord<T>(this.client.codec, await this.client.command("HGETALL", key));
  }

  async hexists(key: string, field: string): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("HEXISTS", key, field));
  }

  async hkeys(key: string): Promise<string[]> {
    return mapArray(await this.client.command("HKEYS", key), "HKEYS", textToken);
  }

  async hvals<T = unknown>(key: string): Promise<(T | null)[]> {
    return mapArray(
      await this.client.command("HVALS", key),
      "HVALS",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async hlen(key: string): Promise<number> {
    return number(await this.client.command("HLEN", key));
  }

  async hincrby(key: string, field: string, increment: number | bigint): Promise<IntegerReply> {
    return integerReply(await this.client.command("HINCRBY", key, field, increment));
  }

  async hincrbyfloat(key: string, field: string, increment: number): Promise<string> {
    return string(await this.client.command("HINCRBYFLOAT", key, field, increment));
  }

  async hsetnx(key: string, field: string, value: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("HSETNX", key, field, encode(this.client.codec, value)));
  }

  async hstrlen(key: string, field: string): Promise<number> {
    return number(await this.client.command("HSTRLEN", key, field));
  }

  hrandfield(key: string): Promise<string | null>;
  hrandfield(key: string, count: number, withValues?: false): Promise<string[]>;
  hrandfield<T = unknown>(
    key: string,
    count: number,
    withValues: true
  ): Promise<(string | T | null)[]>;
  hrandfield<T = unknown>(
    key: string,
    count: number,
    withValues: boolean
  ): Promise<string[] | (string | T | null)[]>;
  async hrandfield(key: string, count?: number, withValues = false): Promise<unknown> {
    if (withValues && count == null) {
      throw new TypeError("HRANDFIELD WITHVALUES requires a count");
    }
    const args: CommandArgument[] = ["HRANDFIELD", key];
    if (count != null) args.push(count);
    if (withValues) args.push("WITHVALUES");
    const response = await commandArgs(this.client, args);
    if (count == null) return response == null ? null : textToken(response);
    if (withValues) return decodeHashFlatPairs(this.client.codec, response);
    return mapArray(response, "HRANDFIELD", textToken);
  }

  async hscan<T = unknown>(
    key: string,
    cursor: string | number,
    options: CollectionScanOptions = {}
  ): Promise<HashScanResult<T>> {
    return decodeScanTuple(
      await this.client.command("HSCAN", key, cursor, ...scanOptions(options, "HSCAN")),
      (items) => decodeHashFlatPairs<T>(this.client.codec, items)
    );
  }

  async hexpire(key: string, seconds: number, fields: string[]): Promise<number[]> {
    requireNonEmpty(fields, "HEXPIRE", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HEXPIRE", key, seconds, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HEXPIRE",
      number
    );
  }

  async httl(key: string, fields: string[]): Promise<IntegerReply[]> {
    requireNonEmpty(fields, "HTTL", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HTTL", key, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HTTL",
      integerReply
    );
  }

  async hpersist(key: string, fields: string[]): Promise<number[]> {
    requireNonEmpty(fields, "HPERSIST", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HPERSIST", key, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HPERSIST",
      number
    );
  }

  async hpexpire(key: string, milliseconds: number, fields: string[]): Promise<number[]> {
    requireNonEmpty(fields, "HPEXPIRE", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HPEXPIRE", key, milliseconds, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HPEXPIRE",
      number
    );
  }

  async hpttl(key: string, fields: string[]): Promise<IntegerReply[]> {
    requireNonEmpty(fields, "HPTTL", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HPTTL", key, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HPTTL",
      integerReply
    );
  }

  async hexpiretime(key: string, fields: string[]): Promise<IntegerReply[]> {
    requireNonEmpty(fields, "HEXPIRETIME", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HEXPIRETIME", key, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HEXPIRETIME",
      integerReply
    );
  }

  async hgetdel<T = unknown>(key: string, fields: string[]): Promise<(T | null)[]> {
    requireNonEmpty(fields, "HGETDEL", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["HGETDEL", key, "FIELDS", fieldCount], fields)),
      fieldCount,
      "HGETDEL",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async hgetex<T = unknown>(key: string, fields: string[], options: GetExOptions = {}): Promise<(T | null)[]> {
    requireNonEmpty(fields, "HGETEX", "field");
    const fieldCount = fields.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(
        ["HGETEX", key],
        getexOptions(options),
        ["FIELDS", fieldCount],
        fields
      )),
      fieldCount,
      "HGETEX",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async hsetex(key: string, seconds: number, entries: Record<string, unknown> | [string, unknown][]): Promise<number> {
    return number(await commandArgs(this.client, keyValueArgs(["HSETEX", key, seconds], this.client.codec, entries)));
  }
}
