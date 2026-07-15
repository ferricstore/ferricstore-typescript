import type { Codec } from "./codecs.js";
import { setOwnValue, textResponse, type CommandArgument } from "./internal.js";
import { array, decode, mapArray } from "./store-utilities.js";

export function decodeHashRecord<T>(codec: Codec, value: unknown): Record<string, T | null> {
  const result: Record<string, T | null> = {};
  for (const [field, item] of hashPairs(value)) {
    setOwnValue(result, textToken(field), decode<T>(codec, item));
  }
  return result;
}

export function decodeHashFlatPairs<T = unknown>(codec: Codec, value: unknown): (string | T | null)[] {
  const result: (string | T | null)[] = [];
  for (const [field, item] of hashPairs(value)) {
    result.push(textToken(field), decode<T>(codec, item));
  }
  return result;
}

function hashPairs(value: unknown): [unknown, unknown][] {
  if (value instanceof Map) return [...value.entries()];
  if (plainResponseObject(value)) return Object.entries(value);
  const items = array(value);
  if (items.every((item) => Array.isArray(item))) {
    return mapArray(items, "hash pair", (item) => {
      const pair = item as unknown[];
      if (pair.length !== 2 || !Object.hasOwn(pair, 0) || !Object.hasOwn(pair, 1)) {
        throw new TypeError("server returned an invalid hash pair response");
      }
      return [pair[0], pair[1]];
    });
  }
  if (items.length % 2 !== 0) {
    throw new TypeError("server returned an invalid hash pair response");
  }
  const pairs: [unknown, unknown][] = [];
  for (let index = 0; index < items.length; index += 2) {
    if (!Object.hasOwn(items, index) || !Object.hasOwn(items, index + 1)) {
      throw new TypeError("server returned an invalid hash pair response");
    }
    pairs.push([items[index], items[index + 1]]);
  }
  return pairs;
}

export function decodeScanTuple<T>(value: unknown, decodeItems: (items: unknown) => T): [string, T] {
  const tuple = requiredPair(value, "server returned an invalid scan response");
  return [textToken(tuple[0]), decodeItems(tuple[1])];
}

export function decodeBlockingListPop(codec: Codec, value: unknown): [string, unknown] | null {
  if (value == null) return null;
  const pair = requiredPair(value, "server returned an invalid blocking list pop response");
  return [textToken(pair[0]), decode(codec, pair[1])];
}

export function decodeBlockingListMPop(codec: Codec, value: unknown): [string, unknown[]] | null {
  if (value == null) return null;
  const pair = requiredPair(value, "server returned an invalid BLMPOP response");
  return [textToken(pair[0]), mapArray(pair[1], "BLMPOP", (item) => decode(codec, item))];
}

export function decodeSortedSetMembers<T = unknown>(
  codec: Codec,
  value: unknown,
  withScores: boolean
): (T | null | string)[] {
  const items = array(value);
  if (!withScores) {
    const result = new Array<T | null>(items.length);
    for (let index = 0; index < items.length; index += 1) {
      if (!Object.hasOwn(items, index)) {
        throw new TypeError("server returned an invalid sorted-set member response");
      }
      result[index] = decode<T>(codec, items[index]);
    }
    return result;
  }
  let nestedPairs: boolean | undefined;
  for (let index = 0; index < items.length; index += 1) {
    if (!Object.hasOwn(items, index)) {
      throw new TypeError("server returned an invalid sorted-set member/score response");
    }
    const item = items[index];
    const nested = Array.isArray(item);
    if (nestedPairs != null && nestedPairs !== nested) {
      throw new TypeError("server returned an invalid sorted-set member/score response");
    }
    nestedPairs = nested;
    if (nested && (
      item.length !== 2
      || !Object.hasOwn(item, 0)
      || !Object.hasOwn(item, 1)
    )) {
      throw new TypeError("server returned an invalid sorted-set member/score response");
    }
  }
  if (nestedPairs === true) {
    const result: (T | null | string)[] = [];
    for (const item of items) {
      const pair = item as unknown[];
      result.push(decode<T>(codec, pair[0]), textToken(pair[1]));
    }
    return result;
  }
  if (items.length % 2 !== 0) {
    throw new TypeError("server returned an invalid sorted-set member/score response");
  }
  return items.map((item, index) => index % 2 === 0 ? decode<T>(codec, item) : textToken(item));
}

export function decodeStreamEntries(codec: Codec, value: unknown): unknown[] {
  const entries = value instanceof Map ? [...value.entries()] : array(value);
  return mapArray(entries, "stream entry", (entry) => {
    const pair = array(entry);
    if (pair.length < 2 || !Object.hasOwn(pair, 0)) {
      throw new TypeError("server returned an invalid stream entry");
    }
    let fields: unknown;
    if (pair.length === 2) {
      if (!Object.hasOwn(pair, 1)) throw new TypeError("server returned an invalid stream entry");
      fields = pair[1];
    } else {
      for (let index = 1; index < pair.length; index += 1) {
        if (!Object.hasOwn(pair, index)) throw new TypeError("server returned an invalid stream entry");
      }
      fields = pair.slice(1);
    }
    return [textToken(pair[0]), decodeStreamFieldValues(codec, fields)];
  });
}

export function decodeStreamReads(codec: Codec, value: unknown): unknown {
  if (value == null) return null;
  const streams = value instanceof Map ? [...value.entries()] : array(value);
  return mapArray(streams, "stream read", (stream) => {
    const pair = requiredPair(stream, "server returned an invalid stream read response");
    return [textToken(pair[0]), decodeStreamEntries(codec, pair[1])];
  });
}

function requiredPair(value: unknown, message: string): unknown[] {
  const pair = array(value);
  if (pair.length !== 2 || !Object.hasOwn(pair, 0) || !Object.hasOwn(pair, 1)) {
    throw new TypeError(message);
  }
  return pair;
}

function decodeStreamFieldValues(codec: Codec, value: unknown): unknown[] {
  const result: unknown[] = [];
  for (const [field, item] of hashPairs(value)) result.push(textToken(field), decode(codec, item));
  return result;
}

const geoSearchMetadataTokens = new Set(["WITHCOORD", "WITHDIST", "WITHHASH"]);

export function geoSearchMetadataCount(args: readonly CommandArgument[]): number {
  let index = 0;
  let count = 0;
  while (index < args.length) {
    const token = commandToken(args[index]);
    if (geoSearchMetadataTokens.has(token)) {
      count += 1;
      index += 1;
    } else if (token === "FROMMEMBER" || token === "COUNT") index += 2;
    else if (token === "FROMLONLAT" || token === "BYRADIUS") index += 3;
    else if (token === "BYBOX") index += 4;
    else index += 1;
  }
  return count;
}

export function encodeGeoSearchArgs(codec: Codec, args: readonly CommandArgument[]): CommandArgument[] {
  const encoded = new Array<CommandArgument>(args.length);
  for (let index = 0; index < args.length; index += 1) {
    if (!Object.hasOwn(args, index)) {
      throw new TypeError("GEOSEARCH arguments must be dense");
    }
    encoded[index] = args[index];
  }
  for (let index = 0; index < encoded.length - 1; index += 1) {
    if (commandToken(encoded[index]) !== "FROMMEMBER") continue;
    const member = encoded[index + 1];
    if (!Buffer.isBuffer(member) && !(member instanceof Uint8Array)) encoded[index + 1] = codec.encode(member);
    index += 1;
  }
  return encoded;
}

export function decodeGeoSearchMembers(codec: Codec, value: unknown, metadataCount: number): unknown[] {
  const items = array(value);
  if (metadataCount === 0) return mapArray(items, "GEOSEARCH", (item) => decode(codec, item));
  return mapArray(items, "GEOSEARCH", (item) => {
    const result = array(item);
    if (result.length !== metadataCount + 1) throw new TypeError("server returned an invalid GEOSEARCH result");
    return mapArray(result, "GEOSEARCH", (part, index) =>
      index === 0 ? decode(codec, part) : normalizeGeoMetadata(part)
    );
  });
}

function normalizeGeoMetadata(value: unknown): unknown {
  if (Array.isArray(value)) return mapArray(value, "GEOSEARCH", normalizeGeoMetadata);
  if (typeof value === "string" || Buffer.isBuffer(value) || value instanceof Uint8Array) return textToken(value);
  return value;
}

function commandToken(value: CommandArgument | undefined): string {
  if (typeof value === "string") return value.toUpperCase();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase();
  }
  return "";
}

export function textToken(value: unknown): string {
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  return textResponse(value);
}

function plainResponseObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) &&
    !Buffer.isBuffer(value) && !(value instanceof Uint8Array) && !(value instanceof Map);
}
