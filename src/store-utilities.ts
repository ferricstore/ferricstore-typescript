import { Buffer } from "node:buffer";
import type { Codec } from "./codecs.js";
import {
  binaryBooleanResponse,
  integer,
  textResponse,
  type CommandArgument
} from "./internal.js";
import type {
  GeoAddOptions,
  GetExOptions,
  RangeLimit,
  ScanOptions,
  SetOptions,
  StoreCommandClient,
  XReadStream,
  ZAddOptions
} from "./store.js";
import { assertSafeVariadicDispatch } from "./variadic-dispatch.js";

export function encode(codec: Codec, value: unknown): Buffer {
  return codec.encode(value);
}

export function decode<T>(codec: Codec, value: unknown): T | null {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return codec.decode(value) as T | null;
  if (value instanceof Uint8Array) return codec.decode(Buffer.from(value)) as T | null;
  return value as T;
}

export function number(value: unknown): number {
  return integer(value);
}

export function binaryInteger(value: unknown): number {
  return binaryBooleanResponse(value) ? 1 : 0;
}

export function string(value: unknown): string {
  return textResponse(value);
}

export function array(value: unknown, expectedItems?: number, command?: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new TypeError("server returned an invalid array response");
  }
  if (expectedItems != null && value.length !== expectedItems) {
    throw new TypeError(`${command ?? "server"} returned ${value.length} items; expected ${expectedItems}`);
  }
  return value;
}

/** @internal Validate a variable-length response without copying it. */
export function denseArray(value: unknown, command: string): unknown[] {
  return assertDenseArray(array(value), command);
}

/** @internal Validate a positional response without copying it. */
export function denseArrayResponse(
  value: unknown,
  expectedItems: number,
  command: string
): unknown[] {
  return assertDenseArray(array(value, expectedItems, command), command);
}

function assertDenseArray(items: unknown[], command: string): unknown[] {
  for (let index = 0; index < items.length; index += 1) {
    if (!Object.hasOwn(items, index)) {
      throw new TypeError(`${command} response item ${index} is missing`);
    }
  }
  return items;
}

/** @internal Validate and transform a variable-length response in one pass. */
export function mapArray<T>(
  value: unknown,
  command: string,
  transform: (item: unknown, index: number) => T
): T[] {
  const items = array(value);
  const result = new Array<T>(items.length);
  for (let index = 0; index < items.length; index += 1) {
    if (!Object.hasOwn(items, index)) {
      throw new TypeError(`${command} response item ${index} is missing`);
    }
    result[index] = transform(items[index], index);
  }
  return result;
}

/** @internal Validate and transform a positional response in one pass. */
export function mapArrayResponse<T>(
  value: unknown,
  expectedItems: number,
  command: string,
  transform: (item: unknown, index: number) => T
): T[] {
  const items = array(value, expectedItems, command);
  const result = new Array<T>(items.length);
  for (let index = 0; index < items.length; index += 1) {
    if (!Object.hasOwn(items, index)) {
      throw new TypeError(`${command} response item ${index} is missing`);
    }
    result[index] = transform(items[index], index);
  }
  return result;
}

/** @internal Reject an empty required collection without copying or scanning it. */
export function requireNonEmpty<T>(
  values: readonly T[],
  command: string,
  noun = "argument"
): readonly T[] {
  if (values.length === 0) throw new TypeError(`${command} requires at least one ${noun}`);
  return values;
}

function commandName(args: readonly CommandArgument[]): string {
  return typeof args[0] === "string" ? args[0] : "command";
}

export function commandArgs(
  client: StoreCommandClient,
  args: readonly CommandArgument[]
): Promise<unknown> {
  if (client.commandArgs != null) return client.commandArgs(args);
  assertSafeVariadicDispatch(args.length, "commandArgs");
  return client.command(...args);
}

/** @internal Normalize an unambiguous scalar rest parameter or one array argument. */
export function arrayOrRest<T>(values: readonly (T | readonly T[])[]): readonly T[] {
  const first = values[0];
  return values.length === 1 && Array.isArray(first)
    ? first as readonly T[]
    : values as readonly T[];
}

export function appendXReadStreams(
  args: CommandArgument[],
  streams: readonly XReadStream[]
): void {
  const offset = args.length;
  const streamCount = streams.length;
  requireNonEmpty(streams, commandName(args), "stream");
  args.length = offset + 1 + streamCount * 2;
  args[offset] = "STREAMS";
  for (let index = 0; index < streamCount; index += 1) {
    if (!Object.hasOwn(streams, index)) {
      throw new TypeError("XREAD streams must be dense");
    }
    const stream = streams[index];
    if (stream == null) throw new TypeError("XREAD streams must contain only stream descriptors");
    if (!Object.hasOwn(stream, "key") || !Object.hasOwn(stream, "id")) {
      throw new TypeError(`${commandName(args)} streams require own key and id fields`);
    }
    args[offset + 1 + index] = stream.key;
    args[offset + 1 + streamCount + index] = stream.id;
  }
}

export function concatArgs(
  ...parts: readonly (readonly CommandArgument[])[]
): CommandArgument[] {
  let length = 0;
  for (const part of parts) length += part.length;
  const args = new Array<CommandArgument>(length);
  let offset = 0;
  for (const part of parts) {
    for (let index = 0; index < part.length; index += 1) {
      if (!Object.hasOwn(part, index)) {
        throw new TypeError("command argument arrays must be dense");
      }
      args[offset++] = part[index];
    }
  }
  return args;
}

export function encodedArgs(
  prefix: readonly CommandArgument[],
  codec: Codec,
  values: readonly unknown[],
  allowEmpty = false
): CommandArgument[] {
  if (!allowEmpty) requireNonEmpty(values, commandName(prefix));
  const args = new Array<CommandArgument>(prefix.length + values.length);
  for (let index = 0; index < prefix.length; index += 1) args[index] = prefix[index];
  for (let index = 0; index < values.length; index += 1) {
    if (!Object.hasOwn(values, index)) {
      throw new TypeError("command argument arrays must be dense");
    }
    args[prefix.length + index] = codec.encode(values[index]);
  }
  return args;
}

export function keyValueArgs(
  prefix: readonly CommandArgument[],
  codec: Codec,
  entries: Record<string, unknown> | [string, unknown][]
): CommandArgument[] {
  const validateTuples = Array.isArray(entries);
  const pairs = validateTuples ? entries : Object.entries(entries);
  requireNonEmpty(pairs, commandName(prefix), "entry");
  const args = new Array<CommandArgument>(prefix.length + pairs.length * 2);
  for (let index = 0; index < prefix.length; index += 1) args[index] = prefix[index];
  for (let index = 0; index < pairs.length; index += 1) {
    const pair = pairs[index];
    if (
      !Object.hasOwn(pairs, index)
      || pair == null
      || validateTuples && (
        !Array.isArray(pair)
        || pair.length !== 2
        || !Object.hasOwn(pair, 0)
        || !Object.hasOwn(pair, 1)
      )
    ) {
      throw new TypeError("key/value entries must be dense two-item tuples");
    }
    args[index * 2 + prefix.length] = pair[0];
    args[index * 2 + prefix.length + 1] = codec.encode(pair[1]);
  }
  return args;
}

export function encodedEntryArgs(
  prefix: readonly CommandArgument[],
  codec: Codec,
  entries: readonly (readonly [unknown, CommandArgument])[]
): CommandArgument[] {
  requireNonEmpty(entries, commandName(prefix), "entry");
  const args = new Array<CommandArgument>(prefix.length + entries.length * 2);
  for (let index = 0; index < prefix.length; index += 1) args[index] = prefix[index];
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    if (
      !Object.hasOwn(entries, index)
      || entry == null
      || !Array.isArray(entry)
      || entry.length !== 2
      || !Object.hasOwn(entry, 0)
      || !Object.hasOwn(entry, 1)
    ) {
      throw new TypeError("entries must be dense two-item tuples");
    }
    args[prefix.length + index * 2] = codec.encode(entry[0]);
    args[prefix.length + index * 2 + 1] = entry[1];
  }
  return args;
}

export function setOptions(options: SetOptions): CommandArgument[] {
  const ex = ownOption(options, "ex");
  const exat = ownOption(options, "exat");
  const get = ownOption(options, "get");
  const keepTtl = ownOption(options, "keepTtl");
  const nx = ownOption(options, "nx");
  const px = ownOption(options, "px");
  const pxat = ownOption(options, "pxat");
  const xx = ownOption(options, "xx");
  if (nx === true && xx === true) {
    throw new TypeError("SET NX and XX options are mutually exclusive");
  }
  let expiryModes = keepTtl === true ? 1 : 0;
  if (ex != null) expiryModes += 1;
  if (px != null) expiryModes += 1;
  if (exat != null) expiryModes += 1;
  if (pxat != null) expiryModes += 1;
  if (expiryModes > 1) {
    throw new TypeError("SET expiry options are mutually exclusive");
  }
  const args: CommandArgument[] = [];
  if (ex != null) args.push("EX", ex);
  if (px != null) args.push("PX", px);
  if (exat != null) args.push("EXAT", exat);
  if (pxat != null) args.push("PXAT", pxat);
  if (nx === true) args.push("NX");
  if (xx === true) args.push("XX");
  if (get === true) args.push("GET");
  if (keepTtl === true) args.push("KEEPTTL");
  return args;
}

export function getexOptions(options: GetExOptions): CommandArgument[] {
  const persist = ownOption(options, "persist");
  const ex = ownOption(options, "ex");
  const px = ownOption(options, "px");
  const exat = ownOption(options, "exat");
  const pxat = ownOption(options, "pxat");
  let expiryModes = persist === true ? 1 : 0;
  if (ex != null) expiryModes += 1;
  if (px != null) expiryModes += 1;
  if (exat != null) expiryModes += 1;
  if (pxat != null) expiryModes += 1;
  if (expiryModes > 1) {
    throw new TypeError("GETEX expiry options are mutually exclusive");
  }
  if (persist === true) return ["PERSIST"];
  if (ex != null) return ["EX", ex];
  if (px != null) return ["PX", px];
  if (exat != null) return ["EXAT", exat];
  if (pxat != null) return ["PXAT", pxat];
  return [];
}

export function scanOptions(options: ScanOptions, command = "SCAN"): CommandArgument[] {
  const type = ownOption(options, "type");
  const match = ownOption(options, "match");
  const count = ownOption(options, "count");
  if (type != null && command !== "SCAN") {
    throw new TypeError(`${command} does not support the TYPE option`);
  }
  const args: CommandArgument[] = [];
  if (match != null) args.push("MATCH", match);
  if (count != null) args.push("COUNT", count);
  if (type != null) args.push("TYPE", type);
  return args;
}

export function zaddOptions(options: ZAddOptions): CommandArgument[] {
  const ch = ownOption(options, "ch");
  const gt = ownOption(options, "gt");
  const lt = ownOption(options, "lt");
  const nx = ownOption(options, "nx");
  const xx = ownOption(options, "xx");
  if (nx === true && xx === true) {
    throw new TypeError("ZADD NX and XX options are mutually exclusive");
  }
  if (gt === true && lt === true) {
    throw new TypeError("ZADD GT and LT options are mutually exclusive");
  }
  if (nx === true && (gt === true || lt === true)) {
    throw new TypeError("ZADD NX cannot be combined with GT or LT");
  }
  const args: CommandArgument[] = [];
  if (nx === true) args.push("NX");
  if (xx === true) args.push("XX");
  if (gt === true) args.push("GT");
  if (lt === true) args.push("LT");
  if (ch === true) args.push("CH");
  return args;
}

export function geoAddOptions(options: GeoAddOptions): CommandArgument[] {
  const ch = ownOption(options, "ch");
  const nx = ownOption(options, "nx");
  const xx = ownOption(options, "xx");
  if (nx === true && xx === true) {
    throw new TypeError("GEOADD NX and XX options are mutually exclusive");
  }
  const args: CommandArgument[] = [];
  if (nx === true) args.push("NX");
  if (xx === true) args.push("XX");
  if (ch === true) args.push("CH");
  return args;
}

export function rangeScoreOptions(
  options: { withScores?: boolean; limit?: RangeLimit },
  withScores = ownOption(options, "withScores") === true
): CommandArgument[] {
  const limit = ownOption(options, "limit");
  const args: CommandArgument[] = [];
  if (withScores) args.push("WITHSCORES");
  if (limit != null) {
    if (!Object.hasOwn(limit, "offset") || !Object.hasOwn(limit, "count")) {
      throw new TypeError("LIMIT requires own offset and count values");
    }
    args.push("LIMIT", limit.offset, limit.count);
  }
  return args;
}

export function ownOption<T extends object, K extends keyof T>(options: T, key: K): T[K] | undefined {
  return Object.hasOwn(options, key) ? options[key] : undefined;
}
