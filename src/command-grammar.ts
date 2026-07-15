import { Buffer } from "node:buffer";
import {
  allArgumentMultiKeyCommands,
  firstKeyCommands,
  secondKeyCommands,
  trailingArgumentMultiKeyCommands,
  twoKeyCommands
} from "./command-metadata.js";
import type { CommandArgument } from "./internal.js";

export interface CommandView {
  readonly argumentStart: number;
  readonly name?: string;
}

export interface FlowValueMGetArguments {
  readonly hasMaxBytes: boolean;
  readonly maxBytes?: CommandArgument;
  readonly refs: readonly CommandArgument[];
  readonly suffix: readonly CommandArgument[];
}

export type RoutingKeyArguments =
  | { readonly handled: false }
  | { readonly handled: true; readonly keys: readonly CommandArgument[] };

const compoundCommandPrefixes = new Set(["CLIENT", "CLUSTER", "FLOW"]);
const flowValueMGetMaxBytesTokens = new Set(["MAX_BYTES", "MAXBYTES"]);
const secondKeySubcommands = new Map<string, ReadonlySet<string>>([
  ["MEMORY", new Set(["USAGE"])],
  ["OBJECT", new Set(["ENCODING", "FREQ", "IDLETIME", "REFCOUNT"])],
  ["XGROUP", new Set(["CREATE", "SETID", "DESTROY", "CREATECONSUMER", "DELCONSUMER"])],
  ["XINFO", new Set(["STREAM", "GROUPS", "CONSUMERS"])]
]);

/** Normalize a command or option token without coercing arbitrary values. */
export function commandToken(value: unknown): string | undefined {
  if (typeof value === "string") return value.toUpperCase();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase();
  }
  return undefined;
}

/** Parse the command name once and expose where its real arguments begin. */
export function commandView(command: readonly CommandArgument[]): CommandView {
  const first = commandToken(command[0]);
  if (first == null) return { argumentStart: 1 };
  if (compoundCommandPrefixes.has(first) && command.length > 1) {
    const second = commandToken(command[1]);
    if (second != null) return { argumentStart: 2, name: `${first}.${second}` };
  }
  return { argumentStart: 1, name: first };
}

/** Parse only a trailing value-MGET size option; token-looking refs remain refs. */
export function splitFlowValueMGetArguments(
  args: readonly CommandArgument[],
  argumentStart: number
): FlowValueMGetArguments {
  const optionIndex = args.length - 2;
  const hasMaxBytes = optionIndex >= argumentStart
    && flowValueMGetMaxBytesTokens.has(commandToken(args[optionIndex]) ?? "")
    && isNonNegativeSafeIntegerArgument(args[optionIndex + 1]);
  return hasMaxBytes
    ? {
        hasMaxBytes: true,
        maxBytes: args[args.length - 1],
        refs: args.slice(argumentStart, optionIndex),
        suffix: args.slice(optionIndex)
      }
    : {
        hasMaxBytes: false,
        refs: args.slice(argumentStart),
        suffix: []
      };
}

/** Extract command key arguments according to the core's specialized grammar. */
export function routingKeyArguments(
  name: string | undefined,
  args: readonly CommandArgument[]
): RoutingKeyArguments {
  if (name == null) return { handled: false };
  if (name === "MGET" || name === "DEL" || name === "EXISTS" || name === "UNLINK") {
    return { handled: true, keys: args.slice(1) };
  }
  if (name === "MSET" || name === "MSETNX") {
    if (args.length < 3 || (args.length - 1) % 2 !== 0) return { handled: true, keys: [] };
    const keys: CommandArgument[] = [];
    for (let index = 1; index < args.length; index += 2) keys.push(args[index]);
    return { handled: true, keys };
  }
  if (name === "BITOP") return { handled: true, keys: bitopKeys(args) };
  if (twoKeyCommands.has(name)) {
    return { handled: true, keys: args.length >= 3 ? args.slice(1, 3) : [] };
  }
  if (name === "XREAD" || name === "XREADGROUP") {
    return { handled: true, keys: streamReadKeys(args) };
  }
  if (firstKeyCommands.has(name)) return { handled: true, keys: args.slice(1, 2) };
  if (secondKeyCommands.has(name)) {
    const allowed = secondKeySubcommands.get(name);
    return {
      handled: true,
      keys: allowed?.has(commandToken(args[1]) ?? "") === true ? args.slice(2, 3) : []
    };
  }
  if (trailingArgumentMultiKeyCommands.has(name)) {
    return { handled: true, keys: args.length >= 3 ? args.slice(1, -1) : [] };
  }
  if (allArgumentMultiKeyCommands.has(name)) return { handled: true, keys: args.slice(1) };
  if (name === "BLMPOP") return { handled: true, keys: countedKeys(args, 2, 3) };
  if (name === "SINTERCARD") return { handled: true, keys: countedKeys(args, 1, 2) };
  if (name === "CMS.MERGE" || name === "TDIGEST.MERGE") {
    return { handled: true, keys: countedKeys(args, 2, 3, true) };
  }
  return { handled: false };
}

/** Specialized forms deliberately excluded from the first-key parity catalog. */
export const specializedRoutingCommandNames = new Set([
  "BITOP",
  "BLMPOP",
  "MSET",
  "MSETNX",
  "SINTERCARD",
  "CMS.MERGE",
  "TDIGEST.MERGE",
  "XREAD",
  "XREADGROUP",
  ...twoKeyCommands,
  ...secondKeyCommands,
  ...trailingArgumentMultiKeyCommands,
  ...allArgumentMultiKeyCommands,
  "DEL",
  "EXISTS",
  "MGET",
  "UNLINK"
]);

function bitopKeys(args: readonly CommandArgument[]): readonly CommandArgument[] {
  return args.length < 4 ? [] : args.slice(2);
}

function countedKeys(
  args: readonly CommandArgument[],
  countIndex: number,
  keysIndex: number,
  includeDestination = false
): readonly CommandArgument[] {
  const count = commandArgumentInteger(args[countIndex]);
  if (count == null || count <= 0 || keysIndex + count > args.length) return [];
  return includeDestination
    ? [args[1], ...args.slice(keysIndex, keysIndex + count)]
    : args.slice(keysIndex, keysIndex + count);
}

function commandArgumentInteger(value: CommandArgument): number | undefined {
  return safeIntegerArgument(value);
}

function isNonNegativeSafeIntegerArgument(value: CommandArgument): boolean {
  const parsed = safeIntegerArgument(value);
  return parsed != null && parsed >= 0;
}

function safeIntegerArgument(value: CommandArgument): number | undefined {
  if (typeof value === "number") return Number.isSafeInteger(value) ? value : undefined;
  if (typeof value === "bigint") {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : undefined;
  }
  const source = typeof value === "string"
    ? value
    : Buffer.isBuffer(value) || value instanceof Uint8Array
      ? Buffer.from(value).toString("utf8")
      : undefined;
  if (source == null || !/^[+-]?\d+$/u.test(source)) return undefined;
  const parsed = BigInt(source);
  return parsed >= BigInt(Number.MIN_SAFE_INTEGER) && parsed <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(parsed)
    : undefined;
}

function streamReadKeys(args: readonly CommandArgument[]): readonly CommandArgument[] {
  const command = commandToken(args[0]);
  let index: number;
  if (command === "XREADGROUP") {
    if (commandToken(args[1]) !== "GROUP" || args.length < 5) return [];
    index = 4;
  } else if (command === "XREAD") {
    index = 1;
  } else {
    return [];
  }

  while (index < args.length) {
    const token = commandToken(args[index]);
    if (token === "STREAMS") break;
    if (token === "COUNT" || token === "BLOCK") {
      if (index + 1 >= args.length) return [];
      index += 2;
    } else if (command === "XREADGROUP" && token === "NOACK") {
      index += 1;
    } else {
      return [];
    }
  }
  if (index >= args.length) return [];
  const streamArgs = args.slice(index + 1);
  return streamArgs.length === 0 || streamArgs.length % 2 !== 0
    ? []
    : streamArgs.slice(0, streamArgs.length / 2);
}
