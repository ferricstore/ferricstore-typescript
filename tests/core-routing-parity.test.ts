import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { specializedRoutingCommandNames } from "../src/command-grammar.js";
import {
  allArgumentMultiKeyCommands,
  firstKeyCommands,
  secondKeyCommands,
  trailingArgumentMultiKeyCommands,
  twoKeyCommands
} from "../src/command-metadata.js";
import { routingKeyFromArgs } from "../src/topology-routing.js";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const coreRoot = process.env.FERRICSTORE_CORE_DIR ?? resolve(repositoryRoot, "../ferricstore");
const parserPath = resolve(coreRoot, "apps/ferricstore/lib/ferricstore/commands/native_ast_parser.ex");
const catalogPath = resolve(coreRoot, "apps/ferricstore/lib/ferricstore/commands/catalog/entries.ex");
const keyDiscoveryPath = resolve(coreRoot, "apps/ferricstore/lib/ferricstore/commands/key_discovery.ex");
const coreAvailable = existsSync(parserPath) && existsSync(catalogPath) && existsSync(keyDiscoveryPath);

if (process.env.FERRICSTORE_CORE_REQUIRED === "1" && !coreAvailable) {
  throw new Error(`required FerricStore core routing sources were not found under ${coreRoot}`);
}

const nonDataSpecializedCommands = new Set([
  "PSUBSCRIBE",
  "PUBLISH",
  "PUBSUB",
  "PUNSUBSCRIBE",
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "WATCH"
]);

function sigilWords(source: string, attribute: string): string[] {
  const match = new RegExp(`@${attribute} ~w\\(([\\s\\S]*?)\\n  \\)`, "u").exec(source);
  if (match?.[1] == null) throw new Error(`core routing attribute ${attribute} was not found`);
  return match[1].trim().split(/\s+/u);
}

function catalogSingleKeyCommands(source: string): string[] {
  const commands: string[] = [];
  for (const match of source.matchAll(/%\{([\s\S]*?)\n {4}\}/gu)) {
    const body = match[1] ?? "";
    const name = /name: "([^"]+)"/u.exec(body)?.[1];
    const first = /first_key: (-?\d+)/u.exec(body)?.[1];
    const last = /last_key: (-?\d+)/u.exec(body)?.[1];
    if (name != null && first === "1" && last === "1") commands.push(name.toUpperCase());
  }
  return commands;
}

function coreSpecializedKeyCommands(parser: string, keyDiscovery: string): Set<string> {
  const extraStart = parser.indexOf("defp extra_command_keys");
  const extraEnd = parser.indexOf("defp flow_partition_keys_or_global", extraStart);
  if (extraStart < 0 || extraEnd < 0) throw new Error("core specialized key parser was not found");
  const extra = parser.slice(extraStart, extraEnd);
  const commands = [
    ...[...extra.matchAll(/extra_command_keys\("([^"]+)"/gu)].flatMap((match) =>
      match[1] == null ? [] : [match[1]]
    ),
    ...[...extra.matchAll(/when cmd in ~w\(([\s\S]*?)\)/gu)].flatMap((match) =>
      match[1]?.trim().split(/\s+/u) ?? []
    )
  ];

  const extractStart = keyDiscovery.indexOf("@spec extract");
  const extractEnd = keyDiscovery.indexOf("@spec describe", extractStart);
  if (extractStart < 0 || extractEnd < 0) throw new Error("core dynamic key discovery was not found");
  const extract = keyDiscovery.slice(extractStart, extractEnd);
  commands.push(
    ...[...extract.matchAll(/def extract\("([^"]+)"/gu)].flatMap((match) =>
      match[1] == null ? [] : [match[1]]
    ),
    ...[...extract.matchAll(/when command in \[([^\]]+)\]/gu)].flatMap((match) =>
      [...(match[1] ?? "").matchAll(/"([^"]+)"/gu)].flatMap((word) =>
        word[1] == null ? [] : [word[1]]
      )
    )
  );

  return new Set(commands.filter((command) =>
    !command.startsWith("FLOW.") && !nonDataSpecializedCommands.has(command)
  ));
}

describe.skipIf(!coreAvailable)("FerricStore core routing parity", () => {
  it("keeps every core specialized-key command in the canonical grammar", () => {
    const specialized = coreSpecializedKeyCommands(
      readFileSync(parserPath, "utf8"),
      readFileSync(keyDiscoveryPath, "utf8")
    );
    expect([...specializedRoutingCommandNames].sort()).toEqual([...specialized].sort());
  });

  it("extracts the core key shape for every specialized command family", () => {
    const first = "{routing-parity}:first";
    const second = "{routing-parity}:second";
    const commands: (readonly [string, readonly (string | number)[]])[] = [
      ["BITOP", ["BITOP", "AND", first, second]],
      ["BLMPOP", ["BLMPOP", 0, 2, first, second, "LEFT"]],
      ["CMS.MERGE", ["CMS.MERGE", first, 1, second]],
      ["DEL", ["DEL", first, second]],
      ["EXISTS", ["EXISTS", first, second]],
      ["MGET", ["MGET", first, second]],
      ["MSET", ["MSET", first, "one", second, "two"]],
      ["MSETNX", ["MSETNX", first, "one", second, "two"]],
      ["SINTERCARD", ["SINTERCARD", 2, first, second]],
      ["TDIGEST.MERGE", ["TDIGEST.MERGE", first, 1, second]],
      ["UNLINK", ["UNLINK", first, second]],
      ["XREAD", ["XREAD", "STREAMS", first, second, "0", "0"]],
      ["XREADGROUP", ["XREADGROUP", "GROUP", "group", "consumer", "STREAMS", first, second, ">", ">"]]
    ];
    for (const name of twoKeyCommands) commands.push([name, [name, first, second]]);
    for (const name of trailingArgumentMultiKeyCommands) {
      commands.push([name, [name, first, second, 0]]);
    }
    for (const name of allArgumentMultiKeyCommands) commands.push([name, [name, first, second]]);
    const secondKeySubcommands = new Map([
      ["MEMORY", "USAGE"],
      ["OBJECT", "ENCODING"],
      ["XGROUP", "CREATE"],
      ["XINFO", "STREAM"]
    ]);
    for (const name of secondKeyCommands) {
      commands.push([name, [name, secondKeySubcommands.get(name) ?? "", first]]);
    }

    const specialized = coreSpecializedKeyCommands(
      readFileSync(parserPath, "utf8"),
      readFileSync(keyDiscoveryPath, "utf8")
    );
    expect(new Set(commands.map(([name]) => name))).toEqual(specialized);
    for (const [name, args] of commands) {
      expect(routingKeyFromArgs(name, args), name).toEqual({ handled: true, key: first });
    }
  });

  it("does not invent keys for malformed two-key or keyless subcommand forms", () => {
    expect(routingKeyFromArgs("COPY", ["COPY", "source-only"])).toEqual({ handled: true });
    for (const name of secondKeyCommands) {
      expect(routingKeyFromArgs(name, [name, "HELP", "not-a-key"]), name).toEqual({ handled: true });
    }
  });

  it("matches core first-key routing in both directions", () => {
    const parser = readFileSync(parserPath, "utf8");
    const catalog = readFileSync(catalogPath, "utf8");
    const specialized = coreSpecializedKeyCommands(
      parser,
      readFileSync(keyDiscoveryPath, "utf8")
    );
    const coreFirstKeyCommands = new Set([
      ...sigilWords(parser, "extra_first_key_commands"),
      ...catalogSingleKeyCommands(catalog)
    ].filter((command) => !command.startsWith("FLOW.") && !specialized.has(command)));

    const sdkOnly = [...firstKeyCommands]
      .filter((command) => !coreFirstKeyCommands.has(command))
      .sort();
    const coreOnly = [...coreFirstKeyCommands]
      .filter((command) => !firstKeyCommands.has(command))
      .sort();

    expect(sdkOnly).toEqual([]);
    expect(coreOnly).toEqual([]);
  });
});
