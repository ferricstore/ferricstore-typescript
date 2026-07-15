import { Buffer } from "node:buffer";
import {
  commandToken as commandPart,
  commandView,
  splitFlowValueMGetArguments
} from "./command-grammar.js";
import {
  flowIdReadAutoBatchCommands,
  flowIdWriteAutoBatchCommands,
  flowManyWriteAutoBatchCommands,
  readOnlyAutoBatchCommands,
  safeAutoBatchCommands
} from "./command-metadata.js";
import type { Command, CommandArgument } from "./internal.js";

export interface AutoBatchOrderingPlan {
  readonly accesses: ReadonlyMap<string, "read" | "write">;
  readonly fallbackDependencies: readonly (readonly number[])[];
}

export function autoBatchOrderingPlan(
  batch: readonly { readonly command: Command }[]
): AutoBatchOrderingPlan | undefined {
  const accesses = new Map<string, "read" | "write">();
  const fallbackDependencies: number[][] = [];
  const lastWrites = new Map<string, number>();
  const reads = new Map<string, Set<number>>();
  for (let index = 0; index < batch.length; index += 1) {
    const item = batch[index];
    if (item == null) return undefined;
    const rawCommandAccesses = autoBatchCommandOrderingAccesses(item.command);
    if (rawCommandAccesses == null) return undefined;
    const commandAccesses = new Map<string, "read" | "write">();
    for (const [key, access] of rawCommandAccesses) {
      if (access === "write" || !commandAccesses.has(key)) commandAccesses.set(key, access);
      if (access === "write" || !accesses.has(key)) accesses.set(key, access);
    }
    const dependencies = new Set<number>();
    for (const [key, access] of commandAccesses) {
      const lastWrite = lastWrites.get(key);
      if (lastWrite != null) dependencies.add(lastWrite);
      if (access === "write") {
        for (const read of reads.get(key) ?? []) dependencies.add(read);
      }
    }
    fallbackDependencies[index] = [...dependencies].sort((left, right) => left - right);
    for (const [key, access] of commandAccesses) {
      if (access === "read") {
        let keyReads = reads.get(key);
        if (keyReads == null) {
          keyReads = new Set();
          reads.set(key, keyReads);
        }
        keyReads.add(index);
      } else {
        reads.delete(key);
        lastWrites.set(key, index);
      }
    }
  }
  return accesses.size === 0 ? undefined : { accesses, fallbackDependencies };
}

function autoBatchCommandOrderingAccesses(
  command: Command
): readonly (readonly [string, "read" | "write"])[] | undefined {
  const name = autoBatchCommandName(command);
  if (name == null || !safeAutoBatchCommands.has(name)) return undefined;
  if (name.startsWith("FLOW.")) return flowAutoBatchOrderingAccesses(command, name);
  let values: readonly CommandArgument[];
  if (name === "DEL" || name === "EXISTS" || name === "MGET") {
    values = command.slice(1);
  } else if (name === "MSET") {
    if (command.length < 3 || (command.length - 1) % 2 !== 0) return undefined;
    const pairs: CommandArgument[] = [];
    for (let index = 1; index < command.length; index += 2) {
      const key = command[index];
      if (!isAutoBatchResourceValue(key)) return undefined;
      pairs.push(key);
    }
    values = pairs;
  } else {
    values = command.slice(1, 2);
  }

  const access = readOnlyAutoBatchCommands.has(name) ? "read" : "write";
  const keys: (readonly [string, "read" | "write"])[] = [];
  for (const value of values) {
    if (!isAutoBatchResourceValue(value)) return undefined;
    keys.push([autoBatchResourceKey("kv", value), access]);
  }
  return keys.length === 0 ? undefined : keys;
}

function flowAutoBatchOrderingAccesses(
  command: Command,
  name: string
): readonly (readonly [string, "read" | "write"])[] | undefined {
  if (name === "FLOW.VALUE.MGET") return flowValueMGetAutoBatchOrderingAccesses(command);
  if (name === "FLOW.VALUE.PUT") return flowValuePutAutoBatchOrderingAccesses(command);
  if (flowManyWriteAutoBatchCommands.has(name)) {
    const ids = flowManyAutoBatchIds(command, name);
    return ids?.map((id) => [autoBatchResourceKey("flow", id), "write"] as const);
  }
  const access = flowIdReadAutoBatchCommands.has(name)
    ? "read"
    : flowIdWriteAutoBatchCommands.has(name)
      ? "write"
      : undefined;
  if (access == null) return undefined;
  const idIndex = commandView(command).argumentStart;
  const id = command[idIndex];
  if (!isAutoBatchResourceValue(id)) return undefined;
  return [[autoBatchResourceKey("flow", id), access]];
}

function flowValueMGetAutoBatchOrderingAccesses(
  command: Command
): readonly (readonly [string, "read"])[] | undefined {
  const { refs } = splitFlowValueMGetArguments(command, commandView(command).argumentStart);
  const accesses: (readonly [string, "read"])[] = [];
  for (const value of refs) {
    if (!isAutoBatchResourceValue(value)) return undefined;
    accesses.push([autoBatchResourceKey("flow_value", value), "read"]);
  }
  return accesses.length === 0 ? undefined : accesses;
}

function flowValuePutAutoBatchOrderingAccesses(
  command: Command
): readonly (readonly [string, "write"])[] | undefined {
  const valueIndex = commandView(command).argumentStart;
  if (command[valueIndex] == null) return undefined;
  const owner = flowValuePutOwner(command, valueIndex + 1);
  if (isAutoBatchResourceValue(owner)) {
    return [[autoBatchResourceKey("flow", owner), "write"]];
  }
  // Shared values are content addressed, but hashing a potentially large value
  // here would duplicate encoding work on the batching hot path.
  return [["flow_value:shared", "write"]];
}

function flowManyAutoBatchIds(command: Command, name: string): readonly (string | Buffer)[] | undefined {
  if (name === "FLOW.RUN_STEPS_MANY") return runStepsManyAutoBatchIds(command);
  if (name === "FLOW.CREATE_MANY") return createManyAutoBatchIds(command);
  const mixed = commandPart(command[commandView(command).argumentStart]) === "MIXED";
  if (name === "FLOW.CANCEL_MANY") {
    return fixedFlowItemIds(command, mixed ? 3 : 2, (itemIndex) =>
      isAutoBatchResourceValue(command[itemIndex])
      && (!mixed || isAutoBatchResourceValue(command[itemIndex + 1]))
      && isFencingToken(command[itemIndex + (mixed ? 2 : 1)])
    );
  }
  if (name === "FLOW.TRANSITION_MANY") {
    return fixedFlowItemIds(command, mixed ? 4 : 3, (itemIndex) =>
      isAutoBatchResourceValue(command[itemIndex])
      && (!mixed || isAutoBatchResourceValue(command[itemIndex + 1]))
      && isFencingToken(command[itemIndex + (mixed ? 2 : 1)])
      && Buffer.isBuffer(command[itemIndex + (mixed ? 3 : 2)])
    );
  }
  return fixedFlowItemIds(command, mixed ? 4 : 3, (itemIndex) =>
    isAutoBatchResourceValue(command[itemIndex])
    && (!mixed || isAutoBatchResourceValue(command[itemIndex + 1]))
    && Buffer.isBuffer(command[itemIndex + (mixed ? 2 : 1)])
    && isFencingToken(command[itemIndex + (mixed ? 3 : 2)])
  );
}

function createManyAutoBatchIds(command: Command): readonly (string | Buffer)[] | undefined {
  const argumentStart = commandView(command).argumentStart;
  const mixed = commandPart(command[argumentStart]) === "MIXED";
  for (let markerIndex = argumentStart + 1; markerIndex < command.length; markerIndex += 1) {
    const marker = commandPart(command[markerIndex]);
    if (marker === "ITEMS") {
      const width = mixed ? 3 : 2;
      const ids = fixedFlowItemIds(command, width, (itemIndex) =>
        isAutoBatchResourceValue(command[itemIndex])
        && (!mixed || isAutoBatchResourceValue(command[itemIndex + 1]))
        && Buffer.isBuffer(command[itemIndex + width - 1])
      , markerIndex);
      if (ids != null) return ids;
    } else if (marker === "ITEMS_EXT") {
      const ids = extendedCreateManyAutoBatchIds(command, markerIndex);
      if (ids != null) return ids;
    }
  }
  return undefined;
}

function extendedCreateManyAutoBatchIds(
  command: Command,
  markerIndex: number
): readonly (string | Buffer)[] | undefined {
  const itemCount = nonNegativeItemCount(command[markerIndex + 1]);
  if (itemCount == null || itemCount === 0) return undefined;
  const ids: (string | Buffer)[] = [];
  let cursor = markerIndex + 2;
  for (let itemIndex = 0; itemIndex < itemCount; itemIndex += 1) {
    const id = command[cursor];
    if (
      !isAutoBatchResourceValue(id)
      || !isAutoBatchResourceValue(command[cursor + 1])
      || !Buffer.isBuffer(command[cursor + 2])
    ) return undefined;
    ids.push(id);
    cursor += 3;
    const afterValues = skipExtendedNamedItems(command, cursor, true);
    if (afterValues == null) return undefined;
    const afterRefs = skipExtendedNamedItems(command, afterValues, false);
    if (afterRefs == null) return undefined;
    cursor = afterRefs;
  }
  return cursor === command.length ? ids : undefined;
}

function skipExtendedNamedItems(command: Command, countIndex: number, encodedValues: boolean): number | undefined {
  const count = nonNegativeItemCount(command[countIndex]);
  if (count == null) return undefined;
  const firstItem = countIndex + 1;
  if (count > Math.floor((command.length - firstItem) / 2)) return undefined;
  for (let index = 0; index < count; index += 1) {
    const name = command[firstItem + index * 2];
    const value = command[firstItem + index * 2 + 1];
    if (
      !isAutoBatchResourceValue(name)
      || (encodedValues ? !Buffer.isBuffer(value) : !isAutoBatchResourceValue(value))
    ) return undefined;
  }
  return firstItem + count * 2;
}

function runStepsManyAutoBatchIds(command: Command): readonly (string | Buffer)[] | undefined {
  for (let markerIndex = commandView(command).argumentStart; markerIndex + 1 < command.length; markerIndex += 1) {
    if (commandPart(command[markerIndex]) !== "ITEMS") continue;
    const items = command[markerIndex + 1];
    if (!Array.isArray(items) || markerIndex + 2 !== command.length) continue;
    const ids: (string | Buffer)[] = [];
    for (const item of items) {
      if (isAutoBatchResourceValue(item)) {
        ids.push(item);
        continue;
      }
      if (typeof item !== "object" || item == null || Array.isArray(item) || Buffer.isBuffer(item)) return undefined;
      const id = (item as Record<string, unknown>).id;
      if (!isAutoBatchResourceValue(id)) return undefined;
      ids.push(id);
    }
    return ids.length === 0 ? undefined : ids;
  }
  return undefined;
}

function fixedFlowItemIds(
  command: Command,
  width: number,
  validItem: (itemIndex: number) => boolean,
  onlyMarkerIndex?: number
): readonly (string | Buffer)[] | undefined {
  const start = onlyMarkerIndex ?? commandView(command).argumentStart;
  const end = onlyMarkerIndex == null ? command.length : onlyMarkerIndex + 1;
  for (let markerIndex = start; markerIndex < end; markerIndex += 1) {
    if (commandPart(command[markerIndex]) !== "ITEMS") continue;
    const itemCount = command.length - markerIndex - 1;
    if (itemCount <= 0 || itemCount % width !== 0) continue;
    const ids: (string | Buffer)[] = [];
    let valid = true;
    for (let itemIndex = markerIndex + 1; itemIndex < command.length; itemIndex += width) {
      const id = command[itemIndex];
      if (!isAutoBatchResourceValue(id) || !validItem(itemIndex)) {
        valid = false;
        break;
      }
      ids.push(id);
    }
    if (valid) return ids;
  }
  return undefined;
}

function flowValuePutOwner(command: Command, start: number): CommandArgument {
  let owner: CommandArgument;
  for (let index = start; index < command.length; index += 2) {
    const token = commandPart(command[index]);
    if (index + 1 >= command.length || token == null || !flowValuePutOptionTokens.has(token)) return undefined;
    if (token === "OWNER_FLOW_ID") owner = command[index + 1];
  }
  return owner;
}

const flowValuePutOptionTokens = new Set(["NAME", "NOW", "OVERRIDE", "OWNER_FLOW_ID", "PARTITION", "TTL", "TTL_MS"]);

function isAutoBatchResourceValue(value: unknown): value is string | Buffer {
  return typeof value === "string" || Buffer.isBuffer(value);
}

function isFencingToken(value: unknown): boolean {
  return (typeof value === "number" && Number.isSafeInteger(value)) || typeof value === "bigint";
}

function nonNegativeItemCount(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function autoBatchResourceKey(namespace: "flow" | "flow_value" | "kv", value: string | Buffer): string {
  return `${namespace}:${Buffer.from(value).toString("base64")}`;
}

export function autoBatchCommandName(command: Command): string | null {
  return commandView(command).name ?? null;
}
