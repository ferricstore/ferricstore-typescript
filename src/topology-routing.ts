import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import {
  flowApprovalIdCommands,
  flowGovernanceScopeCommands,
  flowScheduleCommands,
  flowStateIdCommands,
  typeScopedFlowCommands
} from "./command-metadata.js";
import {
  commandToken,
  commandView,
  routingKeyArguments,
  splitFlowValueMGetArguments
} from "./command-grammar.js";
import { FerricStoreError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import type { RoutingRoute } from "./routing-topology.js";
import type { RoutedCommandData, RoutedKeyGroup } from "./topology-execution-types.js";
import {
  buildProtocolCommand,
  OPCODES,
  type ProtocolCommand
} from "./protocol.js";
import {
  crc32,
  crc32Utf8,
  getField,
  isPlainObject,
  routingSlotForKey
} from "./topology-utilities.js";

type RoutingKeyLookup =
  | { readonly handled: false }
  | { readonly handled: true; readonly key?: string | Buffer };

export interface FlowRoutingData {
  readonly command?: ProtocolCommand;
  readonly key: string | Buffer;
}

const FLOW_PARTITION_ROUTE_CACHE_MAX = 1_024;
const flowPartitionRouteCache = new Map<string, string>();

export function routingKeyFromProtocolPayload(
  name: string | undefined,
  command: ProtocolCommand
): string | Buffer | undefined {
  if (command.opcode < 0x0100 || name === "CLUSTER.KEYSLOT" || name === "SHARDS" || name === "ROUTE") {
    return undefined;
  }

  if (isPlainObject(command.payload)) {
    for (const field of [
      "key", "partition_key", "id", "owner_flow_id", "parent_flow_id", "root_flow_id", "correlation_id", "scope"
    ]) {
      const value = getField(command.payload, field);
      if (typeof value === "string" || Buffer.isBuffer(value)) {
        return value;
      }
    }

    const keys = getField(command.payload, "keys");
    if (Array.isArray(keys)) {
      return singleShardKey(keys);
    }

    const pairsValue = getField(command.payload, "pairs");
    if (Array.isArray(pairsValue)) {
      return singleShardKey(
        pairsValue
          .filter((pair): pair is readonly unknown[] => Array.isArray(pair) && pair.length > 0)
          .map((pair) => pair[0])
      );
    }
  }

  return undefined;
}

export function routingKeyFromArgs(
  name: string | undefined,
  args: readonly CommandArgument[]
): RoutingKeyLookup {
  const routed = routingKeyArguments(name, args);
  return routed.handled
    ? { handled: true, key: singleShardKey(routed.keys) }
    : { handled: false };
}

export function flowRoutingData(name: string, args: readonly CommandArgument[]): FlowRoutingData | undefined {
  const parsedCommand = commandView(args);
  const flowArgs = args.slice(parsedCommand.argumentStart);
  if (flowArgs.length === 0) {
    return undefined;
  }

  if (name === "FLOW.VALUE.MGET") {
    const { refs } = splitFlowValueMGetArguments(args, parsedCommand.argumentStart);
    return flowRoutingResult(singleShardKey(refs));
  }

  // Schedule storage uses :erlang.phash2/2. There is no stable, portable
  // JavaScript implementation of that VM-specific hash, so schedule commands
  // intentionally use the control path and let the server route them.
  if (flowScheduleCommands.has(name)) {
    return undefined;
  }

  if (flowApprovalIdCommands.has(name)) {
    const id = flowArgs[0];
    return flowRoutingResult(flowLogicalPartitionRoutingKey(id));
  }

  if (flowGovernanceScopeCommands.has(name)) {
    const scope = flowArgs[0];
    return flowRoutingResult(flowLogicalPartitionRoutingKey(scope));
  }

  if (name === "FLOW.CREATE_MANY" || name === "FLOW.COMPLETE_MANY" || name === "FLOW.TRANSITION_MANY" || name === "FLOW.RETRY_MANY" || name === "FLOW.FAIL_MANY" || name === "FLOW.CANCEL_MANY") {
    const partition = flowArgs[0];
    if (typeof partition === "string" && partition.toUpperCase() !== "AUTO" && partition.toUpperCase() !== "MIXED") {
      return flowRoutingResult(flowLogicalPartitionRoutingKey(partition));
    }
    if (Buffer.isBuffer(partition)) {
      const text = partition.toString("utf8").toUpperCase();
      if (text !== "AUTO" && text !== "MIXED") {
        return flowRoutingResult(flowLogicalPartitionRoutingKey(partition));
      }
    }
    return undefined;
  }

  const hasPartitionOption = flowArgs.some((arg) => {
    const token = commandPart(arg);
    return token === "PARTITION" || token === "PARTITIONS";
  });
  let protocolCommand: ProtocolCommand | undefined;
  if (hasPartitionOption || name === "FLOW.VALUE.PUT") {
    try {
      protocolCommand = buildProtocolCommand(args, Number.MAX_SAFE_INTEGER, false);
    } catch {
      return undefined;
    }
  }

  if (hasPartitionOption) {
    const partition = flowPartitionRoutingKeyFromCommand(
      protocolCommand,
      name === "FLOW.CLAIM_DUE" || name === "FLOW.RECLAIM"
    );
    if (!partition.handled) {
      // If the protocol grammar cannot parse the command, use the control path
      // instead of guessing option widths and risking a wrong-shard route.
      return undefined;
    }
    // Claim requests may use the compact custom-binary payload. Route from the
    // parsed generic form, then let the selected adapter rebuild with its real
    // frame limit so the hot path keeps compact encoding and safe preflight.
    return flowRoutingResult(
      partition.key,
      name === "FLOW.CLAIM_DUE" ? undefined : protocolCommand
    );
  }

  if (name === "FLOW.VALUE.PUT") {
    if (protocolCommand == null || protocolCommand.opcode === OPCODES.commandExec || !isPlainObject(protocolCommand.payload)) {
      return undefined;
    }
    const owner = getField(protocolCommand.payload, "owner_flow_id");
    const valueName = getField(protocolCommand.payload, "name");
    return isRoutingKey(owner) && isRoutingKey(valueName)
      ? flowRoutingResult(flowAutoIdRoutingKey(owner), protocolCommand)
      : undefined;
  }

  if (typeScopedFlowCommands.has(name)) {
    return undefined;
  }

  if (!flowStateIdCommands.has(name)) {
    return undefined;
  }

  return flowRoutingResult(flowAutoIdRoutingKey(flowArgs[0]), protocolCommand);
}

function flowRoutingResult(
  key: string | Buffer | undefined,
  command?: ProtocolCommand
): FlowRoutingData | undefined {
  if (key == null) return undefined;
  return command == null ? { key } : { command, key };
}

function flowPartitionRoutingKeyFromCommand(
  command: ProtocolCommand | undefined,
  claim: boolean
): RoutingKeyLookup {
  if (command == null) {
    return { handled: false };
  }
  if (command.routing != null && Object.hasOwn(command.routing, "flowPartitionKey")) {
    const partition = command.routing.flowPartitionKey;
    return {
      handled: true,
      key: claim
        ? flowClaimLogicalPartitionRoutingKey(partition)
        : flowLogicalPartitionRoutingKey(partition)
    };
  }
  if (command.routing != null && Object.hasOwn(command.routing, "flowPartitionKeys")) {
    const partitions = command.routing.flowPartitionKeys;
    return {
      handled: true,
      key: Array.isArray(partitions)
        ? claim
          ? singleShardFlowClaimPartitionKey(partitions)
          : singleShardFlowPartitionKey(partitions)
        : undefined
    };
  }
  if (command.opcode === OPCODES.commandExec || !isPlainObject(command.payload)) {
    return { handled: false };
  }
  if (Object.hasOwn(command.payload, "partition_key")) {
    const partition = getField(command.payload, "partition_key");
    return {
      handled: true,
      key: claim
        ? flowClaimLogicalPartitionRoutingKey(partition)
        : flowLogicalPartitionRoutingKey(partition)
    };
  }
  if (Object.hasOwn(command.payload, "partition_keys")) {
    const partitions = getField(command.payload, "partition_keys");
    return {
      handled: true,
      key: Array.isArray(partitions)
        ? claim
          ? singleShardFlowClaimPartitionKey(partitions)
          : singleShardFlowPartitionKey(partitions)
        : undefined
    };
  }
  return { handled: false };
}

function isRoutingKey(value: unknown): value is string | Buffer {
  return typeof value === "string" || Buffer.isBuffer(value);
}

function flowLogicalPartitionRoutingKey(value: unknown): string | undefined {
  if (typeof value !== "string" && !Buffer.isBuffer(value)) {
    return undefined;
  }
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
  const text = bytes.toString("utf8");
  const autoMatch = /^__flow_auto__:(0|[1-9]\d{0,2})$/u.exec(text);
  if (autoMatch != null) {
    const bucket = Number(autoMatch[1]);
    if (bucket < 256) {
      return `{fa:${bucket}}`;
    }
  }
  const cacheKey = `${Buffer.isBuffer(value) ? "b" : "s"}:${bytes.toString("base64")}`;
  const cached = flowPartitionRouteCache.get(cacheKey);
  if (cached != null) {
    flowPartitionRouteCache.delete(cacheKey);
    flowPartitionRouteCache.set(cacheKey, cached);
    return cached;
  }

  const digest = createHash("sha256").update(bytes).digest("base64url");
  const routeKey = `{f:${digest}}`;
  if (flowPartitionRouteCache.size >= FLOW_PARTITION_ROUTE_CACHE_MAX) {
    const oldest = flowPartitionRouteCache.keys().next().value;
    if (oldest != null) flowPartitionRouteCache.delete(oldest);
  }
  flowPartitionRouteCache.set(cacheKey, routeKey);
  return routeKey;
}

function flowAutoIdRoutingKey(value: unknown): string | undefined {
  if (typeof value !== "string" && !Buffer.isBuffer(value)) {
    return undefined;
  }
  const bucket = (Buffer.isBuffer(value) ? crc32(value) : crc32Utf8(value)) & 0xff;
  return `{fa:${bucket}}`;
}

function flowClaimLogicalPartitionRoutingKey(value: unknown): string | undefined {
  if (typeof value !== "string" && !Buffer.isBuffer(value)) return undefined;
  const selector = commandPart(value);
  if (selector === "AUTO" || selector === "ANY") return undefined;
  if (selector === "GLOBAL") return "{f}";
  return flowLogicalPartitionRoutingKey(value);
}

function singleShardFlowPartitionKey(values: readonly unknown[]): string | Buffer | undefined {
  const keys = values.map((value) => flowLogicalPartitionRoutingKey(value));
  return keys.some((key) => key == null)
    ? undefined
    : singleShardKey(keys);
}

function singleShardFlowClaimPartitionKey(values: readonly unknown[]): string | Buffer | undefined {
  const keys = values.map((value) => flowClaimLogicalPartitionRoutingKey(value));
  return keys.some((key) => key == null) ? undefined : singleShardKey(keys);
}

function singleShardKey(keys: readonly unknown[]): string | Buffer | undefined {
  if (
    keys.length === 0 ||
    keys.some((key) => typeof key !== "string" && !Buffer.isBuffer(key))
  ) {
    return undefined;
  }
  const usable = keys as readonly (string | Buffer)[];
  const first = usable[0];
  if (first == null) {
    return undefined;
  }
  const firstSlot = routingSlotForKey(first);
  return usable.every((key) => routingSlotForKey(key) === firstSlot) ? first : undefined;
}

export function commandName(args: readonly CommandArgument[]): string | undefined {
  return commandView(args).name;
}

export const commandPart = commandToken;

export function routedCommandArgs(
  name: string,
  entries: readonly { readonly key: string | Buffer }[],
  suffix: readonly CommandArgument[] = []
): CommandArgument[] {
  const args = new Array<CommandArgument>(1 + entries.length + suffix.length);
  args[0] = name;
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    if (entry == null) throw new FerricStoreError("routed command contained an empty key entry");
    args[index + 1] = entry.key;
  }
  for (let index = 0; index < suffix.length; index += 1) {
    args[1 + entries.length + index] = suffix[index];
  }
  return args;
}

export function routedKeyGroups(
  keys: readonly CommandArgument[],
  routeKey: (key: string | Buffer) => RoutingRoute
): RoutedKeyGroup[] | undefined {
  if (keys.length === 0) return undefined;
  const groups = new Map<string, RoutedKeyGroup>();
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (typeof key !== "string" && !Buffer.isBuffer(key)) return undefined;
    const route = routeKey(key);
    const groupKey = `${route.endpointKey}\0${route.laneId}`;
    const group = groups.get(groupKey);
    const entry = { index, key };
    if (group == null) groups.set(groupKey, { entries: [entry], route });
    else group.entries.push(entry);
  }
  return [...groups.values()];
}

export function singleRouteForCommands(
  commands: readonly Command[],
  routeData: (command: Command) => RoutedCommandData | undefined
): RoutingRoute | undefined {
  let route: RoutingRoute | undefined;
  for (const command of commands) {
    const current = routeData(command)?.route;
    if (current == null) return undefined;
    if (route == null) route = current;
    else if (route.endpointKey !== current.endpointKey || route.laneId !== current.laneId) return undefined;
  }
  return route;
}
