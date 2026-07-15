import type { CommandArgument } from "./internal.js";
import * as core from "./protocol-core.js";
import type { ProtocolCommand, ProtocolRoutingHints } from "./protocol-constants.js";
import { isPlainObject, parseFlowOptions } from "./protocol-flow-options.js";

export function withFlowPartitionRouting(
  command: ProtocolCommand,
  payload: unknown
): ProtocolCommand {
  if (!isPlainObject(payload)) return command;
  const routing: ProtocolRoutingHints = {
    ...(Object.hasOwn(payload, "partition_key")
      ? { flowPartitionKey: payload.partition_key }
      : {}),
    ...(Array.isArray(payload.partition_keys)
      ? { flowPartitionKeys: payload.partition_keys }
      : {})
  };
  return Object.keys(routing).length === 0 ? command : { ...command, routing };
}

export function flowCommandExecWithRouting(
  command: string,
  args: readonly CommandArgument[],
  optionStart: number,
  allowed: ReadonlySet<string>
): ProtocolCommand {
  const fallback = core.commandExec([command, ...args]);
  let hasPartitionOption = false;
  for (let index = optionStart; index < args.length; index += 1) {
    if (core.commandTokenIs(args[index], "PARTITION") || core.commandTokenIs(args[index], "PARTITIONS")) {
      hasPartitionOption = true;
      break;
    }
  }
  if (!hasPartitionOption) return fallback;
  try {
    const options = parseFlowOptions(args, optionStart, args.length, {
      allowed,
      payloadValue: true
    });
    return options == null ? fallback : withFlowPartitionRouting(fallback, options);
  } catch {
    return fallback;
  }
}

export function expandFlowClaimStates(args: readonly CommandArgument[]): CommandArgument[] {
  const expanded: CommandArgument[] = [];
  for (let index = 0; index < args.length; ) {
    if (core.commandTokenIs(args[index], "STATES")) {
      const count = core.safeIntegerNumberArg(args[index + 1]);
      for (let stateIndex = 0; stateIndex < count; stateIndex += 1) {
        expanded.push("STATE", args[index + 2 + stateIndex]);
      }
      index += 2 + count;
    } else {
      expanded.push(args[index]);
      index += 1;
    }
  }
  return expanded;
}
