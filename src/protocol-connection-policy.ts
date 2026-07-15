import { InvalidCommandError } from "./errors.js";
import type { CommandArgument } from "./internal.js";
import {
  connectionPinnedCommands,
  connectionStateMutationCommands
} from "./command-metadata.js";
import {
  commandName,
  isConnectionBlockingCommand
} from "./protocol-core.js";

/** Reject commands whose correctness depends on retaining exclusive socket state. */
export function assertCommandDoesNotRequirePinnedConnection(
  args: readonly CommandArgument[]
): void {
  const outer = commandName(args[0]);
  assertNormalizedCommandDoesNotRequirePinnedConnection(args, outer);
}

/** Return whether a command depends on connection-local state or an exclusive session. */
export function commandRequiresDedicatedConnection(
  args: readonly CommandArgument[],
  normalizedOuter: string | undefined = commandName(args[0])
): boolean {
  const command = connectionScopedCommandName(args, normalizedOuter);
  return command != null
    && (connectionPinnedCommands.has(command) || connectionStateMutationCommands.has(command));
}

/** Return whether a normalized command changes state attached to one socket. */
export function commandMutatesConnectionState(
  args: readonly CommandArgument[]
): boolean {
  const command = connectionScopedCommandName(args, commandName(args[0]));
  return command != null && connectionStateMutationCommands.has(command);
}

function connectionScopedCommandName(
  args: readonly CommandArgument[],
  normalizedOuter: string | undefined
): string | undefined {
  let index = 0;
  let command = normalizedOuter;
  if (command === "COMMAND_EXEC") {
    index += 1;
    command = commandName(args[index]);
  }
  if (command === "CLIENT" && commandName(args[index + 1]) === "SETNAME") {
    return "CLIENT.SETNAME";
  }
  return command;
}

export function assertNormalizedCommandDoesNotRequirePinnedConnection(
  args: readonly CommandArgument[],
  outer: string | undefined
): void {
  const command = connectionScopedCommandName(args, outer);
  if (command != null && connectionPinnedCommands.has(command)) {
    throw new InvalidCommandError(
      `${command} requires a pinned connection session, which the multiplexed native client does not support`
    );
  }
}

/** Reject connection-local state mutations on pooled or reconnecting executors. */
export function assertCommandHasStableConnectionState(
  args: readonly CommandArgument[]
): void {
  const command = connectionScopedCommandName(args, commandName(args[0]));
  if (command != null && connectionStateMutationCommands.has(command)) {
    const display = command.replaceAll(".", " ");
    throw new InvalidCommandError(
      `${display} requires a stable single connection; configure connection state when creating reconnecting or topology clients`
    );
  }
}

/** @internal Connection-scoped and blocking fallbacks must execute in order. */
export function commandRequiresOrderedPipelineExecution(
  args: readonly CommandArgument[]
): boolean {
  return commandRequiresDedicatedConnection(args) || isConnectionBlockingCommand(args);
}
