import type { Command, CommandArgument } from "./internal.js";
import { snapshotCommandRequestContext } from "./request-context.js";

const commandSnapshots = new WeakSet<readonly CommandArgument[]>();

/** Capture positional command arguments once across asynchronous executor layers. */
export function snapshotCommandArguments(
  args: readonly CommandArgument[],
  sparseMessage = "command arguments must be dense"
): Command {
  if (commandSnapshots.has(args)) return args;
  const snapshot = new Array<CommandArgument>(args.length);
  for (let index = 0; index < args.length; index += 1) {
    if (!Object.hasOwn(args, index)) throw new TypeError(sparseMessage);
    snapshot[index] = args[index];
  }
  // Invocation context is the only command-specific deep snapshot. Keep the
  // overwhelmingly common KV/Flow path to one cheap first-byte check.
  const command = snapshot[0];
  const firstByte = typeof command === "string" ? command.charCodeAt(0) : undefined;
  if (
    firstByte == null
    || firstByte === 73
    || firstByte === 105
  ) snapshotCommandRequestContext(snapshot);
  Object.freeze(snapshot);
  commandSnapshots.add(snapshot);
  return snapshot;
}
