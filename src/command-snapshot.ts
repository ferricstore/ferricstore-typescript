import type { Command, CommandArgument } from "./internal.js";

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
  Object.freeze(snapshot);
  commandSnapshots.add(snapshot);
  return snapshot;
}
