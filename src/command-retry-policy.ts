import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";

/** CAS mutations must surface their first outcome and are never automatically replayed. */
export function isCasMutation(args: readonly CommandArgument[]): boolean {
  const offset = commandName(args[0]) === "COMMAND_EXEC" ? 1 : 0;
  const name = commandName(args[offset]);
  if (name === "CAS") return true;
  if (name !== "FLOW.POLICY.SET") return false;
  for (let index = offset + 2; index < args.length; index += 2) {
    if (!Object.hasOwn(args, index)) return false;
    if (commandName(args[index]) === "EXPECTED_GENERATION") return true;
  }
  return false;
}

function commandName(value: unknown): string | undefined {
  if (typeof value === "string") return value.toUpperCase();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase();
  }
  return undefined;
}
