import { Buffer } from "node:buffer";
import type { CommandArgument } from "./internal.js";
import { routingSlotForKey } from "./topology-utilities.js";

const validatedCommands = new WeakSet<readonly CommandArgument[]>();

export function assertAtomicKeyValueCommandSharesSlot(args: readonly CommandArgument[]): void {
  const value = args[0];
  const command = (typeof value === "string"
    ? value
    : Buffer.isBuffer(value)
      ? value.toString("utf8")
      : "").toUpperCase();
  if (command === "MSET" || command === "MSETNX") {
    assertKeyValueCommandSharesSlot(args, command);
  }
}

/** Validate MSET-family keys in one pass without inspecting or copying values. */
export function assertKeyValueCommandSharesSlot(
  args: readonly CommandArgument[],
  command: "MSET" | "MSETNX"
): void {
  if (validatedCommands.has(args) || args.length < 3 || args.length % 2 === 0) return;
  let expectedSlot: number | undefined;
  for (let index = 1; index < args.length; index += 2) {
    const key = args[index];
    if (typeof key !== "string" && !Buffer.isBuffer(key)) return;
    const slot = routingSlotForKey(key);
    expectedSlot ??= slot;
    if (slot !== expectedSlot) {
      throw new TypeError(`${command} keys must share a slot`);
    }
  }
  validatedCommands.add(args);
}
