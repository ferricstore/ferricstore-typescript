import type { CommandArgument } from "./internal.js";
import * as wire from "./protocol-constants.js";
import { parseFlowOptions } from "./protocol-flow-options.js";

export function flowSignalPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 3) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set([
      "SIGNAL", "PARTITION", "IDEMPOTENCY", "IF_STATE", "TRANSITION_TO", "RUN_AT", "NOW",
      "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE"
    ]),
    required: new Set(["SIGNAL"])
  });
  if (options == null) return undefined;
  return { opcode: wire.OPCODES.flowSignal, payload: { id: args[0], ...options } };
}
