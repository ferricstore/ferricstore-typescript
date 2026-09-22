import type { CommandArgument } from "./internal.js";
import * as core from "./protocol-core.js";

export const MAX_FLOW_BLOCK_MS = 4_294_967_295;
export const FLOW_BLOCK_MS_ERROR = "blockMs must be a safe non-negative integer no greater than 4294967295";

export function validateFlowBlockMs(value: unknown): void {
  if (
    value != null &&
    (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > MAX_FLOW_BLOCK_MS)
  ) {
    throw new TypeError(FLOW_BLOCK_MS_ERROR);
  }
}

export function validateRawFlowBlockMs(value: CommandArgument): void {
  let parsed: number | bigint;
  try {
    parsed = core.integerArg(value);
  } catch {
    throw new TypeError(FLOW_BLOCK_MS_ERROR);
  }
  validateFlowBlockMs(parsed);
}

export function validateRawFlowClaimBlockOptions(args: readonly CommandArgument[]): void {
  for (let index = 1; index < args.length; ) {
    const token = core.asText(args[index]).toUpperCase();
    switch (token) {
      case "BLOCK":
      case "BLOCK_MS":
        validateRawFlowBlockMs(args[index + 1]);
        index += 2;
        break;
      case "STATE":
      case "PARTITION":
      case "WORKER":
      case "LEASE_MS":
      case "LIMIT":
      case "PRIORITY":
      case "NOW":
      case "RECLAIM_RATIO":
      case "RETURN":
      case "MAXBYTES":
      case "VALUE":
      case "VALUE_MAX_BYTES":
        index += 2;
        break;
      case "STATES":
      case "PARTITIONS": {
        const count = Number(args[index + 1]);
        const valuesStart = index + 2;
        if (
          !Number.isSafeInteger(count) ||
          count < 0 ||
          count > args.length - valuesStart
        ) {
          validateRawFlowClaimBlockFallback(args, valuesStart);
          return;
        }
        index = valuesStart + count;
        break;
      }
      case "RECLAIM_EXPIRED":
      case "INCLUDE_STATE":
        index += index + 1 < args.length && core.isBoolToken(args[index + 1]) ? 2 : 1;
        break;
      case "PAYLOAD":
      case "NOPAYLOAD":
        index += 1;
        break;
      default:
        validateRawFlowClaimBlockFallback(args, index);
        return;
    }
  }
}

function validateRawFlowClaimBlockFallback(args: readonly CommandArgument[], start: number): void {
  // The unsupported tail has no reliable option arity. Every marker is treated as
  // an option so malformed commands fail closed rather than bypassing validation.
  for (let index = start; index < args.length; index += 1) {
    const token = core.asText(args[index]).toUpperCase();
    if (token === "BLOCK" || token === "BLOCK_MS") validateRawFlowBlockMs(args[index + 1]);
  }
}
