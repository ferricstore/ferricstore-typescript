import type { CommandArgument } from "./internal.js";
import { setOwnValue } from "./protocol-core.js";

export function setFlowOptionValue(
  payload: Record<string, unknown>,
  field: string,
  value: CommandArgument,
  repeat: boolean
): void {
  if (!repeat || !Object.hasOwn(payload, field)) {
    setOwnValue(payload, field, value);
    return;
  }
  const previous = payload[field];
  setOwnValue(
    payload,
    field,
    Array.isArray(previous) ? [...(previous as unknown[]), value] : [previous, value]
  );
}
