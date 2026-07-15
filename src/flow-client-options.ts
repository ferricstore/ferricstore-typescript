import type { ValueConfig } from "./types.js";

interface FlowClientOptions {
  readonly type: string;
  readonly valueConfig?: ValueConfig;
}

/** Capture constructor configuration without accepting prototype-provided settings. */
export function snapshotFlowClientOptions<T extends FlowClientOptions>(
  options: T,
  context: string
): T {
  const own = { ...options };
  if (typeof own.type !== "string" || own.type.length === 0) {
    throw new TypeError(`${context} type must be an own non-empty string`);
  }
  const valueConfig = own.valueConfig;
  return Object.freeze({
    ...own,
    ...(valueConfig == null ? {} : { valueConfig: Object.freeze({ ...valueConfig }) })
  });
}
