import type { Codec } from "./codecs.js";
import { encodeFlowValue } from "./flow-value-snapshot.js";
import type { CommandArgument } from "./internal.js";

export function appendNamedValues(
  args: CommandArgument[],
  codec: Codec,
  options: {
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: readonly string[];
    overrideValues?: readonly string[];
  }
): void {
  for (const [name, value] of Object.entries(options.values ?? {})) {
    args.push("VALUE", name, encodeFlowValue(codec, value));
  }
  for (const [name, ref] of Object.entries(options.valueRefs ?? {})) {
    if (typeof ref !== "string") throw new TypeError("valueRefs must contain string references");
    args.push("VALUE_REF", name, ref);
  }
  appendStringArguments(args, "DROP_VALUE", options.dropValues, "dropValues");
  appendStringArguments(args, "OVERRIDE_VALUE", options.overrideValues, "overrideValues");
}

export function appendValueReturn(
  args: CommandArgument[],
  options: { values?: readonly string[]; valueMaxBytes?: number }
): void {
  appendStringArguments(args, "VALUE", options.values, "Flow value names");
  if (options.valueMaxBytes != null) args.push("VALUE_MAX_BYTES", options.valueMaxBytes);
}

export function appendAttributeMutations(
  args: CommandArgument[],
  options: {
    attributesMerge?: Record<string, CommandArgument>;
    attributesDelete?: readonly string[];
  }
): void {
  for (const [name, value] of Object.entries(options.attributesMerge ?? {})) {
    args.push("ATTRIBUTE_MERGE", name, value);
  }
  appendStringArguments(args, "ATTRIBUTE_DELETE", options.attributesDelete, "attributesDelete");
}

function appendStringArguments(
  args: CommandArgument[],
  token: string,
  values: readonly string[] | undefined,
  label: string
): void {
  if (values == null) return;
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!Object.hasOwn(values, index) || typeof value !== "string") {
      const requirement = label === "Flow value names" ? "dense strings" : "a dense array of strings";
      throw new TypeError(`${label} must be ${requirement}`);
    }
    args.push(token, value);
  }
}
