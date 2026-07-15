import type { CommandArgument } from "./internal.js";
import * as core from "./protocol-core.js";

export function findItemToken(args: readonly CommandArgument[], start: number): number {
  for (let index = start; index < args.length; ) {
    const token = core.asText(args[index]).toUpperCase();
    if (token === "ITEMS" || token === "ITEMS_EXT") return index;
    if (
      token === "ATTRIBUTE" || token === "ATTRIBUTE_MERGE" ||
      token === "VALUE" || token === "VALUE_REF" || token === "STATE_META"
    ) {
      index += 3;
    } else if (token === "ATTRIBUTE_DELETE") {
      index += 2;
    } else if (
      token === "IDEMPOTENT" ||
      token === "INDEPENDENT" ||
      token === "RECLAIM_EXPIRED" ||
      token === "INCLUDE_STATE" ||
      token === "FULL" ||
      token === "OVERRIDE"
    ) {
      index += index + 1 < args.length && core.isBoolToken(args[index + 1]) ? 2 : 1;
    } else if (token === "STATES" || token === "PARTITIONS") {
      const count = core.safeIntegerNumberArg(args[index + 1]);
      if (count < 0) return -1;
      index += 2 + count;
    } else if (token === "NOPAYLOAD") {
      index += 1;
    } else {
      index += 2;
    }
  }
  return -1;
}

export function parseFlowCreateItemsExt(
  values: readonly CommandArgument[],
  count: number,
  mixed: boolean
): Record<string, unknown>[] | undefined {
  const items: Record<string, unknown>[] = [];
  let index = 0;
  for (let itemIndex = 0; itemIndex < count; itemIndex += 1) {
    if (index + 3 > values.length) return undefined;
    const item: Record<string, unknown> = { id: values[index], payload: values[index + 2] };
    const partition = values[index + 1];
    if (!core.commandTokenIs(partition, "-") && (mixed || partition != null)) {
      item.partition_key = partition;
    }
    index += 3;

    const itemValues = parseNamedCountMap(values, index);
    if (itemValues == null) return undefined;
    index = itemValues.next;
    if (Object.keys(itemValues.value).length > 0) item.values = itemValues.value;

    const itemRefs = parseNamedCountMap(values, index);
    if (itemRefs == null) return undefined;
    index = itemRefs.next;
    if (Object.keys(itemRefs.value).length > 0) item.value_refs = itemRefs.value;
    items.push(item);
  }
  return index === values.length ? items : undefined;
}

export function parseFlowSpawnChildrenExt(
  values: readonly CommandArgument[],
  count: number
): Record<string, unknown>[] | undefined {
  const children: Record<string, unknown>[] = [];
  let index = 0;
  for (let childIndex = 0; childIndex < count; childIndex += 1) {
    if (index + 4 > values.length) return undefined;
    const child: Record<string, unknown> = {
      id: values[index],
      type: values[index + 2],
      payload: values[index + 3]
    };
    const partition = values[index + 1];
    if (!core.commandTokenIs(partition, "-")) child.partition_key = partition;
    index += 4;

    const childValues = parseNamedCountMap(values, index);
    if (childValues == null) return undefined;
    index = childValues.next;
    if (Object.keys(childValues.value).length > 0) child.values = childValues.value;

    const childRefs = parseNamedCountMap(values, index);
    if (childRefs == null) return undefined;
    index = childRefs.next;
    if (Object.keys(childRefs.value).length > 0) child.value_refs = childRefs.value;
    children.push(child);
  }
  return index === values.length ? children : undefined;
}

export function parseFlowSpawnChildren(
  values: readonly CommandArgument[],
  mixed: boolean
): Record<string, unknown>[] | undefined {
  const width = mixed ? 4 : 3;
  if (values.length === 0 || values.length % width !== 0) return undefined;
  const children: Record<string, unknown>[] = [];
  for (let index = 0; index < values.length; index += width) {
    const child: Record<string, unknown> = {
      id: values[index],
      type: values[index + (mixed ? 2 : 1)],
      payload: values[index + (mixed ? 3 : 2)]
    };
    if (mixed) child.partition_key = values[index + 1];
    children.push(child);
  }
  return children;
}

export function parseNamedCountMap(
  values: readonly CommandArgument[],
  start: number
): { readonly next: number; readonly value: Record<string, unknown> } | undefined {
  if (start >= values.length) return undefined;
  const count = core.safeIntegerNumberArg(values[start]);
  if (count < 0 || start + 1 + count * 2 > values.length) return undefined;
  const value: Record<string, unknown> = {};
  let index = start + 1;
  for (let itemIndex = 0; itemIndex < count; itemIndex += 1) {
    core.setOwnValue(value, core.asText(values[index]), values[index + 1]);
    index += 2;
  }
  return { next: index, value };
}
