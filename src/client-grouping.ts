import { autoPartitionKeyForId } from "./internal.js";
import type { CreateItem } from "./types.js";

interface AutoPartitionItemGroup {
  readonly bucket: string;
  readonly indices: number[];
  readonly items: CreateItem[];
}

/** @internal Reject sparse producer arrays before any batch can be dispatched. */
export function assertDenseCreateItems(items: readonly CreateItem[], operation: string): void {
  for (let index = 0; index < items.length; index += 1) {
    if (!Object.hasOwn(items, index) || items[index] == null) {
      throw new TypeError(`${operation} items must be dense`);
    }
  }
}

/** @internal Group auto-partition producer items in one linear pass. */
export function groupAutoPartitionItems(items: readonly CreateItem[]): AutoPartitionItemGroup[] {
  const byBucket = new Map<string, AutoPartitionItemGroup>();
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!Object.hasOwn(items, index) || item == null) {
      throw new TypeError("enqueueMany items must be dense");
    }
    const bucket = autoPartitionKeyForId(item.id);
    const existing = byBucket.get(bucket);
    if (existing == null) {
      byBucket.set(bucket, { bucket, indices: [index], items: [item] });
    } else {
      existing.indices.push(index);
      existing.items.push(item);
    }
  }
  return [...byBucket.values()];
}
