import { Buffer } from "node:buffer";
import { crc32 } from "./crc32.js";

export const AUTO_PARTITION_PREFIX = "__flow_auto__:";
export const AUTO_PARTITION_BUCKETS = 256;

export function autoPartitionKeyForId(id: string): string {
  return `${AUTO_PARTITION_PREFIX}${crc32(Buffer.from(id)) % AUTO_PARTITION_BUCKETS}`;
}
