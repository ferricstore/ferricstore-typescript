import { isUtf8, type Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { setOwnValue } from "./protocol-core.js";

/** Decode a wire map key without replacement-character collisions or duplicate loss. */
export function setProtocolMapEntry<T>(
  target: Record<string, T>,
  encodedKey: Buffer | string,
  value: T
): void {
  if (typeof encodedKey === "string" && !encodedKey.isWellFormed()) {
    throw new FerricStoreError("native protocol map key must be valid UTF-8");
  }
  if (typeof encodedKey !== "string" && !isUtf8(encodedKey)) {
    throw new FerricStoreError("native protocol map key must be valid UTF-8");
  }
  const key = typeof encodedKey === "string" ? encodedKey : encodedKey.toString("utf8");
  if (Object.hasOwn(target, key)) {
    throw new FerricStoreError(`native protocol response contains duplicate map key ${JSON.stringify(key)}`);
  }
  setOwnValue(target, key, value);
}
