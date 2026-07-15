import { Buffer } from "node:buffer";
import type { FerricStoreClient } from "./client.js";
import { FerricStoreError } from "./errors.js";
import { append, type CommandArgument } from "./internal.js";

interface ValueMGetEntry {
  readonly found: boolean;
  readonly value?: unknown;
}

/** @internal */
export async function valueMGetEntries(
  client: FerricStoreClient,
  refs: readonly string[],
  options: { readonly maxBytes?: number } = {}
): Promise<ValueMGetEntry[]> {
  if (refs.length === 0) {
    return [];
  }
  const args = new Array<CommandArgument>(1 + refs.length);
  args[0] = "FLOW.VALUE.MGET";
  for (let index = 0; index < refs.length; index += 1) {
    if (!Object.hasOwn(refs, index)) {
      throw new TypeError("FLOW.VALUE.MGET refs must be dense");
    }
    args[index + 1] = refs[index];
  }
  append(args, "MAX_BYTES", options.maxBytes);
  const response = await client.commandArgs(args);
  if (!Array.isArray(response)) {
    throw new FerricStoreError("FLOW.VALUE.MGET returned an invalid response", { raw: response });
  }
  if (response.length !== refs.length) {
    throw new FerricStoreError(
      `FLOW.VALUE.MGET response length ${response.length} did not match request length ${refs.length}`,
      { raw: response }
    );
  }
  const entries = new Array<ValueMGetEntry>(response.length);
  for (let index = 0; index < response.length; index += 1) {
    if (!Object.hasOwn(response, index)) {
      throw new FerricStoreError(`FLOW.VALUE.MGET response item ${index} is missing`, {
        raw: response
      });
    }
    const item: unknown = response[index];
    if (item == null) {
      entries[index] = { found: false };
    } else if (Buffer.isBuffer(item)) {
      entries[index] = { found: true, value: client.codec.decode(item) };
    } else if (item instanceof Uint8Array) {
      entries[index] = { found: true, value: client.codec.decode(Buffer.from(item)) };
    } else if (typeof item === "string") {
      entries[index] = { found: true, value: client.codec.decode(Buffer.from(item)) };
    } else {
      entries[index] = { found: true, value: item };
    }
  }
  return entries;
}
