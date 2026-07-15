import { Buffer } from "node:buffer";
import { FerricStoreError, classifyServerError } from "./errors.js";
import { asText } from "./protocol-core.js";
import * as wire from "./protocol-constants.js";
import { tryDecodeCompactResponse } from "./protocol-compact-response.js";
import { decodeValue } from "./protocol-value.js";

export function decodeResponse(
  frame: wire.ResponseFrame,
  expectedOpcode: number,
  hints: wire.ResponseDecodeHints = {}
): unknown {
  if (frame.opcode !== expectedOpcode) {
    throw new FerricStoreError(
      `protocol response mismatch: expected opcode ${expectedOpcode}, got ${frame.opcode}`,
      { raw: frame }
    );
  }
  if ((frame.flags & wire.FLAG_COMPRESSED) !== 0) {
    throw new FerricStoreError("compressed native protocol responses are not supported by this SDK yet");
  }
  if (frame.body.byteLength < 2) {
    throw new FerricStoreError("short native protocol response body", { raw: frame.body });
  }
  const status = frame.body.readUInt16BE(0);
  const body = frame.body.subarray(2);
  const value = decodeResponseValue(frame.opcode, body, hints);
  if (status === wire.STATUS_OK) return value;
  const message = errorMessage(status, value);
  throw classifyServerError(message, value, undefined, status);
}

export function unwrapPipelineResponse(
  value: unknown,
  options: { readonly throwOnItemError?: boolean } = {},
  expectedItems?: number
): unknown[] {
  if (!Array.isArray(value)) {
    throw new FerricStoreError("native pipeline returned an invalid response");
  }
  const items = value as unknown[];
  if (expectedItems != null && items.length !== expectedItems) {
    throw new FerricStoreError(
      `native pipeline returned ${items.length} items; expected ${expectedItems} items`
    );
  }

  if ((items as unknown as Record<symbol, unknown>)[wire.COMPACT_PIPELINE_DECODED] === true) {
    if (options.throwOnItemError !== false) {
      for (const item of items) if (item instanceof Error) throw item;
    }
    return items;
  }

  let hasStatusTuple = false;
  for (const item of items) {
    if (Array.isArray(item) && item.length >= 2 && pipelineStatus(item[0]) != null) {
      hasStatusTuple = true;
      break;
    }
  }
  if (!hasStatusTuple) return items;

  const out = new Array<unknown>(items.length);
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (Array.isArray(item) && item.length >= 2) {
      const status = pipelineStatus(item[0]);
      if (status != null) {
        const payload: unknown = item[1];
        if (status === "ok") {
          out[index] = payload;
          continue;
        }
        const error = classifyServerError(
          errorMessage(status === "busy" ? 4 : 1, payload),
          payload,
          undefined,
          status
        );
        if (options.throwOnItemError !== false) throw error;
        out[index] = error;
        continue;
      }
    }
    out[index] = item;
  }
  return out;
}

function pipelineStatus(value: unknown): "busy" | "error" | "ok" | null {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) return null;
  const status = asText(value).toLowerCase();
  return status === "ok" || status === "busy" || status === "error" ? status : null;
}

function decodeResponseValue(opcode: number, body: Buffer, hints: wire.ResponseDecodeHints): unknown {
  const compact = tryDecodeCompactResponse(opcode, body, hints);
  if (compact.found) return compact.value;
  const decoded = decodeValue(body);
  if (decoded.offset !== body.byteLength) {
    throw new FerricStoreError("native protocol response has trailing bytes", { raw: body });
  }
  return decoded.value;
}

function errorMessage(status: number | string, value: unknown): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  if (typeof value === "object" && value != null && Object.hasOwn(value, "message")) {
    const message = (value as Record<string, unknown>).message;
    return typeof message === "string" ? message : String(message);
  }
  return `ERR native request failed status=${status}: ${String(value)}`;
}
