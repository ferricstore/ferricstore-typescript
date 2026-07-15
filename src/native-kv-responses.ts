import { Buffer } from "node:buffer";
import type { Codec } from "./codecs.js";
import { bytes, integer, normalizeRefMeta, setOwnValue, text, toStringKeyMap } from "./internal.js";
import type { FetchOrComputeResult, KeyInfo, RateLimitResult } from "./types.js";

export function rateLimitResultFromResp(value: unknown): RateLimitResult {
  if (!Array.isArray(value) || value.length !== 4) {
    throw new TypeError("RATELIMIT.ADD returned an unexpected response");
  }
  const status = requiredResponseString(value[0], "RATELIMIT.ADD");
  if (status !== "allowed" && status !== "denied") {
    throw new TypeError("RATELIMIT.ADD returned an unexpected response");
  }
  return {
    status,
    count: requiredNonNegativeInteger(value[1], "RATELIMIT.ADD"),
    remaining: requiredNonNegativeInteger(value[2], "RATELIMIT.ADD"),
    resetMs: requiredNonNegativeInteger(value[3], "RATELIMIT.ADD"),
    allowed: status === "allowed"
  };
}

export function keyInfoFromResp(value: unknown): KeyInfo {
  const raw = keyInfoMap(value);
  return {
    type: requiredResponseString(raw.type, "FERRICSTORE.KEY_INFO"),
    valueSize: requiredNonNegativeInteger(raw.value_size, "FERRICSTORE.KEY_INFO"),
    ttlMs: requiredInteger(raw.ttl_ms, "FERRICSTORE.KEY_INFO"),
    hotCacheStatus: requiredResponseString(raw.hot_cache_status, "FERRICSTORE.KEY_INFO"),
    lastWriteShard: requiredNonNegativeInteger(raw.last_write_shard, "FERRICSTORE.KEY_INFO"),
    raw
  };
}

export function fetchOrComputeResultFromResp<T = unknown>(
  value: unknown,
  codec: Codec
): FetchOrComputeResult<T> {
  if (!Array.isArray(value) || value.length < 2 || value.length > 3) {
    throw new TypeError("FETCH_OR_COMPUTE returned an unexpected response");
  }
  const status = requiredResponseString(value[0], "FETCH_OR_COMPUTE");
  if (status === "hit") {
    if (value.length !== 2) throw new TypeError("FETCH_OR_COMPUTE returned an unexpected response");
    return {
      computeMode: "hit",
      status,
      value: decodePayload(codec, value[1]) as T | null,
      hit: true,
      shouldCompute: false
    };
  }
  if (status !== "compute") throw new TypeError("FETCH_OR_COMPUTE returned an unexpected response");
  return {
    status,
    computeHint: responseBytes(value[1], "FETCH_OR_COMPUTE"),
    ...(value.length === 3 ? {
      computeMode: "fenced" as const,
      computeToken: responseBytes(value[2], "FETCH_OR_COMPUTE")
    } : {
      computeMode: "legacy" as const,
      computeToken: null
    }),
    hit: false,
    shouldCompute: true
  };
}

function decodePayload(codec: Codec, value: unknown): unknown {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return codec.decode(value);
  if (value instanceof Uint8Array) return codec.decode(Buffer.from(value));
  if (typeof value === "string") return codec.decode(Buffer.from(value));
  return normalizeRefMeta(value);
}

function requiredResponseString(value: unknown, context: string): string {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  const result = text(value);
  if (result.length === 0) throw new TypeError(`${context} returned an unexpected response`);
  return result;
}

function requiredInteger(value: unknown, context: string): number {
  if (value == null || value === "") throw new TypeError(`${context} returned an unexpected response`);
  return integer(value);
}

function requiredNonNegativeInteger(value: unknown, context: string): number {
  const result = requiredInteger(value, context);
  if (result < 0) throw new TypeError(`${context} returned an unexpected response`);
  return result;
}

function responseBytes(value: unknown, context: string): Buffer {
  if (
    typeof value !== "string" &&
    !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array)
  ) {
    throw new TypeError(`${context} returned an unexpected response`);
  }
  return bytes(value);
}

function keyInfoMap(value: unknown): Record<string, unknown> {
  if (Array.isArray(value)) {
    if (value.length % 2 !== 0) throw new TypeError("FERRICSTORE.KEY_INFO returned an unexpected response");
    const raw: Record<string, unknown> = {};
    for (let index = 0; index < value.length; index += 2) {
      const key = requiredResponseString(value[index], "FERRICSTORE.KEY_INFO");
      setOwnValue(raw, key, value[index + 1]);
    }
    return raw;
  }
  const raw = toStringKeyMap(value);
  if (raw == null) throw new TypeError("FERRICSTORE.KEY_INFO returned an unexpected response");
  return raw;
}
