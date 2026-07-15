import net from "node:net";
import tls from "node:tls";
import { Buffer } from "node:buffer";
import type { NativeAdapterOptions } from "./adapter-types.js";
import { FerricStoreError, classifyServerError } from "./errors.js";
import {
  field,
  normalizeFerricUrlHost,
  setLongTimeout
} from "./internal.js";

export interface ParsedUrl {
  readonly host: string;
  readonly password?: string;
  readonly port: number;
  readonly tls: boolean;
  readonly username?: string;
}

export function parseFerricUrl(value: string): ParsedUrl {
  const url = new URL(value);
  if (url.protocol !== "ferric:" && url.protocol !== "ferrics:") {
    throw new FerricStoreError(`unsupported FerricStore URL scheme: ${url.protocol}`);
  }
  return {
    host: normalizeFerricUrlHost(url.hostname),
    ...(url.password === "" ? {} : { password: decodeURIComponent(url.password) }),
    port: Number(url.port || (url.protocol === "ferrics:" ? 6389 : 6388)),
    tls: url.protocol === "ferrics:",
    ...(url.username === "" ? {} : { username: decodeURIComponent(url.username) })
  };
}

export async function connect(
  parsed: ParsedUrl,
  options: NativeAdapterOptions
): Promise<net.Socket | tls.TLSSocket> {
  const timeoutMs = normalizePositiveLimit(
    options.connectTimeoutMs ?? options.timeoutMs ?? 30_000,
    30_000
  );
  if (options.signal?.aborted) throw nativeBootstrapAbortError(options.signal.reason);
  return await new Promise((resolve, reject) => {
    const tlsOptions = options.tlsOptions ?? {};
    const servername = tlsOptions.servername ?? (net.isIP(parsed.host) === 0 ? parsed.host : undefined);
    const socket = parsed.tls
      ? tls.connect({
          ...tlsOptions,
          host: parsed.host,
          port: parsed.port,
          ...(servername == null ? {} : { servername })
        })
      : net.createConnection({ host: parsed.host, port: parsed.port });
    const connectEvent = parsed.tls ? "secureConnect" : "connect";
    const cleanup = (): void => {
      timer.cancel();
      socket.off(connectEvent, onConnect);
      socket.off("error", onError);
      options.signal?.removeEventListener("abort", onAbort);
    };
    const onConnect = (): void => {
      cleanup();
      resolve(socket);
    };
    const onError = (error: unknown): void => {
      cleanup();
      reject(error instanceof Error ? error : classifyServerError(String(error), error));
    };
    const onAbort = (): void => {
      cleanup();
      socket.destroy();
      reject(nativeBootstrapAbortError(options.signal?.reason));
    };
    const timer = setLongTimeout(() => {
      cleanup();
      socket.destroy();
      reject(new FerricStoreError(`FerricStore connection timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref();

    socket.setNoDelay(true);
    socket.setKeepAlive(
      options.keepAlive ?? true,
      normalizeKeepAliveInitialDelay(options.keepAliveInitialDelayMs)
    );
    socket.once(connectEvent, onConnect);
    socket.once("error", onError);
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) onAbort();
  });
}

export function nativeBootstrapAbortError(reason: unknown): FerricStoreError {
  return new FerricStoreError("FerricStore connection bootstrap aborted", {
    ...(reason instanceof Error ? { cause: reason } : { raw: reason })
  });
}

export function normalizeHeartbeatInterval(value: number | undefined): number | undefined {
  if (value != null && value <= 0) return undefined;
  return normalizePositiveLimit(value ?? 60_000, 60_000);
}

export function normalizeKeepAliveInitialDelay(value: number | undefined): number {
  return normalizeNonNegativeInteger(value ?? 30_000, 30_000);
}

export function flowControlLimits(value: unknown): {
  readonly connection?: number;
  readonly lane?: number;
} {
  let connection: number | undefined;
  let lane: number | undefined;
  let level: unknown[] = [value];
  for (let depth = 0; depth < 3 && level.length > 0; depth += 1) {
    const next: unknown[] = [];
    for (const candidate of level) {
      connection ??= nonNegativeSafeInteger(field(candidate, "max_inflight_per_connection"));
      lane ??= nonNegativeSafeInteger(field(candidate, "max_inflight_per_lane"));
      if (connection != null && lane != null) return { connection, lane };
      for (const key of ["flow_control", "limits", "payload"] as const) {
        const nested = field(candidate, key);
        if (nested != null) next.push(nested);
      }
    }
    level = next;
  }
  return { connection, lane };
}

export function startupLimits(value: unknown): {
  readonly frameBytes?: number;
  readonly laneQueue?: number;
  readonly lanes?: number;
  readonly pipelineCommands?: number;
} {
  let frameBytes: number | undefined;
  let laneQueue: number | undefined;
  let lanes: number | undefined;
  let pipelineCommands: number | undefined;
  let level: unknown[] = [value];
  for (let depth = 0; depth < 3 && level.length > 0; depth += 1) {
    const next: unknown[] = [];
    for (const candidate of level) {
      frameBytes ??= positiveSafeInteger(field(candidate, "max_frame_bytes"));
      laneQueue ??= nonNegativeSafeInteger(field(candidate, "max_lane_queue"));
      lanes ??= positiveSafeInteger(field(candidate, "max_lanes_per_connection"));
      pipelineCommands ??= nonNegativeSafeInteger(field(candidate, "max_pipeline_commands"));
      if (frameBytes != null && laneQueue != null && lanes != null && pipelineCommands != null) {
        return { frameBytes, laneQueue, lanes, pipelineCommands };
      }
      for (const key of ["limits", "multiplexing", "payload"] as const) {
        const nested = field(candidate, key);
        if (nested != null) next.push(nested);
      }
    }
    level = next;
  }
  return { frameBytes, laneQueue, lanes, pipelineCommands };
}

function nonNegativeSafeInteger(value: unknown): number | undefined {
  if (typeof value === "bigint") {
    return value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : undefined;
  }
  const source = Buffer.isBuffer(value) || value instanceof Uint8Array
    ? Buffer.from(value).toString("utf8")
    : value;
  if (typeof source !== "number" && (typeof source !== "string" || !/^\d+$/u.test(source))) {
    return undefined;
  }
  const parsed = Number(source);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function positiveSafeInteger(value: unknown): number | undefined {
  const parsed = nonNegativeSafeInteger(value);
  return parsed != null && parsed > 0 ? parsed : undefined;
}

export function normalizePositiveLimit(value: number, fallback: number): number {
  return Number.isFinite(value) ? Math.max(1, Math.trunc(value)) : fallback;
}

export function normalizeNonNegativeInteger(value: number, fallback: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : fallback;
}
