import type { NativeAdapterOptions, TopologyNativeAdapterOptions } from "./adapters.js";
import { snapshotNativeClientOptions, topologyNativeOptions } from "./client-native-options.js";
import { ConnectionClosedError, RerouteError } from "./errors.js";
import { getField, parseFerricUrl } from "./topology-utilities.js";

const DEFAULT_TOPOLOGY_CONCURRENCY = 16;

export function snapshotTopologyNativeAdapterOptions(
  options: TopologyNativeAdapterOptions
): TopologyNativeAdapterOptions {
  return topologyNativeOptions(snapshotNativeClientOptions(options));
}

export function normalizeTopologyConcurrency(value: number | undefined): number {
  if (value == null) return DEFAULT_TOPOLOGY_CONCURRENCY;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError("topologyConcurrency must be a positive safe integer");
  }
  return value;
}

export function nativeOnlyOptions(options: TopologyNativeAdapterOptions): NativeAdapterOptions {
  const {
    endpointPolicy,
    endpointValidator,
    topologyConcurrency,
    trustedHosts,
    warmConnections,
    ...nativeOptions
  } = options;
  void endpointPolicy;
  void endpointValidator;
  void topologyConcurrency;
  void trustedHosts;
  void warmConnections;
  return nativeOptions;
}

export function withSeedAuthDefaults(
  urls: readonly string[],
  options: TopologyNativeAdapterOptions
): TopologyNativeAdapterOptions {
  const next: TopologyNativeAdapterOptions = { ...options };
  const seeds = urls.map((url) => parseFerricUrl(url));
  if (next.password == null) {
    const credentials = seeds.find((seed) => seed.password != null);
    if (credentials != null) {
      next.password = credentials.password;
      next.username ??= credentials.username ?? "default";
    }
  } else {
    next.username ??= seeds.find((seed) => seed.username != null)?.username ?? "default";
  }
  return next;
}

export function assertTopologyNativeAdapterOptions(options: TopologyNativeAdapterOptions): void {
  const supplied = options as unknown as Record<string, unknown>;
  for (const name of ["autoReconnect", "haRouting", "seeds"]) {
    if (Object.hasOwn(supplied, name)) {
      throw new TypeError(`FerricStoreClient option ${name} is not accepted by TopologyNativeAdapterPool`);
    }
  }
}

export function seedConnectionOptions(
  defaults: NativeAdapterOptions,
  explicit: NativeAdapterOptions
): NativeAdapterOptions {
  if (explicit.password != null) return defaults;
  const options = { ...defaults };
  delete options.password;
  if (explicit.username == null) delete options.username;
  return options;
}

export function seedAuthIdentity(url: string, options: NativeAdapterOptions): string {
  const parsed = parseFerricUrl(url);
  const password = options.password ?? parsed.password;
  if (password == null || password === "") return "none";
  return JSON.stringify([options.username ?? parsed.username ?? "default", password]);
}

export function isRetryableRouteError(error: unknown): boolean {
  if (error instanceof RerouteError || error instanceof ConnectionClosedError) return true;
  if (!(error instanceof Error)) return false;
  const message = error.message.toLowerCase();
  return message.includes("connection closed") ||
    message.includes("connection is closed") ||
    message.includes("shard not available") ||
    message.includes("shard is not available") ||
    /\bnot (?:the )?leader\b/u.test(message) ||
    /\bstale leader\b/u.test(message);
}

export function isExplicitlySafeReroute(error: unknown): boolean {
  if (error instanceof ConnectionClosedError) {
    return error.requestDisposition === "unsent";
  }
  return error instanceof RerouteError && getField(error.raw, "safe_to_retry") === true;
}
