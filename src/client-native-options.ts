import { Buffer } from "node:buffer";
import type { ConnectionOptions } from "node:tls";
import type {
  NativeAdapterOptions,
  NativeClientOptions,
  TopologyNativeAdapterOptions
} from "./adapters.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";

/** Capture reconnect inputs once so later caller mutation cannot change transport trust or destinations. */
export function snapshotNativeClientOptions(options: NativeClientOptions): NativeClientOptions {
  const own = { ...options };
  const snapshot: NativeClientOptions = {
    ...own,
    ...(typeof own.autoReconnect === "object" && own.autoReconnect != null
      ? { autoReconnect: Object.freeze({ ...own.autoReconnect }) }
      : {}),
    ...(own.endpointPolicy == null
      ? {}
      : { endpointPolicy: snapshotEndpointPolicy(own.endpointPolicy) }),
    ...(own.events == null ? {} : { events: snapshotOwnStringArray(own.events, "events") }),
    ...(own.seeds == null ? {} : { seeds: snapshotOwnStringArray(own.seeds, "seeds") }),
    ...(own.tlsOptions == null ? {} : { tlsOptions: snapshotTlsOptions(own.tlsOptions) }),
    ...(own.trustedHosts == null
      ? {}
      : { trustedHosts: snapshotOwnStringArray(own.trustedHosts, "trustedHosts") })
  };
  return Object.freeze(snapshot);
}

export function snapshotNativeAdapterOptions(options: NativeAdapterOptions): NativeAdapterOptions {
  return nativeAdapterOptions(snapshotNativeClientOptions(options));
}

export function snapshotFerricUrls(urls: readonly string[]): readonly string[] {
  return snapshotOwnStringArray(urls, "FerricStore URLs");
}

export function nativeAdapterOptions(options: NativeClientOptions): NativeAdapterOptions {
  const {
    autoReconnect,
    endpointPolicy,
    endpointValidator,
    haRouting,
    seeds,
    topologyConcurrency,
    trustedHosts,
    warmConnections,
    ...native
  } = options;
  void autoReconnect;
  void endpointPolicy;
  void endpointValidator;
  void haRouting;
  void seeds;
  void topologyConcurrency;
  void trustedHosts;
  void warmConnections;
  return native;
}

export function topologyNativeOptions(options: NativeClientOptions): TopologyNativeAdapterOptions {
  const { autoReconnect, haRouting, seeds, ...topology } = options;
  void autoReconnect;
  void haRouting;
  void seeds;
  return topology;
}

function snapshotEndpointPolicy(
  policy: NonNullable<NativeClientOptions["endpointPolicy"]>
): NonNullable<NativeClientOptions["endpointPolicy"]> {
  if (typeof policy !== "object") return policy;
  const own = { ...policy };
  if (!Object.hasOwn(own, "allowHosts") || !Array.isArray(own.allowHosts)) {
    throw new TypeError("endpointPolicy.allowHosts must be an own array of strings");
  }
  return Object.freeze({
    allowHosts: snapshotOwnStringArray(own.allowHosts, "endpointPolicy.allowHosts")
  });
}

function snapshotTlsOptions(options: ConnectionOptions): ConnectionOptions {
  const source = options as unknown as Record<string, unknown>;
  const snapshot: Record<string, unknown> = {};
  for (const key of Object.keys(source)) {
    snapshot[key] = snapshotTlsValue(source[key], `tlsOptions.${key}`);
  }
  return Object.freeze(snapshot);
}

function snapshotTlsValue(value: unknown, name: string): unknown {
  if (Buffer.isBuffer(value)) return Buffer.from(value);
  if (value instanceof Uint8Array) return value.slice();
  if (Array.isArray(value)) {
    const snapshot = new Array<unknown>(value.length);
    for (let index = 0; index < value.length; index += 1) {
      if (!Object.hasOwn(value, index)) throw new TypeError(`${name} must be a dense array`);
      snapshot[index] = snapshotTlsValue(value[index], name);
    }
    return Object.freeze(snapshot);
  }
  return snapshotTlsIdentity(value, name);
}

function snapshotTlsIdentity(value: unknown, name: string): unknown {
  if (typeof value !== "object" || value == null) return value;
  const source = value as Record<string, unknown>;
  const materialKey = Object.hasOwn(source, "pem")
    ? "pem"
    : Object.hasOwn(source, "buf") ? "buf" : undefined;
  if (materialKey == null) return value;
  const snapshot = { ...source };
  snapshot[materialKey] = snapshotTlsValue(source[materialKey], `${name}.${materialKey}`);
  return Object.freeze(snapshot);
}
