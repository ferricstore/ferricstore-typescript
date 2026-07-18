import type { NativeAdapterOptions } from "./adapter-types.js";

export const DEFAULT_MAX_CHUNK_BYTES = 64 * 1024 * 1024;
export const DEFAULT_MAX_CHUNK_FRAMES = 65_536;
export const DEFAULT_MAX_PENDING_CONTROL_REQUESTS = 4_096;
export const DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
export const DEFAULT_MAX_QUEUED_REQUESTS = 65_536;
export const DEFAULT_MAX_QUEUED_WRITE_BYTES = 64 * 1024 * 1024;
export const UNAUTHENTICATED_MAX_FRAME_BYTES = 64 * 1024;

export function assertNativeAdapterOptions(options: NativeAdapterOptions): void {
  const supplied = options as unknown as Record<string, unknown>;
  for (const name of ["autoReconnect", "haRouting", "seeds"]) {
    if (Object.hasOwn(supplied, name)) {
      throw new TypeError(`FerricStoreClient option ${name} is not accepted by NativeAdapter.fromUrl`);
    }
  }
  for (const name of ["endpointPolicy", "endpointValidator", "topologyConcurrency", "trustedHosts", "warmConnections"]) {
    if (Object.hasOwn(supplied, name)) {
      throw new TypeError(`TopologyNativeAdapterPool option ${name} is not accepted by NativeAdapter.fromUrl`);
    }
  }
}
