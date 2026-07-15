import net from "node:net";
import tls from "node:tls";
import { FerricStoreError } from "./errors.js";
import type { NativeAdapterOptions, NativeProtocolEvent } from "./adapter-types.js";
import {
  assertNativeAdapterOptions,
  DEFAULT_MAX_CHUNK_BYTES,
  DEFAULT_MAX_CHUNK_FRAMES,
  DEFAULT_MAX_PENDING_CONTROL_REQUESTS,
  DEFAULT_MAX_QUEUED_REQUESTS,
  DEFAULT_MAX_QUEUED_WRITE_BYTES,
  DEFAULT_MAX_RESPONSE_BYTES
} from "./native-adapter-config.js";
import { connect, parseFerricUrl } from "./native-connection.js";
import { snapshotNativeAdapterOptions } from "./client-native-options.js";
import { DEFAULT_MAX_FRAME_BYTES } from "./protocol.js";

export type NativeAdapterConstructorArguments = [
  socket: net.Socket | tls.TLSSocket,
  timeoutMs: number,
  protocolLanes: number,
  maxChunkBytes: number,
  maxChunkFrames: number,
  maxFrameBytes: number,
  maxResponseBytes: number,
  maxPendingControlRequests: number,
  maxQueuedRequests: number,
  heartbeatIntervalMs: number | undefined,
  onEvent: ((event: NativeProtocolEvent) => unknown) | undefined,
  maxQueuedWriteBytes: number
];

interface NativeAdapterBootstrapHandle<T> {
  readonly adapter: T;
  readonly auth: (username: string, password: string) => Promise<void>;
  readonly close: () => Promise<void>;
  readonly startHeartbeat: () => void;
  readonly startup: (clientName?: string, events?: readonly string[]) => Promise<boolean>;
}

export async function bootstrapNativeAdapter<T>(
  url: string,
  options: NativeAdapterOptions,
  create: (args: NativeAdapterConstructorArguments) => NativeAdapterBootstrapHandle<T>
): Promise<T> {
  assertNativeAdapterOptions(options);
  const nativeOptions = snapshotNativeAdapterOptions(options);
  const parsed = parseFerricUrl(url);
  const socket = await connect(parsed, nativeOptions);
  const handle = create([
    socket,
    nativeOptions.timeoutMs ?? 30_000,
    nativeOptions.protocolLanes ?? 8,
    nativeOptions.maxChunkBytes ?? DEFAULT_MAX_CHUNK_BYTES,
    nativeOptions.maxChunkFrames ?? DEFAULT_MAX_CHUNK_FRAMES,
    nativeOptions.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
    nativeOptions.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES,
    nativeOptions.maxPendingControlRequests ?? DEFAULT_MAX_PENDING_CONTROL_REQUESTS,
    nativeOptions.maxQueuedRequests ?? DEFAULT_MAX_QUEUED_REQUESTS,
    nativeOptions.heartbeatIntervalMs,
    nativeOptions.onEvent,
    nativeOptions.maxQueuedWriteBytes ?? DEFAULT_MAX_QUEUED_WRITE_BYTES
  ]);
  try {
    const authRequired = await handle.startup(nativeOptions.clientName, nativeOptions.events);
    const password = nativeOptions.password ?? parsed.password;
    if (authRequired && (password == null || password === "")) {
      throw new FerricStoreError(
        "FerricStore server requires authentication but no password was provided"
      );
    }
    if (password != null && password !== "") {
      await handle.auth(nativeOptions.username ?? parsed.username ?? "default", password);
    }
    handle.startHeartbeat();
    return handle.adapter;
  } catch (error) {
    await handle.close();
    throw error;
  }
}
