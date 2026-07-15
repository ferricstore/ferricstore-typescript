import type { ConnectionOptions } from "node:tls";
import type { Command, CommandArgument } from "./internal.js";
import type { ExecutePipelineOptions } from "./pipeline-execution.js";
import type { EndpointPolicy, RoutingEndpoint, RoutingRoute, RoutingTopology } from "./topology.js";

export interface CommandExecutor {
  executeCommand(...args: CommandArgument[]): Promise<unknown>;
  /** Execute an already-built command without a JavaScript variadic-call limit. */
  executeCommandArgs?(args: readonly CommandArgument[]): Promise<unknown>;
  executePipeline?(commands: readonly Command[], options?: ExecutePipelineOptions): Promise<unknown[]>;
  /**
   * Execute all commands in one native transport request. Returning undefined
   * guarantees that no command was submitted, so the caller may fall back.
   */
  executeFusedPipeline?(
    commands: readonly Command[],
    options?: ExecutePipelineOptions
  ): Promise<unknown[] | undefined>;
  refreshTopology?(): Promise<RoutingTopology>;
  route?(key: string | Buffer): Promise<RoutingRoute> | RoutingRoute;
  close?(): Promise<void> | void;
}

export interface NativeProtocolEvent {
  readonly flags: number;
  readonly laneId: number;
  readonly opcode: number;
  readonly value: unknown;
}

export interface NativeAdapterOptions {
  clientName?: string;
  connectTimeoutMs?: number;
  /** Cancels connection, STARTUP, or AUTH while the native adapter is being created. */
  signal?: AbortSignal;
  heartbeatIntervalMs?: number;
  keepAlive?: boolean;
  keepAliveInitialDelayMs?: number;
  /** Maximum source bytes buffered across chunked responses, including final assembly. */
  maxChunkBytes?: number;
  /** Maximum source frames buffered across chunked responses, including final assembly. */
  maxChunkFrames?: number;
  /** Maximum body bytes declared by any single native frame. */
  maxFrameBytes?: number;
  /** Maximum decoded response body bytes, including all chunks. */
  maxResponseBytes?: number;
  /** Maximum correlated control requests awaiting responses on one native connection. */
  maxPendingControlRequests?: number;
  /** Maximum requests waiting locally for native flow-control credit. */
  maxQueuedRequests?: number;
  /** Maximum encoded request bytes waiting locally for socket backpressure. */
  maxQueuedWriteBytes?: number;
  /** Native management events requested during STARTUP. */
  events?: readonly string[];
  /** Non-blocking callback for unsolicited native management frames. */
  onEvent?: (event: NativeProtocolEvent) => unknown;
  protocolLanes?: number;
  timeoutMs?: number;
  username?: string;
  password?: string;
  tlsOptions?: ConnectionOptions;
}

export interface TopologyNativeAdapterOptions extends NativeAdapterOptions {
  /** Learned endpoint trust policy; defaults to exact seeds plus `trustedHosts`. */
  endpointPolicy?: EndpointPolicy;
  endpointValidator?: (endpoint: RoutingEndpoint) => boolean | void;
  /** Additional learned hosts allowed by the default `seed_hosts` policy. */
  trustedHosts?: readonly string[];
  /** Maximum concurrent per-route fan-out operations; defaults to 16. */
  topologyConcurrency?: number;
  warmConnections?: boolean;
}

/** Options accepted by FerricStoreClient.fromUrl(s), including client-side HA and reconnect policy. */
export interface NativeClientOptions extends TopologyNativeAdapterOptions {
  autoReconnect?: boolean | ReconnectOptions;
  haRouting?: boolean;
  seeds?: readonly string[];
}

export interface ReconnectOptions {
  /** Base delay after a failed reconnect attempt. Defaults to 25ms. */
  baseDelayMs?: number;
  /** Percentage of positive random jitter added to reconnect delays. Defaults to 20. */
  jitterPct?: number;
  /** Maximum reconnect delay. Defaults to 1,000ms. */
  maxDelayMs?: number;
  maxRetries?: number;
}
