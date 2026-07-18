import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import {
  assertCommandHasStableConnectionState,
  assertCommandDoesNotRequirePinnedConnection
} from "./protocol.js";
import {
  NativeAdapter,
  pipelineFallbackOptions,
  snapshotCommandArguments,
  snapshotPipelineCommands,
  snapshotPipelineOptions,
  type CommandExecutor,
  type ExecutePipelineOptions,
  type NativeAdapterOptions,
  type TopologyNativeAdapterOptions
} from "./adapters.js";
import { singleRouteForCommands } from "./topology-routing.js";
import { RoutingTopology, type RoutingRoute } from "./routing-topology.js";
export { RoutingTopology, type RoutingRoute } from "./routing-topology.js";
import {
  assertTopologyNativeAdapterOptions,
  isRetryableRouteError,
  nativeOnlyOptions,
  normalizeTopologyConcurrency,
  seedAuthIdentity,
  seedConnectionOptions,
  snapshotTopologyNativeAdapterOptions,
  waitForExplicitlySafeReroute,
  withSeedAuthDefaults
} from "./topology-options.js";
import {
  connectionKeyForEndpoint,
  connectionKeyFromUrl,
  mapSettledWithConcurrency,
  nativeEventName,
  parseFerricUrl,
  urlFromEndpoint,
  type RoutingEndpoint
} from "./topology-utilities.js";
export type { RoutingEndpoint } from "./topology-utilities.js";
import { TopologyEndpointTrust } from "./topology-endpoint-trust.js";
export type { EndpointPolicy } from "./topology-endpoint-trust.js";
import type { RefreshCandidate, RoutedCommandData } from "./topology-execution-types.js";
import { topologyRefreshCandidates } from "./topology-refresh-candidates.js";
import { topologyForInstallation } from "./topology-installation.js";
import { TopologyPipelineExecutor } from "./topology-pipeline.js";
import { TopologyAdapterRegistry } from "./topology-adapter-registry.js";
import { MAX_TOPOLOGY_PLANNING_ATTEMPTS, StaleTopologyRouteError, topologyRouteIsCurrent } from "./topology-route-snapshot.js";
import { topologyRouteData } from "./topology-route-planner.js";
import { TopologyScatterExecutor } from "./topology-scatter.js";

export class TopologyNativeAdapterPool implements CommandExecutor {
  private readonly adapterOptions: NativeAdapterOptions;
  private readonly seedAdapterOptions: NativeAdapterOptions;
  private closed = false;
  private readonly adapterRegistry = new TopologyAdapterRegistry(
    () => this.assertOpen(),
    () => this.closed
  );
  private closePromise?: Promise<void>;
  private readonly endpointTrust: TopologyEndpointTrust;
  private readonly seedUrlByEndpointKey: ReadonlyMap<string, string>;
  private readonly seedUrls: readonly string[];
  private readonly tls: boolean;
  private readonly topologyConcurrency: number;
  private readonly pipelineExecutor: TopologyPipelineExecutor;
  private readonly scatterExecutor: TopologyScatterExecutor;
  private readonly warmConnections: boolean;
  private eventRefreshPromise?: Promise<void>;
  private lastSuccessfulRefreshKey?: string;
  private refreshPromise?: Promise<RoutingTopology>;
  private refreshRequested = false;
  private topologyValue = RoutingTopology.empty();

  private constructor(urls: readonly string[], options: TopologyNativeAdapterOptions = {}) {
    if (urls.length === 0) {
      throw new FerricStoreError("TopologyNativeAdapterPool requires at least one seed URL");
    }
    assertTopologyNativeAdapterOptions(options);
    const seedTransports = urls.map((url) => parseFerricUrl(url).tls);
    const tls = seedTransports[0] ?? false;
    if (seedTransports.some((secure) => secure !== tls)) {
      throw new FerricStoreError("TopologyNativeAdapterPool cannot mix ferric:// and ferrics:// seed URLs");
    }
    this.topologyConcurrency = normalizeTopologyConcurrency(options.topologyConcurrency);
    this.warmConnections = options.warmConnections ?? false;
    this.tls = tls;
    const adapterOptions = nativeOnlyOptions(withSeedAuthDefaults(urls, options));
    const userOnEvent = adapterOptions.onEvent;
    this.adapterOptions = {
      ...adapterOptions,
      events: [...new Set([...(adapterOptions.events ?? []), "TOPOLOGY_CHANGED"])],
      onEvent: (event) => {
        try {
          const callback = userOnEvent?.(event);
          if (callback != null) {
            void Promise.resolve(callback).catch(() => undefined);
          }
        } catch {
          // User callbacks are isolated from topology maintenance.
        }
        if (!this.closed && nativeEventName(event) === "TOPOLOGY_CHANGED") {
          this.requestTopologyRefresh();
        }
      }
    };
    this.seedAdapterOptions = seedConnectionOptions(this.adapterOptions, options);
    const seedUrlByEndpointKey = new Map<string, string>();
    const seedAuthByEndpointKey = new Map<string, string>();
    for (const url of urls) {
      const key = connectionKeyFromUrl(url);
      const auth = seedAuthIdentity(url, this.seedAdapterOptions);
      const existingAuth = seedAuthByEndpointKey.get(key);
      if (existingAuth != null && existingAuth !== auth) {
        throw new FerricStoreError(`duplicate seed endpoint ${key} has conflicting credentials`);
      }
      seedAuthByEndpointKey.set(key, auth);
      if (!seedUrlByEndpointKey.has(key)) seedUrlByEndpointKey.set(key, url);
    }
    this.seedUrlByEndpointKey = seedUrlByEndpointKey;
    this.seedUrls = [...seedUrlByEndpointKey.values()];
    this.endpointTrust = new TopologyEndpointTrust(
      options.endpointPolicy,
      options.endpointValidator,
      new Set(seedUrlByEndpointKey.keys()),
      options.trustedHosts ?? [],
      this.tls
    );
    this.pipelineExecutor = new TopologyPipelineExecutor({
      concurrency: this.topologyConcurrency,
      controlPipeline: async (commands, pipelineOptions) =>
        await (await this.controlAdapter()).executePipeline(commands, pipelineOptions),
      executeCommandArgs: async (command) => await this.executeCommandArgs(command),
      executePipelineOnRoute: async (commands, route, pipelineOptions) =>
        await this.executePipelineOnRoute(commands, route, pipelineOptions),
      routeData: (args) => this.routeData(args)
    });
    this.scatterExecutor = new TopologyScatterExecutor({
      concurrency: this.topologyConcurrency,
      executeOnRoute: async (args, route) => await this.executeOnRoute(args, route),
      route: (key) => this.route(key)
    });
  }

  static async fromUrls(urls: readonly string[], options: TopologyNativeAdapterOptions = {}): Promise<TopologyNativeAdapterPool> {
    const pool = new TopologyNativeAdapterPool(urls, snapshotTopologyNativeAdapterOptions(options));
    try {
      await pool.refreshTopology();
      return pool;
    } catch (error) {
      await pool.close();
      throw error;
    }
  }

  get topology(): RoutingTopology {
    return this.topologyValue;
  }

  private requestTopologyRefresh(): void {
    this.refreshRequested = true;
    queueMicrotask(() => {
      if (this.closed || this.eventRefreshPromise != null) return;
      const refresh = this.drainTopologyRefreshRequests();
      this.eventRefreshPromise = refresh;
      void refresh.catch(() => undefined).finally(() => {
        if (this.eventRefreshPromise !== refresh) return;
        this.eventRefreshPromise = undefined;
        if (!this.closed && this.refreshRequested) this.requestTopologyRefresh();
      });
    });
  }

  private async drainTopologyRefreshRequests(): Promise<void> {
    while (!this.closed && this.refreshRequested) {
      if (this.refreshPromise != null) {
        await this.refreshPromise;
        continue;
      }
      this.refreshRequested = false;
      await this.refreshTopology();
    }
  }

  async refreshTopology(): Promise<RoutingTopology> {
    this.assertOpen();
    if (this.refreshPromise != null) {
      return await this.refreshPromise;
    }
    const refresh = this.loadTopology();
    this.refreshPromise = refresh;
    try {
      return await refresh;
    } finally {
      if (this.refreshPromise === refresh) {
        this.refreshPromise = undefined;
      }
    }
  }

  private async loadTopology(): Promise<RoutingTopology> {
    let lastError: unknown;
    for (const candidate of this.refreshCandidates()) {
      try {
        const adapter = candidate.seed
          ? await this.adapterForSeedUrl(candidate.url)
          : await this.adapterForEndpoint(candidate.endpoint);
        const candidateTopology = RoutingTopology.build(await adapter.executeCommand("SHARDS"));
        this.endpointTrust.validateTopology(candidateTopology);
        this.assertOpen();
        const topology = topologyForInstallation(this.topologyValue, candidateTopology);
        const changed = topology !== this.topologyValue;
        if (changed) this.topologyValue = topology;
        this.lastSuccessfulRefreshKey = connectionKeyFromUrl(candidate.url);
        if (!changed) return topology;
        if (this.warmConnections) {
          await mapSettledWithConcurrency(
            [...topology.endpoints.values()],
            this.topologyConcurrency,
            async (endpoint) => await this.adapterForEndpoint(endpoint)
          );
          this.assertOpen();
        }
        this.retireRemovedAdapters(topology);
        return topology;
      } catch (error) {
        if (this.closed) {
          this.assertOpen();
        }
        lastError = error;
      }
    }
    throw new FerricStoreError("no FerricStore topology endpoint reachable", { raw: lastError });
  }

  route(key: string | Buffer): RoutingRoute {
    this.assertOpen();
    const route = this.topologyValue.routeKey(key);
    this.endpointTrust.validate(route.endpoint);
    return route;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    this.assertOpen();
    const snapshot = snapshotCommandArguments(args);
    assertCommandHasStableConnectionState(snapshot);
    let rerouteAttempt = 0;
    for (let planningAttempt = 0; planningAttempt < MAX_TOPOLOGY_PLANNING_ATTEMPTS; planningAttempt += 1) {
      const routed = this.routeData(snapshot);
      if (routed == null) {
        const scattered = await this.scatterExecutor.execute(snapshot);
        if (scattered.handled) {
          return scattered.value;
        }
        return await (await this.controlAdapter()).executeCommandArgs(snapshot);
      }

      const adapter = await this.adapterForCurrentRoute(routed.route);
      if (adapter == null) continue;
      try {
        return routed.command == null
          ? await adapter.executeCommandOnLane(snapshot, routed.route.laneId)
          : await adapter.executeProtocolCommand(routed.command, routed.route.laneId);
      } catch (error) {
        if (await this.refreshAndCanRetrySafeReroute(error, rerouteAttempt)) {
          rerouteAttempt += 1;
          continue;
        }
        throw error;
      }
    }
    throw new StaleTopologyRouteError();
  }

  async executePipeline(commands: readonly Command[], options: ExecutePipelineOptions = {}): Promise<unknown[]> {
    this.assertOpen();
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options) ?? {};
    if (snapshot.length === 0) return [];
    for (const command of snapshot) {
      assertCommandDoesNotRequirePinnedConnection(command);
      assertCommandHasStableConnectionState(command);
    }

    for (let planningAttempt = 0; planningAttempt < MAX_TOPOLOGY_PLANNING_ATTEMPTS; planningAttempt += 1) {
      const route = singleRouteForCommands(snapshot, (command) => this.routeData(command));
      if (route == null) {
        return await this.executeSplitPipeline(snapshot, pipelineFallbackOptions(snapshot, snapshotOptions));
      }
      try {
        return await this.executePipelineOnRoute(snapshot, route, snapshotOptions);
      } catch (error) {
        if (error instanceof StaleTopologyRouteError) continue;
        throw error;
      }
    }
    throw new StaleTopologyRouteError();
  }

  async executeFusedPipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[] | undefined> {
    this.assertOpen();
    const snapshot = snapshotPipelineCommands(commands);
    const snapshotOptions = snapshotPipelineOptions(options) ?? {};
    if (snapshot.length === 0) return [];
    for (const command of snapshot) {
      assertCommandDoesNotRequirePinnedConnection(command);
      assertCommandHasStableConnectionState(command);
    }
    let rerouteAttempt = 0;
    for (let planningAttempt = 0; planningAttempt < MAX_TOPOLOGY_PLANNING_ATTEMPTS; planningAttempt += 1) {
      const route = singleRouteForCommands(snapshot, (command) => this.routeData(command));
      if (route == null) return undefined;
      const adapter = await this.adapterForCurrentRoute(route);
      if (adapter == null) continue;
      try {
        return await adapter.executeFusedPipelineOnLane(snapshot, route.laneId, snapshotOptions);
      } catch (error) {
        if (await this.refreshAndCanRetrySafeReroute(error, rerouteAttempt)) {
          rerouteAttempt += 1;
          continue;
        }
        throw error;
      }
    }
    throw new StaleTopologyRouteError();
  }

  private async refreshAndCanRetrySafeReroute(error: unknown, attempt: number): Promise<boolean> {
    if (attempt !== 0 || !isRetryableRouteError(error)) return false;
    const refreshed = await this.refreshTopology().then(
      () => true,
      () => false
    );
    if (!refreshed || !(await waitForExplicitlySafeReroute(error))) return false;
    this.assertOpen();
    return true;
  }

  private async executePipelineOnRoute(
    commands: readonly Command[],
    route: RoutingRoute,
    options: ExecutePipelineOptions
  ): Promise<unknown[]> {
    const adapter = await this.adapterForCurrentRoute(route);
    if (adapter == null) throw new StaleTopologyRouteError();
    try {
      return await adapter.executePipelineOnLane(commands, route.laneId, options);
    } catch (error) {
      if (isRetryableRouteError(error)) {
        await this.refreshTopology().catch(() => undefined);
      }
      throw error;
    }
  }

  private async executeSplitPipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions
  ): Promise<unknown[]> {
    return await this.pipelineExecutor.execute(commands, options);
  }

  async close(): Promise<void> {
    if (this.closePromise != null) {
      await this.closePromise;
      return;
    }
    this.closed = true;
    this.closePromise = this.adapterRegistry.close(this.topologyConcurrency);
    await this.closePromise;
  }

  private routeData(args: readonly CommandArgument[]): RoutedCommandData | undefined {
    return topologyRouteData(args, (key) => this.route(key));
  }

  private async executeOnRoute(args: readonly CommandArgument[], route: RoutingRoute): Promise<unknown> {
    const adapter = await this.adapterForCurrentRoute(route);
    if (adapter == null) throw new StaleTopologyRouteError();
    try {
      return await adapter.executeCommandOnLane(args, route.laneId);
    } catch (error) {
      if (isRetryableRouteError(error)) {
        await this.refreshTopology().catch(() => undefined);
      }
      throw error;
    }
  }

  private async controlAdapter(): Promise<NativeAdapter> {
    this.assertOpen();
    for (const candidate of this.refreshCandidates()) {
      try {
        return candidate.seed
          ? await this.adapterForSeedUrl(candidate.url)
          : await this.adapterForEndpoint(candidate.endpoint);
      } catch {
        // Try the next known control endpoint.
      }
    }
    return await this.adapterForSeedUrl(this.seedUrls[0] ?? "ferric://127.0.0.1:6388");
  }

  private async adapterForSeedUrl(url: string): Promise<NativeAdapter> {
    const key = connectionKeyFromUrl(url);
    return await this.adapterRegistry.get(key, url, this.seedAdapterOptions);
  }

  private async adapterForEndpoint(endpoint: RoutingEndpoint): Promise<NativeAdapter> {
    this.endpointTrust.validate(endpoint);
    const key = connectionKeyForEndpoint(endpoint, this.tls);
    const seedUrl = this.seedUrlByEndpointKey.get(key);
    if (seedUrl != null) {
      return await this.adapterForSeedUrl(seedUrl);
    }
    return await this.adapterRegistry.get(key, urlFromEndpoint(endpoint, this.tls), this.adapterOptions);
  }

  private async adapterForCurrentRoute(route: RoutingRoute): Promise<NativeAdapter | undefined> {
    if (!topologyRouteIsCurrent(this.topologyValue, route)) return undefined;
    const adapter = await this.adapterForEndpoint(route.endpoint);
    return topologyRouteIsCurrent(this.topologyValue, route) ? adapter : undefined;
  }

  private retireRemovedAdapters(topology: RoutingTopology): void {
    const active = this.endpointTrust.activeConnectionKeys(topology);
    this.adapterRegistry.retireRemoved(
      active,
      (key) => this.endpointTrust.activeConnectionKeys(this.topologyValue).has(key)
    );
  }

  private refreshCandidates(): RefreshCandidate[] {
    return topologyRefreshCandidates(this.seedUrls, this.topologyValue.endpoints.values(), this.tls,
      this.lastSuccessfulRefreshKey);
  }
  private assertOpen(): void {
    if (this.closed) throw new FerricStoreError("FerricStore topology adapter pool is closed");
  }
}
