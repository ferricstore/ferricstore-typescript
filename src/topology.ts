import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { Command, CommandArgument } from "./internal.js";
import {
  buildProtocolCommand,
  pipelineCommand,
  unwrapPipelineResponse,
  type ProtocolCommand
} from "./protocol.js";
import {
  NativeAdapter,
  type CommandExecutor,
  type ExecutePipelineOptions,
  type NativeAdapterOptions
} from "./adapters.js";

export type EndpointPolicy = "seed_hosts" | "any" | "none" | { readonly allowHosts: readonly string[] };

export interface RoutingEndpoint {
  readonly node: string;
  readonly host: string;
  readonly nativePort: number;
  readonly nativeTlsPort?: number;
}

export interface RoutingRoute {
  readonly shard: number;
  readonly laneId: number;
  readonly endpointKey: string;
  readonly endpoint: RoutingEndpoint;
  readonly leaderNode: string;
  readonly slot?: number;
}

interface ParsedUrl {
  readonly host: string;
  readonly password?: string;
  readonly port: number;
  readonly tls: boolean;
  readonly username?: string;
}

const ROUTE_SLOT_COUNT = 1024;
const ROUTE_SLOT_MASK = ROUTE_SLOT_COUNT - 1;

export class RoutingTopology {
  readonly routeEpoch: number;
  readonly shardCount: number;
  readonly slots: readonly (RoutingRoute | undefined)[];
  readonly endpoints: ReadonlyMap<string, RoutingEndpoint>;

  private constructor(
    routeEpoch: number,
    shardCount: number,
    slots: readonly (RoutingRoute | undefined)[],
    endpoints: ReadonlyMap<string, RoutingEndpoint>
  ) {
    this.routeEpoch = routeEpoch;
    this.shardCount = shardCount;
    this.slots = slots;
    this.endpoints = endpoints;
  }

  static empty(): RoutingTopology {
    return new RoutingTopology(0, 0, Array.from({ length: ROUTE_SLOT_COUNT }), new Map());
  }

  static build(payload: unknown): RoutingTopology {
    const ranges = getField(payload, "ranges");
    if (!Array.isArray(ranges)) {
      throw new FerricStoreError("invalid SHARDS topology payload", { raw: payload });
    }

    const slots: (RoutingRoute | undefined)[] = Array.from({ length: ROUTE_SLOT_COUNT });
    const endpoints = new Map<string, RoutingEndpoint>();

    for (const item of ranges) {
      if (textOrUndefined(getField(item, "hint")) === "leader_unknown") {
        throw new FerricStoreError("SHARDS range has no leader", { raw: item });
      }

      const first = numberOrUndefined(getField(item, "first_slot"));
      const last = numberOrUndefined(getField(item, "last_slot"));
      const shard = numberOrUndefined(getField(item, "shard"));
      const laneId = numberOrUndefined(getField(item, "lane_id"));
      const endpoint = endpointFromRange(item);

      if (
        first == null ||
        last == null ||
        shard == null ||
        laneId == null ||
        first < 0 ||
        last < first ||
        last >= ROUTE_SLOT_COUNT
      ) {
        throw new FerricStoreError("invalid SHARDS range", { raw: item });
      }

      const endpointKey = endpointKeyFor(endpoint);
      const route: RoutingRoute = {
        endpoint,
        endpointKey,
        laneId,
        leaderNode: endpoint.node,
        shard
      };
      for (let slot = first; slot <= last; slot += 1) {
        slots[slot] = route;
      }
      endpoints.set(endpointKey, endpoint);
    }

    return new RoutingTopology(
      numberOrUndefined(getField(payload, "route_epoch")) ?? 0,
      numberOrUndefined(getField(payload, "shard_count")) ?? 0,
      slots,
      endpoints
    );
  }

  static slotForKey(key: string | Buffer): number {
    const textKey = Buffer.isBuffer(key) ? key.toString("utf8") : key;
    let hashInput: string;
    if (textKey.startsWith("f:{")) {
      hashInput = flowHashTag(textKey.slice(3), textKey);
    } else if (textKey.startsWith("X:f:{")) {
      hashInput = flowHashTag(textKey.slice(5), textKey);
    } else {
      hashInput = hashTagOrKey(textKey);
    }
    return crc32(Buffer.from(hashInput)) & ROUTE_SLOT_MASK;
  }

  routeKey(key: string | Buffer): RoutingRoute {
    const slot = RoutingTopology.slotForKey(key);
    const route = this.slots[slot];
    if (route == null) {
      throw new FerricStoreError(`no route for slot ${slot}`);
    }
    return { ...route, slot };
  }
}

export class TopologyNativeAdapterPool implements CommandExecutor {
  private readonly adapterOptions: NativeAdapterOptions;
  private readonly adapters = new Map<string, NativeAdapter>();
  private readonly endpointPolicy: EndpointPolicy;
  private readonly endpointValidator?: (endpoint: RoutingEndpoint) => boolean | void;
  private readonly seedEndpointKeys: ReadonlySet<string>;
  private readonly seedUrls: readonly string[];
  private readonly tls: boolean;
  private readonly trustedHosts: ReadonlySet<string>;
  private readonly warmConnections: boolean;
  private topologyValue = RoutingTopology.empty();

  private constructor(urls: readonly string[], options: NativeAdapterOptions = {}) {
    if (urls.length === 0) {
      throw new FerricStoreError("TopologyNativeAdapterPool requires at least one seed URL");
    }
    this.seedUrls = [...urls];
    this.endpointPolicy = options.endpointPolicy ?? "seed_hosts";
    this.endpointValidator = options.endpointValidator;
    this.warmConnections = options.warmConnections ?? false;
    this.tls = options.tlsOptions != null || urls.some((url) => parseFerricUrl(url).tls);
    this.adapterOptions = nativeOnlyOptions(withSeedAuthDefaults(urls, options));
    this.seedEndpointKeys = new Set(urls.map((url) => endpointKeyFor(endpointFromUrl(url))));
    this.trustedHosts = normalizedHostSet(options.trustedHosts ?? []);
  }

  static async fromUrls(urls: readonly string[], options: NativeAdapterOptions = {}): Promise<TopologyNativeAdapterPool> {
    const pool = new TopologyNativeAdapterPool(urls, options);
    await pool.refreshTopology();
    return pool;
  }

  get topology(): RoutingTopology {
    return this.topologyValue;
  }

  async refreshTopology(): Promise<RoutingTopology> {
    let lastError: unknown;
    for (const url of this.refreshCandidateUrls()) {
      try {
        const adapter = await this.adapterForUrl(url);
        const topology = RoutingTopology.build(await adapter.executeCommand("SHARDS"));
        this.topologyValue = topology;
        if (this.warmConnections) {
          await Promise.allSettled([...topology.endpoints.values()].map(async (endpoint) => await this.adapterForEndpoint(endpoint)));
        }
        return topology;
      } catch (error) {
        lastError = error;
      }
    }
    throw new FerricStoreError("no FerricStore topology endpoint reachable", { raw: lastError });
  }

  route(key: string | Buffer): RoutingRoute {
    const route = this.topologyValue.routeKey(key);
    this.validateEndpoint(route.endpoint);
    return route;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    const routed = this.routeData(args);
    if (routed == null) {
      return await (await this.controlAdapter()).executeCommand(...args);
    }

    const adapter = await this.adapterForEndpoint(routed.route.endpoint);
    try {
      return await adapter.executeProtocolCommand(routed.command, routed.route.laneId);
    } catch (error) {
      if (isRetryableRouteError(error)) {
        await this.refreshTopology().catch(() => undefined);
      }
      throw error;
    }
  }

  async executePipeline(commands: readonly Command[], options: ExecutePipelineOptions = {}): Promise<unknown[]> {
    if (commands.length === 0) {
      return [];
    }

    const route = this.singleRouteForCommands(commands);
    if (route == null) {
      return await (await this.controlAdapter()).executePipeline(commands, options);
    }

    const adapter = await this.adapterForEndpoint(route.endpoint);
    try {
      const response = await adapter.executeProtocolCommand(pipelineCommand(commands), route.laneId);
      return unwrapPipelineResponse(response, options);
    } catch (error) {
      if (isRetryableRouteError(error)) {
        await this.refreshTopology().catch(() => undefined);
      }
      throw error;
    }
  }

  async close(): Promise<void> {
    await Promise.allSettled([...this.adapters.values()].map(async (adapter) => await adapter.close()));
    this.adapters.clear();
  }

  private routeData(args: readonly CommandArgument[]): { readonly command: ProtocolCommand; readonly route: RoutingRoute } | undefined {
    if (args.length === 0) {
      return undefined;
    }
    let command: ProtocolCommand;
    try {
      command = buildProtocolCommand(args);
    } catch {
      return undefined;
    }
    const key = routingKey(args, command);
    if (key == null) {
      return undefined;
    }
    return { command, route: this.route(key) };
  }

  private singleRouteForCommands(commands: readonly Command[]): RoutingRoute | undefined {
    let route: RoutingRoute | undefined;
    for (const command of commands) {
      const routed = this.routeData(command);
      if (routed == null) {
        return undefined;
      }
      const current = routed.route;
      if (route == null) {
        route = current;
      } else if (route.endpointKey !== current.endpointKey || route.laneId !== current.laneId) {
        return undefined;
      }
    }
    return route;
  }

  private async controlAdapter(): Promise<NativeAdapter> {
    for (const url of this.refreshCandidateUrls()) {
      try {
        return await this.adapterForUrl(url);
      } catch {
        // Try the next known control endpoint.
      }
    }
    return await this.adapterForUrl(this.seedUrls[0] ?? "ferric://127.0.0.1:6388");
  }

  private async adapterForUrl(url: string): Promise<NativeAdapter> {
    const endpoint = endpointFromUrl(url);
    const key = endpointKeyFor(endpoint);
    const existing = this.adapters.get(key);
    if (existing != null) {
      return existing;
    }
    const adapter = await NativeAdapter.fromUrl(url, this.adapterOptions);
    this.adapters.set(key, adapter);
    return adapter;
  }

  private async adapterForEndpoint(endpoint: RoutingEndpoint): Promise<NativeAdapter> {
    this.validateEndpoint(endpoint);
    const key = endpointKeyFor(endpoint);
    const existing = this.adapters.get(key);
    if (existing != null) {
      return existing;
    }
    const adapter = await NativeAdapter.fromUrl(urlFromEndpoint(endpoint, this.tls), this.adapterOptions);
    this.adapters.set(key, adapter);
    return adapter;
  }

  private validateEndpoint(endpoint: RoutingEndpoint): void {
    let allowed: boolean;
    if (this.endpointPolicy === "any" || this.endpointPolicy === "none") {
      allowed = true;
    } else if (this.endpointPolicy === "seed_hosts") {
      allowed = this.seedEndpointKeys.has(endpointKeyFor(endpoint)) || this.trustedHosts.has(endpoint.host.toLowerCase());
    } else if ("allowHosts" in this.endpointPolicy) {
      allowed = normalizedHostSet(this.endpointPolicy.allowHosts).has(endpoint.host.toLowerCase());
    } else {
      throw new FerricStoreError("invalid endpoint policy", { raw: this.endpointPolicy });
    }
    if (!allowed) {
      throw new FerricStoreError("unsafe learned endpoint", { raw: endpoint });
    }
    if (this.endpointValidator != null && !this.endpointValidator(endpoint)) {
      throw new FerricStoreError("unsafe learned endpoint", { raw: endpoint });
    }
  }

  private refreshCandidateUrls(): string[] {
    const urls = [
      ...this.seedUrls,
      ...[...this.topologyValue.endpoints.values()].map((endpoint) => urlFromEndpoint(endpoint, this.tls))
    ];
    return [...new Set(urls)];
  }
}

function routingKey(args: readonly CommandArgument[], command: ProtocolCommand): string | Buffer | undefined {
  const name = commandName(args);
  if (command.opcode < 0x0100 || name === "CLUSTER.KEYSLOT" || name === "SHARDS" || name === "ROUTE") {
    return undefined;
  }

  const keyFromArgs = routingKeyFromArgs(name, args);
  if (keyFromArgs != null) {
    return keyFromArgs;
  }

  if (!isPlainObject(command.payload)) {
    return undefined;
  }

  for (const field of ["key", "partition_key", "id", "owner_flow_id", "parent_id", "root_id", "correlation_id", "scope"]) {
    const value = getField(command.payload, field);
    if (typeof value === "string" || Buffer.isBuffer(value)) {
      return value;
    }
  }

  const keys = getField(command.payload, "keys");
  if (Array.isArray(keys)) {
    return singleShardKey(keys);
  }

  const pairsValue = getField(command.payload, "pairs");
  if (Array.isArray(pairsValue)) {
    return singleShardKey(
      pairsValue
        .filter((pair): pair is readonly unknown[] => Array.isArray(pair) && pair.length > 0)
        .map((pair) => pair[0])
    );
  }

  return undefined;
}

function routingKeyFromArgs(name: string | undefined, args: readonly CommandArgument[]): string | Buffer | undefined {
  if (name == null) {
    return undefined;
  }

  if (name === "MGET" || name === "DEL") {
    return singleShardKey(args.slice(1));
  }
  if (name === "MSET") {
    const keys: CommandArgument[] = [];
    for (let index = 1; index < args.length; index += 2) {
      keys.push(args[index]);
    }
    return singleShardKey(keys);
  }
  if (name === "BITOP") {
    return bitopRoutingKey(args);
  }
  if (name === "RENAME" || name === "RENAMENX") {
    return singleShardKey(args.slice(1, 3));
  }
  if (name === "XREAD" || name === "XREADGROUP") {
    return streamReadRoutingKey(args);
  }
  if (name.startsWith("FLOW.")) {
    return flowRoutingKey(name, args);
  }
  if (firstKeyCommands.has(name)) {
    const key = args[1];
    return typeof key === "string" || Buffer.isBuffer(key) ? key : undefined;
  }

  return undefined;
}

function flowRoutingKey(name: string, args: readonly CommandArgument[]): string | Buffer | undefined {
  const flowArgs = args.slice(1);
  if (flowArgs.length === 0) {
    return undefined;
  }

  if (name === "FLOW.CLAIM_DUE" || name === "FLOW.RECLAIM") {
    return flowPartitionRoutingKey(flowArgs, 1);
  }

  if (name === "FLOW.CREATE_MANY" || name === "FLOW.COMPLETE_MANY" || name === "FLOW.TRANSITION_MANY" || name === "FLOW.RETRY_MANY" || name === "FLOW.FAIL_MANY" || name === "FLOW.CANCEL_MANY") {
    const partition = flowArgs[0];
    if (typeof partition === "string" && partition.toUpperCase() !== "AUTO" && partition.toUpperCase() !== "MIXED") {
      return partition;
    }
    if (Buffer.isBuffer(partition)) {
      const text = partition.toString("utf8").toUpperCase();
      if (text !== "AUTO" && text !== "MIXED") {
        return partition;
      }
    }
    return undefined;
  }

  const partition = flowPartitionRoutingKey(flowArgs, 1);
  if (partition != null) {
    return partition;
  }

  if (typeScopedFlowCommands.has(name)) {
    return undefined;
  }

  const id = flowArgs[0];
  return typeof id === "string" || Buffer.isBuffer(id) ? id : undefined;
}

function bitopRoutingKey(args: readonly CommandArgument[]): string | Buffer | undefined {
  if (args.length < 4) {
    return undefined;
  }
  return singleShardKey(args.slice(2));
}

function streamReadRoutingKey(args: readonly CommandArgument[]): string | Buffer | undefined {
  const streamsIndex = args.findIndex((arg) => commandPart(arg) === "STREAMS");
  if (streamsIndex < 0) {
    return undefined;
  }
  const streamArgs = args.slice(streamsIndex + 1);
  if (streamArgs.length === 0 || streamArgs.length % 2 !== 0) {
    return undefined;
  }
  return singleShardKey(streamArgs.slice(0, streamArgs.length / 2));
}

function flowPartitionRoutingKey(args: readonly CommandArgument[], start: number): string | Buffer | undefined {
  for (let index = start; index < args.length; index += 1) {
    const token = commandPart(args[index]);
    if (token === "PARTITION") {
      const key = args[index + 1];
      return typeof key === "string" || Buffer.isBuffer(key) ? key : undefined;
    }
    if (token === "PARTITIONS") {
      const count = Number(args[index + 1]);
      if (!Number.isInteger(count) || count < 0) {
        return undefined;
      }
      return singleShardKey(args.slice(index + 2, index + 2 + count));
    }
  }
  return undefined;
}

function singleShardKey(keys: readonly unknown[]): string | Buffer | undefined {
  const usable = keys.filter((key): key is string | Buffer => typeof key === "string" || Buffer.isBuffer(key));
  if (usable.length === 0) {
    return undefined;
  }
  const first = usable[0];
  if (first == null) {
    return undefined;
  }
  const firstSlot = RoutingTopology.slotForKey(first);
  return usable.every((key) => RoutingTopology.slotForKey(key) === firstSlot) ? first : undefined;
}

function commandName(args: readonly CommandArgument[]): string | undefined {
  const first = commandPart(args[0]);
  if (first == null) {
    return undefined;
  }
  if (first === "FLOW" && args.length > 1) {
    const second = commandPart(args[1]);
    return second == null ? first : `${first}.${second}`;
  }
  if (first === "CLIENT" && args.length > 1) {
    const second = commandPart(args[1]);
    return second == null ? first : `${first}.${second}`;
  }
  if (first === "CLUSTER" && args.length > 1) {
    const second = commandPart(args[1]);
    return second == null ? first : `${first}.${second}`;
  }
  return first;
}

function commandPart(value: CommandArgument): string | undefined {
  if (typeof value === "string") {
    return value.toUpperCase();
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8").toUpperCase();
  }
  return undefined;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value);
}

function getField(source: unknown, ...keys: string[]): unknown {
  if (source instanceof Map) {
    for (const key of keys) {
      if (source.has(key)) return source.get(key);
      const bufferKey = Buffer.from(key);
      for (const [itemKey, value] of source.entries()) {
        if (Buffer.isBuffer(itemKey) && itemKey.equals(bufferKey)) {
          return value;
        }
      }
    }
    return undefined;
  }
  if (isPlainObject(source)) {
    for (const key of keys) {
      if (key in source) return source[key];
    }
  }
  return undefined;
}

function textOrUndefined(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  return undefined;
}

function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) ? value : undefined;
}

function endpointFromRange(item: unknown): RoutingEndpoint {
  const endpointValue = getField(item, "endpoint");
  const raw = isPlainObject(endpointValue) || endpointValue instanceof Map ? endpointValue : item;
  const host = textOrUndefined(getField(raw, "host", "native_host"));
  const nativePort = numberOrUndefined(getField(raw, "native_port"));
  if (host == null || nativePort == null) {
    throw new FerricStoreError("invalid SHARDS endpoint", { raw: item });
  }
  const nativeTlsPort = numberOrUndefined(getField(raw, "native_tls_port"));
  return {
    host,
    nativePort,
    node: textOrUndefined(getField(raw, "node", "leader_node", "owner_node")) ?? host,
    ...(nativeTlsPort == null ? {} : { nativeTlsPort })
  };
}

function endpointFromUrl(url: string): RoutingEndpoint {
  const parsed = parseFerricUrl(url);
  return {
    host: parsed.host,
    nativePort: parsed.port,
    node: parsed.host,
    ...(parsed.tls ? { nativeTlsPort: parsed.port } : {})
  };
}

function parseFerricUrl(value: string): ParsedUrl {
  const url = new URL(value);
  if (url.protocol !== "ferric:" && url.protocol !== "ferrics:") {
    throw new FerricStoreError(`unsupported FerricStore URL scheme: ${url.protocol}`);
  }
  return {
    host: url.hostname || "127.0.0.1",
    ...(url.password === "" ? {} : { password: decodeURIComponent(url.password) }),
    port: Number(url.port || (url.protocol === "ferrics:" ? 6389 : 6388)),
    tls: url.protocol === "ferrics:",
    ...(url.username === "" ? {} : { username: decodeURIComponent(url.username) })
  };
}

function endpointKeyFor(endpoint: RoutingEndpoint): string {
  return `${endpoint.host.toLowerCase()}:${endpoint.nativePort}`;
}

function urlFromEndpoint(endpoint: RoutingEndpoint, useTls: boolean): string {
  const port = useTls && endpoint.nativeTlsPort != null ? endpoint.nativeTlsPort : endpoint.nativePort;
  const host = endpoint.host.includes(":") && !endpoint.host.startsWith("[") ? `[${endpoint.host}]` : endpoint.host;
  return `${useTls ? "ferrics" : "ferric"}://${host}:${port}`;
}

function nativeOnlyOptions(options: NativeAdapterOptions): NativeAdapterOptions {
  const nativeOptions = { ...options };
  delete nativeOptions.endpointPolicy;
  delete nativeOptions.endpointValidator;
  delete nativeOptions.haRouting;
  delete nativeOptions.seeds;
  delete nativeOptions.trustedHosts;
  delete nativeOptions.warmConnections;
  return nativeOptions;
}

function withSeedAuthDefaults(urls: readonly string[], options: NativeAdapterOptions): NativeAdapterOptions {
  const next: NativeAdapterOptions = { ...options };
  for (const url of urls) {
    const parsed = parseFerricUrl(url);
    if (next.username == null && parsed.username != null) {
      next.username = parsed.username;
    }
    if (next.password == null && parsed.password != null) {
      next.password = parsed.password;
    }
    if (next.username != null && next.password != null) {
      break;
    }
  }
  return next;
}

function isRetryableRouteError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const message = error.message.toLowerCase();
  return (
    message.includes("connection closed") ||
    message.includes("connection is closed") ||
    message.includes("shard not available") ||
    message.includes("leader") ||
    message.includes("route")
  );
}

function normalizedHostSet(hosts: readonly (string | undefined)[]): ReadonlySet<string> {
  return new Set(hosts.filter((host): host is string => host != null && host !== "").map((host) => host.toLowerCase()));
}

function hashTagOrKey(key: string): string {
  const start = key.indexOf("{");
  if (start < 0) {
    return key;
  }
  const end = key.indexOf("}", start + 1);
  return end > start + 1 ? key.slice(start + 1, end) : key;
}

function flowHashTag(rest: string, fallbackKey: string): string {
  const end = rest.indexOf("}");
  return end > 0 ? rest.slice(0, end) : hashTagOrKey(fallbackKey);
}

const crc32Table = new Uint32Array(256);
for (let index = 0; index < 256; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) !== 0 ? 0xedb8_8320 ^ (value >>> 1) : value >>> 1;
  }
  crc32Table[index] = value >>> 0;
}

function crc32(buffer: Buffer): number {
  let value = 0xffff_ffff;
  for (const byte of buffer) {
    value = (value >>> 8) ^ (crc32Table[(value ^ byte) & 0xff] ?? 0);
  }
  return (value ^ 0xffff_ffff) >>> 0;
}

const firstKeyCommands = new Set([
  "BITCOUNT",
  "BITFIELD",
  "BITPOS",
  "CAS",
  "EXISTS",
  "EXPIRE",
  "EXPIREAT",
  "FETCH_OR_COMPUTE",
  "FETCH_OR_COMPUTE_ERROR",
  "FETCH_OR_COMPUTE_RESULT",
  "FERRICSTORE.KEY_INFO",
  "GET",
  "GETBIT",
  "GETDEL",
  "GETEX",
  "HDEL",
  "HEXISTS",
  "HGET",
  "HGETALL",
  "HINCRBY",
  "HKEYS",
  "HLEN",
  "HMGET",
  "HMSET",
  "HSET",
  "HVALS",
  "LOCK",
  "LPOP",
  "LPUSH",
  "LRANGE",
  "LREM",
  "RATELIMIT.ADD",
  "RPOP",
  "RPUSH",
  "SADD",
  "SCARD",
  "SISMEMBER",
  "SMEMBERS",
  "SREM",
  "SET",
  "SETBIT",
  "STRLEN",
  "TTL",
  "TYPE",
  "UNLINK",
  "UNLOCK",
  "XADD",
  "XLEN",
  "XRANGE",
  "ZADD",
  "ZCARD",
  "ZRANGE",
  "ZREM",
  "ZSCORE"
]);

const typeScopedFlowCommands = new Set([
  "FLOW.APPROVAL.LIST",
  "FLOW.ATTRIBUTE_VALUES",
  "FLOW.ATTRIBUTES",
  "FLOW.BUDGET.LIST",
  "FLOW.FAILURES",
  "FLOW.GOVERNANCE.OVERVIEW",
  "FLOW.INFO",
  "FLOW.LIMIT.LIST",
  "FLOW.LIST",
  "FLOW.POLICY.GET",
  "FLOW.POLICY.SET",
  "FLOW.RETENTION_CLEANUP",
  "FLOW.SCHEDULE.FIRE_DUE",
  "FLOW.SCHEDULE.LIST",
  "FLOW.SEARCH",
  "FLOW.STATS",
  "FLOW.STUCK",
  "FLOW.TERMINALS"
]);
