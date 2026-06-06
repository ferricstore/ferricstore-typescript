import type { Codec } from "./codecs.js";
import { RawCodec } from "./codecs.js";
import { OverloadedError, mapException } from "./errors.js";
import {
  append,
  appendBool,
  appendEncoded,
  appendNamedValues,
  appendValueReturn,
  arrayResponse,
  autoPartitionKeyForId,
  expandManyResponse,
  nowMs,
  okResponse,
  parseKvResponse,
  sleep,
  text,
  type Command,
  type CommandArgument
} from "./internal.js";
import { RedisAdapter, type RedisCommandExecutor } from "./adapters.js";
import {
  BitmapStore,
  GeoStore,
  HashStore,
  HyperLogLogStore,
  KeyValueStore,
  ListStore,
  SetStore,
  SortedSetStore,
  StreamStore
} from "./store.js";
import {
  BloomFilterStore,
  CountMinSketchStore,
  CuckooFilterStore,
  JsonStore,
  TDigestStore,
  TopKStore
} from "./modules.js";
import {
  claimedItemFromResp,
  fetchOrComputeResultFromResp,
  flowRecordFromResp,
  keyInfoFromResp,
  rateLimitResultFromResp,
  type BackpressurePolicy,
  type ChildSpec,
  type ClaimedItem,
  type CreateItem,
  type FencedItem,
  type FetchOrComputeResult,
  type FlowRecord,
  type KeyInfo,
  type RateLimitResult,
  type RetryPolicy
} from "./types.js";

export interface FlowClientOptions {
  codec?: Codec;
  backpressure?: BackpressurePolicy;
}

export interface CreateOptions {
  type: string;
  state?: string;
  payload?: unknown;
  partitionKey?: string;
  parentFlowId?: string;
  rootFlowId?: string;
  correlationId?: string;
  runAtMs?: number;
  nowMs?: number;
  priority?: number;
  idempotent?: boolean;
  retentionTtlMs?: number;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  returnRecord?: boolean;
}

export interface CreateManyOptions extends Omit<CreateOptions, "payload" | "partitionKey" | "returnRecord"> {
  partitionKey?: string;
  independent?: boolean;
}

export interface ClaimDueOptions {
  state?: string;
  states?: string[];
  worker: string;
  partitionKey?: string;
  partitionKeys?: string[];
  leaseMs?: number;
  limit?: number;
  priority?: number;
  nowMs?: number;
  blockMs?: number;
  reclaimExpired?: boolean;
  reclaimRatio?: number;
  jobOnly?: boolean;
  payload?: boolean;
  payloadMaxBytes?: number;
  values?: string[];
  valueMaxBytes?: number;
  includeState?: boolean;
}

export interface ReclaimOptions extends Omit<ClaimDueOptions, "state" | "states" | "blockMs" | "reclaimExpired" | "reclaimRatio" | "includeState"> {
  state?: "running";
}

export interface MutateOptions {
  partitionKey?: string;
  payload?: unknown;
  values?: Record<string, unknown>;
  valueRefs?: Record<string, string>;
  dropValues?: string[];
  overrideValues?: string[];
  nowMs?: number;
  returnRecord?: boolean;
}

export interface LeaseMutationOptions extends MutateOptions {
  leaseToken: Buffer;
  fencingToken: number;
}

export interface TransitionOptions extends LeaseMutationOptions {
  fromState: string;
  toState: string;
  runAtMs?: number;
  priority?: number;
}

export interface CompleteOptions extends LeaseMutationOptions {
  result?: unknown;
  ttlMs?: number;
}

export interface RetryOptions extends LeaseMutationOptions {
  error?: unknown;
  runAtMs?: number;
}

export interface FailOptions extends LeaseMutationOptions {
  error?: unknown;
  ttlMs?: number;
}

export interface CancelOptions {
  fencingToken: number;
  leaseToken?: Buffer;
  partitionKey?: string;
  reason?: unknown;
  ttlMs?: number;
  nowMs?: number;
  returnRecord?: boolean;
}

export interface ReadOptions {
  partitionKey?: string;
  count?: number;
  fromMs?: number;
  toMs?: number;
  rev?: boolean;
  state?: string;
  terminalOnly?: boolean;
  includeCold?: boolean;
  consistentProjection?: boolean;
}

export class FlowClient {
  readonly executor: RedisCommandExecutor;
  readonly codec: Codec;
  readonly backpressure: Required<BackpressurePolicy>;
  readonly bitmap: BitmapStore;
  readonly bloom: BloomFilterStore;
  readonly cms: CountMinSketchStore;
  readonly cuckoo: CuckooFilterStore;
  readonly geo: GeoStore;
  readonly hash: HashStore;
  readonly hyperloglog: HyperLogLogStore;
  readonly json: JsonStore;
  readonly kv: KeyValueStore;
  readonly lists: ListStore;
  readonly sets: SetStore;
  readonly stream: StreamStore;
  readonly tdigest: TDigestStore;
  readonly topk: TopKStore;
  readonly zset: SortedSetStore;

  constructor(executor: RedisCommandExecutor, options: FlowClientOptions = {}) {
    this.executor = new ErrorMappingExecutor(executor);
    this.codec = options.codec ?? new RawCodec();
    this.backpressure = {
      baseDelayMs: options.backpressure?.baseDelayMs ?? 25,
      jitterPct: options.backpressure?.jitterPct ?? 20,
      maxDelayMs: options.backpressure?.maxDelayMs ?? 1_000,
      maxRetries: options.backpressure?.maxRetries ?? 8
    };
    this.bitmap = new BitmapStore(this);
    this.bloom = new BloomFilterStore(this);
    this.cms = new CountMinSketchStore(this);
    this.cuckoo = new CuckooFilterStore(this);
    this.geo = new GeoStore(this);
    this.hash = new HashStore(this);
    this.hyperloglog = new HyperLogLogStore(this);
    this.json = new JsonStore(this);
    this.kv = new KeyValueStore(this);
    this.lists = new ListStore(this);
    this.sets = new SetStore(this);
    this.stream = new StreamStore(this);
    this.tdigest = new TDigestStore(this);
    this.topk = new TopKStore(this);
    this.zset = new SortedSetStore(this);
  }

  static async fromUrl(url: string, options: FlowClientOptions & { redisOptions?: Record<string, unknown> } = {}): Promise<FlowClient> {
    return new FlowClient(await RedisAdapter.fromUrl(url, options.redisOptions), options);
  }

  async command(...args: CommandArgument[]): Promise<unknown> {
    return await this.executor.executeCommand(...args);
  }

  async pipeline(commands: readonly Command[]): Promise<unknown[]> {
    if (this.executor.executePipeline != null) {
      return await this.executor.executePipeline(commands);
    }
    return await Promise.all(commands.map((command) => this.command(...command)));
  }

  async close(): Promise<void> {
    await this.executor.close?.();
  }

  async ping(message?: CommandArgument): Promise<unknown> {
    return await this.command("PING", ...(message == null ? [] : [message]));
  }

  async echo(message: CommandArgument): Promise<unknown> {
    return await this.command("ECHO", message);
  }

  async serverInfo(section?: string): Promise<string> {
    return text(await this.command("INFO", ...(section == null ? [] : [section])));
  }

  async configGet(pattern: string): Promise<unknown> {
    return await this.command("CONFIG", "GET", pattern);
  }

  async configSet(key: string, value: CommandArgument, options: { local?: boolean } = {}): Promise<boolean> {
    return okResponse(await this.command("CONFIG", "SET", ...(options.local === true ? ["LOCAL"] : []), key, value));
  }

  async configGetLocal(key: string): Promise<unknown> {
    return await this.command("CONFIG", "GET", "LOCAL", key);
  }

  async configResetStat(): Promise<boolean> {
    return okResponse(await this.command("CONFIG", "RESETSTAT"));
  }

  async configRewrite(): Promise<boolean> {
    return okResponse(await this.command("CONFIG", "REWRITE"));
  }

  async slowlogGet(count?: number): Promise<unknown[]> {
    return arrayResponse(await this.command("SLOWLOG", "GET", ...(count == null ? [] : [count])));
  }

  async slowlogLen(): Promise<number> {
    return Number(await this.command("SLOWLOG", "LEN"));
  }

  async slowlogReset(): Promise<boolean> {
    return okResponse(await this.command("SLOWLOG", "RESET"));
  }

  async commandMetadata(): Promise<unknown> {
    return await this.command("COMMAND");
  }

  async commandCount(): Promise<number> {
    return Number(await this.command("COMMAND", "COUNT"));
  }

  async commandList(): Promise<unknown[]> {
    return arrayResponse(await this.command("COMMAND", "LIST"));
  }

  async commandInfo(...names: string[]): Promise<unknown[]> {
    return arrayResponse(await this.command("COMMAND", "INFO", ...names));
  }

  async commandDocs(...names: string[]): Promise<unknown> {
    return await this.command("COMMAND", "DOCS", ...names);
  }

  async commandGetKeys(command: Command): Promise<unknown[]> {
    return arrayResponse(await this.command("COMMAND", "GETKEYS", ...command));
  }

  async clientId(): Promise<number> {
    return Number(await this.command("CLIENT", "ID"));
  }

  async clientSetName(name: string): Promise<boolean> {
    return okResponse(await this.command("CLIENT", "SETNAME", name));
  }

  async clientGetName(): Promise<string | null> {
    const response = await this.command("CLIENT", "GETNAME");
    return response == null ? null : text(response);
  }

  async clientInfo(): Promise<string> {
    return text(await this.command("CLIENT", "INFO"));
  }

  async clientList(options: { type?: string } = {}): Promise<string> {
    return text(await this.command("CLIENT", "LIST", ...(options.type == null ? [] : ["TYPE", options.type])));
  }

  async clientTracking(mode: "ON" | "OFF", options: {
    redirect?: number;
    prefixes?: string[];
    bcast?: boolean;
    optin?: boolean;
    optout?: boolean;
    noloop?: boolean;
  } = {}): Promise<boolean> {
    const args: CommandArgument[] = ["CLIENT", "TRACKING", mode];
    append(args, "REDIRECT", options.redirect);
    for (const prefix of options.prefixes ?? []) {
      args.push("PREFIX", prefix);
    }
    if (options.bcast === true) args.push("BCAST");
    if (options.optin === true) args.push("OPTIN");
    if (options.optout === true) args.push("OPTOUT");
    if (options.noloop === true) args.push("NOLOOP");
    return okResponse(await this.command(...args));
  }

  async clientCaching(mode: "YES" | "NO"): Promise<boolean> {
    return okResponse(await this.command("CLIENT", "CACHING", mode));
  }

  async clientTrackingInfo(): Promise<unknown> {
    return await this.command("CLIENT", "TRACKINGINFO");
  }

  async clientGetRedir(): Promise<number> {
    return Number(await this.command("CLIENT", "GETREDIR"));
  }

  async save(): Promise<boolean> {
    return okResponse(await this.command("SAVE"));
  }

  async bgsave(): Promise<boolean> {
    const response = await this.command("BGSAVE");
    return okResponse(response) || text(response) === "Background saving started";
  }

  async lastsave(): Promise<number> {
    return Number(await this.command("LASTSAVE"));
  }

  async lolwut(version?: number): Promise<string> {
    return text(await this.command("LOLWUT", ...(version == null ? [] : ["VERSION", version])));
  }

  async moduleList(): Promise<unknown[]> {
    return arrayResponse(await this.command("MODULE", "LIST"));
  }

  async publish(channel: string, message: CommandArgument): Promise<number> {
    return Number(await this.command("PUBLISH", channel, message));
  }

  async pubsubChannels(pattern?: string): Promise<unknown[]> {
    return arrayResponse(await this.command("PUBSUB", "CHANNELS", ...(pattern == null ? [] : [pattern])));
  }

  async pubsubNumSub(...channels: string[]): Promise<unknown[]> {
    return arrayResponse(await this.command("PUBSUB", "NUMSUB", ...channels));
  }

  async pubsubNumPat(): Promise<number> {
    return Number(await this.command("PUBSUB", "NUMPAT"));
  }

  async aclSetUser(username: string, rules: string[]): Promise<boolean> {
    return okResponse(await this.command("ACL", "SETUSER", username, ...rules));
  }

  async aclDelUser(...usernames: string[]): Promise<number> {
    return Number(await this.command("ACL", "DELUSER", ...usernames));
  }

  async aclGetUser(username: string): Promise<unknown> {
    return await this.command("ACL", "GETUSER", username);
  }

  async aclList(): Promise<unknown[]> {
    return arrayResponse(await this.command("ACL", "LIST"));
  }

  async aclWhoami(): Promise<string> {
    return text(await this.command("ACL", "WHOAMI"));
  }

  async aclSave(): Promise<boolean> {
    return okResponse(await this.command("ACL", "SAVE"));
  }

  async aclLoad(): Promise<boolean> {
    return okResponse(await this.command("ACL", "LOAD"));
  }

  async auth(password: string, username?: string): Promise<boolean> {
    return okResponse(await this.command("AUTH", ...(username == null ? [] : [username]), password));
  }

  async cas(key: string, expected: unknown, value: unknown, options: { ex?: number } = {}): Promise<boolean> {
    const args: CommandArgument[] = ["CAS", key, this.codec.encode(expected), this.codec.encode(value)];
    append(args, "EX", options.ex);
    return Boolean(await this.command(...args));
  }

  async lock(key: string, owner: string, ttlMs: number): Promise<boolean> {
    return okResponse(await this.command("LOCK", key, owner, ttlMs));
  }

  async unlock(key: string, owner: string): Promise<number> {
    return Number(await this.command("UNLOCK", key, owner));
  }

  async extendLock(key: string, owner: string, ttlMs: number): Promise<number> {
    return Number(await this.command("EXTEND", key, owner, ttlMs));
  }

  async rateLimitAdd(key: string, options: { windowMs: number; max: number; count?: number }): Promise<RateLimitResult> {
    return rateLimitResultFromResp(
      await this.command("RATELIMIT.ADD", key, options.windowMs, options.max, options.count ?? 1)
    );
  }

  async keyInfo(key: string): Promise<KeyInfo> {
    return keyInfoFromResp(await this.command("FERRICSTORE.KEY_INFO", key));
  }

  async fetchOrCompute<T = unknown>(
    key: string,
    options: { ttlMs: number; hint?: string }
  ): Promise<FetchOrComputeResult<T>> {
    const args: CommandArgument[] = ["FETCH_OR_COMPUTE", key, options.ttlMs];
    if (options.hint != null) {
      args.push(options.hint);
    }
    return fetchOrComputeResultFromResp<T>(await this.command(...args), this.codec);
  }

  async fetchOrComputeResult(key: string, value: unknown, options: { ttlMs: number }): Promise<boolean> {
    return okResponse(await this.command("FETCH_OR_COMPUTE_RESULT", key, this.codec.encode(value), options.ttlMs));
  }

  async fetchOrComputeError(key: string, message: string): Promise<boolean> {
    return okResponse(await this.command("FETCH_OR_COMPUTE_ERROR", key, message));
  }

  async clusterHealth(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.HEALTH"));
  }

  async clusterStats(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.STATS"));
  }

  async clusterKeyslot(key: string): Promise<number> {
    return Number(await this.command("CLUSTER.KEYSLOT", key));
  }

  async clusterSlots(): Promise<unknown> {
    return await this.command("CLUSTER.SLOTS");
  }

  async clusterStatus(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.STATUS"));
  }

  async clusterRole(): Promise<unknown> {
    return await this.command("CLUSTER.ROLE");
  }

  async clusterJoin(node: string, options: { replace?: boolean } = {}): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.JOIN", node, ...(options.replace === true ? ["REPLACE"] : [])));
  }

  async clusterLeave(): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.LEAVE"));
  }

  async clusterFailover(shardIndex: number, targetNode: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.FAILOVER", shardIndex, targetNode));
  }

  async clusterPromote(node: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.PROMOTE", node));
  }

  async clusterDemote(node: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.DEMOTE", node));
  }

  async ferricstoreConfig(...args: CommandArgument[]): Promise<unknown> {
    return await this.command("FERRICSTORE.CONFIG", ...args);
  }

  async ferricstoreHotness(...args: CommandArgument[]): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FERRICSTORE.HOTNESS", ...args));
  }

  async ferricstoreMetrics(...args: CommandArgument[]): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FERRICSTORE.METRICS", ...args));
  }

  async ferricstoreBlobgc(...args: CommandArgument[]): Promise<unknown> {
    return await this.command("FERRICSTORE.BLOBGC", ...args);
  }

  async ferricstoreDoctor(...args: CommandArgument[]): Promise<unknown> {
    return await this.command("FERRICSTORE.DOCTOR", ...args);
  }

  async create(id: string, options: CreateOptions): Promise<FlowRecord | Buffer | unknown> {
    const currentNowMs = options.nowMs ?? nowMs();
    const args: CommandArgument[] = [
      "FLOW.CREATE",
      id,
      "TYPE",
      options.type,
      "STATE",
      options.state ?? "queued",
      "NOW",
      currentNowMs
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "PARENT_FLOW_ID", options.parentFlowId);
    append(args, "ROOT_FLOW_ID", options.rootFlowId);
    append(args, "CORRELATION_ID", options.correlationId);
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendBool(args, "IDEMPOTENT", options.idempotent);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    appendNamedValues(args, this.codec, options);

    const response = await this.executeProducerWrite(args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async enqueue(id: string, options: Omit<CreateOptions, "state"> & { state?: string }): Promise<FlowRecord | Buffer | unknown> {
    return await this.create(id, {
      ...options,
      priority: options.priority ?? 0,
      state: options.state ?? "queued"
    });
  }

  async enqueueMany(items: CreateItem[], options: CreateManyOptions): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }

    if (options.partitionKey != null || items.some((item) => item.partitionKey != null)) {
      return await this.createMany(options.partitionKey, items, {
        ...options,
        independent: options.independent ?? true,
        priority: options.priority ?? 0,
        state: options.state ?? "queued"
      });
    }

    const grouped = new Map<string, [number, CreateItem][]>();
    items.forEach((item, index) => {
      const bucket = autoPartitionKeyForId(item.id);
      grouped.set(bucket, [...(grouped.get(bucket) ?? []), [index, item]]);
    });

    const results = Array<unknown>(items.length);
    for (const [bucket, indexedItems] of grouped.entries()) {
      const groupItems = indexedItems.map(([, item]) => item);
      const response = await this.createMany(bucket, groupItems, {
        ...options,
        independent: options.independent ?? true,
        priority: options.priority ?? 0,
        state: options.state ?? "queued"
      });
      const expanded = expandManyResponse(response, indexedItems.length);
      indexedItems.forEach(([index], resultIndex) => {
        results[index] = expanded[resultIndex];
      });
    }
    return results;
  }

  async createMany(partitionKey: string | undefined, items: CreateItem[], options: CreateManyOptions): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }

    if (partitionKey != null) {
      for (const item of items) {
        if (item.partitionKey != null && item.partitionKey !== partitionKey) {
          throw new Error("createMany item partitionKey does not match batch partitionKey");
        }
      }
    }

    const currentNowMs = options.nowMs ?? nowMs();
    const mixed = partitionKey == null && items.some((item) => item.partitionKey != null);
    const auto = partitionKey == null && !mixed;
    const wirePartition = mixed ? "MIXED" : auto ? "AUTO" : partitionKey;
    const args: CommandArgument[] = [
      "FLOW.CREATE_MANY",
      wirePartition,
      "TYPE",
      options.type,
      "STATE",
      options.state ?? "queued",
      "NOW",
      currentNowMs
    ];
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendBool(args, "IDEMPOTENT", options.idempotent);
    appendBool(args, "INDEPENDENT", options.independent);
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);

    const extendedItems = items.some((item) => item.values != null || item.valueRefs != null) || (mixed && items.some((item) => item.partitionKey == null));
    if (extendedItems) {
      args.push("ITEMS_EXT", items.length);
      for (const item of items) {
        const itemValues = { ...(options.values ?? {}), ...(item.values ?? {}) };
        const itemRefs = { ...(options.valueRefs ?? {}), ...(item.valueRefs ?? {}) };
        args.push(item.id, mixed ? item.partitionKey ?? "-" : "-", this.codec.encode(item.payload));
        appendNamedCounts(args, this.codec, itemValues, itemRefs);
      }
    } else {
      appendNamedValues(args, this.codec, options);
      args.push("ITEMS");
      for (const item of items) {
        if (mixed) {
          if (item.partitionKey == null) {
            throw new Error("mixed createMany items require partitionKey");
          }
          args.push(item.id, item.partitionKey, this.codec.encode(item.payload));
        } else {
          args.push(item.id, this.codec.encode(item.payload));
        }
      }
    }

    return this.recordsOrResponse(await this.executeProducerWrite(args));
  }

  async valuePut(
    value: unknown,
    options: {
      partitionKey?: string;
      ownerFlowId?: string;
      name?: string;
      override?: boolean;
      ttlMs?: number;
      nowMs?: number;
    } = {}
  ): Promise<unknown> {
    const args: CommandArgument[] = ["FLOW.VALUE.PUT", this.codec.encode(value), "NOW", options.nowMs ?? nowMs()];
    append(args, "PARTITION", options.partitionKey);
    append(args, "OWNER_FLOW_ID", options.ownerFlowId);
    append(args, "NAME", options.name);
    appendBool(args, "OVERRIDE", options.override);
    append(args, "TTL", options.ttlMs);
    return await this.command(...args);
  }

  async valueMGet(refs: string[], options: { maxBytes?: number } = {}): Promise<unknown[]> {
    if (refs.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.VALUE.MGET", ...refs];
    append(args, "MAX_BYTES", options.maxBytes);
    const response = await this.command(...args);
    if (!Array.isArray(response)) {
      return [];
    }
    return (response as unknown[]).map((item) => (Buffer.isBuffer(item) ? this.codec.decode(item) : item));
  }

  async signal(id: string, options: {
    signal: string;
    partitionKey?: string;
    idempotencyKey?: string;
    ifState?: string | string[];
    transitionTo?: string;
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
  }): Promise<unknown> {
    const args: CommandArgument[] = ["FLOW.SIGNAL", id, "SIGNAL", options.signal];
    append(args, "PARTITION", options.partitionKey);
    append(args, "IDEMPOTENCY", options.idempotencyKey);
    if (Array.isArray(options.ifState)) {
      for (const state of options.ifState) {
        append(args, "IF_STATE", state);
      }
    } else {
      append(args, "IF_STATE", options.ifState);
    }
    append(args, "TRANSITION_TO", options.transitionTo);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    append(args, "PRIORITY", options.priority);
    appendNamedValues(args, this.codec, options);
    return await this.command(...args);
  }

  async flowSignal(id: string, options: Parameters<FlowClient["signal"]>[1]): Promise<unknown> {
    return await this.signal(id, options);
  }

  async claimDue(type: string, options: ClaimDueOptions): Promise<(FlowRecord | ClaimedItem)[]> {
    if (options.state != null && options.states != null) {
      throw new Error("state and states are mutually exclusive");
    }
    const args: CommandArgument[] = ["FLOW.CLAIM_DUE", type];
    if (options.states != null) {
      if (options.states.length === 0) {
        throw new Error("states must be non-empty");
      }
      for (const state of options.states) {
        append(args, "STATE", state);
      }
    } else {
      append(args, "STATE", options.state);
    }

    args.push("WORKER", options.worker, "LEASE_MS", options.leaseMs ?? 30_000, "LIMIT", options.limit ?? 1);
    append(args, "NOW", options.nowMs);
    this.appendPartitionOptions(args, options);
    append(args, "PRIORITY", options.priority);
    if (options.includeState === true && options.jobOnly !== true) {
      throw new Error("includeState requires jobOnly=true");
    }
    if (options.jobOnly === true) {
      append(args, "RETURN", options.includeState === true ? "JOBS_COMPACT_STATE" : "JOBS_COMPACT");
    }
    append(args, "BLOCK", options.blockMs);
    appendPayloadRead(args, options.payload, options.payloadMaxBytes);
    appendValueReturn(args, { values: options.values, valueMaxBytes: options.valueMaxBytes });
    appendBool(args, "RECLAIM_EXPIRED", options.reclaimExpired);
    append(args, "RECLAIM_RATIO", options.reclaimRatio);

    const response = await this.command(...args);
    if (!Array.isArray(response)) {
      return [];
    }
    if (options.jobOnly === true) {
      return response.map((item) => claimedItemFromResp(item, this.codec));
    }
    return this.records(response);
  }

  async claimJobs(type: string, options: Omit<ClaimDueOptions, "jobOnly">): Promise<ClaimedItem[]> {
    return (await this.claimDue(type, {
      ...options,
      includeState: options.includeState ?? false,
      jobOnly: true,
      limit: options.limit ?? 100,
      priority: options.priority ?? 0
    }));
  }

  async reclaim(type: string, options: ReclaimOptions): Promise<(FlowRecord | ClaimedItem)[]> {
    if (options.state != null && options.state !== "running") {
      throw new Error("FLOW.RECLAIM only supports running state");
    }
    const args: CommandArgument[] = [
      "FLOW.RECLAIM",
      type,
      "WORKER",
      options.worker,
      "LEASE_MS",
      options.leaseMs ?? 30_000,
      "LIMIT",
      options.limit ?? 1,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    this.appendPartitionOptions(args, options);
    append(args, "PRIORITY", options.priority);
    if (options.jobOnly === true) {
      append(args, "RETURN", "JOBS_COMPACT");
    }
    appendPayloadRead(args, options.payload, options.payloadMaxBytes);
    appendValueReturn(args, { values: options.values, valueMaxBytes: options.valueMaxBytes });
    const response = await this.command(...args);
    if (!Array.isArray(response)) {
      return [];
    }
    if (options.jobOnly === true) {
      return response.map((item) => claimedItemFromResp(item, this.codec));
    }
    return this.records(response);
  }

  async extendLease(id: string, options: {
    leaseToken: Buffer;
    fencingToken: number;
    leaseMs: number;
    partitionKey?: string;
    nowMs?: number;
  }): Promise<FlowRecord> {
    const args: CommandArgument[] = [
      "FLOW.EXTEND_LEASE",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "LEASE_MS",
      options.leaseMs,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    return await this.recordOrGet(await this.command(...args), id, options.partitionKey);
  }

  async transition(id: string, options: TransitionOptions): Promise<FlowRecord | Buffer | unknown> {
    const currentNowMs = options.nowMs ?? nowMs();
    const args: CommandArgument[] = [
      "FLOW.TRANSITION",
      id,
      options.fromState,
      options.toState,
      "LEASE_TOKEN",
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      currentNowMs
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs ?? currentNowMs);
    append(args, "PRIORITY", options.priority);
    appendNamedValues(args, this.codec, options);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async complete(id: string, options: CompleteOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.COMPLETE",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "RESULT", this.codec, options.result);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    appendNamedValues(args, this.codec, options);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async completeMany(partitionKey: string | undefined, items: ClaimedItem[], options: {
    result?: unknown;
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    ttlMs?: number;
    nowMs?: number;
    independent?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.COMPLETE_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "RESULT", this.codec, options.result);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendNamedValues(args, this.codec, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.COMPLETE_MANY");
    return this.recordsOrResponse(await this.command(...args));
  }

  async completeJobs(jobs: ClaimedItem[], options: Parameters<FlowClient["completeMany"]>[2] = {}): Promise<unknown[] | unknown> {
    if (jobs.length === 0) {
      return [];
    }
    const firstPartition = jobs[0]?.partitionKey;
    const partitionKey = firstPartition != null && jobs.every((job) => job.partitionKey === firstPartition) ? firstPartition : undefined;
    return await this.completeMany(partitionKey, jobs, { ...options, independent: options.independent ?? true });
  }

  async transitionMany(partitionKey: string | undefined, options: {
    fromState: string;
    toState: string;
    items: FencedItem[];
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    runAtMs?: number;
    nowMs?: number;
    priority?: number;
    independent?: boolean;
  }): Promise<unknown[] | unknown> {
    if (options.items.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.TRANSITION_MANY", partitionKey ?? "MIXED", options.fromState, options.toState];
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "PRIORITY", options.priority);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendNamedValues(args, this.codec, options);
    appendFencedItems(args, partitionKey, options.items, "FLOW.TRANSITION_MANY", true);
    return this.recordsOrResponse(await this.command(...args));
  }

  async retry(id: string, options: RetryOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.RETRY",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    appendNamedValues(args, this.codec, options);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async retryMany(partitionKey: string | undefined, items: ClaimedItem[], options: {
    error?: unknown;
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    runAtMs?: number;
    nowMs?: number;
    independent?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.RETRY_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "RUN_AT", options.runAtMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendNamedValues(args, this.codec, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.RETRY_MANY");
    return this.recordsOrResponse(await this.command(...args));
  }

  async fail(id: string, options: FailOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = [
      "FLOW.FAIL",
      id,
      options.leaseToken,
      "FENCING",
      options.fencingToken,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    appendNamedValues(args, this.codec, options);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async failMany(partitionKey: string | undefined, items: ClaimedItem[], options: {
    error?: unknown;
    payload?: unknown;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
    ttlMs?: number;
    nowMs?: number;
    independent?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.FAIL_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "ERROR", this.codec, options.error);
    appendEncoded(args, "PAYLOAD", this.codec, options.payload);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendNamedValues(args, this.codec, options);
    appendClaimedItems(args, partitionKey, items, "FLOW.FAIL_MANY");
    return this.recordsOrResponse(await this.command(...args));
  }

  async cancel(id: string, options: CancelOptions): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = ["FLOW.CANCEL", id, "FENCING", options.fencingToken, "NOW", options.nowMs ?? nowMs()];
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "PARTITION", options.partitionKey);
    appendEncoded(args, "REASON", this.codec, options.reason);
    append(args, "TTL", options.ttlMs);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async cancelMany(partitionKey: string | undefined, items: FencedItem[], options: {
    reason?: unknown;
    ttlMs?: number;
    nowMs?: number;
    independent?: boolean;
  } = {}): Promise<unknown[] | unknown> {
    if (items.length === 0) {
      return [];
    }
    const args: CommandArgument[] = ["FLOW.CANCEL_MANY", partitionKey ?? "MIXED"];
    appendEncoded(args, "REASON", this.codec, options.reason);
    append(args, "TTL", options.ttlMs);
    append(args, "NOW", options.nowMs ?? nowMs());
    appendBool(args, "INDEPENDENT", options.independent);
    appendFencedItems(args, partitionKey, items, "FLOW.CANCEL_MANY", true);
    return this.recordsOrResponse(await this.command(...args));
  }

  async rewind(id: string, options: {
    partitionKey?: string;
    toEvent?: string;
    expectState?: string;
    nowMs?: number;
    returnRecord?: boolean;
  } = {}): Promise<FlowRecord | Buffer | unknown> {
    const args: CommandArgument[] = ["FLOW.REWIND", id, "NOW", options.nowMs ?? nowMs()];
    append(args, "PARTITION", options.partitionKey);
    append(args, "TO_EVENT", options.toEvent);
    append(args, "EXPECT_STATE", options.expectState);
    const response = await this.command(...args);
    if (options.returnRecord === true) {
      return await this.recordOrGet(response, id, options.partitionKey);
    }
    return response;
  }

  async get<TPayload = unknown>(id: string, options: { partitionKey?: string; full?: boolean } = {}): Promise<FlowRecord<TPayload> | undefined> {
    const args: CommandArgument[] = ["FLOW.GET", id];
    append(args, "PARTITION", options.partitionKey);
    appendBool(args, "FULL", options.full);
    const response = await this.command(...args);
    if (response == null) {
      return undefined;
    }
    return flowRecordFromResp<TPayload>(response, this.codec);
  }

  async list(type: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.LIST", type, options);
  }

  async terminals(type: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.TERMINALS", type, options);
  }

  async failures(type: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.FAILURES", type, options);
  }

  async byParent(parentFlowId: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.BY_PARENT", parentFlowId, options);
  }

  async byRoot(rootFlowId: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.BY_ROOT", rootFlowId, options);
  }

  async byCorrelation(correlationId: string, options: ReadOptions = {}): Promise<FlowRecord[]> {
    return await this.indexQuery("FLOW.BY_CORRELATION", correlationId, options);
  }

  async info(type: string): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FLOW.INFO", type));
  }

  async stuck(type: string, options: {
    partitionKey?: string;
    count?: number;
    olderThanMs?: number;
    nowMs?: number;
  } = {}): Promise<FlowRecord[]> {
    const args: CommandArgument[] = ["FLOW.STUCK", type];
    append(args, "PARTITION", options.partitionKey);
    append(args, "COUNT", options.count);
    append(args, "OLDER_THAN", options.olderThanMs);
    append(args, "NOW", options.nowMs);
    return this.records(arrayResponse(await this.command(...args)));
  }

  async history(id: string, options: { partitionKey?: string; count?: number; fromEvent?: string; rev?: boolean } = {}): Promise<unknown[]> {
    const args: CommandArgument[] = ["FLOW.HISTORY", id];
    append(args, "PARTITION", options.partitionKey);
    append(args, "COUNT", options.count);
    append(args, "FROM_EVENT", options.fromEvent);
    appendBool(args, "REV", options.rev);
    return arrayResponse(await this.command(...args));
  }

  async spawnChildren(parentId: string, children: ChildSpec[], options: {
    groupId?: string;
    partitionKey?: string;
    leaseToken?: Buffer;
    fencingToken?: number;
    wait?: string;
    waitState?: string;
    success?: string;
    failure?: string;
    fromState?: string;
    nowMs?: number;
    onChildFailed?: string;
    onParentClosed?: string;
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
  } = {}): Promise<unknown> {
    const args: CommandArgument[] = [
      "FLOW.SPAWN_CHILDREN",
      parentId,
      "GROUP",
      options.groupId ?? "default",
      "WAIT",
      options.wait ?? "all",
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "FENCING", options.fencingToken);
    append(args, "WAIT_STATE", options.waitState);
    append(args, "SUCCESS", options.success);
    append(args, "FAILURE", options.failure);
    append(args, "FROM_STATE", options.fromState);
    append(args, "ON_CHILD_FAILED", options.onChildFailed);
    append(args, "ON_PARENT_CLOSED", options.onParentClosed);
    appendNamedValues(args, this.codec, options);
    args.push("ITEMS");
    const mixed = children.some((child) => child.partitionKey != null);
    if (mixed) {
      args.push("MIXED");
    }
    for (const child of children) {
      if (mixed) {
        if (child.partitionKey == null) {
          throw new Error("mixed spawnChildren children require partitionKey");
        }
        args.push(child.id, child.partitionKey, child.type, this.codec.encode(child.payload));
      } else {
        args.push(child.id, child.type, this.codec.encode(child.payload));
      }
    }
    return await this.command(...args);
  }

  async installPolicy(type: string, options: { state?: string; retry?: RetryPolicy; retentionTtlMs?: number } = {}): Promise<unknown> {
    const args: CommandArgument[] = ["FLOW.POLICY.SET", type];
    append(args, "STATE", options.state);
    if (options.retry != null) {
      appendRetryPolicy(args, options.retry);
    }
    append(args, "RETENTION_TTL_MS", options.retentionTtlMs);
    return await this.command(...args);
  }

  async policyGet(type: string, options: { state?: string } = {}): Promise<Record<string, unknown>> {
    const args: CommandArgument[] = ["FLOW.POLICY.GET", type];
    append(args, "STATE", options.state);
    return parseKvResponse(await this.command(...args));
  }

  async retentionCleanup(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("FLOW.RETENTION_CLEANUP"));
  }

  private async executeProducerWrite(args: CommandArgument[]): Promise<unknown> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.command(...args);
      } catch (error) {
        if (!(error instanceof OverloadedError) || attempt >= this.backpressure.maxRetries) {
          throw error;
        }
        const retryAfterMs = error.retryAfterMs;
        const exponential = Math.min(
          this.backpressure.maxDelayMs,
          this.backpressure.baseDelayMs * 2 ** attempt
        );
        const base = retryAfterMs ?? exponential;
        const jitter = base * (this.backpressure.jitterPct / 100) * Math.random();
        await sleep(base + jitter);
      }
    }
  }

  private async recordOrGet(response: unknown, id: string, partitionKey: string | undefined): Promise<FlowRecord> {
    if (isRecordLike(response)) {
      return flowRecordFromResp(response, this.codec);
    }
    const record = await this.get(id, { partitionKey });
    if (record == null) {
      throw new Error(`Flow ${id} was not returned and could not be loaded`);
    }
    return record;
  }

  private records(values: unknown[]): FlowRecord[] {
    return values.map((value) => flowRecordFromResp(value, this.codec));
  }

  private recordsOrResponse(value: unknown): unknown[] | unknown {
    if (Array.isArray(value) && value.every(isRecordLike)) {
      return this.records(value);
    }
    return value;
  }

  private async indexQuery(command: string, key: string, options: ReadOptions): Promise<FlowRecord[]> {
    const args: CommandArgument[] = [command, key];
    appendReadOptions(args, options);
    const response = await this.command(...args);
    return Array.isArray(response) ? this.records(response) : [];
  }

  private appendPartitionOptions(
    args: CommandArgument[],
    options: { partitionKey?: string; partitionKeys?: string[] }
  ): void {
    if (options.partitionKey != null && options.partitionKeys != null) {
      throw new Error("partitionKey and partitionKeys are mutually exclusive");
    }
    append(args, "PARTITION", options.partitionKey);
    if (options.partitionKeys != null) {
      if (options.partitionKeys.length === 0) {
        throw new Error("partitionKeys must be non-empty");
      }
      args.push("PARTITIONS", options.partitionKeys.length, ...options.partitionKeys);
    }
  }
}

class ErrorMappingExecutor implements RedisCommandExecutor {
  constructor(private readonly executor: RedisCommandExecutor) {}

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    try {
      return await this.executor.executeCommand(...args);
    } catch (error) {
      throwMapped(error);
    }
  }

  async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
    try {
      if (this.executor.executePipeline != null) {
        return await this.executor.executePipeline(commands);
      }
      return await Promise.all(commands.map((command) => this.executor.executeCommand(...command)));
    } catch (error) {
      throwMapped(error);
    }
  }

  async close(): Promise<void> {
    await this.executor.close?.();
  }
}

function appendReadOptions(args: CommandArgument[], options: ReadOptions): void {
  append(args, "COUNT", options.count);
  append(args, "PARTITION", options.partitionKey);
  append(args, "FROM_MS", options.fromMs);
  append(args, "TO_MS", options.toMs);
  appendBool(args, "REV", options.rev);
  append(args, "STATE", options.state);
  appendBool(args, "TERMINAL_ONLY", options.terminalOnly);
  appendBool(args, "INCLUDE_COLD", options.includeCold);
  appendBool(args, "CONSISTENT_PROJECTION", options.consistentProjection);
}

function appendNamedCounts(
  args: CommandArgument[],
  codec: Codec,
  values: Record<string, unknown>,
  valueRefs: Record<string, string>
): void {
  args.push(Object.keys(values).length);
  for (const [name, value] of Object.entries(values)) {
    args.push(name, codec.encode(value));
  }
  args.push(Object.keys(valueRefs).length);
  for (const [name, ref] of Object.entries(valueRefs)) {
    args.push(name, ref);
  }
}

function appendClaimedItems(
  args: CommandArgument[],
  partitionKey: string | undefined,
  items: ClaimedItem[],
  command: string
): void {
  if (partitionKey != null) {
    for (const item of items) {
      if (item.partitionKey != null && item.partitionKey !== partitionKey) {
        throw new Error(`${command} item partitionKey does not match batch partitionKey`);
      }
    }
  }
  args.push("ITEMS");
  for (const item of items) {
    if (partitionKey == null) {
      args.push(item.id, item.partitionKey ?? "-", item.leaseToken, item.fencingToken);
    } else {
      args.push(item.id, item.leaseToken, item.fencingToken);
    }
  }
}

function appendFencedItems(
  args: CommandArgument[],
  partitionKey: string | undefined,
  items: FencedItem[],
  command: string,
  includeLease: boolean
): void {
  if (partitionKey != null) {
    for (const item of items) {
      if (item.partitionKey != null && item.partitionKey !== partitionKey) {
        throw new Error(`${command} item partitionKey does not match batch partitionKey`);
      }
    }
  }
  args.push("ITEMS");
  for (const item of items) {
    if (partitionKey == null) {
      args.push(item.id, item.partitionKey ?? "-", item.fencingToken);
    } else {
      args.push(item.id, item.fencingToken);
    }
    if (includeLease) {
      args.push(item.leaseToken ?? Buffer.alloc(0));
    }
  }
}

function appendRetryPolicy(args: CommandArgument[], policy: RetryPolicy): void {
  append(args, "MAX_RETRIES", policy.maxRetries);
  append(args, "BACKOFF", policy.backoff);
  append(args, "BASE_MS", policy.baseMs);
  append(args, "MAX_MS", policy.maxMs);
  append(args, "JITTER_PCT", policy.jitterPct);
  append(args, "EXHAUSTED_TO", policy.exhaustedTo);
}

function appendPayloadRead(args: CommandArgument[], payload: boolean | undefined, maxBytes: number | undefined): void {
  if (payload === false) {
    args.push("NOPAYLOAD");
    return;
  }
  if (payload === true || maxBytes != null) {
    args.push("PAYLOAD");
  }
  if (maxBytes != null) {
    args.push("MAXBYTES", maxBytes);
  }
}

function isRecordLike(value: unknown): boolean {
  return typeof value === "object" && value != null && !Buffer.isBuffer(value);
}

function throwMapped(error: unknown): never {
  const mapped = mapException(error);
  throw mapped instanceof Error ? mapped : error;
}
