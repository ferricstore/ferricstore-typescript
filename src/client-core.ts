import { Buffer } from "node:buffer";
import { infoText, ruleArgs } from "./client-helpers.js";
import type { FerricStoreClientOptions } from "./client-options.js";
import {
  bgsaveResponse,
  clientNameResponse,
  concatCommandArgs,
  fetchOrComputeCompletionToken,
  unsupportedClientCaching,
  unsupportedClientTracking
} from "./client-core-helpers.js";
import {
  append,
  arrayResponse,
  booleanResponse,
  integer,
  okResponse,
  textResponse,
  type Command,
  type CommandArgument
} from "./internal.js";
import type { CommandExecutor } from "./adapters.js";
import { FerricStoreAdministrationClient } from "./client-administration.js";
import {
  BloomFilterStore,
  CountMinSketchStore,
  CuckooFilterStore,
  TDigestStore,
  TopKStore
} from "./modules.js";
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
import { fetchOrComputeResultFromResp, keyInfoFromResp, rateLimitResultFromResp, type FetchOrComputeResult, type KeyInfo, type RateLimitResult } from "./types.js";

export class FerricStoreClientCore extends FerricStoreAdministrationClient {
  protected extendLeaseOkResponseSupport?: boolean | "probing";
  readonly bitmap: BitmapStore;
  readonly bloom: BloomFilterStore;
  readonly cms: CountMinSketchStore;
  readonly cuckoo: CuckooFilterStore;
  readonly geo: GeoStore;
  readonly hash: HashStore;
  readonly hyperloglog: HyperLogLogStore;
  readonly kv: KeyValueStore;
  readonly lists: ListStore;
  readonly sets: SetStore;
  readonly stream: StreamStore;
  readonly tdigest: TDigestStore;
  readonly topk: TopKStore;
  readonly zset: SortedSetStore;

  constructor(executor: CommandExecutor, options: FerricStoreClientOptions = {}) {
    super(executor, options);
    this.bitmap = new BitmapStore(this);
    this.bloom = new BloomFilterStore(this);
    this.cms = new CountMinSketchStore(this);
    this.cuckoo = new CuckooFilterStore(this);
    this.geo = new GeoStore(this);
    this.hash = new HashStore(this);
    this.hyperloglog = new HyperLogLogStore(this);
    this.kv = new KeyValueStore(this);
    this.lists = new ListStore(this);
    this.sets = new SetStore(this);
    this.stream = new StreamStore(this);
    this.tdigest = new TDigestStore(this);
    this.topk = new TopKStore(this);
    this.zset = new SortedSetStore(this);
  }
  async ping(message?: CommandArgument): Promise<unknown> {
    return await this.command("PING", ...(message == null ? [] : [message]));
  }

  async echo(message: CommandArgument): Promise<unknown> {
    return await this.command("ECHO", message);
  }

  async serverInfo(section?: string): Promise<string> {
    return textResponse(await this.command("INFO", ...(section == null ? [] : [section])), "INFO");
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
    return integer(await this.command("SLOWLOG", "LEN"));
  }

  async slowlogReset(): Promise<boolean> {
    return okResponse(await this.command("SLOWLOG", "RESET"));
  }

  async commandMetadata(): Promise<unknown> {
    return await this.command("COMMAND");
  }

  async commandCount(): Promise<number> {
    return integer(await this.command("COMMAND", "COUNT"));
  }

  async commandList(): Promise<unknown[]> {
    return arrayResponse(await this.command("COMMAND", "LIST"));
  }

  async commandInfo(...names: string[]): Promise<unknown[]> {
    return arrayResponse(await this.commandArgs(concatCommandArgs(["COMMAND", "INFO"], names)));
  }

  async commandDocs(...names: string[]): Promise<unknown> {
    return await this.commandArgs(concatCommandArgs(["COMMAND", "DOCS"], names));
  }

  async commandGetKeys(command: Command): Promise<unknown[]> {
    return arrayResponse(await this.commandArgs(concatCommandArgs(["COMMAND", "GETKEYS"], command)));
  }

  async clientId(): Promise<number> {
    return integer(await this.command("CLIENT", "ID"));
  }

  async clientSetName(name: string): Promise<boolean> {
    return okResponse(await this.command("CLIENT", "SETNAME", name));
  }

  async clientGetName(): Promise<string | null> {
    return clientNameResponse(await this.command("CLIENT", "GETNAME"));
  }

  async clientInfo(): Promise<string> {
    return infoText(await this.command("CLIENT", "INFO"));
  }

  async clientList(options: { type?: string } = {}): Promise<string> {
    return textResponse(
      await this.command("CLIENT", "LIST", ...(options.type == null ? [] : ["TYPE", options.type])),
      "CLIENT LIST"
    );
  }

  async clientTracking(_mode: "ON" | "OFF", _options: {
    redirect?: number;
    prefixes?: string[];
    bcast?: boolean;
    optin?: boolean;
    optout?: boolean;
    noloop?: boolean;
  } = {}): Promise<boolean> {
    void _mode;
    void _options;
    return unsupportedClientTracking();
  }

  async clientCaching(_mode: "YES" | "NO"): Promise<boolean> {
    void _mode;
    return unsupportedClientCaching();
  }

  async clientTrackingInfo(): Promise<unknown> {
    return await this.command("CLIENT", "TRACKINGINFO");
  }

  async clientGetRedir(): Promise<number> {
    return integer(await this.command("CLIENT", "GETREDIR"));
  }

  async save(): Promise<boolean> {
    return okResponse(await this.command("SAVE"));
  }

  async bgsave(): Promise<boolean> {
    return bgsaveResponse(await this.command("BGSAVE"));
  }

  async lastsave(): Promise<number> {
    return integer(await this.command("LASTSAVE"));
  }

  async lolwut(version?: number): Promise<string> {
    return textResponse(await this.command("LOLWUT", ...(version == null ? [] : ["VERSION", version])), "LOLWUT");
  }

  async moduleList(): Promise<unknown[]> {
    return arrayResponse(await this.command("MODULE", "LIST"));
  }

  async publish(channel: string, message: CommandArgument): Promise<number> {
    return integer(await this.command("PUBLISH", channel, message));
  }

  async pubsubChannels(pattern?: string): Promise<unknown[]> {
    return arrayResponse(await this.command("PUBSUB", "CHANNELS", ...(pattern == null ? [] : [pattern])));
  }

  async pubsubNumSub(...channels: string[]): Promise<unknown[]> {
    return arrayResponse(await this.commandArgs(concatCommandArgs(["PUBSUB", "NUMSUB"], channels)));
  }

  async pubsubNumPat(): Promise<number> {
    return integer(await this.command("PUBSUB", "NUMPAT"));
  }

  async aclSetUser(username: string, rules: string | readonly string[]): Promise<boolean> {
    return okResponse(
      await this.commandArgs(concatCommandArgs(["ACL", "SETUSER", username], ruleArgs(rules)))
    );
  }

  async aclDelUser(...usernames: string[]): Promise<number> {
    return integer(await this.commandArgs(concatCommandArgs(["ACL", "DELUSER"], usernames)));
  }

  async aclGetUser(username: string): Promise<unknown> {
    return await this.command("ACL", "GETUSER", username);
  }

  async aclList(): Promise<unknown[]> {
    return arrayResponse(await this.command("ACL", "LIST"));
  }

  async aclListUsers(): Promise<unknown[]> {
    return await this.aclList();
  }

  async aclWhoami(): Promise<string> {
    return textResponse(await this.command("ACL", "WHOAMI"), "ACL WHOAMI");
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
    return booleanResponse(await this.commandArgs(args));
  }

  async lock(key: string, owner: string, ttlMs: number): Promise<boolean> {
    return okResponse(await this.command("LOCK", key, owner, ttlMs));
  }

  async unlock(key: string, owner: string): Promise<number> {
    return integer(await this.command("UNLOCK", key, owner));
  }

  async extendLock(key: string, owner: string, ttlMs: number): Promise<number> {
    return integer(await this.command("EXTEND", key, owner, ttlMs));
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
    return fetchOrComputeResultFromResp<T>(await this.commandArgs(args), this.codec);
  }

  async fetchOrComputeResult(
    key: string,
    value: unknown,
    options: { ttlMs: number; computeToken: Buffer | null }
  ): Promise<boolean> {
    const computeToken = fetchOrComputeCompletionToken(options);
    const encoded = this.codec.encode(value);
    return okResponse(await this.command(
      "FETCH_OR_COMPUTE_RESULT",
      key,
      ...(computeToken === null ? [encoded] : [computeToken, encoded]),
      options.ttlMs
    ));
  }

  async fetchOrComputeError(
    key: string,
    message: string,
    options: { computeToken: Buffer | null }
  ): Promise<boolean> {
    const computeToken = fetchOrComputeCompletionToken(options);
    return okResponse(await this.command(
      "FETCH_OR_COMPUTE_ERROR",
      key,
      ...(computeToken === null ? [] : [computeToken]),
      message
    ));
  }

}
