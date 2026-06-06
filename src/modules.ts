import { okResponse, type CommandArgument } from "./internal.js";
import type { StoreCommandClient } from "./store.js";

export interface TopKReserveOptions {
  width?: number;
  depth?: number;
  decay?: number;
}

export interface CountMinMergeOptions {
  weights?: number[];
}

export interface TDigestCreateOptions {
  compression?: number;
}

export interface TDigestMergeOptions extends TDigestCreateOptions {
  override?: boolean;
}

export interface JsonSetOptions {
  nx?: boolean;
  xx?: boolean;
}

export class BloomFilterStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, errorRate: number, capacity: number): Promise<boolean> {
    return okResponse(await this.client.command("BF.RESERVE", key, errorRate, capacity));
  }

  async add(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("BF.ADD", key, encode(this.client, element))) === 1;
  }

  async madd(key: string, ...elements: unknown[]): Promise<number[]> {
    return array(await this.client.command("BF.MADD", key, ...elements.map((item) => encode(this.client, item)))).map(number);
  }

  async exists(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("BF.EXISTS", key, encode(this.client, element))) === 1;
  }

  async mexists(key: string, ...elements: unknown[]): Promise<number[]> {
    return array(await this.client.command("BF.MEXISTS", key, ...elements.map((item) => encode(this.client, item)))).map(number);
  }

  async card(key: string): Promise<number> {
    return number(await this.client.command("BF.CARD", key));
  }

  async info(key: string): Promise<unknown[]> {
    return array(await this.client.command("BF.INFO", key));
  }
}

export class CuckooFilterStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, capacity: number): Promise<boolean> {
    return okResponse(await this.client.command("CF.RESERVE", key, capacity));
  }

  async add(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("CF.ADD", key, encode(this.client, element))) === 1;
  }

  async addnx(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("CF.ADDNX", key, encode(this.client, element))) === 1;
  }

  async del(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("CF.DEL", key, encode(this.client, element))) === 1;
  }

  async exists(key: string, element: unknown): Promise<boolean> {
    return number(await this.client.command("CF.EXISTS", key, encode(this.client, element))) === 1;
  }

  async mexists(key: string, ...elements: unknown[]): Promise<number[]> {
    return array(await this.client.command("CF.MEXISTS", key, ...elements.map((item) => encode(this.client, item)))).map(number);
  }

  async count(key: string, element: unknown): Promise<number> {
    return number(await this.client.command("CF.COUNT", key, encode(this.client, element)));
  }

  async info(key: string): Promise<unknown[]> {
    return array(await this.client.command("CF.INFO", key));
  }
}

export class CountMinSketchStore {
  constructor(private readonly client: StoreCommandClient) {}

  async initByDim(key: string, width: number, depth: number): Promise<boolean> {
    return okResponse(await this.client.command("CMS.INITBYDIM", key, width, depth));
  }

  async initByProb(key: string, error: number, probability: number): Promise<boolean> {
    return okResponse(await this.client.command("CMS.INITBYPROB", key, error, probability));
  }

  async incrBy(key: string, entries: [unknown, number][]): Promise<number[]> {
    return array(await this.client.command("CMS.INCRBY", key, ...flattenEntries(this.client, entries))).map(number);
  }

  async query(key: string, ...items: unknown[]): Promise<number[]> {
    return array(await this.client.command("CMS.QUERY", key, ...items.map((item) => encode(this.client, item)))).map(number);
  }

  async merge(destination: string, sources: string[], options: CountMinMergeOptions = {}): Promise<boolean> {
    const args: CommandArgument[] = ["CMS.MERGE", destination, sources.length, ...sources];
    if (options.weights != null) {
      args.push("WEIGHTS", ...options.weights);
    }
    return okResponse(await this.client.command(...args));
  }

  async info(key: string): Promise<unknown[]> {
    return array(await this.client.command("CMS.INFO", key));
  }
}

export class TopKStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, k: number, options: TopKReserveOptions = {}): Promise<boolean> {
    const args: CommandArgument[] = ["TOPK.RESERVE", key, k];
    if (options.width != null) {
      args.push(options.width);
      if (options.depth != null) {
        args.push(options.depth);
        if (options.decay != null) {
          args.push(options.decay);
        }
      }
    }
    return okResponse(await this.client.command(...args));
  }

  async add<T = unknown>(key: string, ...elements: unknown[]): Promise<(T | null)[]> {
    return array(await this.client.command("TOPK.ADD", key, ...elements.map((item) => encode(this.client, item)))).map((item) =>
      decode<T>(this.client, item)
    );
  }

  async incrBy<T = unknown>(key: string, entries: [unknown, number][]): Promise<(T | null)[]> {
    return array(await this.client.command("TOPK.INCRBY", key, ...flattenEntries(this.client, entries))).map((item) =>
      decode<T>(this.client, item)
    );
  }

  async query(key: string, ...elements: unknown[]): Promise<number[]> {
    return array(await this.client.command("TOPK.QUERY", key, ...elements.map((item) => encode(this.client, item)))).map(number);
  }

  async list<T = unknown>(key: string, options: { withCount?: boolean } = {}): Promise<(T | number | null)[]> {
    return array(await this.client.command("TOPK.LIST", key, ...(options.withCount === true ? ["WITHCOUNT"] : []))).map((item, index) =>
      options.withCount === true && index % 2 === 1 ? number(item) : decode<T>(this.client, item)
    );
  }

  async info(key: string): Promise<unknown[]> {
    return array(await this.client.command("TOPK.INFO", key));
  }
}

export class TDigestStore {
  constructor(private readonly client: StoreCommandClient) {}

  async create(key: string, options: TDigestCreateOptions = {}): Promise<boolean> {
    return okResponse(await this.client.command("TDIGEST.CREATE", key, ...compressionOptions(options)));
  }

  async add(key: string, ...values: number[]): Promise<boolean> {
    return okResponse(await this.client.command("TDIGEST.ADD", key, ...values));
  }

  async reset(key: string): Promise<boolean> {
    return okResponse(await this.client.command("TDIGEST.RESET", key));
  }

  async quantile(key: string, ...quantiles: number[]): Promise<string[]> {
    return array(await this.client.command("TDIGEST.QUANTILE", key, ...quantiles)).map(string);
  }

  async cdf(key: string, ...values: number[]): Promise<string[]> {
    return array(await this.client.command("TDIGEST.CDF", key, ...values)).map(string);
  }

  async rank(key: string, ...values: number[]): Promise<number[]> {
    return array(await this.client.command("TDIGEST.RANK", key, ...values)).map(number);
  }

  async revrank(key: string, ...values: number[]): Promise<number[]> {
    return array(await this.client.command("TDIGEST.REVRANK", key, ...values)).map(number);
  }

  async byrank(key: string, ...ranks: number[]): Promise<string[]> {
    return array(await this.client.command("TDIGEST.BYRANK", key, ...ranks)).map(string);
  }

  async byrevrank(key: string, ...ranks: number[]): Promise<string[]> {
    return array(await this.client.command("TDIGEST.BYREVRANK", key, ...ranks)).map(string);
  }

  async trimmedMean(key: string, low: number, high: number): Promise<string> {
    return string(await this.client.command("TDIGEST.TRIMMED_MEAN", key, low, high));
  }

  async min(key: string): Promise<string> {
    return string(await this.client.command("TDIGEST.MIN", key));
  }

  async max(key: string): Promise<string> {
    return string(await this.client.command("TDIGEST.MAX", key));
  }

  async info(key: string): Promise<unknown[]> {
    return array(await this.client.command("TDIGEST.INFO", key));
  }

  async merge(destination: string, sources: string[], options: TDigestMergeOptions = {}): Promise<boolean> {
    return okResponse(
      await this.client.command(
        "TDIGEST.MERGE",
        destination,
        sources.length,
        ...sources,
        ...compressionOptions(options),
        ...(options.override === true ? ["OVERRIDE"] : [])
      )
    );
  }
}

export class JsonStore {
  constructor(private readonly client: StoreCommandClient) {}

  async set(key: string, path: string, value: unknown, options: JsonSetOptions = {}): Promise<boolean> {
    const response = await this.client.command(
      "JSON.SET",
      key,
      path,
      JSON.stringify(value),
      ...(options.nx === true ? ["NX"] : options.xx === true ? ["XX"] : [])
    );
    return okResponse(response);
  }

  async get<T = unknown>(key: string, paths: string | string[] = "$"): Promise<T | null> {
    const response = await this.client.command("JSON.GET", key, ...toArray(paths));
    return parseJson<T>(response);
  }

  async del(key: string, path?: string): Promise<number> {
    return number(await this.client.command("JSON.DEL", key, ...(path == null ? [] : [path])));
  }

  async numIncrBy(key: string, path: string, value: number): Promise<string> {
    return string(await this.client.command("JSON.NUMINCRBY", key, path, value));
  }

  async type(key: string, path?: string): Promise<string | null> {
    const response = await this.client.command("JSON.TYPE", key, ...(path == null ? [] : [path]));
    return response == null ? null : string(response);
  }

  async strlen(key: string, path?: string): Promise<number | null> {
    const response = await this.client.command("JSON.STRLEN", key, ...(path == null ? [] : [path]));
    return response == null ? null : number(response);
  }

  async objKeys(key: string, path?: string): Promise<unknown[]> {
    return array(await this.client.command("JSON.OBJKEYS", key, ...(path == null ? [] : [path])));
  }

  async objLen(key: string, path?: string): Promise<number> {
    return number(await this.client.command("JSON.OBJLEN", key, ...(path == null ? [] : [path])));
  }

  async arrAppend(key: string, path: string, ...values: unknown[]): Promise<number> {
    return number(await this.client.command("JSON.ARRAPPEND", key, path, ...values.map((value) => JSON.stringify(value))));
  }

  async arrLen(key: string, path?: string): Promise<number> {
    return number(await this.client.command("JSON.ARRLEN", key, ...(path == null ? [] : [path])));
  }

  async toggle(key: string, path: string): Promise<number> {
    return number(await this.client.command("JSON.TOGGLE", key, path));
  }

  async clear(key: string, path?: string): Promise<number> {
    return number(await this.client.command("JSON.CLEAR", key, ...(path == null ? [] : [path])));
  }

  async mget<T = unknown>(keys: string[], path: string): Promise<(T | null)[]> {
    return array(await this.client.command("JSON.MGET", ...keys, path)).map((item) => parseJson<T>(item));
  }
}

function encode(client: StoreCommandClient, value: unknown): Buffer {
  return client.codec.encode(value);
}

function decode<T>(client: StoreCommandClient, value: unknown): T | null {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return client.codec.decode(value) as T | null;
  if (value instanceof Uint8Array) return client.codec.decode(Buffer.from(value)) as T | null;
  return value as T;
}

function number(value: unknown): number {
  return Number(value);
}

function string(value: unknown): string {
  return Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function flattenEntries(client: StoreCommandClient, entries: [unknown, number][]): CommandArgument[] {
  return entries.flatMap(([item, value]) => [encode(client, item), value]);
}

function compressionOptions(options: TDigestCreateOptions): CommandArgument[] {
  return options.compression == null ? [] : ["COMPRESSION", options.compression];
}

function toArray<T>(value: T | T[]): T[] {
  return Array.isArray(value) ? value : [value];
}

function parseJson<T>(value: unknown): T | null {
  if (value == null) return null;
  const text = string(value);
  return JSON.parse(text) as T;
}
