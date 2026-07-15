import { binaryBooleanResponse, integerReply, okResponse, type CommandArgument } from "./internal.js";
import {
  array,
  arrayOrRest,
  binaryInteger,
  commandArgs,
  concatArgs,
  decode,
  denseArray,
  encode,
  encodedArgs,
  encodedEntryArgs,
  mapArray,
  mapArrayResponse,
  ownOption,
  requireNonEmpty,
  string
} from "./store-utilities.js";
import type { IntegerReply, StoreCommandClient } from "./store.js";

export type TopKReserveOptions =
  | { width?: never; depth?: never; decay?: never }
  | { width: number; depth: number; decay: number };

export interface CountMinMergeOptions {
  weights?: readonly (number | bigint)[];
}

export interface TDigestCreateOptions {
  compression?: number;
}

export interface TDigestMergeOptions extends TDigestCreateOptions {
  override?: boolean;
}

export class BloomFilterStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, errorRate: number, capacity: number): Promise<boolean> {
    return okResponse(await this.client.command("BF.RESERVE", key, errorRate, capacity));
  }

  async add(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("BF.ADD", key, encode(this.client.codec, element)));
  }

  madd(key: string, element: unknown, ...elements: unknown[]): Promise<number[]>;
  madd(key: string, ...elements: unknown[]): Promise<number[]> {
    return this.maddMany(key, elements);
  }

  async maddMany(key: string, elements: readonly unknown[]): Promise<number[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["BF.MADD", key], this.client.codec, elements)),
      elementCount,
      "BF.MADD",
      binaryInteger
    );
  }

  async exists(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("BF.EXISTS", key, encode(this.client.codec, element)));
  }

  mexists(key: string, element: unknown, ...elements: unknown[]): Promise<number[]>;
  mexists(key: string, ...elements: unknown[]): Promise<number[]> {
    return this.mexistsMany(key, elements);
  }

  async mexistsMany(key: string, elements: readonly unknown[]): Promise<number[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["BF.MEXISTS", key], this.client.codec, elements)),
      elementCount,
      "BF.MEXISTS",
      binaryInteger
    );
  }

  async card(key: string): Promise<IntegerReply> {
    return integerReply(await this.client.command("BF.CARD", key));
  }

  async info(key: string): Promise<unknown[]> {
    return denseArray(await this.client.command("BF.INFO", key), "BF.INFO");
  }
}

export class CuckooFilterStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, capacity: number): Promise<boolean> {
    return okResponse(await this.client.command("CF.RESERVE", key, capacity));
  }

  async add(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("CF.ADD", key, encode(this.client.codec, element)));
  }

  async addnx(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("CF.ADDNX", key, encode(this.client.codec, element)));
  }

  async del(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("CF.DEL", key, encode(this.client.codec, element)));
  }

  async exists(key: string, element: unknown): Promise<boolean> {
    return binaryBooleanResponse(await this.client.command("CF.EXISTS", key, encode(this.client.codec, element)));
  }

  mexists(key: string, element: unknown, ...elements: unknown[]): Promise<number[]>;
  mexists(key: string, ...elements: unknown[]): Promise<number[]> {
    return this.mexistsMany(key, elements);
  }

  async mexistsMany(key: string, elements: readonly unknown[]): Promise<number[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["CF.MEXISTS", key], this.client.codec, elements)),
      elementCount,
      "CF.MEXISTS",
      binaryInteger
    );
  }

  async count(key: string, element: unknown): Promise<IntegerReply> {
    return integerReply(await this.client.command("CF.COUNT", key, encode(this.client.codec, element)));
  }

  async info(key: string): Promise<unknown[]> {
    return denseArray(await this.client.command("CF.INFO", key), "CF.INFO");
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

  async incrBy(key: string, entries: [unknown, number | bigint][]): Promise<IntegerReply[]> {
    const entryCount = entries.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedEntryArgs(["CMS.INCRBY", key], this.client.codec, entries)),
      entryCount,
      "CMS.INCRBY",
      integerReply
    );
  }

  query(key: string, item: unknown, ...items: unknown[]): Promise<IntegerReply[]>;
  query(key: string, ...items: unknown[]): Promise<IntegerReply[]> {
    return this.queryMany(key, items);
  }

  async queryMany(key: string, items: readonly unknown[]): Promise<IntegerReply[]> {
    const itemCount = items.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["CMS.QUERY", key], this.client.codec, items)),
      itemCount,
      "CMS.QUERY",
      integerReply
    );
  }

  async merge(destination: string, sources: readonly string[], options: CountMinMergeOptions = {}): Promise<boolean> {
    if (sources.length === 0) throw new TypeError("CMS.MERGE requires at least one source");
    const weights = ownOption(options, "weights");
    if (weights != null && weights.length !== sources.length) {
      throw new TypeError("CMS.MERGE weights must match the source count");
    }
    const args = weights == null
      ? concatArgs(["CMS.MERGE", destination, sources.length], sources)
      : concatArgs(["CMS.MERGE", destination, sources.length], sources, ["WEIGHTS"], weights);
    return okResponse(await commandArgs(this.client, args));
  }

  async info(key: string): Promise<unknown[]> {
    return denseArray(await this.client.command("CMS.INFO", key), "CMS.INFO");
  }
}

export class TopKStore {
  constructor(private readonly client: StoreCommandClient) {}

  async reserve(key: string, k: number, options: TopKReserveOptions = {}): Promise<boolean> {
    const args: CommandArgument[] = ["TOPK.RESERVE", key, k];
    const width = ownOption(options, "width");
    const depth = ownOption(options, "depth");
    const decay = ownOption(options, "decay");
    const tuningCount = [width, depth, decay]
      .filter((value) => value != null)
      .length;
    if (tuningCount !== 0 && tuningCount !== 3) {
      throw new TypeError("TOPK.RESERVE width, depth, and decay must be provided together");
    }
    if (tuningCount === 3) {
      args.push(width, depth, decay);
    }
    return okResponse(await commandArgs(this.client, args));
  }

  add<T = unknown>(key: string, element: unknown, ...elements: unknown[]): Promise<(T | null)[]>;
  add<T = unknown>(key: string, ...elements: unknown[]): Promise<(T | null)[]> {
    return this.addMany<T>(key, elements);
  }

  async addMany<T = unknown>(key: string, elements: readonly unknown[]): Promise<(T | null)[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["TOPK.ADD", key], this.client.codec, elements)),
      elementCount,
      "TOPK.ADD",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  async incrBy<T = unknown>(key: string, entries: [unknown, number | bigint][]): Promise<(T | null)[]> {
    const entryCount = entries.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedEntryArgs(["TOPK.INCRBY", key], this.client.codec, entries)),
      entryCount,
      "TOPK.INCRBY",
      (item) => decode<T>(this.client.codec, item)
    );
  }

  query(key: string, element: unknown, ...elements: unknown[]): Promise<number[]>;
  query(key: string, ...elements: unknown[]): Promise<number[]> {
    return this.queryMany(key, elements);
  }

  async queryMany(key: string, elements: readonly unknown[]): Promise<number[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["TOPK.QUERY", key], this.client.codec, elements)),
      elementCount,
      "TOPK.QUERY",
      binaryInteger
    );
  }

  count(key: string, element: unknown, ...elements: unknown[]): Promise<IntegerReply[]>;
  count(key: string, ...elements: unknown[]): Promise<IntegerReply[]> {
    return this.countMany(key, elements);
  }

  async countMany(key: string, elements: readonly unknown[]): Promise<IntegerReply[]> {
    const elementCount = elements.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["TOPK.COUNT", key], this.client.codec, elements)),
      elementCount,
      "TOPK.COUNT",
      integerReply
    );
  }

  async list<T = unknown>(key: string, options: { withCount?: boolean } = {}): Promise<(T | IntegerReply | null)[]> {
    const withCount = ownOption(options, "withCount") === true;
    const items = array(await this.client.command("TOPK.LIST", key, ...(withCount ? ["WITHCOUNT"] : [])));
    if (withCount && items.length % 2 !== 0) {
      throw new TypeError("TOPK.LIST WITHCOUNT must return item/count pairs");
    }
    return mapArray(items, "TOPK.LIST", (item, index) =>
      withCount && index % 2 === 1 ? integerReply(item) : decode<T>(this.client.codec, item)
    );
  }

  async info(key: string): Promise<unknown[]> {
    return denseArray(await this.client.command("TOPK.INFO", key), "TOPK.INFO");
  }
}

export class TDigestStore {
  constructor(private readonly client: StoreCommandClient) {}

  async create(key: string, options: TDigestCreateOptions = {}): Promise<boolean> {
    return okResponse(await this.client.command("TDIGEST.CREATE", key, ...compressionOptions(options)));
  }

  add(key: string, values: readonly number[]): Promise<boolean>;
  add(key: string, value: number, ...values: number[]): Promise<boolean>;
  async add(key: string, ...valuesOrArray: (number | readonly number[])[]): Promise<boolean> {
    const values = requireNonEmpty(arrayOrRest<number>(valuesOrArray), "TDIGEST.ADD", "value");
    return okResponse(await commandArgs(this.client, concatArgs(
      ["TDIGEST.ADD", key],
      values
    )));
  }

  async reset(key: string): Promise<boolean> {
    return okResponse(await this.client.command("TDIGEST.RESET", key));
  }

  quantile(key: string, quantiles: readonly number[]): Promise<string[]>;
  quantile(key: string, quantile: number, ...quantiles: number[]): Promise<string[]>;
  async quantile(key: string, ...quantilesOrArray: (number | readonly number[])[]): Promise<string[]> {
    const quantiles = requireNonEmpty(arrayOrRest<number>(quantilesOrArray), "TDIGEST.QUANTILE", "quantile");
    const quantileCount = quantiles.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.QUANTILE", key], quantiles)),
      quantileCount,
      "TDIGEST.QUANTILE",
      string
    );
  }

  cdf(key: string, values: readonly number[]): Promise<string[]>;
  cdf(key: string, value: number, ...values: number[]): Promise<string[]>;
  async cdf(key: string, ...valuesOrArray: (number | readonly number[])[]): Promise<string[]> {
    const values = requireNonEmpty(arrayOrRest<number>(valuesOrArray), "TDIGEST.CDF", "value");
    const valueCount = values.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.CDF", key], values)),
      valueCount,
      "TDIGEST.CDF",
      string
    );
  }

  rank(key: string, values: readonly number[]): Promise<IntegerReply[]>;
  rank(key: string, value: number, ...values: number[]): Promise<IntegerReply[]>;
  async rank(key: string, ...valuesOrArray: (number | readonly number[])[]): Promise<IntegerReply[]> {
    const values = requireNonEmpty(arrayOrRest<number>(valuesOrArray), "TDIGEST.RANK", "value");
    const valueCount = values.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.RANK", key], values)),
      valueCount,
      "TDIGEST.RANK",
      integerReply
    );
  }

  revrank(key: string, values: readonly number[]): Promise<IntegerReply[]>;
  revrank(key: string, value: number, ...values: number[]): Promise<IntegerReply[]>;
  async revrank(key: string, ...valuesOrArray: (number | readonly number[])[]): Promise<IntegerReply[]> {
    const values = requireNonEmpty(arrayOrRest<number>(valuesOrArray), "TDIGEST.REVRANK", "value");
    const valueCount = values.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.REVRANK", key], values)),
      valueCount,
      "TDIGEST.REVRANK",
      integerReply
    );
  }

  byrank(key: string, ranks: readonly number[]): Promise<string[]>;
  byrank(key: string, rank: number, ...ranks: number[]): Promise<string[]>;
  async byrank(key: string, ...ranksOrArray: (number | readonly number[])[]): Promise<string[]> {
    const ranks = requireNonEmpty(arrayOrRest<number>(ranksOrArray), "TDIGEST.BYRANK", "rank");
    const rankCount = ranks.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.BYRANK", key], ranks)),
      rankCount,
      "TDIGEST.BYRANK",
      string
    );
  }

  byrevrank(key: string, ranks: readonly number[]): Promise<string[]>;
  byrevrank(key: string, rank: number, ...ranks: number[]): Promise<string[]>;
  async byrevrank(key: string, ...ranksOrArray: (number | readonly number[])[]): Promise<string[]> {
    const ranks = requireNonEmpty(arrayOrRest<number>(ranksOrArray), "TDIGEST.BYREVRANK", "rank");
    const rankCount = ranks.length;
    return mapArrayResponse(
      await commandArgs(this.client, concatArgs(["TDIGEST.BYREVRANK", key], ranks)),
      rankCount,
      "TDIGEST.BYREVRANK",
      string
    );
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
    return denseArray(await this.client.command("TDIGEST.INFO", key), "TDIGEST.INFO");
  }

  async merge(destination: string, sources: string[], options: TDigestMergeOptions = {}): Promise<boolean> {
    requireNonEmpty(sources, "TDIGEST.MERGE", "source");
    const override = ownOption(options, "override") === true;
    return okResponse(
      await commandArgs(this.client, concatArgs(
        ["TDIGEST.MERGE", destination, sources.length],
        sources,
        compressionOptions(options),
        override ? ["OVERRIDE"] : []
      ))
    );
  }
}

function compressionOptions(options: TDigestCreateOptions): CommandArgument[] {
  const compression = ownOption(options, "compression");
  return compression == null ? [] : ["COMPRESSION", compression];
}
