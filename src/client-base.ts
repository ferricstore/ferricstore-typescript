import { Buffer } from "node:buffer";
import type { Codec } from "./codecs.js";
import { RawCodec } from "./codecs.js";
import { maybeAutoBatchExecutor } from "./auto-batch.js";
import { ErrorMappingExecutor } from "./client-executor.js";
import { finiteNonNegativeInteger, finiteNonNegativeNumber } from "./client-helpers.js";
import type { FerricStoreClientOptions } from "./client-options.js";
import { snapshotClientOptions } from "./client-config.js";
import {
  executeCommandArraysIndividually,
  executeCommandArgs,
  pipelineFallbackOptions,
  snapshotPipelineCommands,
  snapshotPipelineOptions,
  validatePipelineResponse,
  type CommandExecutor,
  type ExecutePipelineOptions
} from "./adapters.js";
import type { Command, CommandArgument } from "./internal.js";
import type { RoutingRoute, RoutingTopology } from "./topology.js";
import type { BackpressurePolicy } from "./types.js";
import { RequestNotSentError } from "./errors.js";
import { assertAtomicKeyValueCommandSharesSlot } from "./key-slot-validation.js";

const DEFAULT_FLOW_MANY_BATCH_LIMIT = 1_000;
const DEFAULT_LEGACY_CLAIM_HYDRATION_CONCURRENCY = 16;

export class FerricStoreClientBase {
  readonly executor: CommandExecutor;
  readonly codec: Codec;
  readonly backpressure: Required<BackpressurePolicy>;
  readonly flowManyBatchLimit: number;
  readonly legacyClaimHydrationConcurrency: number;
  private readonly closeAbortController = new AbortController();
  private closePromise?: Promise<void>;

  constructor(executor: CommandExecutor, options: FerricStoreClientOptions = {}) {
    const captured = snapshotClientOptions(options);
    this.executor = maybeAutoBatchExecutor(new ErrorMappingExecutor(executor), captured.autoBatch);
    this.codec = captured.codec ?? new RawCodec();
    this.flowManyBatchLimit = normalizeFlowManyBatchLimit(captured.flowManyBatchLimit);
    this.legacyClaimHydrationConcurrency = normalizeLegacyClaimHydrationConcurrency(
      captured.legacyClaimHydrationConcurrency
    );
    this.backpressure = {
      baseDelayMs: finiteNonNegativeInteger(captured.backpressure?.baseDelayMs, 25),
      jitterPct: finiteNonNegativeNumber(captured.backpressure?.jitterPct, 20),
      maxDelayMs: finiteNonNegativeInteger(captured.backpressure?.maxDelayMs, 1_000),
      maxRetries: finiteNonNegativeInteger(captured.backpressure?.maxRetries, 8)
    };
  }

  async command(...args: CommandArgument[]): Promise<unknown> {
    assertAtomicKeyValueCommandSharesSlot(args);
    return await executeCommandArgs(this.executor, args);
  }

  /** Execute an already-built command without a JavaScript variadic-call limit. */
  async commandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    assertAtomicKeyValueCommandSharesSlot(args);
    return await executeCommandArgs(this.executor, args);
  }

  async pipeline(commands: readonly Command[], options?: ExecutePipelineOptions): Promise<unknown[]> {
    const snapshot = snapshotPipelineCommands(commands);
    for (const command of snapshot) assertAtomicKeyValueCommandSharesSlot(command);
    const snapshotOptions = snapshotPipelineOptions(options);
    const results = this.executor.executePipeline != null
      ? await this.executor.executePipeline(snapshot, snapshotOptions)
      : await executeCommandArraysIndividually(
        async (command) => await this.commandArgs(command),
        snapshot,
        pipelineFallbackOptions(snapshot, snapshotOptions)
      );
    return validatePipelineResponse(results, snapshot.length);
  }

  async close(): Promise<void> {
    this.closeAbortController.abort(new RequestNotSentError("FerricStore client is closed"));
    this.closePromise ??= (async () => await this.executor.close?.())();
    await this.closePromise;
  }

  protected get closeSignal(): AbortSignal {
    return this.closeAbortController.signal;
  }

  async refreshTopology(): Promise<RoutingTopology> {
    if (this.executor.refreshTopology == null) {
      throw new Error("topology refresh requires a topology-aware native executor");
    }
    return await this.executor.refreshTopology();
  }

  async route(key: string | Buffer): Promise<RoutingRoute> {
    if (this.executor.route == null) throw new Error("route lookup requires a topology-aware native executor");
    return await this.executor.route(key);
  }
}

function normalizeFlowManyBatchLimit(value: number | undefined): number {
  if (value == null) return DEFAULT_FLOW_MANY_BATCH_LIMIT;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError("flowManyBatchLimit must be a positive safe integer");
  }
  return value;
}

function normalizeLegacyClaimHydrationConcurrency(value: number | undefined): number {
  if (value == null) return DEFAULT_LEGACY_CLAIM_HYDRATION_CONCURRENCY;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError("legacyClaimHydrationConcurrency must be a positive safe integer");
  }
  return value;
}
