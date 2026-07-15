import { FerricStoreError, InvalidCommandError } from "./errors.js";
import {
  append,
  appendBool,
  appendValueReturn,
  nowMs,
  okResponse,
  type CommandArgument
} from "./internal.js";
import type {
  ClaimDueOptions,
  CompleteJobsAndClaimJobsResult,
  CompleteManyOptions,
  ExtendLeaseOptions,
  ReclaimOptions
} from "./client-options.js";
import {
  appendPayloadRead,
  claimDataRequested,
  compactClaimReturnMode,
  completeJobsResultError,
  errorFromUnknown,
  isCompactClaimTuple,
  requiredArrayResponse
} from "./client-helpers.js";
import {
  claimFailure,
  hydrateClaimedRecords,
  snapshotClaimOptions
} from "./client-claim-hydration.js";
import { validatePipelineResponse } from "./adapters.js";
import { FerricStoreMutationClient } from "./client-mutations.js";
import { claimedItemFromResp, type ClaimedItem, type FlowRecord } from "./types.js";

type ClaimHydrationOptions = Pick<
  ClaimDueOptions,
  "partitionKey" | "payload" | "payloadMaxBytes" | "valueMaxBytes" | "values"
>;

export class FerricStoreClaimClient extends FerricStoreMutationClient {
  async claimDue(type: string, options: ClaimDueOptions): Promise<(FlowRecord | ClaimedItem)[]> {
    const request = this.claimDueRequest(type, options);
    return await this.claimDueResponse(
      await this.commandArgs(request.args),
      type,
      request.options,
      request.jobOnly
    );
  }

  private claimDueRequest(
    type: string,
    options: ClaimDueOptions,
    reusableAfterAwait = false
  ): { readonly args: CommandArgument[]; readonly jobOnly: boolean; readonly options: ClaimDueOptions } {
    const captured = snapshotClaimOptions(options, reusableAfterAwait);
    if (captured.state != null && captured.states != null) {
      throw new Error("state and states are mutually exclusive");
    }
    const jobOnly = captured.jobOnly === true && !claimDataRequested(captured);
    const args: CommandArgument[] = ["FLOW.CLAIM_DUE", type];
    if (captured.states != null) {
      if (captured.states.length === 0) {
        throw new Error("states must be non-empty");
      }
      if (jobOnly) args.push("STATES", captured.states.length);
      for (let index = 0; index < captured.states.length; index += 1) {
        const state = captured.states[index];
        if (!Object.hasOwn(captured.states, index) || typeof state !== "string") {
          throw new TypeError("states must be a dense array of strings");
        }
        if (jobOnly) args.push(state);
        else args.push("STATE", state);
      }
    } else {
      append(args, "STATE", captured.state);
    }

    args.push("WORKER", captured.worker, "LEASE_MS", captured.leaseMs ?? 30_000, "LIMIT", captured.limit ?? 1);
    append(args, "NOW", captured.nowMs);
    this.appendPartitionOptions(args, captured);
    append(args, "PRIORITY", captured.priority);
    if (captured.includeState === true && captured.jobOnly !== true) {
      throw new Error("includeState requires jobOnly=true");
    }
    if (jobOnly) {
      append(args, "RETURN", compactClaimReturnMode(captured.includeState === true, captured.includeAttributes === true));
    }
    append(args, "BLOCK", captured.blockMs);
    appendPayloadRead(args, captured.payload, captured.payloadMaxBytes);
    appendValueReturn(args, { values: captured.values, valueMaxBytes: captured.valueMaxBytes });
    appendBool(args, "RECLAIM_EXPIRED", captured.reclaimExpired);
    append(args, "RECLAIM_RATIO", captured.reclaimRatio);

    return { args, jobOnly, options: captured };
  }

  private async claimDueResponse(
    response: unknown,
    type: string,
    options: ClaimHydrationOptions,
    jobOnly: boolean,
    command = "FLOW.CLAIM_DUE"
  ): Promise<(FlowRecord | ClaimedItem)[]> {
    const items = requiredArrayResponse(response, command);
    if (jobOnly) {
      return this.claimedItems(items, type, command);
    }
    if (items.length > 0 && isCompactClaimTuple(items[0])) {
      const claimed = this.claimedItems(items, type, command);
      return await hydrateClaimedRecords(
        claimed,
        this.legacyClaimHydrationConcurrency,
        async (job) => {
          const record = await this.get(job.id, {
            full: options.payload ?? true,
            partitionKey: job.partitionKey,
            payload: options.payload,
            payloadMaxBytes: options.payloadMaxBytes,
            valueMaxBytes: options.valueMaxBytes,
            values: options.values
          });
          if (record == null) {
            throw new Error(`Claimed Flow ${job.id} could not be hydrated`);
          }
          return record;
        }
      );
    }
    return this.records(items);
  }

  async claimJobs(type: string, options: Omit<ClaimDueOptions, "jobOnly">): Promise<ClaimedItem[]> {
    return (await this.claimDue(type, {
      ...options,
      includeState: options.includeState ?? false,
      jobOnly: true,
      limit: options.limit ?? 100
    }));
  }

  async reclaim(type: string, options: ReclaimOptions): Promise<(FlowRecord | ClaimedItem)[]> {
    const captured = snapshotClaimOptions(options);
    if (captured.state != null && captured.state !== "running") {
      throw new Error("FLOW.RECLAIM only supports running state");
    }
    const jobOnly = captured.jobOnly === true && !claimDataRequested(captured);
    const args: CommandArgument[] = [
      "FLOW.RECLAIM",
      type,
      "WORKER",
      captured.worker,
      "LEASE_MS",
      captured.leaseMs ?? 30_000,
      "LIMIT",
      captured.limit ?? 1,
      "NOW",
      captured.nowMs ?? nowMs()
    ];
    this.appendPartitionOptions(args, captured);
    append(args, "PRIORITY", captured.priority);
    if (jobOnly) {
      append(args, "RETURN", compactClaimReturnMode(false, captured.includeAttributes === true));
    }
    appendPayloadRead(args, captured.payload, captured.payloadMaxBytes);
    appendValueReturn(args, { values: captured.values, valueMaxBytes: captured.valueMaxBytes });
    const response = await this.commandArgs(args);
    return await this.claimDueResponse(response, type, captured, jobOnly, "FLOW.RECLAIM");
  }

  private claimedItems(items: unknown[], type: string, command: string): ClaimedItem[] {
    const claimed = new Array<ClaimedItem>(items.length);
    for (let index = 0; index < items.length; index += 1) {
      if (!Object.hasOwn(items, index)) {
        throw new FerricStoreError(`${command} response item ${index} is missing`, { raw: items });
      }
      claimed[index] = claimedItemFromResp(items[index], this.codec, { type });
    }
    return claimed;
  }

  async extendLease(
    id: string,
    options: ExtendLeaseOptions & { readonly returnOkOnSuccess: true }
  ): Promise<boolean>;
  async extendLease(
    id: string,
    options: ExtendLeaseOptions & { readonly returnOkOnSuccess?: false }
  ): Promise<FlowRecord>;
  async extendLease(id: string, options: ExtendLeaseOptions): Promise<FlowRecord | boolean>;
  async extendLease(id: string, options: ExtendLeaseOptions): Promise<FlowRecord | boolean> {
    const partitionKey = options.partitionKey;
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
    append(args, "PARTITION", partitionKey);
    const wantsOkResponse = options.returnOkOnSuccess === true;
    const ownsOkResponseProbe = wantsOkResponse && this.extendLeaseOkResponseSupport == null;
    const requestOkResponse = wantsOkResponse && this.extendLeaseOkResponseSupport !== false
      && this.extendLeaseOkResponseSupport !== "probing";
    if (ownsOkResponseProbe) this.extendLeaseOkResponseSupport = "probing";
    if (requestOkResponse) {
      args.push("RETURN", "OK_ON_SUCCESS");
    }
    let legacyResponse = !requestOkResponse;
    let response: unknown;
    try {
      response = await this.commandArgs(args);
    } catch (error) {
      if (!requestOkResponse || !(error instanceof InvalidCommandError)) {
        if (ownsOkResponseProbe && this.extendLeaseOkResponseSupport === "probing") {
          this.extendLeaseOkResponseSupport = undefined;
        }
        throw error;
      }

      // RETURN was added after FLOW.EXTEND_LEASE. A syntax rejection cannot
      // have mutated the lease, so retry the legacy form once and remember the
      // capability to keep subsequent renewals on the single-request path.
      this.extendLeaseOkResponseSupport = false;
      legacyResponse = true;
      response = await this.commandArgs(args.slice(0, -2));
    }
    if (wantsOkResponse) {
      if (!legacyResponse) {
        let ok: boolean;
        try {
          ok = okResponse(response);
        } catch (error) {
          if (ownsOkResponseProbe && this.extendLeaseOkResponseSupport === "probing") {
            this.extendLeaseOkResponseSupport = undefined;
          }
          throw error;
        }
        if (this.extendLeaseOkResponseSupport !== false) {
          this.extendLeaseOkResponseSupport = true;
        }
        return ok;
      }
      await this.recordOrGet(response, id, partitionKey);
      return true;
    }
    return await this.recordOrGet(response, id, partitionKey);
  }

  /**
   * Complete leased jobs and claim replacements through one ordered pipeline
   * when both commands can use the same route. Item errors are returned beside
   * any newly leased jobs so callers cannot accidentally discard those leases.
   */
  async completeJobsAndClaimJobs(
    jobs: ClaimedItem[],
    type: string,
    claimOptions: ClaimDueOptions,
    completeOptions: Omit<CompleteManyOptions, "returnOkOnSuccess"> = {}
  ): Promise<CompleteJobsAndClaimJobsResult> {
    const claimRequest = this.claimDueRequest(type, claimOptions, true);
    const jobCount = jobs.length;
    if (jobCount === 0) {
      try {
        return {
          claimed: await this.claimDue(type, claimRequest.options),
          completion: [],
          fused: false
        };
      } catch (error) {
        const failure = claimFailure(error);
        return {
          ...failure,
          completion: [],
          fused: false
        };
      }
    }

    const capturedCompleteOptions = { ...completeOptions };
    const normalizedCompleteOptions: CompleteManyOptions = {
      ...capturedCompleteOptions,
      independent: capturedCompleteOptions.independent ?? true,
      returnOkOnSuccess: true
    };
    const claimPartition = claimRequest.options.partitionKey;
    const firstJobPartition = jobs[0]?.partitionKey;
    const sharedJobPartition = firstJobPartition != null
      && jobs.every((job) => job.partitionKey === firstJobPartition)
      ? firstJobPartition
      : undefined;
    const completionPartition = claimPartition ?? sharedJobPartition;
    const canShareRoute = claimRequest.options.partitionKeys == null && (
      claimPartition == null || jobs.every((job) => job.partitionKey == null || job.partitionKey === claimPartition)
    );

    let preparedCompletionRequest: CommandArgument[] | undefined;
    if (
      jobCount <= this.flowManyBatchLimit &&
      canShareRoute &&
      this.executor.executeFusedPipeline != null
    ) {
      preparedCompletionRequest = this.completeManyRequest(
        completionPartition,
        jobs,
        normalizedCompleteOptions
      );
      const rawResponses = await this.executor.executeFusedPipeline([
        preparedCompletionRequest,
        claimRequest.args
      ], { ordered: true, throwOnItemError: false });
      const responses = rawResponses == null
        ? undefined
        : validatePipelineResponse(rawResponses, 2, "complete-and-claim pipeline");

      if (responses != null) {
        const completion = responses[0];
        const completionError = completeJobsResultError(completion, jobCount);
        const claimResponse = responses[1];
        if (claimResponse instanceof Error) {
          const failure = claimFailure(claimResponse);
          return {
            ...failure,
            completion,
            ...(completionError == null ? {} : { completionError }),
            fused: true
          };
        }
        try {
          return {
            claimed: await this.claimDueResponse(
              claimResponse,
              type,
              claimRequest.options,
              claimRequest.jobOnly
            ),
            completion,
            ...(completionError == null ? {} : { completionError }),
            fused: true
          };
        } catch (error) {
          const failure = claimFailure(error);
          return {
            ...failure,
            completion,
            ...(completionError == null ? {} : { completionError }),
            fused: true
          };
        }
      }
    }

    let completion: unknown;
    try {
      completion = preparedCompletionRequest == null
        ? await this.completeJobs(jobs, normalizedCompleteOptions)
        : this.recordsOrResponse(await this.commandArgs(preparedCompletionRequest));
    } catch (error) {
      const completionError = errorFromUnknown(error);
      return { claimed: [], completion: completionError, completionError, fused: false };
    }
    const completionError = completeJobsResultError(completion, jobCount);
    if (completionError != null) {
      return { claimed: [], completion, completionError, fused: false };
    }
    try {
      return {
        claimed: await this.claimDue(type, claimRequest.options),
        completion,
        fused: false
      };
    } catch (error) {
      const failure = claimFailure(error);
      return {
        ...failure,
        completion,
        fused: false
      };
    }
  }

  async completeFlowsAndClaimFlows(
    jobs: ClaimedItem[],
    type: string,
    claimOptions: ClaimDueOptions,
    completeOptions: Omit<CompleteManyOptions, "returnOkOnSuccess"> = {}
  ): Promise<CompleteJobsAndClaimJobsResult> {
    return await this.completeJobsAndClaimJobs(jobs, type, claimOptions, completeOptions);
  }

}
