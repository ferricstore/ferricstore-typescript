import type { Codec } from "./codecs.js";
import type { StepContinueOptions } from "./client-options.js";
import {
  appendAttributeMutations,
  appendStateMeta
} from "./client-helpers.js";
import { requestNotSentError } from "./errors.js";
import {
  append,
  appendEncoded,
  appendNamedValues,
  nowMs,
  type CommandArgument
} from "./internal.js";

/** Build and encode a continuation entirely before it can reach an adapter. */
export function stepContinueArguments(
  id: string,
  options: StepContinueOptions,
  codec: Codec
): CommandArgument[] {
  try {
    const args: CommandArgument[] = [
      "FLOW.STEP_CONTINUE",
      id,
      options.leaseToken,
      options.fromState,
      options.toState,
      "FENCING",
      options.fencingToken,
      "LEASE_MS",
      options.leaseMs ?? 30_000,
      "NOW",
      options.nowMs ?? nowMs()
    ];
    append(args, "PARTITION", options.partitionKey);
    append(args, "WORKER", options.worker);
    appendEncoded(args, "PAYLOAD", codec, options.payload);
    if (options.returnJob === true) args.push("RETURN", "JOBS_COMPACT");
    appendStateMeta(args, options.stateMeta);
    appendNamedValues(args, codec, options);
    appendAttributeMutations(args, options);
    return args;
  } catch (error) {
    throw requestNotSentError(error);
  }
}
