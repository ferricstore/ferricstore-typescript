import type { CommandExecutor } from "./adapter-types.js";
import type { CommandArgument } from "./internal.js";
export type {
  CommandExecutor,
  NativeAdapterOptions,
  NativeClientOptions,
  NativeProtocolEvent,
  ReconnectOptions,
  TopologyNativeAdapterOptions
} from "./adapter-types.js";
import { assertSafeVariadicDispatch } from "./variadic-dispatch.js";
export { NativeAdapter } from "./native-adapter.js";
export { HTTPAdapter } from "./http-adapter.js";
export type { HTTPAdapterOptions } from "./http-options.js";
export { snapshotCommandArguments } from "./command-snapshot.js";
export {
  assertDensePipelineCommands,
  attachPipelineItemRejectionFlags,
  executeCommandArraysIndividually,
  executeCommandsIndividually,
  pipelineErrorCollectingOptions,
  pipelineFallbackOptions,
  pipelineItemRejectionFlags,
  pipelineSubsetOptions,
  snapshotPipelineCommands,
  snapshotPipelineOptions,
  surfaceFirstPipelineItemError,
  validatePipelineResponse,
  type ExecutePipelineOptions
} from "./pipeline-execution.js";

/** @internal Dispatch an argument array while retaining compatibility with custom executors. */
export function executeCommandArgs(
  executor: CommandExecutor,
  args: readonly CommandArgument[]
): Promise<unknown> {
  if (executor.executeCommandArgs != null) return executor.executeCommandArgs(args);
  assertSafeVariadicDispatch(args.length, "executeCommandArgs");
  return executor.executeCommand(...args);
}
