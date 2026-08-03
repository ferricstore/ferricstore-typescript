import type { Command } from "./internal.js";
import {
  RequestFrameTooLargeError,
  tryPipelineCommand,
  unwrapPipelineResponse,
  type ProtocolCommand
} from "./protocol.js";
import {
  attachPipelineItemRejectionFlags,
  executeCommandArraysIndividually,
  pipelineErrorCollectingOptions,
  pipelineFallbackOptions,
  pipelineItemRejectionFlags,
  pipelineSliceOptions,
  snapshotPipelineCommands,
  snapshotPipelineOptions,
  surfaceFirstPipelineItemError,
  type ExecutePipelineOptions
} from "./pipeline-execution.js";

export interface NativePipelineHost {
  executeCommandArgs(args: Command): Promise<unknown>;
  executeCommandOnLane(args: Command, laneId: number): Promise<unknown>;
  executeProtocolCommand(command: ProtocolCommand, laneId?: number): Promise<unknown>;
}

export async function executeNativeFusedPipeline(
  host: NativePipelineHost,
  commands: readonly Command[],
  laneId: number | undefined,
  options: ExecutePipelineOptions,
  maxPipelineCommands: number,
  maxRequestFrameBytes: number,
  compactStreamXAdd = true,
  compactPubSubPublish = true
): Promise<unknown[] | undefined> {
  commands = snapshotPipelineCommands(commands);
  options = snapshotPipelineOptions(options) ?? options;
  if (commands.length === 0) return [];
  if (maxPipelineCommands === 0 || commands.length > maxPipelineCommands) return undefined;
  try {
    const pipeline = tryPipelineCommand(
      commands,
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
    if (pipeline == null) return undefined;
    const response = await host.executeProtocolCommand(pipeline, laneId);
    return unwrapPipelineResponse(response, options, commands.length);
  } catch (error) {
    if (error instanceof RequestFrameTooLargeError) return undefined;
    throw error;
  }
}

export async function executeNativePipeline(
  host: NativePipelineHost,
  commands: readonly Command[],
  laneId: number | undefined,
  options: ExecutePipelineOptions,
  maxPipelineCommands: number,
  maxRequestFrameBytes: number,
  compactStreamXAdd = true,
  compactPubSubPublish = true
): Promise<unknown[]> {
  commands = snapshotPipelineCommands(commands);
  options = snapshotPipelineOptions(options) ?? options;
  if (commands.length === 0) return [];
  if (maxPipelineCommands === 0) {
    return await executeIndividually(host, commands, laneId, pipelineFallbackOptions(commands, options));
  }
  if (commands.length <= maxPipelineCommands) {
    return await executeChunk(
      host,
      commands,
      laneId,
      options,
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
  }

  const collectingOptions = pipelineErrorCollectingOptions(options);
  const results = new Array<unknown>(commands.length);
  let rejected: boolean[] | undefined;
  for (let start = 0; start < commands.length; start += maxPipelineCommands) {
    const chunk = commands.slice(start, start + maxPipelineCommands);
    const chunkResults = await executeChunk(
      host,
      chunk,
      laneId,
      pipelineSliceOptions(collectingOptions, start, start + chunk.length),
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
    for (let index = 0; index < chunkResults.length; index += 1) results[start + index] = chunkResults[index];
    rejected = collectPipelineRejections(chunkResults, start, rejected);
  }
  if (rejected != null) attachPipelineItemRejectionFlags(results, rejected);
  return surfaceFirstPipelineItemError(results, options);
}

async function executeChunk(
  host: NativePipelineHost,
  commands: readonly Command[],
  laneId: number | undefined,
  options: ExecutePipelineOptions,
  maxRequestFrameBytes: number,
  compactStreamXAdd: boolean,
  compactPubSubPublish: boolean
): Promise<unknown[]> {
  try {
    const pipeline = tryPipelineCommand(
      commands,
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
    if (pipeline != null) {
      const response = await host.executeProtocolCommand(pipeline, laneId);
      return unwrapPipelineResponse(response, options, commands.length);
    }
  } catch (error) {
    if (!(error instanceof RequestFrameTooLargeError)) throw error;
    const collectingOptions = pipelineErrorCollectingOptions(options);
    if (commands.length === 1) {
      const results = await executeIndividually(host, commands, laneId, collectingOptions);
      return surfaceFirstPipelineItemError(results, options);
    }
    const middle = Math.ceil(commands.length / 2);
    const first = await executeChunk(
      host,
      commands.slice(0, middle),
      laneId,
      pipelineSliceOptions(collectingOptions, 0, middle),
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
    const second = await executeChunk(
      host,
      commands.slice(middle),
      laneId,
      pipelineSliceOptions(collectingOptions, middle, commands.length),
      maxRequestFrameBytes,
      compactStreamXAdd,
      compactPubSubPublish,
    );
    return surfaceFirstPipelineItemError(mergePipelineResults(first, second), options);
  }
  return await executeIndividually(host, commands, laneId, pipelineFallbackOptions(commands, options));
}

function mergePipelineResults(first: unknown[], second: unknown[]): unknown[] {
  const results = [...first, ...second];
  let rejected = collectPipelineRejections(first, 0);
  rejected = collectPipelineRejections(second, first.length, rejected);
  if (rejected != null) attachPipelineItemRejectionFlags(results, rejected);
  return results;
}

function collectPipelineRejections(
  results: readonly unknown[],
  offset: number,
  rejected?: boolean[]
): boolean[] | undefined {
  const flags = pipelineItemRejectionFlags(results);
  if (flags == null) return rejected;
  for (let index = 0; index < flags.length; index += 1) {
    if (flags[index] === true) (rejected ??= [])[offset + index] = true;
  }
  return rejected;
}

async function executeIndividually(
  host: NativePipelineHost,
  commands: readonly Command[],
  laneId: number | undefined,
  options: ExecutePipelineOptions
): Promise<unknown[]> {
  return await executeCommandArraysIndividually(async (command) => {
    return laneId == null
      ? await host.executeCommandArgs(command)
      : await host.executeCommandOnLane(command, laneId);
  }, commands, options);
}
