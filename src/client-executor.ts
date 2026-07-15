import { Buffer } from "node:buffer";
import {
  executeCommandArraysIndividually,
  executeCommandArgs,
  pipelineFallbackOptions,
  type CommandExecutor,
  type ExecutePipelineOptions
} from "./adapters.js";
import { throwMapped } from "./client-helpers.js";
import type { Command, CommandArgument } from "./internal.js";
import type { RoutingRoute, RoutingTopology } from "./topology.js";

export class ErrorMappingExecutor implements CommandExecutor {
  constructor(private readonly executor: CommandExecutor) {}

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    try {
      return await executeCommandArgs(this.executor, args);
    } catch (error) {
      throwMapped(error);
    }
  }

  async executePipeline(commands: readonly Command[], options?: ExecutePipelineOptions): Promise<unknown[]> {
    let results: unknown[];
    try {
      if (this.executor.executePipeline != null) {
        results = await this.executor.executePipeline(commands, options);
      } else {
        results = await executeCommandArraysIndividually(
          async (command) => await executeCommandArgs(this.executor, command),
          commands,
          pipelineFallbackOptions(commands, options)
        );
      }
    } catch (error) {
      throwMapped(error);
    }
    return results;
  }

  async executeFusedPipeline(
    commands: readonly Command[],
    options?: ExecutePipelineOptions
  ): Promise<unknown[] | undefined> {
    if (this.executor.executeFusedPipeline == null) return undefined;
    try {
      return await this.executor.executeFusedPipeline(commands, options);
    } catch (error) {
      throwMapped(error);
    }
  }

  async refreshTopology(): Promise<RoutingTopology> {
    if (this.executor.refreshTopology == null) {
      throw new Error("topology refresh requires a topology-aware native executor");
    }
    return await this.executor.refreshTopology();
  }

  async route(key: string | Buffer): Promise<RoutingRoute> {
    if (this.executor.route == null) {
      throw new Error("route lookup requires a topology-aware native executor");
    }
    return await this.executor.route(key);
  }

  async close(): Promise<void> {
    await this.executor.close?.();
  }
}
