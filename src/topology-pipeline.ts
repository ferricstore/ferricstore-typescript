import type { Command, CommandArgument } from "./internal.js";
import {
  attachPipelineItemRejectionFlags,
  executeCommandArraysIndividually,
  pipelineErrorCollectingOptions,
  pipelineItemRejectionFlags,
  pipelineSubsetOptions,
  surfaceFirstPipelineItemError,
  type ExecutePipelineOptions
} from "./adapters.js";
import { decomposableCommandNames } from "./command-metadata.js";
import { FerricStoreError } from "./errors.js";
import type { RoutingRoute } from "./routing-topology.js";
import { commandName } from "./topology-routing.js";
import type { RoutedCommandData, RoutedPipelineGroup } from "./topology-execution-types.js";
import {
  TaskConcurrencyLimiter,
  mapSettledWithConcurrency,
  throwFirstRejected
} from "./topology-utilities.js";

interface TopologyPipelineContext {
  readonly concurrency: number;
  readonly controlPipeline: (
    commands: readonly Command[],
    options: ExecutePipelineOptions
  ) => Promise<unknown[]>;
  readonly executeCommandArgs: (command: Command) => Promise<unknown>;
  readonly executePipelineOnRoute: (
    commands: readonly Command[],
    route: RoutingRoute,
    options: ExecutePipelineOptions
  ) => Promise<unknown[]>;
  readonly routeData: (args: readonly CommandArgument[]) => RoutedCommandData | undefined;
}

export class TopologyPipelineExecutor {
  constructor(private readonly context: TopologyPipelineContext) {}

  async execute(commands: readonly Command[], options: ExecutePipelineOptions): Promise<unknown[]> {
    if (options.ordered === true) {
      return await executeCommandArraysIndividually(this.context.executeCommandArgs, commands, options);
    }
    const output = new Array<unknown>(commands.length);
    const routedGroups = new Map<string, RoutedPipelineGroup>();
    const controlCommands: Command[] = [];
    const controlIndices: number[] = [];
    const standalone: { readonly command: Command; readonly index: number }[] = [];
    const collectingOptions = pipelineErrorCollectingOptions(options);
    let rejected: boolean[] | undefined;
    const captureGroupFailure = (indices: readonly number[], error: unknown): void => {
      rejected ??= [];
      for (const index of indices) {
        output[index] = error;
        rejected[index] = true;
      }
    };
    const captureGroupResponses = (indices: readonly number[], responses: unknown[]): void => {
      if (responses.length !== indices.length) {
        throw new FerricStoreError("topology pipeline response did not match command group", {
          raw: responses
        });
      }
      const responseRejections = pipelineItemRejectionFlags(responses);
      indices.forEach((index, responseIndex) => {
        if (!Object.hasOwn(responses, responseIndex)) {
          throw new FerricStoreError("topology pipeline response did not match command group", {
            raw: responses
          });
        }
        output[index] = responses[responseIndex];
        if (responseRejections?.[responseIndex] === true) {
          (rejected ??= [])[index] = true;
        }
      });
    };

    commands.forEach((command, index) => {
      const routed = this.context.routeData(command);
      if (routed == null) {
        if (decomposableCommandNames.has(commandName(command) ?? "")) standalone.push({ command, index });
        else {
          controlCommands.push(command);
          controlIndices.push(index);
        }
        return;
      }
      const key = `${routed.route.endpointKey}\0${routed.route.laneId}`;
      const group = routedGroups.get(key);
      if (group == null) {
        routedGroups.set(key, { commands: [command], indices: [index], route: routed.route });
      } else {
        group.commands.push(command);
        group.indices.push(index);
      }
    });

    const units: { readonly indices: readonly number[]; readonly run: () => Promise<void> }[] =
      [...routedGroups.values()].map((group) => ({
        indices: group.indices,
        run: async () => {
          try {
            const responses = await this.context.executePipelineOnRoute(
              group.commands,
              group.route,
              pipelineSubsetOptions(collectingOptions, group.indices)
            );
            captureGroupResponses(group.indices, responses);
          } catch (error) {
            captureGroupFailure(group.indices, error);
          }
        }
      }));
    if (controlCommands.length > 0) {
      units.push({
        indices: controlIndices,
        run: async () => {
          try {
            const responses = await this.context.controlPipeline(
              controlCommands,
              pipelineSubsetOptions(collectingOptions, controlIndices)
            );
            captureGroupResponses(controlIndices, responses);
          } catch (error) {
            captureGroupFailure(controlIndices, error);
          }
        }
      });
    }
    for (const { command, index } of standalone) {
      units.push({
        indices: [index],
        run: async () => {
          try {
            output[index] = await this.context.executeCommandArgs(command);
          } catch (error) {
            captureGroupFailure([index], error);
          }
        }
      });
    }

    const commandDependencies = collectingOptions.fallbackDependencies;
    if (commandDependencies == null) {
      throwFirstRejected(await mapSettledWithConcurrency(
        units,
        this.context.concurrency,
        async (unit) => await unit.run()
      ));
    } else {
      const unitByCommand = new Map<number, number>();
      units.forEach((unit, unitIndex) => {
        for (const commandIndex of unit.indices) unitByCommand.set(commandIndex, unitIndex);
      });
      const dependencies = units.map(() => new Set<number>());
      for (let commandIndex = 0; commandIndex < commandDependencies.length; commandIndex += 1) {
        const unitIndex = unitByCommand.get(commandIndex);
        if (unitIndex == null) continue;
        for (const dependencyIndex of commandDependencies[commandIndex] ?? []) {
          if (dependencyIndex < 0 || dependencyIndex >= commandIndex) continue;
          const dependencyUnit = unitByCommand.get(dependencyIndex);
          if (dependencyUnit != null && dependencyUnit !== unitIndex) dependencies[unitIndex]?.add(dependencyUnit);
        }
      }

      if (dependencies.every((unitDependencies) => unitDependencies.size === 0)) {
        throwFirstRejected(await mapSettledWithConcurrency(
          units,
          this.context.concurrency,
          async (unit) => await unit.run()
        ));
      } else {
        const limiter = new TaskConcurrencyLimiter(this.context.concurrency);
        const tasks = new Array<Promise<void>>(units.length);
        const runUnit = (unitIndex: number): Promise<void> => {
          const existing = tasks[unitIndex];
          if (existing != null) return existing;
          const unit = units[unitIndex];
          if (unit == null) return Promise.resolve();
          const task = Promise.all(
            [...(dependencies[unitIndex] ?? [])].map(async (dependency) => await runUnit(dependency))
          ).then(async () => await limiter.run(unit.run));
          tasks[unitIndex] = task;
          return task;
        };
        await Promise.all(units.map(async (_unit, unitIndex) => await runUnit(unitIndex)));
      }
    }
    if (rejected != null) attachPipelineItemRejectionFlags(output, rejected);
    return surfaceFirstPipelineItemError(output, options);
  }
}
