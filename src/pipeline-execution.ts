import type { Command, CommandArgument } from "./internal.js";
import { snapshotCommandArguments } from "./command-snapshot.js";
import {
  COMPACT_PIPELINE_DECODED,
  commandRequiresOrderedPipelineExecution
} from "./protocol.js";

export interface ExecutePipelineOptions {
  /** @internal Earlier command indexes that must settle before each individual fallback command starts. */
  fallbackDependencies?: readonly (readonly number[])[];
  /** Maximum concurrent requests when a native pipeline is unavailable. Defaults to 64. */
  fallbackConcurrency?: number;
  /** Preserve command execution order when a native pipeline is unavailable. */
  ordered?: boolean;
  throwOnItemError?: boolean;
}

const DEFAULT_FALLBACK_CONCURRENCY = 64;

const pipelineItemRejections = Symbol("ferricstore.pipelineItemRejections");
const pipelineCommandSnapshots = new WeakSet<readonly Command[]>();
const pipelineOptionSnapshots = new WeakSet<ExecutePipelineOptions>();

/** @internal Validate the positional contract supplied by a pipeline executor. */
export function validatePipelineResponse(
  value: unknown,
  expectedItems: number,
  operation = "pipeline"
): unknown[] {
  if (!Array.isArray(value)) {
    throw new TypeError(`${operation} returned an invalid response`);
  }
  if (value.length !== expectedItems) {
    throw new TypeError(`${operation} returned ${value.length} items; expected ${expectedItems}`);
  }
  if ((value as unknown as Record<symbol, unknown>)[COMPACT_PIPELINE_DECODED] === true) {
    return value;
  }
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new TypeError(`${operation} response item ${index} is missing`);
    }
  }
  return value;
}

/** @internal Reject holes before any pipeline command can be dispatched. */
export function assertDensePipelineCommands(commands: readonly Command[]): void {
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    for (let argumentIndex = 0; argumentIndex < command.length; argumentIndex += 1) {
      if (!Object.hasOwn(command, argumentIndex)) {
        throw new TypeError("pipeline command arguments must be dense");
      }
    }
  }
}

/** Snapshot pipeline structure at admission while validating every positional slot once. */
export function snapshotPipelineCommands(commands: readonly Command[]): Command[] {
  if (pipelineCommandSnapshots.has(commands)) return commands as Command[];
  const snapshot = new Array<Command>(commands.length);
  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (!Object.hasOwn(commands, index) || !Array.isArray(command)) {
      throw new TypeError("pipeline commands must be a dense array of command arrays");
    }
    snapshot[index] = snapshotCommandArguments(command, "pipeline command arguments must be dense");
  }
  Object.freeze(snapshot);
  pipelineCommandSnapshots.add(snapshot);
  return snapshot;
}

/** Snapshot pipeline options, including dependency rows, once across executor layers. */
export function snapshotPipelineOptions(
  options: ExecutePipelineOptions | undefined
): ExecutePipelineOptions | undefined {
  if (options == null || pipelineOptionSnapshots.has(options)) return options;
  const snapshot = { ...options };
  const dependencies = snapshot.fallbackDependencies;
  if (dependencies != null) {
    const dependencySnapshots = new Array<readonly number[]>(dependencies.length);
    for (let index = 0; index < dependencies.length; index += 1) {
      if (!Object.hasOwn(dependencies, index)) continue;
      const dependencySnapshot = dependencies[index]?.slice() ?? [];
      Object.freeze(dependencySnapshot);
      dependencySnapshots[index] = dependencySnapshot;
    }
    Object.freeze(dependencySnapshots);
    snapshot.fallbackDependencies = dependencySnapshots;
  }
  Object.freeze(snapshot);
  pipelineOptionSnapshots.add(snapshot);
  return snapshot;
}

/** @internal Rejection markers preserve arbitrary JavaScript rejection reasons. */
export function pipelineItemRejectionFlags(results: readonly unknown[]): readonly boolean[] | undefined {
  return (results as unknown as Record<symbol, readonly boolean[]>)[pipelineItemRejections];
}

/** @internal Attach per-item rejection markers without wrapping arbitrary rejection reasons. */
export function attachPipelineItemRejectionFlags(
  results: unknown[],
  rejected: readonly boolean[]
): void {
  Object.defineProperty(results, pipelineItemRejections, { value: rejected });
}

/** @internal Preserve dependencies when a state-changing control command cannot be pipelined. */
export function pipelineFallbackOptions(
  commands: readonly Command[],
  options: ExecutePipelineOptions = {}
): ExecutePipelineOptions {
  if (options.ordered === true) return options;
  for (const command of commands) {
    if (commandRequiresOrderedPipelineExecution(command)) return { ...options, ordered: true };
  }
  return options;
}

/** @internal */
export function pipelineErrorCollectingOptions(options: ExecutePipelineOptions): ExecutePipelineOptions {
  return options.throwOnItemError === false ? options : { ...options, throwOnItemError: false };
}

/** @internal Remap individual-fallback dependency indexes onto a pipeline subset. */
export function pipelineSubsetOptions(
  options: ExecutePipelineOptions,
  originalIndices: readonly number[]
): ExecutePipelineOptions {
  const dependencies = options.fallbackDependencies;
  if (dependencies == null) return options;
  const localIndices = new Map<number, number>();
  originalIndices.forEach((originalIndex, localIndex) => localIndices.set(originalIndex, localIndex));
  return {
    ...options,
    fallbackDependencies: originalIndices.map((originalIndex) =>
      (dependencies[originalIndex] ?? []).flatMap((dependency) => {
        const localIndex = localIndices.get(dependency);
        return localIndex == null ? [] : [localIndex];
      })
    )
  };
}

/** @internal Slice dependencies while preserving their original ordering. */
export function pipelineSliceOptions(
  options: ExecutePipelineOptions,
  start: number,
  end: number
): ExecutePipelineOptions {
  if (options.fallbackDependencies == null) return options;
  return pipelineSubsetOptions(
    options,
    Array.from({ length: end - start }, (_unused, index) => start + index)
  );
}

/** @internal */
export function surfaceFirstPipelineItemError(
  results: unknown[],
  options: ExecutePipelineOptions
): unknown[] {
  if (options.throwOnItemError !== false) {
    const rejected = pipelineItemRejectionFlags(results);
    for (let index = 0; index < results.length; index += 1) {
      const result = results[index];
      if (rejected?.[index] === true || result instanceof Error) throw result;
    }
  }
  return results;
}

/** @internal */
export async function executeCommandArraysIndividually(
  executeCommandArgs: (args: readonly CommandArgument[]) => Promise<unknown>,
  commands: readonly Command[],
  options: ExecutePipelineOptions = {}
): Promise<unknown[]> {
  assertDensePipelineCommands(commands);
  const rejected: boolean[] = [];
  let hasRejected = false;
  if (options.ordered === true) {
    const results: unknown[] = [];
    for (let index = 0; index < commands.length; index += 1) {
      const command = commands[index];
      if (command == null) continue;
      try {
        results.push(await executeCommandArgs(command));
      } catch (error) {
        rejected[index] = true;
        hasRejected = true;
        results.push(error);
      }
    }
    return surfaceIndividualCommandErrors(results, rejected, hasRejected, options);
  }
  const concurrency = fallbackConcurrency(options.fallbackConcurrency, commands.length);
  if (options.fallbackDependencies != null) {
    const results = new Array<unknown>(commands.length);
    const tasks = new Array<Promise<void>>(commands.length);
    const slots = new AsyncSlotPool(concurrency);
    for (let index = 0; index < commands.length; index += 1) {
      const command = commands[index];
      const dependencies = options.fallbackDependencies[index] ?? [];
      const prerequisites = dependencies.flatMap((dependency) => {
        const task = dependency >= 0 && dependency < index ? tasks[dependency] : undefined;
        return task == null ? [] : [task.catch(() => undefined)];
      });
      tasks[index] = Promise.all(prerequisites).then(async () => await slots.run(async () => {
        if (command == null) return;
        try {
          results[index] = await executeCommandArgs(command);
        } catch (error) {
          rejected[index] = true;
          hasRejected = true;
          results[index] = error;
        }
      }));
    }
    await Promise.all(tasks);
    return surfaceIndividualCommandErrors(results, rejected, hasRejected, options);
  }
  const results = new Array<unknown>(commands.length);
  let nextIndex = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (nextIndex < commands.length) {
      const index = nextIndex++;
      const command = commands[index];
      if (command == null) continue;
      try {
        results[index] = await executeCommandArgs(command);
      } catch (error) {
        rejected[index] = true;
        hasRejected = true;
        results[index] = error;
      }
    }
  });
  await Promise.all(workers);
  return surfaceIndividualCommandErrors(results, rejected, hasRejected, options);
}

interface SlotWaiter {
  next?: SlotWaiter;
  readonly resolve: () => void;
}

class AsyncSlotPool {
  private active = 0;
  private waiterHead?: SlotWaiter;
  private waiterTail?: SlotWaiter;

  constructor(private readonly limit: number) {}

  async run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.active >= this.limit) {
      await new Promise<void>((resolve) => {
        const waiter: SlotWaiter = { resolve };
        if (this.waiterTail == null) {
          this.waiterHead = waiter;
        } else {
          this.waiterTail.next = waiter;
        }
        this.waiterTail = waiter;
      });
    } else {
      this.active += 1;
    }
    try {
      return await operation();
    } finally {
      const waiter = this.waiterHead;
      if (waiter == null) {
        this.active -= 1;
      } else {
        this.waiterHead = waiter.next;
        if (this.waiterHead == null) this.waiterTail = undefined;
        waiter.resolve();
      }
    }
  }
}

function fallbackConcurrency(value: number | undefined, commandCount: number): number {
  if (value != null && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new TypeError("fallbackConcurrency must be a positive safe integer");
  }
  return Math.min(commandCount, value ?? DEFAULT_FALLBACK_CONCURRENCY);
}

/** @internal Backward-compatible variadic adapter for custom pipeline executors. */
export async function executeCommandsIndividually(
  executeCommand: (...args: CommandArgument[]) => Promise<unknown>,
  commands: readonly Command[],
  options: ExecutePipelineOptions = {}
): Promise<unknown[]> {
  return await executeCommandArraysIndividually(
    async (command) => await executeCommand(...command),
    commands,
    options
  );
}

function surfaceIndividualCommandErrors(
  results: unknown[],
  rejected: readonly boolean[],
  hasRejected: boolean,
  options: ExecutePipelineOptions
): unknown[] {
  if (options.throwOnItemError !== false) {
    for (let index = 0; index < results.length; index += 1) {
      const result = results[index];
      if (rejected[index] === true || result instanceof Error) throw result;
    }
  } else if (hasRejected) {
    attachPipelineItemRejectionFlags(results, rejected);
  }
  return results;
}
