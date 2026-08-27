import { createHash } from "node:crypto";

import { Command } from "@langchain/langgraph";
import type { RunnableConfig } from "@langchain/core/runnables";

import { complete, fail, transition, type Outcome } from "../outcomes.js";

export type LangGraphInvocationConfig = RunnableConfig & { context?: unknown } & Record<string, unknown>;

export interface InvokableLangGraph<Input = unknown, Output = unknown> {
  invoke(input: Input | Command | null, options?: LangGraphInvocationConfig): Promise<Output>;
  getState?(options: LangGraphInvocationConfig): Promise<unknown>;
}

/** Structural subset of WorkflowContext required by the LangGraph bridge. */
export interface FerricFlowHandlerContext {
  readonly id: string;
  readonly logicalState: string;
  readonly partitionKey?: string;
  readonly payload: unknown;
  readonly state: string;
  readonly type: string;
  readonly values: Record<string, unknown>;
}

export class LangGraphFlowContext {
  readonly flow: FerricFlowHandlerContext;
  readonly threadId: string;
  readonly checkpointNs: string;

  constructor(flow: FerricFlowHandlerContext, threadId: string, checkpointNs: string) {
    this.flow = flow;
    this.threadId = threadId;
    this.checkpointNs = checkpointNs;
  }

  get id(): string { return this.flow.id; }
  get type(): string { return this.flow.type; }
  get state(): string { return this.flow.state; }
  get partitionKey(): string | undefined { return this.flow.partitionKey; }
  get payload(): unknown { return this.flow.payload; }
  get values(): Record<string, unknown> { return this.flow.values; }
}

export class LangGraphFlowRun<Output = unknown> {
  readonly value: Output;
  readonly threadId: string;
  readonly checkpointNs: string;
  readonly interrupts: readonly unknown[];

  constructor(value: Output, threadId: string, checkpointNs: string, interrupts: readonly unknown[]) {
    this.value = value;
    this.threadId = threadId;
    this.checkpointNs = checkpointNs;
    this.interrupts = interrupts;
  }

  get interrupted(): boolean { return this.interrupts.length > 0; }

  get interruptValues(): readonly unknown[] {
    return this.interrupts.map((item) =>
      item != null && typeof item === "object" && "value" in item
        ? (item).value
        : item);
  }
}

export type LangGraphOutcomeMapper<Output> = (
  run: LangGraphFlowRun<Output>,
  flow: FerricFlowHandlerContext
) => Outcome | Promise<Outcome>;

export interface LangGraphFlowOptions<Input, Output> {
  input?: (flow: FerricFlowHandlerContext) => Input | Promise<Input>;
  threadId?: (flow: FerricFlowHandlerContext) => string | Promise<string>;
  checkpointNs?: string | ((flow: FerricFlowHandlerContext) => string | Promise<string>);
  config?: (flow: FerricFlowHandlerContext) => LangGraphInvocationConfig | undefined | Promise<LangGraphInvocationConfig | undefined>;
  context?: (flow: FerricFlowHandlerContext) => unknown | Promise<unknown>;
  onComplete?: LangGraphOutcomeMapper<Output>;
  onInterrupt?: LangGraphOutcomeMapper<Output>;
  interruptState?: string;
  /** Resume an already-checkpointed graph with null input. Defaults to true. */
  recoverExisting?: boolean;
  invokeOptions?: LangGraphInvocationConfig;
}

/** Bind a LangGraph.js invocation to a leased FerricFlow workflow handler. */
export class LangGraphFlow<Input = unknown, Output = unknown> {
  readonly graph: InvokableLangGraph<Input, Output>;
  private readonly options: LangGraphFlowOptions<Input, Output>;

  constructor(graph: InvokableLangGraph<Input, Output>, options: LangGraphFlowOptions<Input, Output> = {}) {
    if (options.onInterrupt != null && options.interruptState != null) {
      throw new TypeError("onInterrupt and interruptState are mutually exclusive");
    }
    if (options.interruptState != null) requireText(options.interruptState, "interruptState");
    this.graph = graph;
    this.options = { ...options };
  }

  async config(flow: FerricFlowHandlerContext, graphContext?: unknown): Promise<LangGraphInvocationConfig> {
    const invokeOptions = this.options.invokeOptions ?? {};
    const additional = await this.options.config?.(flow) ?? {};
    const baseConfigurable = invokeOptions.configurable ?? {};
    if (typeof baseConfigurable !== "object" || Array.isArray(baseConfigurable)) {
      throw new TypeError("LangGraph invokeOptions configurable must be an object");
    }
    const rawConfigurable = additional.configurable ?? {};
    if (typeof rawConfigurable !== "object" || Array.isArray(rawConfigurable)) {
      throw new TypeError("LangGraph config configurable must be an object");
    }
    const baseMetadata = invokeOptions.metadata ?? {};
    if (typeof baseMetadata !== "object" || Array.isArray(baseMetadata)) {
      throw new TypeError("LangGraph invokeOptions metadata must be an object");
    }
    const rawMetadata = additional.metadata ?? {};
    if (typeof rawMetadata !== "object" || Array.isArray(rawMetadata)) {
      throw new TypeError("LangGraph config metadata must be an object");
    }
    const threadId = requireText(
      await (this.options.threadId?.(flow) ?? defaultThreadId(flow)),
      "threadId"
    );
    const checkpointNsOption = this.options.checkpointNs ?? "";
    const checkpointNs = requireText(
      await (typeof checkpointNsOption === "function" ? checkpointNsOption(flow) : checkpointNsOption),
      "checkpointNs",
      true
    );
    const context = graphContext === undefined
      ? this.options.context != null
        ? await this.options.context(flow)
        : Object.hasOwn(additional, "context")
          ? additional.context
          : Object.hasOwn(invokeOptions, "context")
            ? invokeOptions.context
            : new LangGraphFlowContext(flow, threadId, checkpointNs)
      : graphContext;
    return {
      ...invokeOptions,
      ...additional,
      configurable: {
        ...baseConfigurable,
        ...rawConfigurable,
        checkpoint_ns: checkpointNs,
        thread_id: threadId
      },
      context,
      metadata: {
        ...baseMetadata,
        ...rawMetadata,
        ferricflow_id: flow.id,
        ferricflow_state: flow.logicalState,
        ferricflow_type: flow.type
      }
    };
  }

  async invoke(
    flow: FerricFlowHandlerContext,
    graphInput?: Input | Command | null,
    graphContext?: unknown
  ): Promise<LangGraphFlowRun<Output>> {
    const config = await this.config(flow, graphContext);
    let input = graphInput;
    if (input === undefined) {
      let hasCheckpoint = false;
      if ((this.options.recoverExisting ?? true) && this.graph.getState != null) {
        hasCheckpoint = snapshotHasCheckpoint(await this.graph.getState(config));
      }
      input = hasCheckpoint
        ? null
        : this.options.input == null
          ? flow.payload as Input
          : await this.options.input(flow);
    }
    const value = await this.graph.invoke(input, config);
    const configurable = config.configurable;
    return new LangGraphFlowRun(
      value,
      requireText(configurable?.thread_id, "thread_id"),
      requireText(configurable?.checkpoint_ns ?? "", "checkpoint_ns", true),
      interrupts(value)
    );
  }

  async outcome(run: LangGraphFlowRun<Output>, flow: FerricFlowHandlerContext): Promise<Outcome> {
    const mapper = run.interrupted ? this.options.onInterrupt : this.options.onComplete;
    if (mapper != null) return await mapper(run, flow);
    const stateMeta = {
      langgraph_checkpoint_ns: run.checkpointNs,
      langgraph_interrupt_count: run.interrupts.length,
      langgraph_interrupted: run.interrupted,
      langgraph_thread_id: run.threadId
    };
    if (!run.interrupted) return complete({ result: run.value, stateMeta });
    if (this.options.interruptState != null) {
      return transition(this.options.interruptState, { stateMeta });
    }
    return fail({
      error: {
        checkpointNs: run.checkpointNs,
        interruptCount: run.interrupts.length,
        threadId: run.threadId,
        type: "unhandled_langgraph_interrupt"
      },
      stateMeta
    });
  }

  async handle(
    flow: FerricFlowHandlerContext,
    graphInput?: Input | Command | null,
    graphContext?: unknown
  ): Promise<Outcome> {
    return await this.outcome(await this.invoke(flow, graphInput, graphContext), flow);
  }

  async resume(flow: FerricFlowHandlerContext, value: unknown, graphContext?: unknown): Promise<Outcome> {
    return await this.handle(flow, new Command({ resume: value }), graphContext);
  }

  async handler(flow: FerricFlowHandlerContext): Promise<Outcome> {
    return await this.handle(flow);
  }
}

function interrupts(value: unknown): readonly unknown[] {
  if (value == null || typeof value !== "object" || !("__interrupt__" in value)) return [];
  const raw = (value as { __interrupt__?: unknown }).__interrupt__;
  if (raw == null) return [];
  return Array.isArray(raw) ? raw : [raw];
}

function snapshotHasCheckpoint(snapshot: unknown): boolean {
  if (snapshot == null || typeof snapshot !== "object") return false;
  const value = snapshot as Record<string, unknown>;
  if (value.config != null && typeof value.config === "object") {
    const configurable = (value.config as { configurable?: unknown }).configurable;
    if (
      configurable != null &&
      typeof configurable === "object" &&
      typeof (configurable as { checkpoint_id?: unknown }).checkpoint_id === "string"
    ) return true;
  }
  return value.createdAt != null || value.metadata != null;
}

function defaultThreadId(flow: FerricFlowHandlerContext): string {
  const identity = Buffer.concat([
    identityComponent(flow.type),
    identityComponent(flow.partitionKey),
    identityComponent(flow.id)
  ]);
  return `ferricflow:${createHash("sha256").update(identity).digest("hex")}`;
}

function identityComponent(value: string | undefined): Buffer {
  if (value == null) return Buffer.concat([Buffer.from("n"), uint64(0)]);
  const payload = Buffer.from(value, "utf8");
  return Buffer.concat([Buffer.from("s"), uint64(payload.length), payload]);
}

function uint64(value: number): Buffer {
  const result = Buffer.allocUnsafe(8);
  result.writeBigUInt64BE(BigInt(value));
  return result;
}

function requireText(value: unknown, name: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0) || value.includes("\0")) {
    throw new TypeError(`${name} must be ${allowEmpty ? "text" : "non-empty text"} without NUL bytes`);
  }
  return value;
}
