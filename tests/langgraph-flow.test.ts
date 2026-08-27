import { describe, expect, it } from "vitest";

import { FerricStoreClient } from "../src/client.js";
import { LangGraphFlow, LangGraphFlowContext } from "../src/langgraph.js";
import { WorkflowContext } from "../src/workflow-context.js";
import { Workflow } from "../src/workflow.js";
import { FakeExecutor } from "./fake-executor.js";

describe("LangGraphFlow", () => {
  it("binds graph identity, context, completion, recovery, and interrupts to FerricFlow", async () => {
    const flow = workflowContext();
    const invocations: { input: unknown; options: Record<string, unknown> }[] = [];
    const graph = {
      async getState(): Promise<Record<string, unknown>> {
        return {};
      },
      async invoke(input: unknown, options?: Record<string, unknown>): Promise<Record<string, unknown>> {
        invocations.push({ input, options: options ?? {} });
        return { answer: 42 };
      }
    };
    const bridge = new LangGraphFlow(graph);

    const outcome = await bridge.handle(flow);
    expect(outcome.kind).toBe("complete");
    expect(invocations[0]?.input).toEqual({ question: "life" });
    const options = invocations[0]?.options;
    const configurable = options?.configurable as Record<string, unknown>;
    expect(configurable.thread_id).toMatch(/^ferricflow:/u);
    expect(options?.context).toBeInstanceOf(LangGraphFlowContext);

    const recoveringGraph = {
      async getState(): Promise<Record<string, unknown>> {
        return { config: { configurable: { checkpoint_id: "cp-1" } } };
      },
      async invoke(input: unknown): Promise<Record<string, unknown>> {
        return { input };
      }
    };
    const recovered = await new LangGraphFlow(recoveringGraph).invoke(flow);
    expect(recovered.value).toEqual({ input: null });

    const interrupted = new LangGraphFlow({
      async invoke(): Promise<Record<string, unknown>> {
        return { __interrupt__: [{ value: "approval" }] };
      }
    }, { interruptState: "waiting_approval" });
    const interruptOutcome = await interrupted.handle(flow);
    expect(interruptOutcome).toMatchObject({ kind: "transition", toState: "waiting_approval" });
  });

  it("deep-merges invocation options and preserves an intentional null input", async () => {
    const flow = workflowContext();
    const invocations: { input: unknown; options: Record<string, unknown> }[] = [];
    const baseContext = { source: "invokeOptions" };
    const bridge = new LangGraphFlow({
      async invoke(input: unknown, options?: Record<string, unknown>): Promise<Record<string, unknown>> {
        invocations.push({ input, options: options ?? {} });
        return {};
      }
    }, {
      config: () => ({
        configurable: { dynamic: "config" },
        metadata: { dynamic: "metadata" }
      }),
      input: () => null,
      invokeOptions: {
        configurable: { base: "config" },
        context: baseContext,
        metadata: { base: "metadata" }
      }
    });

    await bridge.invoke(flow);
    expect(invocations[0]?.input).toBeNull();
    expect(invocations[0]?.options.context).toBe(baseContext);
    expect(invocations[0]?.options.configurable).toMatchObject({
      base: "config",
      checkpoint_ns: "",
      dynamic: "config"
    });
    expect(invocations[0]?.options.metadata).toMatchObject({
      base: "metadata",
      dynamic: "metadata",
      ferricflow_id: "flow-1"
    });
  });
});

function workflowContext(): WorkflowContext {
  const client = new FerricStoreClient(new FakeExecutor());
  const workflow = new Workflow(client, { type: "agent" });
  return new WorkflowContext(workflow, {
    fencingToken: 1,
    id: "flow-1",
    leaseToken: Buffer.from("lease"),
    partitionKey: "tenant-1",
    payload: { question: "life" },
    state: "running",
    type: "agent"
  }, "running");
}
