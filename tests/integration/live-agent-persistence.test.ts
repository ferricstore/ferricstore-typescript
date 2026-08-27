import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { Agent, run, type AgentInputItem } from "@openai/agents";
import { assistantMessage, ScriptedModel } from "@openai/agents/testing";
import { describe, expect, it } from "vitest";

import { FerricStoreSaver, FerricStoreStore, LangGraphFlow } from "../../src/langgraph.js";
import { FerricStoreSession } from "../../src/openai-agents.js";
import { deletePrefixedKeys, eventually, integrationClient, suffix } from "./live-support.js";

describe("live agent-framework persistence", () => {
  it("persists OpenAI sessions and a compiled LangGraph through the real server", async () => {
    const client = await integrationClient();
    const prefix = `integration:agent-persistence:${suffix()}`;
    try {
      const session = new FerricStoreSession(client, {
        keyPrefix: `${prefix}:session`,
        sessionId: "conversation"
      });
      await session.addItems([message("hello")]);
      await session.applyHistoryTransaction({
        operationId: "turn-1",
        transaction: { items: [message("world")], type: "append_items" }
      });
      await session.applyHistoryTransaction({
        operationId: "turn-1",
        transaction: { items: [message("world")], type: "append_items" }
      });
      const reopened = new FerricStoreSession(client, {
        keyPrefix: `${prefix}:session`,
        sessionId: "conversation"
      });
      expect(await reopened.getItems()).toEqual([message("hello"), message("world")]);

      const runnerSession = new FerricStoreSession(client, {
        keyPrefix: `${prefix}:session`,
        sessionId: "agent-runner"
      });
      const model = new ScriptedModel([[assistantMessage("durable reply")]]);
      const agent = new Agent({ instructions: "Reply from the scripted model.", model, name: "integration" });
      const result = await run(agent, "durable question", { session: runnerSession });
      expect(result.finalOutput).toBe("durable reply");
      const runnerHistory = await new FerricStoreSession(client, {
        keyPrefix: `${prefix}:session`,
        sessionId: "agent-runner"
      }).getItems();
      expect(runnerHistory).toMatchObject([
        { content: "durable question", role: "user", type: "message" },
        { role: "assistant", status: "completed", type: "message" }
      ]);

      const saver = new FerricStoreSaver(client, { keyPrefix: `${prefix}:checkpoint` });
      const store = new FerricStoreStore(client, { keyPrefix: `${prefix}:store` });
      await store.put(["agents", "live"], "preference", { language: "typescript" });
      expect((await store.get(["agents", "live"], "preference"))?.value).toEqual({
        language: "typescript"
      });

      const State = Annotation.Root({
        count: Annotation<number>({ reducer: (left, right) => left + right, default: () => 0 })
      });
      const graph = new StateGraph(State)
        .addNode("increment", () => ({ count: 1 }))
        .addEdge(START, "increment")
        .addEdge("increment", END)
        .compile({ checkpointer: saver, store });
      const config = { configurable: { thread_id: "live-thread" } };
      expect(await graph.invoke({ count: 4 }, config)).toEqual({ count: 5 });
      expect((await saver.getTuple(config))?.checkpoint.channel_values.count).toBe(5);

      const bridge = new LangGraphFlow(graph);
      const flow = {
        id: "live-flow",
        logicalState: "running",
        partitionKey: "tenant",
        payload: { count: 7 },
        state: "running",
        type: "agent",
        values: {}
      };
      expect((await bridge.invoke(flow)).value).toEqual({ count: 8 });
      expect((await bridge.invoke(flow)).value).toEqual({ count: 8 });
      await saver.deleteThread("live-thread");
      expect(await eventually(
        async () => await saver.getTuple(config),
        (tuple) => tuple == null,
        "deleted LangGraph thread remained visible",
        { timeoutMs: 5_000 }
      )).toBeUndefined();
    } finally {
      await deletePrefixedKeys(client, prefix);
      await client.close();
    }
  }, 30_000);
});

function message(content: string): AgentInputItem {
  return { content, role: "user" };
}
