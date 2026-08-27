import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { FerricStoreClient } from "@ferricstore/ferricstore";
import { FerricStoreSaver, FerricStoreStore } from "@ferricstore/ferricstore/langgraph";

const client = await FerricStoreClient.fromUrl(
  process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388"
);
const checkpointer = new FerricStoreSaver(client);
const store = new FerricStoreStore(client);

const State = Annotation.Root({ count: Annotation<number>() });
const graph = new StateGraph(State)
  .addNode("increment", ({ count }) => ({ count: count + 1 }))
  .addEdge(START, "increment")
  .addEdge("increment", END)
  .compile({ checkpointer, store });

const result = await graph.invoke(
  { count: 0 },
  { configurable: { thread_id: "example-agent" } }
);
console.log(result);
await client.close();
