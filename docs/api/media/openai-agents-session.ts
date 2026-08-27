import { Agent, run } from "@openai/agents";
import { FerricStoreClient } from "@ferricstore/ferricstore";
import { FerricStoreSession } from "@ferricstore/ferricstore/openai-agents";

const client = await FerricStoreClient.fromUrl(
  process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388"
);
const session = new FerricStoreSession(client, { sessionId: "example-conversation" });
const agent = new Agent({ name: "Assistant", instructions: "Be concise and helpful." });

const result = await run(agent, "Remember that my preferred language is TypeScript.", { session });
console.log(result.finalOutput);
await client.close();
