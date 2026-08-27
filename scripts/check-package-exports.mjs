import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const esm = await import("@ferricstore/ferricstore");
const cjs = require("@ferricstore/ferricstore");
const langgraphEsm = await import("@ferricstore/ferricstore/langgraph");
const langgraphCjs = require("@ferricstore/ferricstore/langgraph");
const agentsEsm = await import("@ferricstore/ferricstore/openai-agents");
const agentsCjs = require("@ferricstore/ferricstore/openai-agents");

for (const exportName of ["FerricStoreClient", "JsonCodec", "QueueClient", "WorkflowClient"]) {
  assert.equal(typeof esm[exportName], "function", `ESM export ${exportName} must be available`);
  assert.equal(typeof cjs[exportName], "function", `CJS export ${exportName} must be available`);
}

assert.ok(new esm.JsonCodec());
assert.ok(new cjs.JsonCodec());

for (const exportName of ["FerricStoreSaver", "FerricStoreStore", "LangGraphFlow"]) {
  assert.equal(typeof langgraphEsm[exportName], "function", `LangGraph ESM export ${exportName} must be available`);
  assert.equal(typeof langgraphCjs[exportName], "function", `LangGraph CJS export ${exportName} must be available`);
}

assert.equal(typeof agentsEsm.FerricStoreSession, "function", "OpenAI Agents ESM Session export must be available");
assert.equal(typeof agentsCjs.FerricStoreSession, "function", "OpenAI Agents CJS Session export must be available");

console.log("package exports smoke passed");
