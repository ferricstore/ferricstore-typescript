import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const esm = await import("@ferricstore/ferricstore");
const cjs = require("@ferricstore/ferricstore");

for (const exportName of ["FerricStoreClient", "JsonCodec", "QueueClient", "WorkflowClient"]) {
  assert.equal(typeof esm[exportName], "function", `ESM export ${exportName} must be available`);
  assert.equal(typeof cjs[exportName], "function", `CJS export ${exportName} must be available`);
}

assert.ok(new esm.JsonCodec());
assert.ok(new cjs.JsonCodec());

console.log("package exports smoke passed");
