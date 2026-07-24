import { FerricStoreClient } from "../dist/index.js";

const url = process.env.FERRICSTORE_AUTH_BOOTSTRAP_URL ?? "ferric://127.0.0.1:6388";
const password = process.env.FERRICSTORE_AUTH_PASSWORD;

if (password == null || password.length === 0) {
  throw new Error("FERRICSTORE_AUTH_PASSWORD is required");
}

const client = await FerricStoreClient.fromUrl(url, { reconnect: false });

try {
  if (!await client.aclSetUser("default", ["on", `>${password}`, "~*", "+@all"])) {
    throw new Error("ACL SETUSER did not acknowledge the integration credential");
  }
  if (!await client.aclSave()) {
    throw new Error("ACL SAVE did not acknowledge the integration credential");
  }
} finally {
  await client.close();
}
