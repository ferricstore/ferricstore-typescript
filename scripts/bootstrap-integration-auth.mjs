import { FerricStoreClient } from "../dist/index.js";

const url = process.env.FERRICSTORE_AUTH_BOOTSTRAP_URL ?? "ferric://127.0.0.1:6388";
const password = process.env.FERRICSTORE_AUTH_PASSWORD;

if (password == null || password.length === 0) {
  throw new Error("FERRICSTORE_AUTH_PASSWORD is required");
}

const bootstrapClient = await FerricStoreClient.fromUrl(url, { reconnect: false });

try {
  if (!await bootstrapClient.aclSetUser("default", ["on", `>${password}`, "~*", "+@all"])) {
    throw new Error("ACL SETUSER did not acknowledge the integration credential");
  }
} catch {
  // Changing the current user's password may close this unauthenticated session.
} finally {
  await bootstrapClient.close().catch(() => undefined);
}

let authenticatedClient;
try {
  authenticatedClient = await FerricStoreClient.fromUrl(url, {
    nativeOptions: { password, username: "default" },
    reconnect: false
  });
} catch (error) {
  throw new Error(
    "integration credential could not authenticate after ACL SETUSER",
    { cause: error }
  );
}

try {
  await authenticatedClient.ping();
  if (!await authenticatedClient.aclSave()) {
    throw new Error("ACL SAVE did not acknowledge the integration credential");
  }
} finally {
  await authenticatedClient.close();
}
