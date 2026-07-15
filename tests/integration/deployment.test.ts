import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { FerricStoreClient, NativeAdapter, RawCodec, type NativeAdapterOptions } from "../../src/index.js";

const haUrls = splitUrls(process.env.FERRICSTORE_HA_URLS);
const tlsUrl = process.env.FERRICSTORE_TLS_URL;
const authUrl = process.env.FERRICSTORE_AUTH_URL;

describe.skipIf(haUrls.length < 2)("live multi-node HA deployment", () => {
  it("connects every seed and routes cross-node multi-key operations correctly", async () => {
    for (const seed of haUrls) {
      const adapter = await NativeAdapter.fromUrl(seed, haNativeOptions());
      try {
        await expect(adapter.executeCommand("PING")).resolves.toBeDefined();
      } finally {
        await adapter.close();
      }
    }

    const client = await FerricStoreClient.fromUrls(haUrls, {
      autoBatch: { enabled: true, maxCommands: 32, maxDelayMs: 0 },
      codec: new RawCodec(),
      nativeOptions: {
        ...haNativeOptions(),
        endpointPolicy: "seed_hosts",
        warmConnections: true
      },
      reconnect: false
    });
    const keys: string[] = [];

    try {
      const topology = await client.refreshTopology();
      expect(topology.endpoints.size).toBeGreaterThanOrEqual(2);

      const keyByEndpoint = new Map<string, string>();
      const prefix = `ts-sdk:ha:${randomUUID()}`;
      for (let index = 0; index < 10_000 && keyByEndpoint.size < 2; index += 1) {
        const key = `${prefix}:${index}`;
        const route = topology.routeKey(key);
        if (!keyByEndpoint.has(route.endpointKey)) keyByEndpoint.set(route.endpointKey, key);
      }
      keys.push(...keyByEndpoint.values());
      expect(keys).toHaveLength(2);

      await Promise.all(keys.map((key, index) => client.kv.set(key, Buffer.from(`value-${index}`))));
      await expect(Promise.all(keys.map((key) => client.kv.get<Buffer>(key)))).resolves.toEqual([
        Buffer.from("value-0"),
        Buffer.from("value-1")
      ]);
      await expect(client.pipeline(keys.map((key) => ["GET", key]))).resolves.toEqual([
        Buffer.from("value-0"),
        Buffer.from("value-1")
      ]);
      await expect(client.kv.mget<Buffer>(keys)).resolves.toEqual([
        Buffer.from("value-0"),
        Buffer.from("value-1")
      ]);
      await expect(client.kv.exists(keys)).resolves.toBe(2);

      const valueResponses = await Promise.all(keys.map((partitionKey, index) =>
        client.valuePut(Buffer.from(`shared-${index}`), { partitionKey, ttlMs: 10_000 })
      ));
      const refs = valueResponses.map((response) => responseText(responseField(response, "ref")));
      if (!refs.every((ref): ref is string => ref != null)) {
        throw new Error("FLOW.VALUE.PUT did not return string refs");
      }
      await expect(client.valueMGet(refs)).resolves.toEqual([
        Buffer.from("shared-0"),
        Buffer.from("shared-1")
      ]);

      await expect(client.kv.unlink(keys)).resolves.toBe(2);
      keys.length = 0;
    } finally {
      if (keys.length > 0) await client.kv.unlink(keys).catch(() => undefined);
      await client.close();
    }
  }, 30_000);
});

describe.skipIf(tlsUrl == null)("live TLS deployment", () => {
  it("validates the server certificate and uses the native TLS transport", async () => {
    if (tlsUrl == null) throw new Error("FERRICSTORE_TLS_URL is required");
    expect(new URL(tlsUrl).protocol).toBe("ferrics:");
    const client = await FerricStoreClient.fromUrl(tlsUrl, {
      nativeOptions: tlsNativeOptions("FERRICSTORE_TLS_CA_FILE", "FERRICSTORE_TLS_SERVERNAME"),
      reconnect: false
    });
    try {
      await expect(client.ping()).resolves.toBeDefined();
    } finally {
      await client.close();
    }

    const plaintextUrl = process.env.FERRICSTORE_TLS_PLAINTEXT_URL;
    if (plaintextUrl != null) {
      await expectConnectionRejected(plaintextUrl, {}, /TLS|secure|closed|reset/i);
    }
  }, 15_000);
});

describe.skipIf(authUrl == null)("live authenticated deployment", () => {
  it("rejects anonymous and invalid credentials, then authenticates the configured user", async () => {
    if (authUrl == null) throw new Error("FERRICSTORE_AUTH_URL is required");
    const target = new URL(authUrl);
    const username = process.env.FERRICSTORE_AUTH_USERNAME ?? (decodeURIComponent(target.username) || "default");
    const password = process.env.FERRICSTORE_AUTH_PASSWORD ?? decodeURIComponent(target.password);
    if (password.length === 0) {
      throw new Error("set FERRICSTORE_AUTH_PASSWORD or include a password in FERRICSTORE_AUTH_URL");
    }
    target.username = "";
    target.password = "";
    const anonymousUrl = target.toString();

    await expectConnectionRejected(anonymousUrl, {}, /NOAUTH|auth|password|closed/i);
    await expectConnectionRejected(anonymousUrl, {
      password: `${password}-invalid`,
      username
    }, /invalid|auth|password/i);

    const client = await FerricStoreClient.fromUrl(anonymousUrl, {
      nativeOptions: { password, username },
      reconnect: false
    });
    try {
      await expect(client.ping()).resolves.toBeDefined();
    } finally {
      await client.close();
    }
  }, 15_000);
});

function splitUrls(value: string | undefined): string[] {
  return value?.split(",").map((item) => item.trim()).filter((item) => item.length > 0) ?? [];
}

function responseField(value: unknown, name: string): unknown {
  if (value instanceof Map) {
    return value.get(name) ?? value.get(Buffer.from(name));
  }
  if (typeof value === "object" && value != null) {
    return (value as Record<string, unknown>)[name];
  }
  return undefined;
}

function responseText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
  return undefined;
}

function haNativeOptions(): NativeAdapterOptions {
  return {
    ...tlsNativeOptions("FERRICSTORE_HA_TLS_CA_FILE", "FERRICSTORE_HA_TLS_SERVERNAME"),
    password: process.env.FERRICSTORE_HA_PASSWORD,
    username: process.env.FERRICSTORE_HA_USERNAME
  };
}

function tlsNativeOptions(caVariable: string, servernameVariable: string): NativeAdapterOptions {
  const caFile = process.env[caVariable];
  const servername = process.env[servernameVariable];
  if (caFile == null && servername == null) return {};
  return {
    tlsOptions: {
      ...(caFile == null ? {} : { ca: readFileSync(caFile) }),
      ...(servername == null ? {} : { servername })
    }
  };
}

async function expectConnectionRejected(
  url: string,
  nativeOptions: NativeAdapterOptions,
  pattern: RegExp
): Promise<void> {
  let client: FerricStoreClient | undefined;
  let rejection: unknown;
  try {
    client = await FerricStoreClient.fromUrl(url, { nativeOptions, reconnect: false });
    await client.ping();
  } catch (error) {
    rejection = error;
  } finally {
    await client?.close();
  }
  expect(rejection).toBeInstanceOf(Error);
  expect((rejection as Error).message).toMatch(pattern);
}
