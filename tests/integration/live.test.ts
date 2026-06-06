import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { FlowClient, JsonCodec } from "../../src/index.js";

const runIntegration = process.env.FERRICSTORE_INTEGRATION === "1";

describe.runIf(runIntegration)("FerricStore integration", () => {
  it("uses KV helpers and a full Flow claim/complete cycle", async () => {
    const flow = await FlowClient.fromUrl(process.env.FERRICSTORE_URL ?? "redis://127.0.0.1:6379/0", {
      codec: new JsonCodec()
    });

    const suffix = randomUUID();
    const key = `ts-sdk:kv:${suffix}`;
    const id = `ts-sdk:flow:${suffix}`;
    const type = "ts-sdk-integration";

    try {
      await flow.kv.set(key, { ok: true }, { px: 60_000 });
      await expect(flow.kv.get(key)).resolves.toEqual({ ok: true });

      await flow.create(id, {
        idempotent: true,
        partitionKey: id,
        payload: { hello: "world" },
        state: "queued",
        type
      });

      const jobs = await flow.claimDue(type, {
        leaseMs: 30_000,
        limit: 1,
        partitionKey: id,
        payload: true,
        state: "queued",
        worker: "ts-sdk-integration-worker"
      });

      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      if (job == null) {
        throw new Error("expected an integration job");
      }
      expect(job).toMatchObject({
        id,
        partitionKey: id,
        payload: { hello: "world" },
        state: "running",
        type
      });

      await flow.complete(job.id, {
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        result: { ok: true }
      });

      const record = await flow.get(id, { partitionKey: id });
      expect(record?.state).toBe("completed");
    } finally {
      await flow.kv.del(key);
      await flow.close();
    }
  });
});
