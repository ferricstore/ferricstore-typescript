import { describe, expect, it } from "vitest";
import { JsonCodec, WorkflowClient, complete, transition } from "../../src/index.js";
import { processImage } from "../../examples/fanout.js";
import { syncTenant, type TenantSyncActivities } from "../../examples/tenant-sync.js";
import { integrationClient, suffix } from "./live-support.js";

// Servers before 0.11.19 never make a parent claimable again after its child
// group resolves, so fan-in cannot finish on them.
const skipChildJoinResume = process.env.FERRICSTORE_SKIP_CHILD_JOIN_RESUME === "true";

function activities(changes: Record<string, number>, findingsPerChange: number): TenantSyncActivities & {
  calls: string[];
} {
  const calls: string[] = [];
  return {
    calls,
    countRiskyActions: async (_tenantId, findings) => {
      calls.push("countRiskyActions");
      return findings.length * 2;
    },
    deriveFindings: async (_tenantId, changed) => {
      calls.push("deriveFindings");
      return changed.flatMap((result) =>
        Array.from({ length: findingsPerChange }, (_unused, index) => `${result.integration}:${index}`)
      );
    },
    syncIntegration: async (_tenantId, integration) => {
      calls.push(`sync:${integration}`);
      return { changedResources: changes[integration] ?? 0, integration };
    }
  };
}

describe("documented examples against a live server", () => {
  it.skipIf(skipChildJoinResume)("fans out resize children and fans their results back in", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    try {
      const imageId = `ts-example-image-${suffix()}`;
      const record = await processImage(flow, imageId);

      expect(record.state).toBe("completed");
      expect(record.result).toEqual({
        urls: [`s3://images/${imageId}/large.jpg`, `s3://images/${imageId}/small.jpg`]
      });
      const resizes = await flow.childResults(imageId, { groupId: "resizes", partitionKey: record.partitionKey });
      expect(resizes).toMatchObject({
        resolved: "success",
        summary: { cancelled: 0, completed: 2, failed: 0, total: 2 },
        wait: "all"
      });
    } finally {
      await flow.close();
    }
  });

  it("fires a one-hour timer without waiting by claiming with a later nowMs", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    try {
      const runId = suffix();
      const trial = new WorkflowClient(flow).workflow({ initialState: "started", type: `ts-example-trial-${runId}` });
      const startedAt = Date.now();
      trial.state("started", () => transition("reminder_due", { runAtMs: startedAt + 3_600_000 }));
      trial.state("reminder_due", () => complete({ result: { reminded: true } }));
      await trial.start(`ts-example-trial-${runId}`, { payload: {} });

      await expect(trial.worker({ states: ["started"] }).runOnce()).resolves.toMatchObject({ applied: 1 });
      await expect(trial.worker({ states: ["reminder_due"] }).runOnce()).resolves.toMatchObject({ claimed: 0 });
      await expect(trial.worker({ nowMs: startedAt + 3_600_001, states: ["reminder_due"] }).runOnce())
        .resolves.toMatchObject({ applied: 1 });
      await expect(flow.get(`ts-example-trial-${runId}`, { full: true })).resolves.toMatchObject({
        result: { reminded: true },
        state: "completed"
      });
    } finally {
      await flow.close();
    }
  });

  it.skipIf(skipChildJoinResume)("runs the tenant sync through every branch of the ported Temporal workflow", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    try {
      const risky = activities({ github: 3, okta: 0 }, 2);
      await expect(syncTenant(flow, `ts-example-tenant-${suffix()}`, {
        integrations: ["github", "okta"],
        tenantId: "acme"
      }, risky)).resolves.toMatchObject({
        result: { failed: [], findings: 2, riskyActions: 4, tenantId: "acme" },
        state: "completed"
      });
      expect([...risky.calls].sort()).toEqual(["countRiskyActions", "deriveFindings", "sync:github", "sync:okta"]);

      const unchanged = activities({ github: 0, okta: 0 }, 2);
      await expect(syncTenant(flow, `ts-example-tenant-${suffix()}`, {
        integrations: ["github", "okta"],
        tenantId: "acme"
      }, unchanged)).resolves.toMatchObject({
        result: { failed: [], skipped: true, tenantId: "acme" },
        state: "completed"
      });
      expect(unchanged.calls).not.toContain("deriveFindings");

      const noFindings = activities({ github: 1 }, 0);
      await expect(syncTenant(flow, `ts-example-tenant-${suffix()}`, {
        integrations: ["github"],
        tenantId: "acme"
      }, noFindings)).resolves.toMatchObject({
        result: { failed: [], findings: 0, riskyActions: 0, tenantId: "acme" },
        state: "completed"
      });
      expect(noFindings.calls).not.toContain("countRiskyActions");
    } finally {
      await flow.close();
    }
  });
});
