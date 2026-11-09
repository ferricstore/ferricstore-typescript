// A Temporal-style orchestration ported to FerricFlow. The Temporal workflow:
//
//   export async function tenantSync(tenantId: string, integrations: string[]) {
//     const synced = await Promise.all(integrations.map((integration) =>
//       executeChild(integrationSync, { args: [tenantId, integration] })));
//     const changed = synced.filter((result) => result.changedResources > 0);
//     if (changed.length === 0) return { tenantId, skipped: true };
//     const findings = await deriveFindings(tenantId, changed);
//     const riskyActions = findings.length > 0 ? await countRiskyActions(tenantId, findings) : 0;
//     return { tenantId, findings: findings.length, riskyActions };
//   }
//
// Each `await` becomes a named state. A local variable that a later state
// needs becomes a named value on the transition and is read back with
// `ctx.value()`. Nothing is replayed: a worker runs one state's code, the
// outcome is committed, and any worker can run the next state.
import { fileURLToPath } from "node:url";
import {
  FerricStoreClient,
  JsonCodec,
  WorkflowClient,
  complete,
  transition,
  waitForChildren,
  type FlowRecord
} from "../src/index.js";
import { runUntilTerminal } from "./drive-workers.js";

export interface TenantSyncInput {
  tenantId: string;
  integrations: string[];
}

export interface IntegrationSyncResult {
  integration: string;
  changedResources: number;
}

/** The activities: ordinary async functions, called by whichever worker claims the state. */
export interface TenantSyncActivities {
  syncIntegration(tenantId: string, integration: string): Promise<IntegrationSyncResult>;
  deriveFindings(tenantId: string, changed: IntegrationSyncResult[]): Promise<string[]>;
  countRiskyActions(tenantId: string, findings: string[]): Promise<number>;
}

export function defineTenantSync(workflows: WorkflowClient, activities: TenantSyncActivities) {
  const tenantSync = workflows.workflow({ initialState: "start", type: "tenant-sync", worker: "tenant-sync-1" });
  const integrationSync = workflows.workflow({
    initialState: "queued",
    type: "integration-sync",
    worker: "integration-sync-1"
  });

  // Promise.all over executeChild: one child Flow per integration. With
  // onChildFailed: "ignore", a failed integration does not fail the tenant
  // sync; the group resolves to "synced" once every child is terminal.
  tenantSync.state("start", (ctx) => {
    const { tenantId, integrations } = ctx.payload as TenantSyncInput;
    return waitForChildren(
      integrations.map((integration) => ({
        id: `${ctx.id}:${integration}`,
        payload: { integration, tenantId },
        type: "integration-sync"
      })),
      {
        failure: "sync_failed",
        groupId: "integrations",
        onChildFailed: "ignore",
        success: "synced",
        waitState: "syncing"
      }
    );
  });

  integrationSync.state("queued", async (ctx) => {
    const { integration, tenantId } = ctx.payload as { integration: string; tenantId: string };
    return complete({ result: await activities.syncIntegration(tenantId, integration) });
  });

  // The code after Promise.all: read the children's results and branch.
  tenantSync.state("synced", async (ctx) => {
    const { tenantId } = ctx.payload as TenantSyncInput;
    const integrations = await ctx.flow.childResults<IntegrationSyncResult>("integrations");
    const failed = integrations.children.filter((child) => child.status !== "completed").map((child) => child.id);
    const changed = integrations.children
      .flatMap((child) => (child.status === "completed" && child.result != null ? [child.result] : []))
      .filter((result) => result.changedResources > 0);
    if (changed.length === 0) return complete({ result: { failed, skipped: true, tenantId } });
    return transition("deriving", { values: { changed, failed } });
  });

  tenantSync.state("deriving", async (ctx) => {
    const { tenantId } = ctx.payload as TenantSyncInput;
    const changed = (await ctx.value("changed")) as IntegrationSyncResult[];
    const findings = await activities.deriveFindings(tenantId, changed);
    if (findings.length === 0) {
      return complete({ result: { failed: await ctx.value("failed"), findings: 0, riskyActions: 0, tenantId } });
    }
    return transition("counting_risky", { values: { findings } });
  });

  tenantSync.state("counting_risky", async (ctx) => {
    const { tenantId } = ctx.payload as TenantSyncInput;
    const findings = (await ctx.value("findings")) as string[];
    const riskyActions = await activities.countRiskyActions(tenantId, findings);
    return complete({
      result: { failed: await ctx.value("failed"), findings: findings.length, riskyActions, tenantId }
    });
  });

  return { integrationSync, tenantSync };
}

export async function syncTenant(
  flow: FerricStoreClient,
  id: string,
  input: TenantSyncInput,
  activities: TenantSyncActivities
): Promise<FlowRecord> {
  const { integrationSync, tenantSync } = defineTenantSync(new WorkflowClient(flow), activities);
  await tenantSync.start(id, { idempotent: true, payload: input });
  return await runUntilTerminal(flow, id, [
    tenantSync.worker({ states: ["start", "synced", "deriving", "counting_risky"] }),
    integrationSync.worker({ states: ["queued"] })
  ]);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const flow = await FerricStoreClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
    codec: new JsonCodec()
  });
  const record = await syncTenant(flow, "tenant-sync:acme", { integrations: ["github", "okta"], tenantId: "acme" }, {
    countRiskyActions: async (_tenantId, findings) => findings.length,
    deriveFindings: async (_tenantId, changed) => changed.map((result) => `${result.integration}:review`),
    syncIntegration: async (_tenantId, integration) => ({ changedResources: integration.length, integration })
  });
  console.log(record.state, record.result);
  await flow.close();
}
