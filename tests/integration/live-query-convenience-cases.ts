import { expect, it } from "vitest";
import {
  JsonCodec,
  type FlowRecord
} from "../../src/index.js";
import { buildFlowListQuery } from "../../src/flow-query-builder.js";
import { claimOne, eventually, integrationClient, suffix } from "./live-support.js";

export function registerFlowQueryConvenienceIntegrationTests(): void {
  it("executes every typed collection convenience through live FQL", async () => {
    const client = await integrationClient({ codec: new JsonCodec() });
    const run = suffix();
    const partitionKey = `ts-sdk:query-convenience:${run}:partition`;
    const type = `ts-sdk-query-convenience-${run}`;
    const correlationId = `ts-sdk-query-correlation-${run}`;
    const now = Date.now();
    const ids = {
      child: `ts-sdk:query-convenience:${run}:child`,
      completed: `ts-sdk:query-convenience:${run}:completed`,
      failed: `ts-sdk:query-convenience:${run}:failed`,
      listed: `ts-sdk:query-convenience:${run}:listed`,
      parent: `ts-sdk:query-convenience:${run}:parent`,
      root: `ts-sdk:query-convenience:${run}:root`,
      searched: `ts-sdk:query-convenience:${run}:searched`,
      stuck: `ts-sdk:query-convenience:${run}:stuck`
    } as const;
    const create = (id: string, state: string, extra: {
      readonly attributes?: Record<string, string>;
      readonly correlationId?: string;
      readonly parentFlowId?: string;
      readonly rootFlowId?: string;
    } = {}) => client.create(id, {
      ...extra,
      idempotent: true,
      nowMs: now,
      partitionKey,
      runAtMs: now,
      state,
      type
    });

    try {
      await client.installPolicy(type, { indexedAttributes: ["tenant"] });
      await create(ids.root, "lineage-root");
      await Promise.all([
        create(ids.completed, "complete-ready"),
        create(ids.failed, "fail-ready"),
        create(ids.listed, "listed"),
        create(ids.parent, "lineage-active", {
          correlationId,
          parentFlowId: ids.root,
          rootFlowId: ids.root
        }),
        create(ids.searched, "searchable", {
          attributes: { tenant: "acme" }
        }),
        create(ids.stuck, "stuck-ready")
      ]);
      await create(ids.child, "lineage-active", {
        correlationId,
        parentFlowId: ids.parent,
        rootFlowId: ids.root
      });

      const completed = await claimOne(client, type, "complete-ready", partitionKey, {
        nowMs: now + 1,
        worker: "ts-sdk-query-complete"
      });
      await client.complete(completed.id, {
        fencingToken: completed.fencingToken,
        leaseToken: completed.leaseToken,
        nowMs: now + 2,
        partitionKey
      });

      const failed = await claimOne(client, type, "fail-ready", partitionKey, {
        nowMs: now + 3,
        worker: "ts-sdk-query-fail"
      });
      await client.fail(failed.id, {
        error: { reason: "integration fixture" },
        fencingToken: failed.fencingToken,
        leaseToken: failed.leaseToken,
        nowMs: now + 4,
        partitionKey
      });

      await claimOne(client, type, "stuck-ready", partitionKey, {
        leaseMs: 10,
        nowMs: now + 5,
        worker: "ts-sdk-query-stuck"
      });

      const listed = await projected(
        () => client.list(type, { count: 20, partitionKey, state: "listed" }),
        [ids.listed],
        "list"
      );
      expect(recordIds(listed)).toEqual([ids.listed]);
      expect(listed[0]).toMatchObject({
        id: ids.listed,
        partitionKey,
        state: "listed",
        type
      });
      const defaultList = buildFlowListQuery(type, { count: 20, partitionKey, state: "listed" });
      const defaultPlan = await client.explain(defaultList.query, defaultList.params);
      expect(defaultPlan.plan).toMatchObject({ order: "native" });

      const searched = await projected(
        () => client.search(type, {
          attributes: { tenant: "acme" },
          count: 20,
          partitionKey,
          state: "searchable"
        }),
        [ids.searched],
        "search"
      );
      expect(recordIds(searched)).toEqual([ids.searched]);

      const terminals = await projected(
        () => client.terminals(type, { count: 20, partitionKey }),
        [ids.completed, ids.failed],
        "terminals"
      );
      expect(recordIds(terminals).sort()).toEqual([ids.completed, ids.failed].sort());

      const failures = await projected(
        () => client.failures(type, { count: 20, partitionKey }),
        [ids.failed],
        "failures"
      );
      expect(recordIds(failures)).toEqual([ids.failed]);

      const stuck = await projected(
        () => client.stuck(type, {
          count: 20,
          nowMs: now + 1_000,
          olderThanMs: 1,
          partitionKey
        }),
        [ids.stuck],
        "stuck"
      );
      expect(recordIds(stuck)).toEqual([ids.stuck]);

      const children = await projected(
        () => client.byParent(ids.parent, { count: 20, partitionKey }),
        [ids.child],
        "byParent"
      );
      expect(recordIds(children)).toEqual([ids.child]);

      const rooted = await projected(
        () => client.byRoot(ids.root, { count: 20, partitionKey }),
        [ids.parent, ids.child],
        "byRoot"
      );
      expect(recordIds(rooted)).toEqual(expect.arrayContaining([ids.parent, ids.child]));
      expect(recordIds(rooted)).not.toContain(ids.listed);

      const correlated = await projected(
        () => client.byCorrelation(correlationId, { count: 20, partitionKey }),
        [ids.parent, ids.child],
        "byCorrelation"
      );
      expect(recordIds(correlated).sort()).toEqual([ids.child, ids.parent].sort());
    } finally {
      await client.close();
    }
  }, 30_000);
}

async function projected(
  operation: () => Promise<FlowRecord[]>,
  expectedIds: readonly string[],
  name: string
): Promise<FlowRecord[]> {
  return await eventually(
    operation,
    (records) => expectedIds.every((id) => records.some((record) => record.id === id)),
    `FLOW.QUERY ${name} projection did not become ready`
  );
}

function recordIds(records: readonly FlowRecord[]): string[] {
  return records.map((record) => record.id);
}
