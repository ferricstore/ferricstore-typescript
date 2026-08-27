import { describe, expect, it } from "vitest";
import {
  buildFlowFailureQuery,
  buildFlowLineageQuery,
  buildFlowListQuery,
  buildFlowSearchQuery,
  buildFlowStuckQuery,
  buildFlowTerminalQuery
} from "../src/flow-query-builder.js";

describe("Flow query convenience ordering", () => {
  it("defaults newest-first conveniences to the server's native DESC indexes", () => {
    const partitionKey = "tenant-a";
    const queries = [
      buildFlowListQuery("invoice", { partitionKey }),
      buildFlowSearchQuery("invoice", { attributes: { tenant: "acme" }, partitionKey }),
      buildFlowTerminalQuery("invoice", { partitionKey }),
      buildFlowFailureQuery("invoice", { partitionKey }),
      buildFlowLineageQuery("parent_flow_id", "parent", { partitionKey }),
      buildFlowLineageQuery("root_flow_id", "root", { partitionKey }),
      buildFlowLineageQuery("correlation_id", "correlation", { partitionKey })
    ];

    for (const { query } of queries) {
      expect(query).toContain("ORDER BY updated_at_ms DESC");
    }
  });

  it("preserves explicit oldest-first ordering and ascending stuck deadlines", () => {
    expect(buildFlowListQuery("invoice", {
      partitionKey: "tenant-a",
      rev: false
    }).query).toContain("ORDER BY updated_at_ms ASC");
    expect(buildFlowListQuery("invoice", {
      partitionKey: "tenant-a",
      rev: true
    }).query).toContain("ORDER BY updated_at_ms DESC");
    expect(buildFlowStuckQuery("invoice", {
      nowMs: 1_000,
      partitionKey: "tenant-a"
    }).query).toContain("ORDER BY lease_deadline_ms ASC");
  });
});
