import { describe, expect, it } from "vitest";
import {
  FlowProjection,
  projectFlowQuery
} from "../src/index.js";
import { validateFlowQueryText } from "../src/flow-query-request.js";

describe("Flow query projection builder", () => {
  it("builds source-aware run and event projections with escaped metadata names", () => {
    expect(projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "record",
      FlowProjection.run.id,
      FlowProjection.run.state,
      FlowProjection.run.attribute("customer.tier"),
      FlowProjection.run.stateMeta("review's", "risk tier")
    )).toBe(
      "FROM runs WHERE run_id = @id RETURN RECORD " +
      "(run_id, state, attribute['customer.tier'], state_meta['review''s']['risk tier'])"
    );

    expect(projectFlowQuery(
      " FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20; ",
      "records",
      FlowProjection.event.id,
      FlowProjection.event.field("worker's.pool")
    )).toBe(
      "FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20 " +
      "RETURN RECORDS (event_id, fields['worker''s.pool'])"
    );
  });

  it("rejects mixed sources, duplicates, forged fields, and an existing return clause", () => {
    expect(() => projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "record",
      FlowProjection.event.id
    )).toThrow("must belong to runs");
    expect(() => projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "record",
      FlowProjection.run.state,
      FlowProjection.run.state
    )).toThrow("duplicate");
    expect(() => projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "record",
      { source: "runs", selector: "state" } as never
    )).toThrow("only FlowProjection fields");
    expect(() => projectFlowQuery(
      "FROM runs WHERE type = 'RETURN' RETURN RECORD",
      "record",
      FlowProjection.run.id
    )).toThrow("already contains");
    for (const query of [
      "FROM runs WHERE run_id = @id;;",
      "FROM runs WHERE run_id = @id; ;"
    ]) {
      expect(() => projectFlowQuery(
        query,
        "record",
        FlowProjection.run.id
      )).toThrow("at most one trailing semicolon");
    }
    expect(() => projectFlowQuery(
      "\u00a0FROM runs WHERE run_id = @id",
      "record",
      FlowProjection.run.id
    )).toThrow("must start with FROM");
    for (const query of [
      "FROM runs\u00e9 WHERE run_id = @id",
      "FROM run\u017f WHERE run_id = @id"
    ]) {
      expect(() => projectFlowQuery(
        query,
        "record",
        FlowProjection.run.id
      )).toThrow("must start with FROM");
    }
  });

  it("bounds field counts, dynamic names, and the final query bytes", () => {
    expect(() => projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "records"
    )).toThrow("1..32");
    expect(() => projectFlowQuery(
      "FROM runs WHERE run_id = @id",
      "records",
      ...Array.from({ length: 33 }, (_, index) =>
        FlowProjection.run.attribute(`field_${index}`)
      )
    )).toThrow("1..32");
    expect(() => FlowProjection.run.attribute("__private")).toThrow();
    expect(() => FlowProjection.event.field("x".repeat(65))).toThrow();
    expect(() => FlowProjection.event.field("\ud800")).toThrow("valid UTF-8");
    expect(() => validateFlowQueryText("FROM runs WHERE run_id = '\ud800'")).toThrow(
      "valid UTF-8"
    );
    expect(() => projectFlowQuery(
      `FROM runs WHERE type = '${"x".repeat(16_350)}'`,
      "record",
      FlowProjection.run.id
    )).toThrow("exceeds");
  });
});
