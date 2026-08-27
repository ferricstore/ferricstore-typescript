import { afterEach, describe, expect, it, vi } from "vitest";
import { withFlowQueryDeadline } from "../src/flow-query-deadline.js";
import { buildProtocolCommand, pipelineCommand } from "../src/protocol.js";

const QUERY = "FROM runs WHERE run_id = @run RETURN RECORDS";

describe("FLOW.QUERY native deadlines", () => {
  afterEach(() => vi.restoreAllMocks());

  it("does not read the clock or clone commands when no query is present", () => {
    const direct = buildProtocolCommand(["GET", "key"]);
    const pipeline = pipelineCommand([["GET", "one"], ["GET", "two"]]);
    const clock = vi.spyOn(Date, "now");

    expect(withFlowQueryDeadline(direct, 250)).toBe(direct);
    expect(withFlowQueryDeadline(pipeline, 250)).toBe(pipeline);
    expect(clock).not.toHaveBeenCalled();
  });

  it("stamps direct and pipelined query bodies without mutating prepared commands", () => {
    const direct = buildProtocolCommand(["FLOW.QUERY", "FQL1", QUERY, "run", "run-1"]);
    const pipeline = pipelineCommand([
      ["FLOW.QUERY", "FQL1", QUERY, "run", "run-1"],
      ["FLOW.QUERY", "FQL1", QUERY, "run", "run-2"],
    ]);

    const stampedDirect = withFlowQueryDeadline(direct, 250, 1_000);
    const stampedPipeline = withFlowQueryDeadline(pipeline, 250, 1_000);

    expect(stampedDirect.payload).toMatchObject({ deadline_ms: 1_250 });
    expect(stampedPipeline.payload).toMatchObject({
      commands: [
        { body: { deadline_ms: 1_250 } },
        { body: { deadline_ms: 1_250 } },
      ],
    });
    expect(direct.payload).not.toHaveProperty("deadline_ms");
    expect(pipeline.payload).not.toHaveProperty("commands.0.body.deadline_ms");
  });
});
