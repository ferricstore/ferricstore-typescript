import type { CommandExecutor } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";

export class FakeExecutor implements CommandExecutor {
  readonly calls: CommandArgument[][] = [];
  readonly pipelineCalls: CommandArgument[][][] = [];
  private readonly responses: unknown[];
  private policyGeneration = 0;

  constructor(responses: unknown[] = []) {
    this.responses = responses;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    this.calls.push([...args]);
    if (this.responses.length > 0) {
      const response = this.responses.shift();
      if (response instanceof Error) {
        throw response;
      }
      return response;
    }
    if (args[0] === "FLOW.POLICY.SET") {
      this.policyGeneration += 1;
      const type = args[1];
      return fakeFlowPolicySnapshot(
        typeof type === "string" ? type : Buffer.isBuffer(type) ? type.toString("utf8") : "unknown",
        this.policyGeneration
      );
    }
    return Buffer.from("OK");
  }

  async executePipeline(commands: readonly (readonly CommandArgument[])[]): Promise<unknown[]> {
    this.pipelineCalls.push(commands.map((command) => [...command]));
    return await Promise.all(commands.map((command) => this.executeCommandArgs(command)));
  }

  async executeFusedPipeline(commands: readonly (readonly CommandArgument[])[]): Promise<unknown[]> {
    return await this.executePipeline(commands);
  }
}

export function fakeFlowPolicySnapshot(type = "order", generation = 1): Record<string, unknown> {
  return {
    generation,
    indexed_attributes: [],
    indexed_state_meta: null,
    max_active_ms: null,
    retention: { history_max_events: 100_000, ttl_ms: 604_800_000 },
    retry: {
      backoff: { base_ms: 1_000, jitter_pct: 20, kind: "exponential", max_ms: 30_000 },
      exhausted_to: "failed",
      max_retries: 3
    },
    states: {},
    type,
    version: null
  };
}
