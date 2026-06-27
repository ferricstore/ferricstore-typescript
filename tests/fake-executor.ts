import type { CommandExecutor } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";

export class FakeExecutor implements CommandExecutor {
  readonly calls: CommandArgument[][] = [];
  readonly pipelineCalls: CommandArgument[][][] = [];
  private readonly responses: unknown[];

  constructor(responses: unknown[] = []) {
    this.responses = responses;
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    this.calls.push(args);
    if (this.responses.length > 0) {
      const response = this.responses.shift();
      if (response instanceof Error) {
        throw response;
      }
      return response;
    }
    return Buffer.from("OK");
  }

  async executePipeline(commands: readonly (readonly CommandArgument[])[]): Promise<unknown[]> {
    this.pipelineCalls.push(commands.map((command) => [...command]));
    return await Promise.all(commands.map((command) => this.executeCommand(...command)));
  }
}
