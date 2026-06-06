import type { RedisCommandExecutor } from "../src/adapters.js";
import type { CommandArgument } from "../src/internal.js";

export class FakeExecutor implements RedisCommandExecutor {
  readonly calls: CommandArgument[][] = [];
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
    return await Promise.all(commands.map((command) => this.executeCommand(...command)));
  }
}
