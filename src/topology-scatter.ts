import { commandView, splitFlowValueMGetArguments } from "./command-grammar.js";
import { FerricStoreError } from "./errors.js";
import { integer, type CommandArgument } from "./internal.js";
import type { RoutingRoute } from "./routing-topology.js";
import {
  commandName,
  routedCommandArgs,
  routedKeyGroups
} from "./topology-routing.js";
import { mapSettledWithConcurrency } from "./topology-utilities.js";

type ScatterResult =
  | { readonly handled: false }
  | { readonly handled: true; readonly value: unknown };

interface TopologyScatterDependencies {
  readonly concurrency: number;
  readonly executeOnRoute: (
    args: readonly CommandArgument[],
    route: RoutingRoute
  ) => Promise<unknown>;
  readonly route: (key: string | Buffer) => RoutingRoute;
}

/** Decompose only commands whose shard results can be combined exactly. */
export class TopologyScatterExecutor {
  constructor(private readonly dependencies: TopologyScatterDependencies) {}

  async execute(args: readonly CommandArgument[]): Promise<ScatterResult> {
    const name = commandName(args);
    if (name === "MGET") {
      const value = await this.scatterArray(name, args.slice(1));
      return value == null ? { handled: false } : { handled: true, value };
    }
    if (name === "DEL" || name === "EXISTS" || name === "UNLINK") {
      const value = await this.scatterCount(name, args.slice(1));
      return value == null ? { handled: false } : { handled: true, value };
    }
    if (name === "FLOW.VALUE.MGET") {
      const { refs, suffix } = splitFlowValueMGetArguments(
        args,
        commandView(args).argumentStart
      );
      const value = await this.scatterArray(name, refs, suffix);
      return value == null ? { handled: false } : { handled: true, value };
    }
    return { handled: false };
  }

  private async scatterArray(
    name: string,
    keys: readonly CommandArgument[],
    suffix: readonly CommandArgument[] = []
  ): Promise<unknown[] | undefined> {
    const groups = this.keyGroups(keys);
    if (groups == null) return undefined;
    const output = new Array<unknown>(keys.length);
    const settled = await mapSettledWithConcurrency(
      groups,
      this.dependencies.concurrency,
      async (group) => {
        const response = await this.dependencies.executeOnRoute(
          routedCommandArgs(name, group.entries, suffix),
          group.route
        );
        if (!Array.isArray(response) || response.length !== group.entries.length) {
          throw new FerricStoreError(
            `${name} shard response length did not match request length`,
            { raw: response }
          );
        }
        group.entries.forEach((entry, index) => {
          if (!Object.hasOwn(response, index)) {
            throw new FerricStoreError(`${name} shard response item ${index} is missing`, {
              raw: response
            });
          }
          output[entry.index] = response[index];
        });
      }
    );
    for (const result of settled) {
      if (result.status === "rejected") throw result.reason;
    }
    return output;
  }

  private async scatterCount(
    name: string,
    keys: readonly CommandArgument[]
  ): Promise<number | undefined> {
    const groups = this.keyGroups(keys);
    if (groups == null) return undefined;
    const settled = await mapSettledWithConcurrency(
      groups,
      this.dependencies.concurrency,
      async (group) => {
        const response = await this.dependencies.executeOnRoute(
          routedCommandArgs(name, group.entries),
          group.route
        );
        const count = integer(response);
        if (count < 0 || count > group.entries.length) {
          throw new FerricStoreError(`${name} shard returned an invalid count`, { raw: response });
        }
        return count;
      }
    );
    let total = 0;
    for (const result of settled) {
      if (result.status === "rejected") throw result.reason;
      total += result.value;
    }
    return total;
  }

  private keyGroups(keys: readonly CommandArgument[]) {
    return routedKeyGroups(keys, this.dependencies.route);
  }
}
