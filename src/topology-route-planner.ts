import type { CommandArgument } from "./internal.js";
import { buildProtocolCommand, type ProtocolCommand } from "./protocol.js";
import type { RoutingRoute } from "./routing-topology.js";
import type { RoutedCommandData } from "./topology-execution-types.js";
import {
  commandName,
  flowRoutingData,
  routingKeyFromArgs,
  routingKeyFromProtocolPayload
} from "./topology-routing.js";

/** Plan one command against the caller's current routing snapshot. */
export function topologyRouteData(
  args: readonly CommandArgument[],
  route: (key: string | Buffer) => RoutingRoute
): RoutedCommandData | undefined {
  if (args.length === 0) return undefined;

  const name = commandName(args);
  if (name?.startsWith("FLOW.") === true) {
    const routed = flowRoutingData(name, args);
    return routed == null
      ? undefined
      : {
          ...(routed.command == null ? {} : { command: routed.command }),
          route: route(routed.key)
        };
  }

  const keyFromArgs = routingKeyFromArgs(name, args);
  if (keyFromArgs.handled) {
    return keyFromArgs.key == null ? undefined : { route: route(keyFromArgs.key) };
  }

  let command: ProtocolCommand;
  try {
    command = buildProtocolCommand(args);
  } catch {
    return undefined;
  }
  const key = routingKeyFromProtocolPayload(name, command);
  return key == null ? undefined : { command, route: route(key) };
}
