import { FerricStoreError } from "./errors.js";
import type { RoutingRoute, RoutingTopology } from "./routing-topology.js";
import { sameEndpoint } from "./topology-utilities.js";

export const MAX_TOPOLOGY_PLANNING_ATTEMPTS = 8;

export class StaleTopologyRouteError extends FerricStoreError {
  constructor() {
    super("FerricStore topology route became stale before dispatch");
  }
}

export function topologyRouteIsCurrent(
  topology: RoutingTopology,
  route: RoutingRoute
): boolean {
  const current = route.slot == null ? undefined : topology.slots[route.slot];
  return current?.endpointKey === route.endpointKey &&
    current.laneId === route.laneId &&
    current.shard === route.shard &&
    current.leaderNode === route.leaderNode &&
    sameEndpoint(current.endpoint, route.endpoint);
}
