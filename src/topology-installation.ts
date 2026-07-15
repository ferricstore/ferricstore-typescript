import type { RoutingTopology } from "./routing-topology.js";
import { sameEndpoint } from "./topology-utilities.js";

/** Treat the core route epoch as an opaque fingerprint and avoid churning an identical view. */
export function topologyForInstallation(
  current: RoutingTopology,
  candidate: RoutingTopology
): RoutingTopology {
  if (current.shardCount === 0) return candidate;
  return candidate.routeEpoch === current.routeEpoch && sameTopologyView(current, candidate)
    ? current
    : candidate;
}

function sameTopologyView(left: RoutingTopology, right: RoutingTopology): boolean {
  if (left.shardCount !== right.shardCount || left.endpoints.size !== right.endpoints.size) {
    return false;
  }
  for (const [key, endpoint] of left.endpoints) {
    const other = right.endpoints.get(key);
    if (other == null || !sameEndpoint(endpoint, other)) return false;
  }
  for (let slot = 0; slot < left.slots.length; slot += 1) {
    const a = left.slots[slot];
    const b = right.slots[slot];
    if (a == null) return false;
    if (b == null) return false;
    if (
      a.shard !== b.shard || a.laneId !== b.laneId ||
      a.endpointKey !== b.endpointKey || a.leaderNode !== b.leaderNode ||
      !sameEndpoint(a.endpoint, b.endpoint)
    ) return false;
  }
  return left.slots.length === right.slots.length;
}
