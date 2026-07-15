import { FerricStoreError } from "./errors.js";
import { readonlyMapView } from "./readonly-map-view.js";
import {
  endpointFromRange,
  endpointKeyFor,
  getField,
  numberOrUndefined,
  routingSlotForKey,
  sameEndpoint,
  textOrUndefined,
  type RoutingEndpoint
} from "./topology-utilities.js";

const ROUTE_SLOT_COUNT = 1024;

export interface RoutingRoute {
  readonly shard: number;
  readonly laneId: number;
  readonly endpointKey: string;
  readonly endpoint: RoutingEndpoint;
  readonly leaderNode: string;
  readonly slot?: number;
}

export class RoutingTopology {
  readonly routeEpoch: number;
  readonly shardCount: number;
  readonly slots: readonly (RoutingRoute | undefined)[];
  readonly endpoints: ReadonlyMap<string, RoutingEndpoint>;

  private constructor(
    routeEpoch: number,
    shardCount: number,
    slots: readonly (RoutingRoute | undefined)[],
    endpoints: ReadonlyMap<string, RoutingEndpoint>
  ) {
    this.routeEpoch = routeEpoch;
    this.shardCount = shardCount;
    this.slots = slots;
    this.endpoints = endpoints;
    Object.freeze(this);
  }

  static empty(): RoutingTopology {
    return new RoutingTopology(
      0,
      0,
      Object.freeze(new Array<RoutingRoute | undefined>(ROUTE_SLOT_COUNT)),
      readonlyMapView(new Map<string, RoutingEndpoint>())
    );
  }

  static build(payload: unknown): RoutingTopology {
    const ranges = getField(payload, "ranges");
    if (!Array.isArray(ranges)) {
      throw new FerricStoreError("invalid SHARDS topology payload", { raw: payload });
    }

    const declaredSlotsValue = getField(payload, "slots");
    const declaredSlots = numberOrUndefined(declaredSlotsValue);
    const routeEpochValue = getField(payload, "route_epoch");
    const routeEpoch = numberOrUndefined(routeEpochValue);
    const shardCountValue = getField(payload, "shard_count");
    const declaredShardCount = numberOrUndefined(shardCountValue);
    if (
      (declaredSlotsValue != null && declaredSlots !== ROUTE_SLOT_COUNT) ||
      (routeEpochValue != null && (routeEpoch == null || routeEpoch < 0)) ||
      (shardCountValue != null && (declaredShardCount == null || declaredShardCount <= 0))
    ) {
      throw new FerricStoreError("invalid SHARDS topology metadata", { raw: payload });
    }

    const slots: (RoutingRoute | undefined)[] = Array.from({ length: ROUTE_SLOT_COUNT });
    const endpoints = new Map<string, RoutingEndpoint>();
    const routesByShard = new Map<number, RoutingRoute>();
    const shards = new Set<number>();

    for (const item of ranges) {
      if (textOrUndefined(getField(item, "hint")) === "leader_unknown") {
        throw new FerricStoreError("SHARDS range has no leader", { raw: item });
      }

      const first = numberOrUndefined(getField(item, "first_slot"));
      const last = numberOrUndefined(getField(item, "last_slot"));
      const shard = numberOrUndefined(getField(item, "shard"));
      const laneId = numberOrUndefined(getField(item, "lane_id"));
      const endpoint = Object.freeze(endpointFromRange(item));
      if (
        first == null || last == null || shard == null || laneId == null ||
        first < 0 || last < first || last >= ROUTE_SLOT_COUNT ||
        shard < 0 || shard > 0xffff_ffff || laneId <= 0 || laneId > 0xffff_ffff
      ) {
        throw new FerricStoreError("invalid SHARDS range", { raw: item });
      }
      if (declaredShardCount != null && shard >= declaredShardCount) {
        throw new FerricStoreError("invalid SHARDS shard metadata", { raw: item });
      }

      const endpointKey = endpointKeyFor(endpoint);
      const route: RoutingRoute = Object.freeze({
        endpoint,
        endpointKey,
        laneId,
        leaderNode: endpoint.node,
        shard
      });
      const existingRoute = routesByShard.get(shard);
      if (
        existingRoute != null &&
        (existingRoute.endpointKey !== endpointKey || existingRoute.laneId !== laneId ||
          !sameEndpoint(existingRoute.endpoint, endpoint))
      ) {
        throw new FerricStoreError(`invalid SHARDS topology: inconsistent route for shard ${shard}`, { raw: item });
      }
      routesByShard.set(shard, route);

      const existingEndpoint = endpoints.get(endpointKey);
      if (existingEndpoint != null && !sameEndpoint(existingEndpoint, endpoint)) {
        throw new FerricStoreError(`invalid SHARDS topology: inconsistent endpoint ${endpointKey}`, { raw: item });
      }
      for (let slot = first; slot <= last; slot += 1) {
        if (slots[slot] != null) {
          throw new FerricStoreError(`invalid SHARDS topology: overlapping slot ${slot}`, { raw: item });
        }
        slots[slot] = route;
      }
      endpoints.set(endpointKey, endpoint);
      shards.add(shard);
    }

    const missingSlot = slots.findIndex((route) => route == null);
    if (missingSlot !== -1) {
      throw new FerricStoreError(`invalid SHARDS topology: no route for slot ${missingSlot}`, { raw: payload });
    }
    const shardCount = declaredShardCount ?? shards.size;
    if (shardCount === 0 || shards.size !== shardCount) {
      throw new FerricStoreError("invalid SHARDS shard count", { raw: payload });
    }
    for (let shard = 0; shard < shardCount; shard += 1) {
      if (!shards.has(shard)) {
        throw new FerricStoreError(`invalid SHARDS topology: no range for shard ${shard}`, { raw: payload });
      }
    }

    return new RoutingTopology(
      routeEpoch ?? 0,
      shardCount,
      Object.freeze(slots),
      readonlyMapView(endpoints)
    );
  }

  static slotForKey(key: string | Buffer): number {
    return routingSlotForKey(key);
  }

  routeKey(key: string | Buffer): RoutingRoute {
    const slot = RoutingTopology.slotForKey(key);
    const route = this.slots[slot];
    if (route == null) throw new FerricStoreError(`no route for slot ${slot}`);
    return { ...route, slot };
  }
}
