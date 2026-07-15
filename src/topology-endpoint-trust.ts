import { FerricStoreError } from "./errors.js";
import type { RoutingTopology } from "./routing-topology.js";
import {
  connectionKeyForEndpoint,
  normalizedEndpointHost,
  normalizedHostSet,
  type RoutingEndpoint
} from "./topology-utilities.js";

/** Trust policy for endpoints learned from SHARDS topology responses. */
export type EndpointPolicy = "seed_hosts" | "any" | "none" | { readonly allowHosts: readonly string[] };

export class TopologyEndpointTrust {
  private readonly allowedHosts: ReadonlySet<string> | undefined;
  private readonly policy: EndpointPolicy;
  private readonly trustedHosts: ReadonlySet<string>;

  constructor(
    policy: EndpointPolicy | undefined,
    private readonly endpointValidator: ((endpoint: RoutingEndpoint) => boolean | void) | undefined,
    private readonly seedEndpointKeys: ReadonlySet<string>,
    trustedHosts: readonly string[],
    private readonly tls: boolean
  ) {
    this.policy = policy ?? "seed_hosts";
    this.allowedHosts = typeof this.policy === "object"
      ? normalizedHostSet(this.policy.allowHosts)
      : undefined;
    this.trustedHosts = normalizedHostSet(trustedHosts);
  }

  activeConnectionKeys(topology: RoutingTopology): Set<string> {
    const active = new Set(this.seedEndpointKeys);
    for (const endpoint of topology.endpoints.values()) {
      active.add(connectionKeyForEndpoint(endpoint, this.tls));
    }
    return active;
  }

  validate(endpoint: RoutingEndpoint): void {
    let allowed: boolean;
    if (this.policy === "any") {
      allowed = true;
    } else if (this.policy === "none") {
      allowed = this.seedEndpointKeys.has(connectionKeyForEndpoint(endpoint, this.tls));
    } else if (this.policy === "seed_hosts") {
      allowed =
        this.seedEndpointKeys.has(connectionKeyForEndpoint(endpoint, this.tls))
        || this.trustedHosts.has(normalizedEndpointHost(endpoint.host));
    } else if ("allowHosts" in this.policy) {
      allowed = this.allowedHosts?.has(normalizedEndpointHost(endpoint.host)) === true;
    } else {
      throw new FerricStoreError("invalid endpoint policy", { raw: this.policy });
    }
    if (!allowed || this.endpointValidator?.(endpoint) === false) {
      throw new FerricStoreError("unsafe learned endpoint", { raw: endpoint });
    }
  }

  validateTopology(topology: RoutingTopology): void {
    for (const endpoint of topology.endpoints.values()) this.validate(endpoint);
  }
}
