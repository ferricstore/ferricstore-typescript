import type { Command } from "./internal.js";
import type { ProtocolCommand } from "./protocol.js";
import type { RoutingRoute } from "./routing-topology.js";
import type { RoutingEndpoint } from "./topology-utilities.js";

export interface RoutedKeyGroup {
  readonly entries: { readonly index: number; readonly key: string | Buffer }[];
  readonly route: RoutingRoute;
}

export interface RoutedPipelineGroup {
  readonly commands: Command[];
  readonly indices: number[];
  readonly route: RoutingRoute;
}

export interface RoutedCommandData {
  readonly command?: ProtocolCommand;
  readonly route: RoutingRoute;
}

export type RefreshCandidate =
  | { readonly seed: true; readonly url: string }
  | { readonly endpoint: RoutingEndpoint; readonly seed: false; readonly url: string };
