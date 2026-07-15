import type { RefreshCandidate } from "./topology-execution-types.js";
import {
  connectionKeyFromUrl,
  urlFromEndpoint,
  type RoutingEndpoint
} from "./topology-utilities.js";

export function topologyRefreshCandidates(
  seedUrls: readonly string[],
  endpoints: Iterable<RoutingEndpoint>,
  tls: boolean,
  preferred: string | undefined
): RefreshCandidate[] {
  const candidates: RefreshCandidate[] = [
    ...seedUrls.map((url) => ({ seed: true as const, url })),
    ...[...endpoints].map((endpoint) => ({
      endpoint,
      seed: false as const,
      url: urlFromEndpoint(endpoint, tls)
    }))
  ];
  const seen = new Set<string>();
  const unique = candidates.filter((candidate) => {
    const key = connectionKeyFromUrl(candidate.url);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  if (preferred == null) return unique;
  const index = unique.findIndex((candidate) => connectionKeyFromUrl(candidate.url) === preferred);
  if (index <= 0) return unique;
  const [candidate] = unique.splice(index, 1);
  if (candidate != null) unique.unshift(candidate);
  return unique;
}
