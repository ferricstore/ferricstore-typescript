/** Combine the ordinary client deadline with time the server may intentionally block. */
export function serverResponseTimeoutMs(
  requestTimeoutMs: number,
  serverBlockMs: number | undefined
): number | undefined {
  if (serverBlockMs == null) return requestTimeoutMs;
  if (serverBlockMs === 0) return undefined;
  return saturatingAdd(requestTimeoutMs, serverBlockMs);
}

/** Aggregate sequential blocking waits in one ordered batch; zero means unbounded. */
export function combinedServerBlockMs(
  values: readonly (number | undefined)[]
): number | undefined {
  let total: number | undefined;
  for (const value of values) {
    if (value === 0) return 0;
    if (value != null) total = saturatingAdd(total ?? 0, value);
  }
  return total;
}

function saturatingAdd(left: number, right: number): number {
  return Math.min(Number.MAX_SAFE_INTEGER, left + right);
}
