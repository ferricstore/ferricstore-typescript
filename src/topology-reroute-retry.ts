import {
  isRetryableRouteError,
  waitForExplicitlySafeReroute
} from "./topology-options.js";

export async function canRetrySafeReroute(
  error: unknown,
  attempt: number,
  retryAllowed: boolean,
  refresh: () => Promise<unknown>,
  assertOpen: () => void
): Promise<boolean> {
  if (attempt !== 0 || !isRetryableRouteError(error)) return false;
  const refreshed = await refresh().then(
    () => true,
    () => false
  );
  if (!refreshed || !retryAllowed || !(await waitForExplicitlySafeReroute(error))) return false;
  assertOpen();
  return true;
}
