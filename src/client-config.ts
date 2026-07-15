import type {
  FerricStoreClientFromUrlOptions
} from "./client-options.js";

/** Capture public client configuration before construction crosses an async boundary. */
export function snapshotClientOptions<T extends FerricStoreClientFromUrlOptions>(options: T): T {
  const own = { ...options };
  const snapshot = {
    ...own,
    ...(typeof own.autoBatch === "object" && own.autoBatch != null
      ? { autoBatch: Object.freeze({ ...own.autoBatch }) }
      : {}),
    ...(own.backpressure == null
      ? {}
      : { backpressure: Object.freeze({ ...own.backpressure }) }),
    ...(typeof own.reconnect === "object" && own.reconnect != null
      ? { reconnect: Object.freeze({ ...own.reconnect }) }
      : {})
  };
  return Object.freeze(snapshot);
}
