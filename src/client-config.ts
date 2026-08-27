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
      : {}),
    ...(own.httpOptions == null
      ? {}
      : {
          httpOptions: Object.freeze({
            ...own.httpOptions,
            ...(own.httpOptions.headers == null
              ? {}
              : { headers: Object.freeze({ ...own.httpOptions.headers }) }),
            ...(own.httpOptions.tlsOptions == null
              ? {}
              : { tlsOptions: Object.freeze({ ...own.httpOptions.tlsOptions }) })
          })
        })
  };
  return Object.freeze(snapshot);
}
