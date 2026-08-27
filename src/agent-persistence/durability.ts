import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { LockHeldError } from "../errors.js";
import type { Command, CommandArgument } from "../internal.js";

export interface FerricStoreCommandClient {
  command(...args: CommandArgument[]): Promise<unknown>;
  pipeline?(commands: readonly Command[]): Promise<unknown[]>;
}

export interface FerricStoreLockOptions {
  /** Lease duration for adapter mutation locks. Defaults to five minutes. */
  lockTtlMs?: number;
  /** Maximum time to wait for a contended mutation lock. Defaults to 30 seconds. */
  lockWaitMs?: number;
  /** Delay between lock acquisition attempts. Defaults to 10 milliseconds. */
  lockRetryMs?: number;
}

interface RequiredLockOptions {
  readonly lockRetryMs: number;
  readonly lockTtlMs: number;
  readonly lockWaitMs: number;
}

const DEFAULT_LOCK_OPTIONS: RequiredLockOptions = {
  lockRetryMs: 10,
  lockTtlMs: 300_000,
  lockWaitMs: 30_000
};

export function normalizeKeyPrefix(value: string, defaultValue: string): string {
  const prefix = value.length === 0 ? defaultValue : value;
  if (prefix.includes("\0")) throw new TypeError("keyPrefix must not contain NUL bytes");
  const normalized = prefix.replace(/:+$/u, "");
  if (normalized.length === 0) throw new TypeError("keyPrefix must contain a character other than ':'");
  return normalized;
}

export function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const normalized = value ?? fallback;
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return normalized;
}

export function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
  const normalized = value ?? fallback;
  if (!Number.isSafeInteger(normalized) || normalized < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return normalized;
}

export function textResponse(value: unknown, name: string): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
  throw new TypeError(`FerricStore returned an invalid ${name}`);
}

export function arrayResponse(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new TypeError(`FerricStore returned an invalid ${name}`);
  return value;
}

export function integerResponse(value: unknown, name: string): number {
  const parsed = typeof value === "number" ? value : Number(textResponse(value, name));
  if (!Number.isSafeInteger(parsed)) throw new TypeError(`FerricStore returned an invalid ${name}`);
  return parsed;
}

export async function executeCommands(
  client: FerricStoreCommandClient,
  commands: readonly Command[]
): Promise<unknown[]> {
  if (commands.length === 0) return [];
  if (client.pipeline != null) return await client.pipeline(commands);
  return await Promise.all(commands.map(async (command) => await client.command(...command)));
}

export async function withMutationLocks<T>(
  client: FerricStoreCommandClient,
  keys: readonly string[],
  operation: () => Promise<T>,
  options: FerricStoreLockOptions = {}
): Promise<T> {
  const orderedKeys = [...new Set(keys)].sort();
  if (orderedKeys.length === 0) return await operation();

  const normalized: RequiredLockOptions = {
    lockRetryMs: positiveInteger(options.lockRetryMs, DEFAULT_LOCK_OPTIONS.lockRetryMs, "lockRetryMs"),
    lockTtlMs: positiveInteger(options.lockTtlMs, DEFAULT_LOCK_OPTIONS.lockTtlMs, "lockTtlMs"),
    lockWaitMs: nonNegativeInteger(options.lockWaitMs, DEFAULT_LOCK_OPTIONS.lockWaitMs, "lockWaitMs")
  };
  const owner = randomUUID();
  const acquired: string[] = [];
  const deadline = performance.now() + normalized.lockWaitMs;
  let primaryError: unknown;
  let heartbeatError: unknown;
  let releaseError: unknown;
  let result: T | undefined;
  let operationCompleted = false;
  const heartbeatAbort = new AbortController();

  try {
    for (const key of orderedKeys) {
      while (!(await tryAcquireLock(client, key, owner, normalized.lockTtlMs))) {
        if (performance.now() >= deadline) {
          throw new Error(`timed out acquiring FerricStore lock ${JSON.stringify(key)}`);
        }
        await delay(normalized.lockRetryMs);
      }
      acquired.push(key);
    }

    const heartbeat = renewLocks(
      client,
      acquired,
      owner,
      normalized.lockTtlMs,
      heartbeatAbort.signal,
      (error) => {
        heartbeatError ??= error;
      }
    );
    try {
      result = await operation();
      operationCompleted = true;
    } catch (error) {
      primaryError = error;
    } finally {
      heartbeatAbort.abort();
      await heartbeat;
    }
  } catch (error) {
    primaryError ??= error;
  } finally {
    heartbeatAbort.abort();
    for (const key of acquired.reverse()) {
      try {
        await client.command("UNLOCK", key, owner);
      } catch (error) {
        releaseError ??= error;
      }
    }
  }
  if (primaryError != null) throw errorObject(primaryError);
  if (heartbeatError != null) throw errorObject(heartbeatError);
  if (releaseError != null) throw errorObject(releaseError);
  if (!operationCompleted) throw new Error("FerricStore mutation did not complete");
  return result as T;
}

async function tryAcquireLock(
  client: FerricStoreCommandClient,
  key: string,
  owner: string,
  ttlMs: number
): Promise<boolean> {
  try {
    const response = await client.command("LOCK", key, owner, ttlMs);
    return response === true || response === "OK" || Buffer.isBuffer(response) && response.equals(Buffer.from("OK"));
  } catch (error) {
    if (error instanceof LockHeldError) return false;
    throw error;
  }
}

async function renewLocks(
  client: FerricStoreCommandClient,
  keys: readonly string[],
  owner: string,
  ttlMs: number,
  signal: AbortSignal,
  onError: (error: unknown) => void
): Promise<void> {
  const intervalMs = Math.max(Math.floor(ttlMs / 3), 10);
  const retryMs = Math.min(Math.max(Math.floor(intervalMs / 10), 10), 1_000);
  const lastExtended = new Map(keys.map((key) => [key, performance.now()]));
  let waitMs = intervalMs;
  while (!signal.aborted) {
    try {
      await delay(waitMs, undefined, { signal });
    } catch (error) {
      if (signal.aborted) return;
      onError(error);
      return;
    }
    const now = performance.now();
    let retry = false;
    for (const key of keys) {
      try {
        const response = await client.command("EXTEND", key, owner, ttlMs);
        if (integerResponse(response, "EXTEND response") !== 1) {
          onError(new Error(`lost FerricStore lock ${JSON.stringify(key)} while mutating data`));
          return;
        }
        lastExtended.set(key, now);
      } catch (error) {
        if (now - (lastExtended.get(key) ?? 0) >= ttlMs) {
          onError(new Error(`lost FerricStore lock ${JSON.stringify(key)} while mutating data`, { cause: error }));
          return;
        }
        retry = true;
      }
    }
    waitMs = retry ? retryMs : intervalMs;
  }
}

function errorObject(value: unknown): Error {
  return value instanceof Error ? value : new Error("FerricStore mutation failed", { cause: value });
}
