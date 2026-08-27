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

export interface FerricStoreMutationLease {
  /** Aborted as soon as lock ownership is known to have been lost. */
  readonly signal: AbortSignal;
  /** Throw when this mutation no longer owns every requested lock. */
  assertOwned(): void;
  /** Publish an idempotent, add-only discovery entry before its CAS record. */
  publish(...args: CommandArgument[]): Promise<unknown>;
  /** Atomically replace a value only when its last-read bytes are still current. */
  compareAndSet(key: string, expected: Buffer | undefined, value: Buffer): Promise<boolean>;
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

export async function readAtomicValue(
  client: FerricStoreCommandClient,
  key: string,
  name: string
): Promise<Buffer | undefined> {
  const value = await client.command("GET", key);
  if (value == null) return undefined;
  if (typeof value === "string") return Buffer.from(value, "utf8");
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value);
  throw new TypeError(`FerricStore returned a non-binary ${name}`);
}

export async function compareAndSetAtomicValue(
  client: FerricStoreCommandClient,
  key: string,
  expected: Buffer | undefined,
  value: Buffer
): Promise<boolean> {
  if (expected == null) {
    const response = await client.command("SET", key, value, "NX");
    if (response == null || response === false) return false;
    if (response === true) return true;
    return textResponse(response, "SET NX response").toUpperCase() === "OK";
  }
  const response = await client.command("CAS", key, expected, value);
  if (response == null || response === false) return false;
  if (response === true) return true;
  return integerResponse(response, "CAS response") === 1;
}

export async function withMutationLocks<T>(
  client: FerricStoreCommandClient,
  keys: readonly string[],
  operation: (lease: FerricStoreMutationLease) => Promise<T>,
  options: FerricStoreLockOptions = {}
): Promise<T> {
  const orderedKeys = [...new Set(keys)].sort();
  if (orderedKeys.length === 0) {
    const signal = new AbortController().signal;
    return await operation({
      signal,
      assertOwned: () => undefined,
      publish: async (...args) => await additiveCommand(client, args),
      compareAndSet: async (key, expected, value) =>
        await compareAndSetAtomicValue(client, key, expected, value)
    });
  }

  const normalized: RequiredLockOptions = {
    lockRetryMs: positiveInteger(options.lockRetryMs, DEFAULT_LOCK_OPTIONS.lockRetryMs, "lockRetryMs"),
    lockTtlMs: positiveInteger(options.lockTtlMs, DEFAULT_LOCK_OPTIONS.lockTtlMs, "lockTtlMs"),
    lockWaitMs: nonNegativeInteger(options.lockWaitMs, DEFAULT_LOCK_OPTIONS.lockWaitMs, "lockWaitMs")
  };
  if (normalized.lockRetryMs >= normalized.lockTtlMs) {
    throw new TypeError("lockRetryMs must be less than lockTtlMs");
  }
  const owner = randomUUID();
  const acquired: string[] = [];
  const deadline = performance.now() + normalized.lockWaitMs;
  let primaryError: unknown;
  let heartbeatError: unknown;
  let releaseError: unknown;
  let result: T | undefined;
  let operationCompleted = false;
  let conditionalCommitCompleted = false;
  const heartbeatAbort = new AbortController();
  const ownershipAbort = new AbortController();
  const lastExtended = new Map<string, number>();
  const loseOwnership = (error: unknown): Error => {
    const normalizedError = errorObject(error);
    heartbeatError ??= normalizedError;
    if (!ownershipAbort.signal.aborted) ownershipAbort.abort(normalizedError);
    return normalizedError;
  };
  const assertOwned = (): void => {
    if (heartbeatError != null) throw errorObject(heartbeatError);
    if (ownershipAbort.signal.aborted) throw errorObject(ownershipAbort.signal.reason);
  };
  const renewOwned = async (): Promise<void> => {
    assertOwned();
    for (const key of acquired) {
      try {
        const response = await client.command("EXTEND", key, owner, normalized.lockTtlMs);
        if (integerResponse(response, "EXTEND response") !== 1) {
          throw new Error(`lost FerricStore lock ${JSON.stringify(key)} while mutating data`);
        }
        lastExtended.set(key, performance.now());
      } catch (error) {
        throw loseOwnership(new Error(
          `could not validate FerricStore lock ${JSON.stringify(key)} before mutating data`,
          { cause: error }
        ));
      }
    }
    assertOwned();
  };
  const lease: FerricStoreMutationLease = {
    signal: ownershipAbort.signal,
    assertOwned,
    publish: async (...args) => {
      await renewOwned();
      const response = await additiveCommand(client, args);
      assertOwned();
      return response;
    },
    compareAndSet: async (key, expected, value) => {
      await renewOwned();
      const committed = await compareAndSetAtomicValue(client, key, expected, value);
      if (committed) conditionalCommitCompleted = true;
      else assertOwned();
      return committed;
    }
  };

  try {
    for (const key of orderedKeys) {
      while (!(await tryAcquireLock(client, key, owner, normalized.lockTtlMs))) {
        if (performance.now() >= deadline) {
          throw new Error(`timed out acquiring FerricStore lock ${JSON.stringify(key)}`);
        }
        await extendAcquiredLocks(client, acquired, owner, normalized.lockTtlMs);
        await delay(normalized.lockRetryMs);
      }
      acquired.push(key);
    }
    await extendAcquiredLocks(client, acquired, owner, normalized.lockTtlMs);
    for (const key of acquired) lastExtended.set(key, performance.now());

    const heartbeat = renewLocks(
      client,
      acquired,
      owner,
      normalized.lockTtlMs,
      lastExtended,
      heartbeatAbort.signal,
      (error) => {
        loseOwnership(error);
      }
    );
    try {
      result = await operation(lease);
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
  if (heartbeatError != null && !conditionalCommitCompleted) throw errorObject(heartbeatError);
  if (releaseError != null && !(heartbeatError != null && conditionalCommitCompleted)) {
    throw errorObject(releaseError);
  }
  if (!operationCompleted) throw new Error("FerricStore mutation did not complete");
  return result as T;
}

async function additiveCommand(
  client: FerricStoreCommandClient,
  args: readonly CommandArgument[]
): Promise<unknown> {
  const rawName = args[0];
  const name = typeof rawName === "string"
    ? rawName.toUpperCase()
    : Buffer.isBuffer(rawName) || rawName instanceof Uint8Array
      ? Buffer.from(rawName).toString("utf8").toUpperCase()
      : "";
  if (name !== "SADD" && name !== "ZADD") {
    throw new TypeError("FerricStore mutation leases only publish add-only SADD or ZADD indexes");
  }
  if (name === "ZADD" && (
    args.length < 4 ||
    args.length % 2 !== 0 ||
    args.slice(2).some((value, index) => index % 2 === 0 && Number(value) !== 0)
  )) {
    throw new TypeError("FerricStore mutation leases only publish zero-score ZADD indexes");
  }
  return await client.command(...args);
}

async function extendAcquiredLocks(
  client: FerricStoreCommandClient,
  keys: readonly string[],
  owner: string,
  ttlMs: number
): Promise<void> {
  for (const key of keys) {
    const response = await client.command("EXTEND", key, owner, ttlMs);
    if (integerResponse(response, "EXTEND response") !== 1) {
      throw new Error(`lost FerricStore lock ${JSON.stringify(key)} before mutating data`);
    }
  }
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
  lastExtended: Map<string, number>,
  signal: AbortSignal,
  onError: (error: unknown) => void
): Promise<void> {
  const intervalMs = Math.max(Math.floor(ttlMs / 3), 1);
  const retryMs = Math.min(Math.max(Math.floor(intervalMs / 10), 1), 1_000);
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
