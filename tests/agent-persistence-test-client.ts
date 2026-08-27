import { LockHeldError, LockNotOwnedError } from "../src/errors.js";
import type { CommandArgument } from "../src/internal.js";

interface StoredMember {
  readonly raw: string | Buffer;
  score: number;
}

export class MemoryCommandClient {
  readonly calls: CommandArgument[][] = [];
  private readonly strings = new Map<string, string | Buffer>();
  private readonly hashes = new Map<string, Map<string, unknown>>();
  private readonly sets = new Map<string, Map<string, string | Buffer>>();
  private readonly sortedSets = new Map<string, Map<string, StoredMember>>();
  private readonly locks = new Map<string, { expiresAt: number; owner: string }>();

  async command(...args: CommandArgument[]): Promise<unknown> {
    this.calls.push(args);
    const command = text(args[0]).toUpperCase();
    switch (command) {
      case "GET": return clone(this.strings.get(text(args[1]))) ?? null;
      case "SET": return this.setString(args);
      case "CAS": return this.cas(args);
      case "LOCK": return this.lock(text(args[1]), text(args[2]), number(args[3]));
      case "EXTEND": return this.extend(text(args[1]), text(args[2]), number(args[3]));
      case "UNLOCK": return this.unlock(text(args[1]), text(args[2]));
      case "HGET": return clone(this.hash(text(args[1])).get(text(args[2])) ?? null);
      case "HSET": return this.hset(args);
      case "HSETNX": return this.hsetnx(args);
      case "HDEL": return this.hdel(args);
      case "HSCAN": return this.hscan(args);
      case "SADD": return this.sadd(args);
      case "SMEMBERS": return [...this.set(text(args[1])).values()].map(clone);
      case "ZADD": return this.zadd(args);
      case "ZRANGE": return this.zrange(args, false);
      case "ZREVRANGE": return this.zrange(args, true);
      case "ZREM": return this.zrem(args);
      case "DEL": return this.del(args);
      default: throw new Error(`MemoryCommandClient does not implement ${command}`);
    }
  }

  async pipeline(commands: readonly (readonly CommandArgument[])[]): Promise<unknown[]> {
    return await Promise.all(commands.map(async (command) => await this.command(...command)));
  }

  private lock(key: string, owner: string, ttlMs: number): Buffer {
    const existing = this.locks.get(key);
    if (existing != null && existing.expiresAt > Date.now() && existing.owner !== owner) {
      throw new LockHeldError("lock is held");
    }
    this.locks.set(key, { expiresAt: Date.now() + ttlMs, owner });
    return Buffer.from("OK");
  }

  private extend(key: string, owner: string, ttlMs: number): number {
    const existing = this.locks.get(key);
    if (existing?.owner !== owner || existing.expiresAt <= Date.now()) return 0;
    existing.expiresAt = Date.now() + ttlMs;
    return 1;
  }

  private unlock(key: string, owner: string): number {
    const existing = this.locks.get(key);
    if (existing == null) return 0;
    if (existing.owner !== owner) throw new LockNotOwnedError("caller is not the lock owner");
    this.locks.delete(key);
    return 1;
  }

  private hset(args: readonly CommandArgument[]): number {
    const hash = this.hash(text(args[1]));
    let added = 0;
    for (let index = 2; index < args.length; index += 2) {
      const field = text(args[index]);
      if (!hash.has(field)) added += 1;
      hash.set(field, clone(args[index + 1]));
    }
    return added;
  }

  private setString(args: readonly CommandArgument[]): Buffer | null {
    const key = text(args[1]);
    const value = stringOrBuffer(args[2]);
    const options = args.slice(3).map((item) => text(item).toUpperCase());
    if (options.includes("NX") && this.strings.has(key)) return null;
    if (options.includes("XX") && !this.strings.has(key)) return null;
    this.strings.set(key, clone(value));
    return Buffer.from("OK");
  }

  private cas(args: readonly CommandArgument[]): number | null {
    const key = text(args[1]);
    const current = this.strings.get(key);
    if (current == null) return null;
    if (!asBuffer(current).equals(asBuffer(stringOrBuffer(args[2])))) return 0;
    this.strings.set(key, clone(stringOrBuffer(args[3])));
    return 1;
  }

  private hsetnx(args: readonly CommandArgument[]): number {
    const hash = this.hash(text(args[1]));
    const field = text(args[2]);
    if (hash.has(field)) return 0;
    hash.set(field, clone(args[3]));
    return 1;
  }

  private hdel(args: readonly CommandArgument[]): number {
    const hash = this.hash(text(args[1]));
    let deleted = 0;
    for (const field of args.slice(2)) deleted += Number(hash.delete(text(field)));
    return deleted;
  }

  private hscan(args: readonly CommandArgument[]): [number, unknown[]] {
    const hash = this.hash(text(args[1]));
    const matchIndex = args.findIndex((value) => text(value).toUpperCase() === "MATCH");
    const pattern = matchIndex < 0 ? "*" : text(args[matchIndex + 1]);
    const expression = new RegExp(`^${escapePattern(pattern).replaceAll("*", ".*")}$`, "u");
    const items: unknown[] = [];
    for (const [field, value] of hash) {
      if (expression.test(field)) items.push(Buffer.from(field), clone(value));
    }
    return [0, items];
  }

  private sadd(args: readonly CommandArgument[]): number {
    const set = this.set(text(args[1]));
    let added = 0;
    for (const value of args.slice(2)) {
      const raw = stringOrBuffer(value);
      const key = memberKey(raw);
      if (!set.has(key)) added += 1;
      set.set(key, clone(raw));
    }
    return added;
  }

  private zadd(args: readonly CommandArgument[]): number {
    const set = this.sortedSet(text(args[1]));
    let added = 0;
    for (let index = 2; index < args.length; index += 2) {
      const score = number(args[index]);
      const raw = stringOrBuffer(args[index + 1]);
      const key = memberKey(raw);
      if (!set.has(key)) added += 1;
      set.set(key, { raw: clone(raw), score });
    }
    return added;
  }

  private zrange(args: readonly CommandArgument[], reverse: boolean): (string | Buffer)[] {
    const start = number(args[2]);
    const rawStop = number(args[3]);
    const values = [...this.sortedSet(text(args[1])).values()].sort((left, right) =>
      left.score - right.score || Buffer.compare(asBuffer(left.raw), asBuffer(right.raw)));
    if (reverse) values.reverse();
    const stop = rawStop < 0 ? values.length + rawStop : rawStop;
    if (start >= values.length || stop < start) return [];
    return values.slice(start, stop + 1).map((value) => clone(value.raw));
  }

  private zrem(args: readonly CommandArgument[]): number {
    const set = this.sortedSet(text(args[1]));
    let deleted = 0;
    for (const value of args.slice(2)) deleted += Number(set.delete(memberKey(stringOrBuffer(value))));
    return deleted;
  }

  private del(args: readonly CommandArgument[]): number {
    let deleted = 0;
    for (const rawKey of args.slice(1)) {
      const key = text(rawKey);
      deleted += Number(this.strings.delete(key));
      deleted += Number(this.hashes.delete(key));
      deleted += Number(this.sets.delete(key));
      deleted += Number(this.sortedSets.delete(key));
    }
    return deleted;
  }

  private hash(key: string): Map<string, unknown> {
    let value = this.hashes.get(key);
    if (value == null) {
      value = new Map();
      this.hashes.set(key, value);
    }
    return value;
  }

  private set(key: string): Map<string, string | Buffer> {
    let value = this.sets.get(key);
    if (value == null) {
      value = new Map();
      this.sets.set(key, value);
    }
    return value;
  }

  private sortedSet(key: string): Map<string, StoredMember> {
    let value = this.sortedSets.get(key);
    if (value == null) {
      value = new Map();
      this.sortedSets.set(key, value);
    }
    return value;
  }
}

function text(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
  throw new TypeError("expected command text");
}

function number(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(text(value));
  if (!Number.isFinite(parsed)) throw new TypeError("expected command number");
  return parsed;
}

function stringOrBuffer(value: unknown): string | Buffer {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value);
  throw new TypeError("expected string or buffer command argument");
}

function clone<T>(value: T): T {
  return Buffer.isBuffer(value) ? Buffer.from(value) as T : value;
}

function memberKey(value: string | Buffer): string {
  return `${typeof value === "string" ? "s" : "b"}:${asBuffer(value).toString("base64")}`;
}

function asBuffer(value: string | Buffer): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : value;
}

function escapePattern(value: string): string {
  return value.replace(/[|\\{}()[\]^$+?.-]/gu, "\\$&");
}
