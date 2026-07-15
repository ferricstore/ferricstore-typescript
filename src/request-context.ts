import { Buffer } from "node:buffer";
import { InvalidCommandError } from "./errors.js";
import type { CommandArgument } from "./internal.js";

const requestContextCommands = new Set([
  "INVOCATION.CREATE",
  "INVOCATION.DEFINITION.GET",
  "INVOCATION.DEFINITION.LIST",
  "INVOCATION.DEFINITION.PUT",
  "INVOCATION.GET",
  "INVOCATION.PARTITION.LIST"
]);
const requestContextFields = new Set(["scopes", "subject", "tenant"]);
const normalizedRequestContexts = new WeakSet<object>();

export function isRequestContextCommand(command: string): boolean {
  return requestContextCommands.has(command);
}

/** Capture and validate authorization data before command dispatch can await. */
export function normalizeRequestContext(context: unknown): Record<string, unknown> {
  if (typeof context === "object" && context != null && normalizedRequestContexts.has(context)) {
    return context as Record<string, unknown>;
  }
  if (!isPlainObject(context)) {
    throw new InvalidCommandError("request context must be a plain object");
  }
  const prototype = Reflect.getPrototypeOf(context);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new InvalidCommandError("request context must be a plain object");
  }
  for (const key of Object.keys(context)) {
    if (!requestContextFields.has(key)) {
      throw new InvalidCommandError(`request context contains unsupported field ${key}`);
    }
  }

  const snapshot: Record<string, unknown> = {};
  const subject = requestContextText(context, "subject");
  const tenant = requestContextText(context, "tenant");
  if (subject != null) snapshot.subject = subject;
  if (tenant != null) snapshot.tenant = tenant;
  if (Object.hasOwn(context, "scopes") && context.scopes !== undefined) {
    snapshot.scopes = normalizeRequestContextScopes(context.scopes);
  }
  Object.freeze(snapshot);
  normalizedRequestContexts.add(snapshot);
  return snapshot;
}

/** Snapshot raw invocation command context in reconnecting/topology queues too. */
export function snapshotCommandRequestContext(args: CommandArgument[]): void {
  const command = requestContextCommandToken(args[0]);
  if (
    command == null
    || args.length < 3
    || commandToken(args[args.length - 2]) !== "REQUEST_CONTEXT"
  ) return;
  args[args.length - 1] = normalizeRequestContext(args[args.length - 1]);
}

function requestContextCommandToken(value: CommandArgument | undefined): string | undefined {
  if (typeof value === "string") {
    const first = value.charCodeAt(0);
    if (first !== 73 && first !== 105) return undefined;
    if (isRequestContextCommand(value)) return value;
    const upper = value.toUpperCase();
    return isRequestContextCommand(upper) ? upper : undefined;
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    if (value[0] !== 73 && value[0] !== 105) return undefined;
    const command = Buffer.from(value).toString("utf8").toUpperCase();
    return isRequestContextCommand(command) ? command : undefined;
  }
  return undefined;
}

function normalizeRequestContextScopes(scopes: unknown): readonly string[] {
  if (typeof scopes === "string") {
    return Object.freeze([...new Set(scopes.split(/\s+/u).filter((scope) => scope.length > 0))]);
  }
  if (!Array.isArray(scopes)) {
    throw new InvalidCommandError("request context scopes must be a string or an array of strings");
  }
  const unique = new Set<string>();
  for (let index = 0; index < scopes.length; index += 1) {
    const scope: unknown = scopes[index];
    if (!Object.hasOwn(scopes, index) || typeof scope !== "string" || scope.length === 0) {
      throw new InvalidCommandError("request context scopes must be a dense array of own non-empty strings");
    }
    unique.add(scope);
  }
  return Object.freeze([...unique]);
}

function requestContextText(
  context: Record<string, unknown>,
  fieldName: "subject" | "tenant"
): string | undefined {
  if (!Object.hasOwn(context, fieldName) || context[fieldName] === undefined) return undefined;
  const value = context[fieldName];
  if (typeof value !== "string" || value.length === 0) {
    throw new InvalidCommandError(`request context ${fieldName} must be a non-empty string`);
  }
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object"
    && value != null
    && !Array.isArray(value)
    && !Buffer.isBuffer(value)
    && !(value instanceof Uint8Array);
}

function commandToken(value: CommandArgument | undefined): string {
  if (typeof value === "string") return value.toUpperCase();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8").toUpperCase();
  }
  return "";
}
