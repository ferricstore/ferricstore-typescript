import { Buffer } from "node:buffer";
import { validateHeaderName, validateHeaderValue } from "node:http";
import type { ConnectionOptions } from "node:tls";

export interface HTTPAdapterOptions {
  bearerToken?: string;
  headers?: Readonly<Record<string, string>>;
  http2?: boolean;
  maxBatchItems?: number;
  /** Maximum HTTP/1.1 sockets or concurrent streams on one HTTP/2 session. Defaults to 100. */
  maxConnections?: number;
  maxRedirects?: number;
  maxRequestBytes?: number;
  maxResponseBytes?: number;
  password?: string;
  /** Base whole-request deadline. Command-declared blocking waits are added; a zero block disables it. */
  timeoutMs?: number;
  tlsOptions?: ConnectionOptions;
  username?: string;
}

export interface HTTPAdapterConfig {
  readonly commandUrl: URL;
  readonly headers: Readonly<Record<string, string>>;
  readonly http2: boolean;
  readonly maxBatchItems: number;
  readonly maxConnections: number;
  readonly maxRedirects: number;
  readonly maxRequestBytes: number;
  readonly maxResponseBytes: number;
  readonly timeoutMs: number;
  readonly tlsOptions: ConnectionOptions;
}

export function normalizeHTTPOptions(value: string, options: HTTPAdapterOptions): HTTPAdapterConfig {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError(`unsupported HTTP URL scheme: ${url.protocol}`);
  }
  if (url.username !== "" || url.password !== "") throw new TypeError("HTTP URL userinfo is not allowed");
  if (url.search !== "" || url.hash !== "") throw new TypeError("HTTP base URL cannot contain query or fragment");
  const headers = normalizedHeaders(options.headers ?? {});
  const authorization = authorizationHeader(url, options, headers.authorization);
  if (authorization != null) headers.authorization = authorization;
  url.pathname = `${url.pathname.replace(/\/$/u, "")}/v1/commands`;
  return Object.freeze({
    commandUrl: url,
    headers: Object.freeze(headers),
    http2: booleanOption(options.http2, false, "http2"),
    maxBatchItems: positiveInteger(options.maxBatchItems, 1_000, "maxBatchItems"),
    maxConnections: positiveInteger(options.maxConnections, 100, "maxConnections"),
    maxRedirects: nonNegativeInteger(options.maxRedirects, 20, "maxRedirects"),
    maxRequestBytes: positiveInteger(options.maxRequestBytes, 1024 * 1024, "maxRequestBytes"),
    maxResponseBytes: positiveInteger(options.maxResponseBytes, 16 * 1024 * 1024, "maxResponseBytes"),
    timeoutMs: positiveInteger(options.timeoutMs, 30_000, "timeoutMs"),
    tlsOptions: Object.freeze({ ...(options.tlsOptions ?? {}) })
  });
}

function normalizedHeaders(source: Readonly<Record<string, string>>): Record<string, string> {
  // Header names such as `__proto__` are valid HTTP tokens. A null-prototype
  // record preserves them as data instead of invoking Object.prototype setters.
  const headers = Object.create(null) as Record<string, string>;
  for (const [rawName, value] of Object.entries(source)) {
    const name = rawName.toLowerCase();
    if (typeof value !== "string" || !validHeader(name, value)) {
      throw new TypeError(`invalid HTTP header: ${rawName}`);
    }
    headers[name] = value;
  }
  return headers;
}

function authorizationHeader(
  url: URL,
  options: HTTPAdapterOptions,
  custom: string | undefined
): string | undefined {
  const basic = options.username != null || options.password != null;
  const count = Number(custom != null) + Number(options.bearerToken != null) + Number(basic);
  if (count > 1) throw new TypeError("HTTP credentials are mutually exclusive");
  if (options.bearerToken != null) {
    if (options.bearerToken === "" || !validHeader("authorization", `Bearer ${options.bearerToken}`)) {
      throw new TypeError("invalid bearer token");
    }
    return `Bearer ${options.bearerToken}`;
  }
  if (!basic) return custom;
  if (url.protocol !== "https:") throw new TypeError("Basic authentication requires HTTPS");
  if (typeof options.password !== "string") throw new TypeError("Basic authentication requires a password");
  const username = options.username ?? "default";
  if (username === "" || username.includes(":") || !safeHeader(username) || !safeHeader(options.password)) {
    throw new TypeError("invalid Basic authentication credentials");
  }
  return `Basic ${Buffer.from(`${username}:${options.password}`).toString("base64")}`;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result <= 0) throw new TypeError(`${name} must be a positive integer`);
  return result;
}

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 0) throw new TypeError(`${name} must be a non-negative integer`);
  return result;
}

function booleanOption(value: boolean | undefined, fallback: boolean, name: string): boolean {
  const result = value ?? fallback;
  if (typeof result !== "boolean") throw new TypeError(`${name} must be a boolean`);
  return result;
}

function safeHeader(value: string): boolean {
  return !value.includes("\r") && !value.includes("\n");
}

function validHeader(name: string, value: string): boolean {
  try {
    validateHeaderName(name);
    validateHeaderValue(name, value);
    return true;
  } catch {
    return false;
  }
}
