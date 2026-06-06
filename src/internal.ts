import type { Codec } from "./codecs.js";

export type CommandArgument = string | Buffer | number | boolean | null | undefined;
export type Command = readonly CommandArgument[];
export type RespMap = Map<unknown, unknown> | Record<PropertyKey, unknown>;

export const AUTO_PARTITION_PREFIX = "__flow_auto__:";
export const AUTO_PARTITION_BUCKETS = 256;

export function nowMs(): number {
  return Date.now();
}

export function append(args: CommandArgument[], name: string, value: unknown): void {
  if (value != null) {
    args.push(name, value as CommandArgument);
  }
}

export function appendBool(args: CommandArgument[], name: string, value: boolean | null | undefined): void {
  if (value != null) {
    args.push(name, value ? "true" : "false");
  }
}

export function appendEncoded(
  args: CommandArgument[],
  name: string,
  codec: Codec<unknown>,
  value: unknown
): void {
  if (value != null) {
    args.push(name, codec.encode(value));
  }
}

export function appendNamedValues(
  args: CommandArgument[],
  codec: Codec<unknown>,
  options: {
    values?: Record<string, unknown>;
    valueRefs?: Record<string, string>;
    dropValues?: string[];
    overrideValues?: string[];
  }
): void {
  for (const [name, value] of Object.entries(options.values ?? {})) {
    args.push("VALUE", name, codec.encode(value));
  }
  for (const [name, ref] of Object.entries(options.valueRefs ?? {})) {
    args.push("VALUE_REF", name, ref);
  }
  for (const name of options.dropValues ?? []) {
    args.push("DROP_VALUE", name);
  }
  for (const name of options.overrideValues ?? []) {
    args.push("OVERRIDE_VALUE", name);
  }
}

export function appendValueReturn(
  args: CommandArgument[],
  options: { values?: string[]; valueMaxBytes?: number }
): void {
  for (const name of options.values ?? []) {
    args.push("VALUE", name);
  }
  append(args, "VALUE_MAX_BYTES", options.valueMaxBytes);
}

export function okResponse(value: unknown): boolean {
  return value === "OK" || (Buffer.isBuffer(value) && value.toString("utf8") === "OK") || value === true;
}

export function text(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  return String(value);
}

export function optionalString(value: unknown): string | undefined {
  if (value == null || value === "" || (Buffer.isBuffer(value) && value.byteLength === 0)) {
    return undefined;
  }
  return text(value);
}

export function bytes(value: unknown): Buffer {
  if (value == null) {
    return Buffer.alloc(0);
  }
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  return Buffer.from(String(value));
}

export function integer(value: unknown, defaultValue = 0): number {
  if (value == null || value === "") {
    return defaultValue;
  }
  return Number(value);
}

export function field(source: unknown, key: string): unknown {
  if (source instanceof Map) {
    if (source.has(key)) {
      return source.get(key);
    }
    const raw = Buffer.from(key);
    for (const [itemKey, value] of source.entries()) {
      if (Buffer.isBuffer(itemKey) && itemKey.equals(raw)) {
        return value;
      }
    }
    return undefined;
  }

  if (typeof source === "object" && source != null) {
    const record = source as Record<PropertyKey, unknown>;
    if (key in record) {
      return record[key];
    }
    const rawKey = Buffer.from(key).toString();
    if (rawKey in record) {
      return record[rawKey];
    }
  }

  return undefined;
}

export function toStringKeyMap(value: unknown): Record<string, unknown> | undefined {
  if (value == null) {
    return undefined;
  }

  const result: Record<string, unknown> = {};
  if (value instanceof Map) {
    for (const [key, item] of value.entries()) {
      result[text(key)] = normalizeRefMeta(item);
    }
    return result;
  }

  if (typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      result[text(key)] = normalizeRefMeta(item);
    }
    return result;
  }

  return undefined;
}

export function normalizeRefMeta(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      result[text(key)] = normalizeRefMeta(item);
    }
    return result;
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeRefMeta(item));
  }
  if (typeof value === "object" && value != null) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      result[text(key)] = normalizeRefMeta(item);
    }
    return result;
  }
  return value;
}

export function parseKvResponse(value: unknown): Record<string, unknown> {
  if (value instanceof Map) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of value.entries()) {
      result[text(key)] = item;
    }
    return result;
  }
  if (typeof value === "object" && value != null && !Array.isArray(value) && !Buffer.isBuffer(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [text(key), item]));
  }
  if (Array.isArray(value)) {
    const result: Record<string, unknown> = {};
    for (let index = 0; index < value.length - 1; index += 2) {
      result[text(value[index])] = value[index + 1];
    }
    return result;
  }
  if (typeof value === "string" || Buffer.isBuffer(value)) {
    return parseTextSections(text(value));
  }
  return { value };
}

export function autoPartitionKeyForId(id: string): string {
  return `${AUTO_PARTITION_PREFIX}${crc32(Buffer.from(id)) % AUTO_PARTITION_BUCKETS}`;
}

export function expandManyResponse(value: unknown, count: number): unknown[] {
  if (Array.isArray(value) && value.length === count) {
    return value;
  }
  return Array.from({ length: count }, () => value);
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }

    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(signal.reason);
      },
      { once: true }
    );
  });
}

function parseTextSections(value: string): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  let section: Record<string, unknown> | undefined;

  for (const rawLine of value.split(/\r?\n/u)) {
    const line = rawLine.trimEnd();
    if (line.length === 0) {
      continue;
    }
    if (line.endsWith(":") && !line.startsWith(" ")) {
      section = {};
      result[line.slice(0, -1)] = section;
      continue;
    }

    const target = rawLine.startsWith(" ") && section != null ? section : result;
    const colon = line.indexOf(":");
    if (colon >= 0) {
      target[line.slice(0, colon).trim()] = coerceDiagnosticValue(line.slice(colon + 1).trim());
    }
  }

  return result;
}

function coerceDiagnosticValue(value: string): unknown {
  if (value === "") {
    return value;
  }
  if (value === "true" || value === "false") {
    return value === "true";
  }
  if (/^-?[0-9]+$/u.test(value)) {
    return Number.parseInt(value, 10);
  }
  return value;
}

function crc32(buffer: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}
