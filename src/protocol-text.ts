import { Buffer } from "node:buffer";
import { commandToken } from "./command-grammar.js";
import { FerricStoreError } from "./errors.js";

export function toBuffer(value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (typeof value === "string") return Buffer.from(value);
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return Buffer.from(String(value));
  }
  if (typeof value === "symbol") return Buffer.from(value.description ?? value.toString());
  if (value == null) return Buffer.alloc(0);
  throw new FerricStoreError(`unsupported binary command argument type: ${typeof value}`);
}

export function asText(value: unknown): string {
  return toBuffer(value).toString("utf8");
}

export function optionalText(value: unknown): string | undefined {
  if (value == null || value === "") return undefined;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array
  ) {
    return asText(value);
  }
  return undefined;
}

export function commandTokenIs(value: unknown, expected: string): boolean {
  if (typeof value === "string") return value.toUpperCase() === expected;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return asText(value).toUpperCase() === expected;
  }
  return false;
}

export function commandName(value: unknown): string | undefined {
  return commandToken(value);
}

export function commandNameIs(value: unknown, expected: string): boolean {
  if (typeof value === "string") return value === expected || value.toUpperCase() === expected;
  return asText(value).toUpperCase() === expected;
}
