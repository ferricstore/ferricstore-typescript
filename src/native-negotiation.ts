import { Buffer } from "node:buffer";
import { field } from "./internal.js";

export type CompactResponseOpcodes = ReadonlyMap<string, ReadonlySet<number>>;

export interface NativeNegotiation {
  readonly compactResponseOpcodes: CompactResponseOpcodes;
  readonly maxResponseBytes?: number;
}

export const EMPTY_COMPACT_RESPONSE_OPCODES: CompactResponseOpcodes = new Map();

/** Parse the HELLO-shaped capability payload returned by HELLO or STARTUP. */
export function nativeNegotiation(value: unknown): NativeNegotiation {
  const capabilities = field(value, "capabilities") ?? value;
  const limits = field(capabilities, "limits");
  const responseCodecs = field(capabilities, "response_codecs");
  const maxResponseBytes = positiveSafeInteger(field(limits, "max_response_bytes"));
  return {
    compactResponseOpcodes: parseCompactResponseOpcodes(
      field(responseCodecs, "compact_response_opcodes")
    ),
    ...(maxResponseBytes == null ? {} : { maxResponseBytes })
  };
}

export function compactResponseOpcodeSupports(
  capabilities: CompactResponseOpcodes | undefined,
  codec: string,
  opcode: number
): boolean {
  return capabilities?.get(codec)?.has(opcode) === true;
}

function parseCompactResponseOpcodes(value: unknown): CompactResponseOpcodes {
  const entries = mapEntries(value);
  if (entries == null) return EMPTY_COMPACT_RESPONSE_OPCODES;
  const result = new Map<string, ReadonlySet<number>>();
  for (const [rawName, rawOpcodes] of entries) {
    const name = text(rawName);
    if (name == null || name.length === 0 || !Array.isArray(rawOpcodes)) continue;
    const opcodes = new Set<number>();
    let valid = true;
    for (let index = 0; index < rawOpcodes.length; index += 1) {
      if (!Object.hasOwn(rawOpcodes, index)) {
        valid = false;
        break;
      }
      const opcode = unsigned16(rawOpcodes[index]);
      if (opcode == null) {
        valid = false;
        break;
      }
      opcodes.add(opcode);
    }
    if (valid) result.set(name, opcodes);
  }
  return result;
}

function mapEntries(value: unknown): readonly (readonly [unknown, unknown])[] | undefined {
  if (value instanceof Map) return [...value.entries()];
  if (typeof value !== "object" || value == null || Array.isArray(value)) return undefined;
  return Object.entries(value as Record<string, unknown>);
}

function unsigned16(value: unknown): number | undefined {
  const parsed = nonNegativeSafeInteger(value);
  return parsed != null && parsed <= 0xffff ? parsed : undefined;
}

function positiveSafeInteger(value: unknown): number | undefined {
  const parsed = nonNegativeSafeInteger(value);
  return parsed != null && parsed > 0 ? parsed : undefined;
}

function nonNegativeSafeInteger(value: unknown): number | undefined {
  if (typeof value === "bigint") {
    return value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : undefined;
  }
  const source = text(value) ?? value;
  if (typeof source !== "number" && (typeof source !== "string" || !/^\d+$/u.test(source))) {
    return undefined;
  }
  const parsed = Number(source);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function text(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }
  return undefined;
}
