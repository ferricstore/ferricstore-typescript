import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import { field } from "./internal.js";
import {
  decodeError,
  freezeMetadataMap,
  nonNegativeInteger,
  optionalText,
  positiveInteger,
  requiredBoolean,
  requiredBoundedText,
  requiredMap
} from "./flow-query-response-validation.js";
import {
  FlowQueryError,
  type FlowQueryErrorPosition
} from "./flow-query-types.js";

const MIN_SIGNED_64 = -(1n << 63n);
const MAX_SIGNED_64 = (1n << 63n) - 1n;
const DIAGNOSTIC_TEXT_BYTES = 1_024;
const DIAGNOSTIC_CONTEXT_ENTRIES = 16;
const DIAGNOSTIC_CONTEXT_LIST_ITEMS = 32;
const DIAGNOSTIC_CONTEXT_KEY_BYTES = 128;
const DIAGNOSTIC_CONTEXT_DEPTH = 6;
const DIAGNOSTIC_CONTEXT_NODES = 512;

type FlowQueryResponseMap = Map<unknown, unknown> | Record<string, unknown>;

export function tryDecodeFlowQueryError(
  value: unknown,
  cause?: unknown
): FlowQueryError | undefined {
  try {
    const mapping = requiredMap(value, "FLOW.QUERY diagnostic");
    const context = decodeDiagnosticContext(field(mapping, "context"));
    const position = decodePosition(field(mapping, "position"));
    return new FlowQueryError({
      code: requiredBoundedText(
        mapping,
        "code",
        "FLOW.QUERY diagnostic",
        DIAGNOSTIC_TEXT_BYTES
      ),
      message: requiredBoundedText(
        mapping,
        "message",
        "FLOW.QUERY diagnostic",
        DIAGNOSTIC_TEXT_BYTES
      ),
      detail: optionalDiagnosticText(mapping, "detail"),
      hint: optionalDiagnosticText(mapping, "hint"),
      retryable: requiredBoolean(mapping, "retryable", "FLOW.QUERY diagnostic"),
      safeToRetry: requiredBoolean(mapping, "safe_to_retry", "FLOW.QUERY diagnostic"),
      retryAfterMs: nonNegativeInteger(
        field(mapping, "retry_after_ms"),
        "FLOW.QUERY diagnostic retry_after_ms"
      ),
      position,
      context,
      raw: value,
      cause
    });
  } catch (error) {
    if (error instanceof FerricStoreError) return undefined;
    throw error;
  }
}

function optionalDiagnosticText(
  mapping: FlowQueryResponseMap,
  name: string
): string | undefined {
  const value = optionalText(mapping, name, "FLOW.QUERY diagnostic");
  if (value != null && Buffer.byteLength(value, "utf8") > DIAGNOSTIC_TEXT_BYTES) {
    throw decodeError(
      `FLOW.QUERY diagnostic ${name} exceeds ${DIAGNOSTIC_TEXT_BYTES} bytes`,
      mapping
    );
  }
  return value;
}

function decodeDiagnosticContext(
  value: unknown
): Readonly<Record<string, unknown>> | undefined {
  if (value == null) return undefined;
  const mapping = requiredMap(value, "FLOW.QUERY diagnostic context");
  const size = mapping instanceof Map ? mapping.size : Object.keys(mapping).length;
  if (size > DIAGNOSTIC_CONTEXT_ENTRIES) {
    throw decodeError("FLOW.QUERY diagnostic context contains too many entries", value);
  }
  validateDiagnosticContextValue(
    mapping,
    DIAGNOSTIC_CONTEXT_DEPTH,
    { remaining: DIAGNOSTIC_CONTEXT_NODES }
  );
  return freezeMetadataMap(mapping, "FLOW.QUERY diagnostic context");
}

function validateDiagnosticContextValue(
  value: unknown,
  depth: number,
  budget: { remaining: number }
): void {
  consumeDiagnosticContextNode(value, budget);
  if (value == null || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (Number.isSafeInteger(value)) return;
    throw decodeError("FLOW.QUERY diagnostic context contains an invalid integer", value);
  }
  if (typeof value === "bigint") {
    if (value >= MIN_SIGNED_64 && value <= MAX_SIGNED_64) return;
    throw decodeError("FLOW.QUERY diagnostic context contains an invalid integer", value);
  }
  const text = diagnosticContextText(value);
  if (text != null) {
    if (Buffer.byteLength(text, "utf8") <= DIAGNOSTIC_TEXT_BYTES) return;
    throw decodeError("FLOW.QUERY diagnostic context contains oversized text", value);
  }
  if (depth <= 0) {
    throw decodeError("FLOW.QUERY diagnostic context exceeds its maximum depth", value);
  }
  if (Array.isArray(value)) {
    if (value.length > DIAGNOSTIC_CONTEXT_LIST_ITEMS) {
      throw decodeError("FLOW.QUERY diagnostic context list is too large", value);
    }
    for (let index = 0; index < value.length; index += 1) {
      if (!Object.hasOwn(value, index)) {
        throw decodeError("FLOW.QUERY diagnostic context list must be dense", value);
      }
      validateDiagnosticContextValue(value[index], depth - 1, budget);
    }
    return;
  }
  if (value instanceof Map || isDiagnosticContextRecord(value)) {
    const entries = value instanceof Map ? value.entries() : Object.entries(value);
    const size = value instanceof Map ? value.size : Object.keys(value).length;
    if (size > DIAGNOSTIC_CONTEXT_ENTRIES) {
      throw decodeError("FLOW.QUERY diagnostic context map is too large", value);
    }
    for (const [rawKey, item] of entries) {
      const key = diagnosticContextText(rawKey);
      if (
        key == null ||
        key.length === 0 ||
        Buffer.byteLength(key, "utf8") > DIAGNOSTIC_CONTEXT_KEY_BYTES
      ) {
        throw decodeError("FLOW.QUERY diagnostic context contains an invalid key", value);
      }
      validateDiagnosticContextValue(item, depth - 1, budget);
    }
    return;
  }
  throw decodeError("FLOW.QUERY diagnostic context contains an unsupported value", value);
}

function consumeDiagnosticContextNode(
  value: unknown,
  budget: { remaining: number }
): void {
  if (budget.remaining <= 0) {
    throw decodeError("FLOW.QUERY diagnostic context exceeds its node limit", value);
  }
  budget.remaining -= 1;
}

function diagnosticContextText(value: unknown): string | undefined {
  if (typeof value === "string") return value.isWellFormed() ? value : undefined;
  if (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) return undefined;
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(value);
  } catch {
    return undefined;
  }
}

function isDiagnosticContextRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value == null) return false;
  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
}

function decodePosition(value: unknown): FlowQueryErrorPosition | undefined {
  if (value == null) return undefined;
  const mapping = requiredMap(value, "FLOW.QUERY diagnostic position");
  return Object.freeze({
    byte: positiveInteger(field(mapping, "byte"), "FLOW.QUERY diagnostic position byte"),
    line: positiveInteger(field(mapping, "line"), "FLOW.QUERY diagnostic position line"),
    column: positiveInteger(
      field(mapping, "column"),
      "FLOW.QUERY diagnostic position column"
    )
  });
}
