import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import {
  COMMAND_OPCODES,
  COMPACT_BINARY_LIST_LIST,
  COMPACT_BINARY_MAP_LIST,
  COMPACT_FLOW_CANCEL_MANY_OK_REQUEST,
  COMPACT_FLOW_CANCEL_MANY_REQUEST,
  COMPACT_FLOW_CLAIM_DUE_REQUEST,
  COMPACT_FLOW_CLAIM_JOBS,
  COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
  COMPACT_FLOW_COMPLETE_MANY_REQUEST,
  COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST,
  COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST,
  COMPACT_FLOW_CREATE_MANY_REQUEST,
  COMPACT_FLOW_LIST_REQUEST,
  COMPACT_FLOW_RECORD,
  COMPACT_FLOW_RECORD_LIST,
  COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
  COMPACT_FLOW_RETRY_MANY_REQUEST,
  COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST,
  COMPACT_FLOW_TRANSITION_MANY_REQUEST,
  COMPACT_FLOW_VALUE_MGET_REQUEST,
  COMPACT_INTEGER_LIST,
  COMPACT_KV_GET,
  COMPACT_KV_MGET,
  COMPACT_KV_MGET_FIXED,
  COMPACT_OK_LIST,
  COMPACT_PIPELINE_REQUEST,
  COMPACT_PIPELINE_RESPONSE,
  DEFAULT_MAX_VALUE_DEPTH,
  DEFAULT_MAX_VALUE_ITEMS,
  FLAG_COMPRESSED,
  FLAG_CUSTOM_PAYLOAD,
  FLAG_MORE_CHUNKS,
  FLOW_RECORD_FIELD_KEYS,
  HEADER_SIZE,
  MAGIC,
  MAX_FRAMES_PER_DECODE,
  REQUEST_VERSION,
  RESPONSE_VERSION,
  STATUS_OK
} from "../src/protocol-constants.js";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const coreRoot = process.env.FERRICSTORE_CORE_DIR ?? resolve(repositoryRoot, "../ferricstore");
const commandsPath = resolve(
  coreRoot,
  "apps/ferricstore_server/lib/ferricstore_server/native/commands.ex"
);
const codecPath = resolve(
  coreRoot,
  "apps/ferricstore_server/lib/ferricstore_server/native/codec.ex"
);
const nifPath = resolve(
  coreRoot,
  "apps/ferricstore_server/native/native_protocol_nif/src/lib.rs"
);
const coreAvailable = [commandsPath, codecPath, nifPath].every(existsSync);

if (process.env.FERRICSTORE_CORE_REQUIRED === "1" && !coreAvailable) {
  throw new Error(`required FerricStore core native ABI sources were not found under ${coreRoot}`);
}

function commandOpcodes(source: string): Record<string, number> {
  const attributes = new Map(
    [...source.matchAll(/^ {2}@op_(\w+)\s+0x([\dA-Fa-f]+)$/gmu)].map((match) => [
      match[1] ?? "",
      Number.parseInt(match[2] ?? "", 16)
    ])
  );
  const entries = [...source.matchAll(/@op_(\w+)\s*=>\s*"([^"]+)"/gu)].map((match) => {
    const attribute = match[1] ?? "";
    const opcode = attributes.get(attribute);
    if (opcode == null) throw new Error(`core opcode attribute op_${attribute} was not found`);
    return [match[2] ?? "", opcode] as const;
  });
  if (entries.length !== attributes.size) {
    throw new Error(`core exposes ${attributes.size} opcode attributes but maps ${entries.length}`);
  }
  return Object.fromEntries(entries);
}

function compactTags(codec: string, nif: string): Record<string, number> {
  const entries = [...codec.matchAll(/^ {2}@compact_(\w+)\s+0x([\dA-Fa-f]+)$/gmu)].map(
    (match) => [match[1] ?? "", Number.parseInt(match[2] ?? "", 16)] as const
  );
  const fixed = /^const COMPACT_KV_MGET_FIXED: u8 = 0x([\dA-Fa-f]+);$/mu.exec(nif)?.[1];
  if (fixed == null) throw new Error("core compact KV MGET fixed tag was not found");
  entries.push(["kv_mget_fixed", Number.parseInt(fixed, 16)]);
  return Object.fromEntries(entries);
}

function flowRecordFieldKeys(source: string): string[] {
  const body = /@compact_flow_record_field_ids %\{([\s\S]*?)\n {2}\}/u.exec(source)?.[1];
  if (body == null) throw new Error("core compact Flow record field manifest was not found");
  const fields = [...body.matchAll(/"([^"]+)"\s*=>\s*(\d+)/gu)].map(
    (match) => [match[1] ?? "", Number.parseInt(match[2] ?? "", 10)] as const
  );
  const keys = Array.from({ length: Math.max(...fields.map(([, id]) => id)) + 1 }, () => "");
  for (const [key, id] of fields) keys[id] = key;
  return keys;
}

function numericLiteral(value: string): number {
  return value.startsWith("0x")
    ? Number.parseInt(value.slice(2), 16)
    : Number.parseInt(value.replaceAll("_", ""), 10);
}

function sourceNumber(source: string, expression: RegExp, name: string): number {
  const value = expression.exec(source)?.[1];
  if (value == null) throw new Error(`core native protocol constant ${name} was not found`);
  return numericLiteral(value);
}

function nativeProtocolManifest(codec: string, nif: string): Record<string, number | string> {
  const magic = /^const MAGIC:[^=]+= b"([^"]+)";$/mu.exec(nif)?.[1];
  if (magic == null) throw new Error("core native protocol MAGIC was not found");
  const version = sourceNumber(nif, /^const VERSION: u8 = (0x[\dA-Fa-f]+|[\d_]+);$/mu, "VERSION");
  const responseDirection = sourceNumber(
    nif,
    /^const RESPONSE_DIRECTION: u8 = (0x[\dA-Fa-f]+|[\d_]+);$/mu,
    "RESPONSE_DIRECTION"
  );
  return {
    defaultMaxValueDepth: sourceNumber(
      codec,
      /^ {2}@default_max_value_depth (0x[\dA-Fa-f]+|[\d_]+)$/mu,
      "default_max_value_depth"
    ),
    defaultMaxValueItems: sourceNumber(
      codec,
      /^ {2}@default_max_value_items (0x[\dA-Fa-f]+|[\d_]+)$/mu,
      "default_max_value_items"
    ),
    flagCompressed: sourceNumber(codec, /^ {2}@flag_compressed (0x[\dA-Fa-f]+|[\d_]+)$/mu, "flag_compressed"),
    flagCustomPayload: sourceNumber(
      codec,
      /^ {2}@flag_custom_payload (0x[\dA-Fa-f]+|[\d_]+)$/mu,
      "flag_custom_payload"
    ),
    flagMoreChunks: sourceNumber(codec, /^ {2}@flag_more_chunks (0x[\dA-Fa-f]+|[\d_]+)$/mu, "flag_more_chunks"),
    headerSize: sourceNumber(nif, /^const HEADER_SIZE: usize = (0x[\dA-Fa-f]+|[\d_]+);$/mu, "HEADER_SIZE"),
    magic,
    maxFramesPerDecode: sourceNumber(
      nif,
      /^const MAX_FRAMES_PER_DECODE: usize = (0x[\dA-Fa-f]+|[\d_]+);$/mu,
      "MAX_FRAMES_PER_DECODE"
    ),
    requestVersion: version,
    responseVersion: version | responseDirection,
    statusOk: sourceNumber(codec, /^ {4}ok: (0x[\dA-Fa-f]+|[\d_]+),$/mu, "status ok")
  };
}

const sdkCompactTags = {
  binary_list_list: COMPACT_BINARY_LIST_LIST,
  binary_map_list: COMPACT_BINARY_MAP_LIST,
  flow_cancel_many_ok_request: COMPACT_FLOW_CANCEL_MANY_OK_REQUEST,
  flow_cancel_many_request: COMPACT_FLOW_CANCEL_MANY_REQUEST,
  flow_claim_due_request: COMPACT_FLOW_CLAIM_DUE_REQUEST,
  flow_claim_jobs: COMPACT_FLOW_CLAIM_JOBS,
  flow_complete_many_ok_request: COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
  flow_complete_many_request: COMPACT_FLOW_COMPLETE_MANY_REQUEST,
  flow_create_many_mixed_request: COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST,
  flow_create_many_partition_request: COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST,
  flow_create_many_request: COMPACT_FLOW_CREATE_MANY_REQUEST,
  flow_list_request: COMPACT_FLOW_LIST_REQUEST,
  flow_record: COMPACT_FLOW_RECORD,
  flow_record_list: COMPACT_FLOW_RECORD_LIST,
  flow_retry_many_ok_request: COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
  flow_retry_many_request: COMPACT_FLOW_RETRY_MANY_REQUEST,
  flow_transition_many_ok_request: COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST,
  flow_transition_many_request: COMPACT_FLOW_TRANSITION_MANY_REQUEST,
  flow_value_mget_request: COMPACT_FLOW_VALUE_MGET_REQUEST,
  integer_list: COMPACT_INTEGER_LIST,
  kv_get: COMPACT_KV_GET,
  kv_mget: COMPACT_KV_MGET,
  kv_mget_fixed: COMPACT_KV_MGET_FIXED,
  ok_list: COMPACT_OK_LIST,
  pipeline_request: COMPACT_PIPELINE_REQUEST,
  pipeline_response: COMPACT_PIPELINE_RESPONSE
};

describe.skipIf(!coreAvailable)("FerricStore core native ABI parity", () => {
  const commands = coreAvailable ? readFileSync(commandsPath, "utf8") : "";
  const codec = coreAvailable ? readFileSync(codecPath, "utf8") : "";
  const nif = coreAvailable ? readFileSync(nifPath, "utf8") : "";

  it("matches the complete native command opcode table", () => {
    expect(COMMAND_OPCODES).toEqual(commandOpcodes(commands));
  });

  it("matches every compact wire tag", () => {
    expect(sdkCompactTags).toEqual(compactTags(codec, nif));
  });

  it("matches every compact Flow record field ID", () => {
    expect(FLOW_RECORD_FIELD_KEYS).toEqual(flowRecordFieldKeys(codec));
  });

  it("matches framing, flags, status, value limits, and the decode turn budget", () => {
    expect({
      defaultMaxValueDepth: DEFAULT_MAX_VALUE_DEPTH,
      defaultMaxValueItems: DEFAULT_MAX_VALUE_ITEMS,
      flagCompressed: FLAG_COMPRESSED,
      flagCustomPayload: FLAG_CUSTOM_PAYLOAD,
      flagMoreChunks: FLAG_MORE_CHUNKS,
      headerSize: HEADER_SIZE,
      magic: MAGIC,
      maxFramesPerDecode: MAX_FRAMES_PER_DECODE,
      requestVersion: REQUEST_VERSION,
      responseVersion: RESPONSE_VERSION,
      statusOk: STATUS_OK
    }).toEqual(nativeProtocolManifest(codec, nif));
  });
});
