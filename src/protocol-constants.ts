import { Buffer } from "node:buffer";
import { FerricStoreError } from "./errors.js";
import type { CompactResponseOpcodes } from "./native-negotiation.js";
import nativeProtocol from "./native-protocol-manifest.json" with { type: "json" };

export const MAGIC = nativeProtocol.magic;
export const REQUEST_VERSION = nativeProtocol.requestVersion;
export const RESPONSE_VERSION = nativeProtocol.responseVersion;
export const HEADER_SIZE = nativeProtocol.headerSize;
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

export const FLAG_CUSTOM_PAYLOAD = nativeProtocol.flagCustomPayload;
export const FLAG_COMPRESSED = nativeProtocol.flagCompressed;
export const FLAG_MORE_CHUNKS = nativeProtocol.flagMoreChunks;

export const STATUS_OK = nativeProtocol.statusOk;
export const DEFAULT_MAX_VALUE_ITEMS = nativeProtocol.defaultMaxValueItems;
export const DEFAULT_MAX_VALUE_DEPTH = nativeProtocol.defaultMaxValueDepth;
export const MAX_FRAMES_PER_DECODE = nativeProtocol.maxFramesPerDecode;

export interface DecodeValueOptions {
  readonly maxDepth?: number;
  readonly maxItems?: number;
}

export interface DecodeValueLimits {
  readonly maxDepth: number;
  readonly maxItems: number;
}

export interface DecodeValueBudget {
  remainingItems: number;
}

export interface EncodeValueBudget extends DecodeValueBudget {
  readonly maxBytes: number;
  remainingBytes: number;
}

export type EncodeValuePlan =
  | { readonly byteLength: 1; readonly tag: 0 | 1 | 2 }
  | { readonly byteLength: 9; readonly tag: 3; readonly value: bigint }
  | { readonly byteLength: 9; readonly tag: 7; readonly value: number }
  | {
      readonly byteLength: number;
      readonly tag: 4;
      readonly value: string | Buffer | Uint8Array;
      readonly valueByteLength: number;
    }
  | { readonly byteLength: number; readonly items: readonly EncodeValuePlan[]; readonly tag: 5 }
  | { readonly byteLength: number; readonly entries: readonly EncodeMapEntryPlan[]; readonly tag: 6 };

export interface EncodeMapEntryPlan {
  readonly key: string;
  readonly keyByteLength: number;
  readonly value: EncodeValuePlan;
}

export class RequestFrameTooLargeError extends FerricStoreError {}

export const COMMAND_OPCODES = {
  "HELLO": 0x0001,
  "AUTH": 0x0002,
  "PING": 0x0003,
  "CLIENT.SETNAME": 0x0004,
  "CLIENT.INFO": 0x0005,
  "ROUTE": 0x0006,
  "SHARDS": 0x0007,
  "BACKPRESSURE": 0x0008,
  "QUIT": 0x0009,
  "GOAWAY": 0x000a,
  "OPTIONS": 0x000b,
  "STARTUP": 0x000c,
  "WINDOW_UPDATE": 0x000d,
  "PIPELINE": 0x000e,
  "ROUTE_BATCH": 0x000f,
  "EVENT": 0x0010,
  "SUBSCRIBE_EVENTS": 0x0011,
  "UNSUBSCRIBE_EVENTS": 0x0012,
  "COMMAND_EXEC": 0x0100,
  "GET": 0x0101,
  "SET": 0x0102,
  "DEL": 0x0103,
  "MGET": 0x0104,
  "MSET": 0x0105,
  "CAS": 0x0106,
  "LOCK": 0x0107,
  "UNLOCK": 0x0108,
  "EXTEND": 0x0109,
  "RATELIMIT.ADD": 0x010a,
  "FETCH_OR_COMPUTE": 0x010b,
  "FETCH_OR_COMPUTE_RESULT": 0x010c,
  "FETCH_OR_COMPUTE_ERROR": 0x010d,
  "HSET": 0x0110,
  "HGET": 0x0111,
  "HMGET": 0x0112,
  "HGETALL": 0x0113,
  "LPUSH": 0x0120,
  "RPUSH": 0x0121,
  "LPOP": 0x0122,
  "RPOP": 0x0123,
  "LRANGE": 0x0124,
  "SADD": 0x0130,
  "SREM": 0x0131,
  "SMEMBERS": 0x0132,
  "SISMEMBER": 0x0133,
  "ZADD": 0x0140,
  "ZREM": 0x0141,
  "ZRANGE": 0x0142,
  "ZSCORE": 0x0143,
  "FLOW.CREATE": 0x0201,
  "FLOW.GET": 0x0202,
  "FLOW.CLAIM_DUE": 0x0203,
  "FLOW.COMPLETE": 0x0204,
  "FLOW.TRANSITION": 0x0205,
  "FLOW.RETRY": 0x0206,
  "FLOW.FAIL": 0x0207,
  "FLOW.CANCEL": 0x0208,
  "FLOW.EXTEND_LEASE": 0x0209,
  "FLOW.HISTORY": 0x020a,
  "FLOW.VALUE.PUT": 0x020b,
  "FLOW.VALUE.MGET": 0x020c,
  "FLOW.SIGNAL": 0x020d,
  "FLOW.LIST": 0x020e,
  "FLOW.CREATE_MANY": 0x020f,
  "FLOW.COMPLETE_MANY": 0x0210,
  "FLOW.TRANSITION_MANY": 0x0211,
  "FLOW.RETRY_MANY": 0x0212,
  "FLOW.FAIL_MANY": 0x0213,
  "FLOW.CANCEL_MANY": 0x0214,
  "FLOW.RECLAIM": 0x0215,
  "FLOW.REWIND": 0x0216,
  "FLOW.TERMINALS": 0x0217,
  "FLOW.FAILURES": 0x0218,
  "FLOW.BY_PARENT": 0x0219,
  "FLOW.BY_ROOT": 0x021a,
  "FLOW.BY_CORRELATION": 0x021b,
  "FLOW.INFO": 0x021c,
  "FLOW.STUCK": 0x021d,
  "FLOW.POLICY.SET": 0x021e,
  "FLOW.POLICY.GET": 0x021f,
  "FLOW.SPAWN_CHILDREN": 0x0220,
  "FLOW.RETENTION_CLEANUP": 0x0221,
  "FLOW.STEP_CONTINUE": 0x0222,
  "FLOW.START_AND_CLAIM": 0x0223,
  "FLOW.RUN_STEPS_MANY": 0x0224,
  "FLOW.SCHEDULE.CREATE": 0x0225,
  "FLOW.SCHEDULE.GET": 0x0226,
  "FLOW.SCHEDULE.DELETE": 0x0227,
  "FLOW.SCHEDULE.FIRE_DUE": 0x0228,
  "FLOW.SCHEDULE.LIST": 0x0229,
  "FLOW.SCHEDULE.FIRE": 0x022a,
  "FLOW.SCHEDULE.PAUSE": 0x022b,
  "FLOW.SCHEDULE.RESUME": 0x022c,
  "FLOW.STATS": 0x022d,
  "FLOW.ATTRIBUTES": 0x022e,
  "FLOW.ATTRIBUTE_VALUES": 0x022f,
  "FLOW.SEARCH": 0x0230,
  "FLOW.EFFECT.RESERVE": 0x0240,
  "FLOW.EFFECT.CONFIRM": 0x0241,
  "FLOW.EFFECT.FAIL": 0x0242,
  "FLOW.EFFECT.COMPENSATE": 0x0243,
  "FLOW.EFFECT.GET": 0x0244,
  "FLOW.GOVERNANCE.LEDGER": 0x0245,
  "FLOW.APPROVAL.REQUEST": 0x0246,
  "FLOW.APPROVAL.APPROVE": 0x0247,
  "FLOW.APPROVAL.REJECT": 0x0248,
  "FLOW.APPROVAL.GET": 0x0249,
  "FLOW.CIRCUIT.OPEN": 0x024a,
  "FLOW.CIRCUIT.CLOSE": 0x024b,
  "FLOW.CIRCUIT.GET": 0x024c,
  "FLOW.BUDGET.RESERVE": 0x024d,
  "FLOW.BUDGET.GET": 0x024e,
  "FLOW.LIMIT.LEASE": 0x024f,
  "FLOW.LIMIT.SPEND": 0x0250,
  "FLOW.LIMIT.RELEASE": 0x0251,
  "FLOW.LIMIT.GET": 0x0252,
  "FLOW.APPROVAL.LIST": 0x0253,
  "FLOW.GOVERNANCE.OVERVIEW": 0x0254,
  "FLOW.BUDGET.LIST": 0x0255,
  "FLOW.LIMIT.LIST": 0x0256,
  "FLOW.BUDGET.COMMIT": 0x0257,
  "FLOW.BUDGET.RELEASE": 0x0258,
  "CLUSTER.HEALTH": 0x0301,
  "CLUSTER.STATS": 0x0302,
  "CLUSTER.KEYSLOT": 0x0303,
  "CLUSTER.SLOTS": 0x0304,
  "CLUSTER.STATUS": 0x0305,
  "CLUSTER.JOIN": 0x0306,
  "CLUSTER.LEAVE": 0x0307,
  "CLUSTER.FAILOVER": 0x0308,
  "CLUSTER.PROMOTE": 0x0309,
  "CLUSTER.DEMOTE": 0x030a,
  "CLUSTER.ROLE": 0x030b,
  "FERRICSTORE.KEY_INFO": 0x030c,
  "FERRICSTORE.CONFIG": 0x030d,
  "FERRICSTORE.HOTNESS": 0x030e,
  "FERRICSTORE.METRICS": 0x030f,
  "FERRICSTORE.BLOBGC": 0x0310
} as const;

export const OPCODES = {
  startup: COMMAND_OPCODES.STARTUP,
  auth: COMMAND_OPCODES.AUTH,
  ping: COMMAND_OPCODES.PING,
  clientSetName: COMMAND_OPCODES["CLIENT.SETNAME"],
  clientInfo: COMMAND_OPCODES["CLIENT.INFO"],
  route: COMMAND_OPCODES.ROUTE,
  routeBatch: COMMAND_OPCODES.ROUTE_BATCH,
  shards: COMMAND_OPCODES.SHARDS,
  backpressure: COMMAND_OPCODES.BACKPRESSURE,
  goaway: COMMAND_OPCODES.GOAWAY,
  event: COMMAND_OPCODES.EVENT,
  windowUpdate: COMMAND_OPCODES.WINDOW_UPDATE,
  subscribeEvents: COMMAND_OPCODES.SUBSCRIBE_EVENTS,
  unsubscribeEvents: COMMAND_OPCODES.UNSUBSCRIBE_EVENTS,
  options: COMMAND_OPCODES.OPTIONS,
  quit: COMMAND_OPCODES.QUIT,
  commandExec: COMMAND_OPCODES.COMMAND_EXEC,
  get: COMMAND_OPCODES.GET,
  set: COMMAND_OPCODES.SET,
  del: COMMAND_OPCODES.DEL,
  mget: COMMAND_OPCODES.MGET,
  mset: COMMAND_OPCODES.MSET,
  pipeline: COMMAND_OPCODES.PIPELINE,
  flowCreate: COMMAND_OPCODES["FLOW.CREATE"],
  flowGet: COMMAND_OPCODES["FLOW.GET"],
  flowClaimDue: COMMAND_OPCODES["FLOW.CLAIM_DUE"],
  flowComplete: COMMAND_OPCODES["FLOW.COMPLETE"],
  flowTransition: COMMAND_OPCODES["FLOW.TRANSITION"],
  flowRetry: COMMAND_OPCODES["FLOW.RETRY"],
  flowFail: COMMAND_OPCODES["FLOW.FAIL"],
  flowValuePut: COMMAND_OPCODES["FLOW.VALUE.PUT"],
  flowValueMGet: COMMAND_OPCODES["FLOW.VALUE.MGET"],
  flowSignal: COMMAND_OPCODES["FLOW.SIGNAL"],
  flowCreateMany: COMMAND_OPCODES["FLOW.CREATE_MANY"],
  flowCompleteMany: COMMAND_OPCODES["FLOW.COMPLETE_MANY"],
  flowTransitionMany: COMMAND_OPCODES["FLOW.TRANSITION_MANY"],
  flowRetryMany: COMMAND_OPCODES["FLOW.RETRY_MANY"],
  flowFailMany: COMMAND_OPCODES["FLOW.FAIL_MANY"],
  flowReclaim: COMMAND_OPCODES["FLOW.RECLAIM"],
  flowPolicySet: COMMAND_OPCODES["FLOW.POLICY.SET"],
  flowPolicyGet: COMMAND_OPCODES["FLOW.POLICY.GET"],
  flowSpawnChildren: COMMAND_OPCODES["FLOW.SPAWN_CHILDREN"],
  flowStepContinue: COMMAND_OPCODES["FLOW.STEP_CONTINUE"],
  flowStartAndClaim: COMMAND_OPCODES["FLOW.START_AND_CLAIM"],
  flowRunStepsMany: COMMAND_OPCODES["FLOW.RUN_STEPS_MANY"],
  flowSearch: COMMAND_OPCODES["FLOW.SEARCH"]
} as const;

export const COMPACT_FLOW_CLAIM_JOBS = 0x80;
export const COMPACT_OK_LIST = 0x81;
export const COMPACT_KV_GET = 0x82;
export const COMPACT_KV_MGET = 0x83;
export const COMPACT_FLOW_RECORD = 0x84;
export const COMPACT_FLOW_RECORD_LIST = 0x85;
export const COMPACT_BINARY_LIST_LIST = 0x86;
export const COMPACT_BINARY_MAP_LIST = 0x87;
export const COMPACT_INTEGER_LIST = 0x88;
export const COMPACT_KV_MGET_FIXED = 0x89;
export const COMPACT_FLOW_CREATE_MANY_REQUEST = 0x90;
export const COMPACT_FLOW_CLAIM_DUE_REQUEST = 0x91;
export const COMPACT_FLOW_COMPLETE_MANY_REQUEST = 0x92;
export const COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST = 0x93;
export const COMPACT_PIPELINE_REQUEST = 0x94;
export const COMPACT_PIPELINE_RESPONSE = 0x95;
export const COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST = 0x96;
export const COMPACT_FLOW_RETRY_MANY_REQUEST = 0x97;
export const COMPACT_FLOW_RETRY_MANY_OK_REQUEST = 0x98;
export const COMPACT_FLOW_CANCEL_MANY_REQUEST = 0x99;
export const COMPACT_FLOW_CANCEL_MANY_OK_REQUEST = 0x9a;
export const COMPACT_FLOW_TRANSITION_MANY_REQUEST = 0x9b;
export const COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST = 0x9c;
export const COMPACT_FLOW_VALUE_MGET_REQUEST = 0x9d;
export const COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST = 0x9e;
export const COMPACT_FLOW_LIST_REQUEST = 0x9f;
export const COMPACT_PIPELINE_DECODED = Symbol("ferricstore.compactPipelineDecoded");
export const NULL_U32 = 0xffff_ffff;
export const MIN_I64 = -9_223_372_036_854_775_808n;
export const MAX_I64 = 9_223_372_036_854_775_807n;
export const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
export const MIN_SAFE_INTEGER_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

export const FLOW_RECORD_FIELD_KEYS = [
  "",
  "id",
  "type",
  "state",
  "version",
  "priority",
  "partition_key",
  "payload_ref",
  "result_ref",
  "error_ref",
  "payload",
  "result",
  "error",
  "created_at_ms",
  "updated_at_ms",
  "next_run_at_ms",
  "lease_deadline_ms",
  "lease_owner",
  "lease_token",
  "fencing_token",
  "attempts",
  "history_max_events",
  "history_hot_max_events",
  "child_groups",
  "parent_flow_id",
  "parent_partition_key",
  "root_flow_id",
  "correlation_id",
  "terminal_retention_until_ms",
  "ttl_ms",
  "retention_ttl_ms",
  "run_state",
  "value_refs",
  "values",
  "payload_omitted",
  "payload_size",
  "result_omitted",
  "result_size",
  "error_omitted",
  "error_size",
  "max_attempts",
  "attributes"
] as const;

export interface ProtocolCommand {
  readonly opcode: number;
  readonly payload: unknown;
  readonly flags?: number;
  readonly laneId?: number;
  /** @internal Correlated compact claim shape used to decode a direct response without guessing. */
  readonly compactClaimMode?: CompactClaimMode;
  /** @internal Correlated compact claim shapes for native pipeline response items. */
  readonly pipelineClaimModes?: readonly (CompactClaimMode | undefined)[];
  /** @internal Parsed routing metadata retained when a command uses COMMAND_EXEC. */
  readonly routing?: ProtocolRoutingHints;
  /** @internal Server-side blocking duration used to derive a safe response timeout. */
  readonly serverBlockMs?: number;
}

export type CompactClaimMode = "base" | "attrs" | "state" | "stateAttrs";

export interface ResponseDecodeHints {
  readonly compactClaimMode?: CompactClaimMode;
  readonly compactResponseOpcodes?: CompactResponseOpcodes;
  readonly pipelineClaimModes?: readonly (CompactClaimMode | undefined)[];
}

export interface ProtocolRoutingHints {
  readonly flowPartitionKey?: unknown;
  readonly flowPartitionKeys?: readonly unknown[];
}

export interface ResponseFrame {
  readonly flags: number;
  readonly laneId: number;
  readonly opcode: number;
  readonly requestId: bigint;
  readonly bodyLength: number;
  readonly body: Buffer;
}
