import { COMMAND_OPCODES, type ResponseDecodeHints } from "../src/protocol.js";

/** Test-only server advertisement used by unit fixtures that emit compact bodies. */
export const compactResponseHints: ResponseDecodeHints = {
  compactResponseOpcodes: new Map<string, ReadonlySet<number>>([
    ["flow_claim_jobs_v1", new Set([COMMAND_OPCODES["FLOW.CLAIM_DUE"]])],
    ["flow_record_list_v1", new Set([
      COMMAND_OPCODES["FLOW.LIST"],
      COMMAND_OPCODES["FLOW.TERMINALS"],
      COMMAND_OPCODES["FLOW.FAILURES"],
      COMMAND_OPCODES["FLOW.BY_PARENT"],
      COMMAND_OPCODES["FLOW.BY_ROOT"],
      COMMAND_OPCODES["FLOW.BY_CORRELATION"],
      COMMAND_OPCODES["FLOW.STUCK"]
    ])],
    ["flow_record_v1", new Set([COMMAND_OPCODES["FLOW.GET"]])],
    ["kv_get_v1", new Set([COMMAND_OPCODES.GET])],
    ["kv_mget_v1", new Set([COMMAND_OPCODES.MGET, COMMAND_OPCODES["FLOW.VALUE.MGET"]])],
    ["ok_list_v1", new Set([
      COMMAND_OPCODES.SET,
      COMMAND_OPCODES.MSET,
      COMMAND_OPCODES.PIPELINE,
      COMMAND_OPCODES["FLOW.CREATE_MANY"],
      COMMAND_OPCODES["FLOW.COMPLETE_MANY"],
      COMMAND_OPCODES["FLOW.RETRY_MANY"],
      COMMAND_OPCODES["FLOW.FAIL_MANY"],
      COMMAND_OPCODES["FLOW.CANCEL_MANY"]
    ])],
    ["pipeline_v1", new Set([COMMAND_OPCODES.PIPELINE])]
  ])
};
