import type { CommandArgument } from "./internal.js";
import { splitFlowValueMGetArguments } from "./command-grammar.js";
import { denseCommandArgumentTail } from "./protocol-array-validation.js";
import * as core from "./protocol-core.js";
import * as wire from "./protocol-constants.js";
import { parseFlowOptions } from "./protocol-flow-options.js";
export * from "./protocol-flow-options.js";
export * from "./protocol-flow-admin.js";
import {
  compactFlowClaimDuePayload,
  compactFlowCompleteManyPayload,
  compactFlowCreateManyPayload,
  compactFlowRetryManyPayload,
  compactFlowTransitionManyPayload
} from "./protocol-flow-compact.js";
export {
  compactFlowCancelManyPayload,
  compactFlowClaimDuePayload,
  compactFlowCompleteManyPayload,
  compactFlowCreateManyPayload,
  compactFlowRetryManyPayload,
  compactFlowTransitionManyPayload,
  compactManyRequestTag
} from "./protocol-flow-compact.js";
import { compactFlowValueMGetPayload } from "./protocol-flow-compact-single.js";
export { compactFlowValueMGetPayload } from "./protocol-flow-compact-single.js";
import {
  findItemToken,
  parseFlowCreateItemsExt,
  parseFlowSpawnChildren,
  parseFlowSpawnChildrenExt
} from "./protocol-flow-items.js";
export * from "./protocol-flow-items.js";
import {
  expandFlowClaimStates,
  withFlowPartitionRouting
} from "./protocol-flow-routing.js";
export * from "./protocol-flow-routing.js";

export function flowCreatePayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 7) return undefined;
  const id = args[0];
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set([
      "TYPE",
      "STATE",
      "NOW",
      "PARTITION",
      "PAYLOAD",
      "PARENT_FLOW_ID",
      "ROOT_FLOW_ID",
      "CORRELATION_ID",
      "RUN_AT",
      "PRIORITY",
      "IDEMPOTENT",
      "MAX_ACTIVE_MS",
      "RETENTION_TTL_MS",
      "ATTRIBUTE",
      "STATE_META",
      "VALUE",
      "VALUE_REF"
    ]),
    payloadValue: true,
    required: new Set(["TYPE", "STATE", "NOW"])
  });
  if (options == null) return undefined;
  return { opcode: wire.OPCODES.flowCreate, payload: { id, ...options } };
}

export function flowGetPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 1) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set(["PARTITION", "FULL", "PAYLOAD", "NOPAYLOAD", "MAXBYTES", "VALUE", "VALUE_MAX_BYTES"]),
    readValues: true
  });
  if (options == null) return undefined;
  return { opcode: wire.OPCODES.flowGet, payload: { id: args[0], ...options } };
}

export function flowValuePutPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 1) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set(["NOW", "PARTITION", "OWNER_FLOW_ID", "NAME", "OVERRIDE", "TTL"])
  });
  if (options == null) return undefined;
  return { opcode: wire.OPCODES.flowValuePut, payload: { value: args[0], ...options } };
}

export function flowValueMGetPayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number,
  allowCompact: boolean
): wire.ProtocolCommand | undefined {
  if (args.length === 0) return undefined;
  const { hasMaxBytes, maxBytes, refs } = splitFlowValueMGetArguments(args, 0);
  if (refs.length === 0) return undefined;
  const payload: Record<string, unknown> = { refs };
  if (hasMaxBytes) {
    core.setOwnValue(payload, "max_bytes", core.safeIntegerNumberArg(maxBytes));
  }
  if (allowCompact) {
    const compact = compactFlowValueMGetPayload(refs, payload.max_bytes, maxBodyBytes);
    if (compact != null) return compact;
  }
  return { opcode: wire.OPCODES.flowValueMGet, payload };
}

export function flowCreateManyPayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number,
  allowCompact: boolean
): wire.ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const partition = core.asText(args[0]);
  const itemsIndex = findItemToken(args, 1);
  if (itemsIndex < 0) return undefined;

  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed: new Set([
      "TYPE", "STATE", "NOW", "RUN_AT", "PRIORITY", "IDEMPOTENT", "INDEPENDENT",
      "MAX_ACTIVE_MS", "RETENTION_TTL_MS", "ATTRIBUTE", "STATE_META", "VALUE", "VALUE_REF"
    ]),
    required: new Set(["TYPE"])
  });
  if (options == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const auto = ["AUTO", "NONE"].includes(partition.toUpperCase());
  const itemToken = core.asText(args[itemsIndex]).toUpperCase();

  if (itemToken === "ITEMS_EXT") {
    const count = core.safeIntegerNumberArg(args[itemsIndex + 1]);
    if (count < 0) return undefined;
    const items = parseFlowCreateItemsExt(denseCommandArgumentTail(args, itemsIndex + 2, "ITEMS_EXT"), count, mixed);
    if (items == null) return undefined;
    const payload: Record<string, unknown> = { ...options, items };
    if (!auto && !mixed) payload.partition_key = partition;
    return { opcode: wire.OPCODES.flowCreateMany, payload };
  }

  const rawItems = denseCommandArgumentTail(args, itemsIndex + 1, "ITEMS");
  const width = mixed ? 3 : 2;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;

  if (allowCompact) {
    const compact = compactFlowCreateManyPayload(
      partition,
      rawItems,
      mixed,
      auto,
      options,
      maxBodyBytes
    );
    if (compact != null) return compact;
  }

  const items: unknown[][] = [];
  for (let index = 0; index < rawItems.length; index += width) {
    if (mixed) {
      items.push([rawItems[index], rawItems[index + 1], rawItems[index + 2]]);
    } else {
      items.push([rawItems[index], rawItems[index + 1]]);
    }
  }

  const payload: Record<string, unknown> = { ...options, items };
  if (!auto && !mixed) payload.partition_key = partition;
  return { opcode: wire.OPCODES.flowCreateMany, payload };
}

export function flowClaimDuePayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number,
  allowCompact: boolean
): wire.ProtocolCommand | undefined {
  if (args.length < 1) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set([
      "STATE",
      "STATES",
      "PARTITION",
      "PARTITIONS",
      "WORKER",
      "LEASE_MS",
      "LIMIT",
      "PRIORITY",
      "NOW",
      "BLOCK",
      "BLOCK_MS",
      "RECLAIM_EXPIRED",
      "RECLAIM_RATIO",
      "RETURN",
      "PAYLOAD",
      "NOPAYLOAD",
      "MAXBYTES",
      "INCLUDE_STATE",
      "VALUE",
      "VALUE_MAX_BYTES"
    ]),
    readValues: true
  });
  if (options == null) return undefined;
  const returnMode = options.return == null ? "" : core.asText(options.return).toUpperCase();
  if (!returnMode.startsWith("JOBS_COMPACT")) {
    const fallback = Array.isArray(options.states)
      ? core.commandExec(["FLOW.CLAIM_DUE", ...expandFlowClaimStates(args)])
      : core.commandExec(["FLOW.CLAIM_DUE", ...args]);
    return withFlowPartitionRouting(fallback, options);
  }
  const compact = allowCompact
    ? compactFlowClaimDuePayload(args[0], options, maxBodyBytes)
    : undefined;
  const compactClaimMode = core.compactClaimResponseMode(options.return);
  const serverBlockMs = typeof options.block_ms === "number" && options.block_ms >= 0
    ? options.block_ms
    : undefined;
  const metadata = {
    ...(compactClaimMode == null ? {} : { compactClaimMode }),
    ...(serverBlockMs == null ? {} : { serverBlockMs })
  };
  if (compact != null) return { ...compact, ...metadata };
  return { opcode: wire.OPCODES.flowClaimDue, payload: { ...options, type: args[0] }, ...metadata };
}

export function flowReclaimPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 1) return undefined;
  const options = parseFlowOptions(args, 1, args.length, {
    allowed: new Set([
      "STATE", "PARTITION", "PARTITIONS", "WORKER", "LEASE_MS", "LIMIT", "PRIORITY", "NOW",
      "RETURN", "PAYLOAD", "NOPAYLOAD", "MAXBYTES", "INCLUDE_STATE", "VALUE", "VALUE_MAX_BYTES"
    ]),
    readValues: true
  });
  if (options == null) return undefined;
  return { opcode: wire.OPCODES.flowReclaim, payload: { ...options, type: args[0] } };
}

export function flowCompletePayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  return flowLeaseMutationPayload(wire.OPCODES.flowComplete, args, new Set([
    "FENCING", "NOW", "PARTITION", "RESULT", "PAYLOAD", "TTL", "STATE_META",
    "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
  ]));
}

export function flowTransitionPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 3) return undefined;
  const options = parseFlowOptions(args, 3, args.length, {
    allowed: new Set([
      "LEASE_TOKEN", "FENCING", "NOW", "PARTITION", "PAYLOAD", "RUN_AT", "PRIORITY",
      "STATE_META", "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE",
      "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
    ]),
    payloadValue: true,
    required: new Set(["LEASE_TOKEN", "FENCING", "NOW"])
  });
  if (options == null) return undefined;
  return {
    opcode: wire.OPCODES.flowTransition,
    payload: { id: args[0], from_state: args[1], to_state: args[2], ...options }
  };
}

export function flowLeaseMutationPayload(
  opcode: number,
  args: readonly CommandArgument[],
  allowed: ReadonlySet<string>
): wire.ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const options = parseFlowOptions(args, 2, args.length, {
    allowed,
    payloadValue: true,
    required: new Set(["FENCING", "NOW"])
  });
  if (options == null) return undefined;
  return { opcode, payload: { id: args[0], lease_token: args[1], ...options } };
}

export function flowClaimedManyPayload(
  command: "FLOW.COMPLETE_MANY" | "FLOW.RETRY_MANY" | "FLOW.FAIL_MANY",
  opcode: number,
  args: readonly CommandArgument[],
  maxBodyBytes = Number.MAX_SAFE_INTEGER,
  allowCompact = true
): wire.ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const partition = core.asText(args[0]);
  const itemsIndex = findItemToken(args, 1);
  if (itemsIndex < 0) return undefined;

  const allowed = new Set([
    "NOW", "INDEPENDENT", "PAYLOAD", "STATE_META", "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE",
    "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE"
  ]);
  if (command === "FLOW.COMPLETE_MANY") {
    allowed.add("RESULT");
    allowed.add("TTL");
  } else {
    allowed.add("ERROR");
    if (command === "FLOW.RETRY_MANY") allowed.add("RUN_AT");
    if (command === "FLOW.FAIL_MANY") allowed.add("TTL");
  }
  allowed.add("RETURN");
  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed,
    payloadValue: true
  });
  if (options == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const auto = partition.toUpperCase() === "AUTO";
  const rawItems = denseCommandArgumentTail(args, itemsIndex + 1, "ITEMS");
  const width = mixed ? 4 : 3;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;

  if (allowCompact) {
    const compact = command === "FLOW.RETRY_MANY"
      ? compactFlowRetryManyPayload(partition, rawItems, mixed, auto, options, maxBodyBytes)
      : compactFlowCompleteManyPayload(opcode, partition, rawItems, mixed, auto, options, maxBodyBytes);
    if (compact != null) return compact;
  }

  const items: unknown[][] = [];
  for (let index = 0; index < rawItems.length; index += width) {
    if (mixed) {
      items.push([rawItems[index], rawItems[index + 1], rawItems[index + 2], rawItems[index + 3]]);
    } else {
      items.push([rawItems[index], rawItems[index + 1], rawItems[index + 2]]);
    }
  }

  const payload: Record<string, unknown> = { ...options, items };
  if (!auto && !mixed) payload.partition_key = partition;
  return { opcode, payload };
}

export function flowTransitionManyPayload(
  args: readonly CommandArgument[],
  maxBodyBytes: number,
  allowCompact: boolean
): wire.ProtocolCommand | undefined {
  if (args.length < 4) return undefined;
  const partition = core.asText(args[0]);
  const itemsIndex = findItemToken(args, 3);
  if (itemsIndex < 0) return undefined;
  const options = parseFlowOptions(args, 3, itemsIndex, {
    allowed: new Set([
      "PAYLOAD", "RUN_AT", "PRIORITY", "NOW", "INDEPENDENT", "STATE_META",
      "VALUE", "VALUE_REF", "DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_MERGE", "ATTRIBUTE_DELETE",
      "RETURN"
    ]),
    payloadValue: true
  });
  if (options == null) return undefined;

  const mixed = partition.toUpperCase() === "MIXED";
  const rawItems = denseCommandArgumentTail(args, itemsIndex + 1, "ITEMS");
  const width = mixed ? 4 : 3;
  if (rawItems.length === 0 || rawItems.length % width !== 0) return undefined;
  if (allowCompact) {
    const compact = core.isCompactBinaryArgument(args[0])
      && core.isCompactBinaryArgument(args[1])
      && core.isCompactBinaryArgument(args[2])
      ? compactFlowTransitionManyPayload(
          args[0],
          args[1],
          args[2],
          rawItems,
          mixed,
          options,
          maxBodyBytes
      )
      : undefined;
    if (compact != null) return compact;
  }
  const items: Record<string, unknown>[] = [];
  for (let index = 0; index < rawItems.length; index += width) {
    const item: Record<string, unknown> = {
      id: rawItems[index],
      fencing_token: rawItems[index + (mixed ? 2 : 1)]
    };
    if (mixed) item.partition_key = rawItems[index + 1];
    const leaseToken = rawItems[index + (mixed ? 3 : 2)];
    if (!core.commandTokenIs(leaseToken, "-")) item.lease_token = leaseToken;
    items.push(item);
  }

  const payload: Record<string, unknown> = {
    from_state: args[1],
    to_state: args[2],
    ...options,
    items
  };
  if (!mixed) payload.partition_key = args[0];
  return { opcode: wire.OPCODES.flowTransitionMany, payload };
}

export function flowSpawnChildrenPayload(args: readonly CommandArgument[]): wire.ProtocolCommand | undefined {
  if (args.length < 2) return undefined;
  const itemsIndex = findItemToken(args, 1);
  if (itemsIndex < 0) return undefined;
  const options = parseFlowOptions(args, 1, itemsIndex, {
    allowed: new Set([
      "GROUP", "WAIT", "NOW", "PARTITION", "LEASE_TOKEN", "FENCING", "WAIT_STATE", "SUCCESS",
      "FAILURE", "FROM_STATE", "ON_CHILD_FAILED", "ON_PARENT_CLOSED", "MAX_ACTIVE_MS", "VALUE", "VALUE_REF"
    ])
  });
  if (options == null) return undefined;

  const itemToken = core.asText(args[itemsIndex]).toUpperCase();
  let children: Record<string, unknown>[] | undefined;
  if (itemToken === "ITEMS_EXT") {
    const count = core.safeIntegerNumberArg(args[itemsIndex + 1]);
    if (count < 0) return undefined;
    children = parseFlowSpawnChildrenExt(denseCommandArgumentTail(args, itemsIndex + 2, "ITEMS_EXT"), count);
  } else {
    const mixed = itemsIndex + 1 < args.length && core.commandTokenIs(args[itemsIndex + 1], "MIXED");
    const start = itemsIndex + (mixed ? 2 : 1);
    children = parseFlowSpawnChildren(denseCommandArgumentTail(args, start, "ITEMS"), mixed);
  }
  if (children == null) return undefined;
  return {
    opcode: wire.OPCODES.flowSpawnChildren,
    payload: { id: args[0], ...options, children }
  };
}
