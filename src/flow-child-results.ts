import type { FerricStoreClient } from "./client.js";
import { valueMGetEntries } from "./client-values.js";
import { FerricStoreError } from "./errors.js";
import { append, field, text, type CommandArgument } from "./internal.js";

export type ChildStatus = "running" | "completed" | "failed" | "cancelled";

/** One child of a group: its status and, once terminal, its decoded result or error. */
export interface ChildResult<TResult = unknown> {
  id: string;
  partitionKey?: string;
  status: ChildStatus;
  result?: TResult;
  error?: unknown;
}

/** A parent's child group, with children ordered by id. */
export interface ChildGroupResults<TResult = unknown> {
  groupId: string;
  wait: "all" | "any" | "none";
  /** Set once the group resolved and the parent moved to its success or failure state. */
  resolved?: "success" | "failure";
  summary: { total: number; completed: number; failed: number; cancelled: number };
  children: ChildResult<TResult>[];
}

const CHILD_STATUSES = new Set<string>(["running", "completed", "failed", "cancelled"]);
const GROUP_WAITS = new Set<string>(["all", "any", "none"]);

/** @internal Reads one child group from the parent record and its child values in one read. */
export async function readChildResults<TResult>(
  client: FerricStoreClient,
  parentId: string,
  options: { readonly partitionKey?: string; readonly groupId?: string; readonly maxBytes?: number }
): Promise<ChildGroupResults<TResult> | undefined> {
  const groupId = options.groupId ?? "default";
  const args: CommandArgument[] = ["FLOW.GET", parentId];
  append(args, "PARTITION", options.partitionKey);
  const record = await client.commandArgs(args);
  if (record == null) return undefined;
  const group = field(field(record, "child_groups"), groupId);
  if (group == null) return undefined;

  const partitions = new Map(entries(field(group, "child_partitions"), "child group partitions"));
  const results = new Map(entries(field(group, "results"), "child group results"));
  const children: ChildResult<TResult>[] = [];
  const refs: string[] = [];
  const refTargets: { child: ChildResult<TResult>; key: "result" | "error" }[] = [];

  for (const [id, rawStatus] of entries(field(group, "children"), "child group children")) {
    const child: ChildResult<TResult> = { id, status: childStatus(rawStatus) };
    const partitionKey = partitions.get(id);
    if (partitionKey != null) child.partitionKey = text(partitionKey);
    const terminal = results.get(id);
    for (const [refField, key] of [["result_ref", "result"], ["error_ref", "error"]] as const) {
      const ref = field(terminal, refField);
      if (ref != null) {
        refs.push(text(ref));
        refTargets.push({ child, key });
      }
    }
    children.push(child);
  }

  const values = await valueMGetEntries(client, refs, { maxBytes: options.maxBytes });
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    const target = refTargets[index];
    if (value?.found === true && target != null) target.child[target.key] = value.value as TResult;
  }

  children.sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  const resolved = field(group, "resolved");
  return {
    children,
    groupId,
    ...(resolved == null ? {} : { resolved: groupResolution(resolved) }),
    summary: {
      cancelled: count(group, "cancelled"),
      completed: count(group, "completed"),
      failed: count(group, "failed"),
      total: count(group, "total")
    },
    wait: groupWait(field(group, "wait"))
  };
}

function entries(source: unknown, context: string): [string, unknown][] {
  if (source == null) return [];
  if (source instanceof Map) return [...source.entries()].map(([key, value]) => [text(key), value]);
  if (typeof source === "object" && !Array.isArray(source)) return Object.entries(source);
  throw new FerricStoreError(`FLOW.GET ${context} returned an unexpected response`, { raw: source });
}

function childStatus(value: unknown): ChildStatus {
  const status = text(value);
  if (!CHILD_STATUSES.has(status)) {
    throw new FerricStoreError(`FLOW.GET child group returned unknown child status ${status}`, { raw: value });
  }
  return status as ChildStatus;
}

function groupWait(value: unknown): ChildGroupResults["wait"] {
  const wait = text(value);
  if (!GROUP_WAITS.has(wait)) {
    throw new FerricStoreError(`FLOW.GET child group returned unknown wait ${wait}`, { raw: value });
  }
  return wait as ChildGroupResults["wait"];
}

function groupResolution(value: unknown): "success" | "failure" {
  const resolved = text(value);
  if (resolved !== "success" && resolved !== "failure") {
    throw new FerricStoreError(`FLOW.GET child group returned unknown resolution ${resolved}`, { raw: value });
  }
  return resolved;
}

function count(group: unknown, name: string): number {
  const value = field(field(group, "summary"), name);
  const number = typeof value === "bigint" ? Number(value) : value;
  if (typeof number !== "number" || !Number.isSafeInteger(number) || number < 0) {
    throw new FerricStoreError(`FLOW.GET child group returned an invalid ${name} count`, { raw: value });
  }
  return number;
}
