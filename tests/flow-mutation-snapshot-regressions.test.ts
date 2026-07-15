import { describe, expect, it } from "vitest";

import {
  FerricStoreClient,
  JsonCodec,
  RawCodec,
  type ClaimedItem,
  type CommandArgument,
  type CommandExecutor
} from "../src/index.js";

interface MutableValue {
  version: string;
}
type MutationInvocation = (
  client: FerricStoreClient,
  items: ClaimedItem[],
  value: MutableValue
) => Promise<unknown>;

const mutationCases: readonly (readonly [string, string, MutationInvocation])[] = [
  ["retryMany", "ERROR", async (client, items, value) => await client.retryMany("tenant-a", items, {
    error: value,
    independent: true
  })],
  ["failMany", "ERROR", async (client, items, value) => await client.failMany("tenant-a", items, {
    error: value,
    independent: true
  })],
  ["cancelMany", "REASON", async (client, items, value) => await client.cancelMany("tenant-a", items, {
    independent: true,
    reason: value
  })],
  ["transitionMany", "PAYLOAD", async (client, items, value) => await client.transitionMany("tenant-a", {
    fromState: "running",
    independent: true,
    items,
    payload: value,
    toState: "done"
  })]
];

describe("Flow mutation batch snapshots", () => {
  it.each(mutationCases)("captures %s encoded options for later chunks", async (_name, token, invoke) => {
    const firstRequest = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) await firstRequest.promise;
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, {
      codec: new JsonCodec(),
      flowManyBatchLimit: 1
    });
    const value = { version: "original" };

    const execution = invoke(client, claimedItems(), value);
    await waitFor(() => calls.length === 1);
    value.version = "mutated";
    firstRequest.resolve();

    await execution;
    const second = calls[1] ?? [];
    const valueIndex = second.indexOf(token);
    expect(second[valueIndex + 1]).toEqual(Buffer.from('{"version":"original"}'));
  });

  it("owns RawCodec payload bytes before a later create chunk is dispatched", async () => {
    const firstRequest = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) await firstRequest.promise;
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, {
      codec: new RawCodec(),
      flowManyBatchLimit: 1
    });
    const secondPayload = Buffer.from("payload-original");

    const execution = client.createMany("tenant-a", [
      { id: "flow-1", payload: Buffer.from("payload-1") },
      { id: "flow-2", payload: secondPayload }
    ], {
      independent: true,
      type: "order"
    });
    await waitFor(() => calls.length === 1);
    secondPayload.fill(0x78);
    firstRequest.resolve();

    await execution;
    expect(calls[1]).toContainEqual(Buffer.from("payload-original"));
  });
});

function claimedItems(): ClaimedItem[] {
  return [1, 2].map((index) => ({
    fencingToken: index,
    id: `flow-${index}`,
    leaseToken: Buffer.from(`lease-${index}`),
    partitionKey: "tenant-a",
    state: "running"
  }));
}

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve;
  });
  return { promise, resolve: () => resolvePromise?.() };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error("condition was not reached");
}
