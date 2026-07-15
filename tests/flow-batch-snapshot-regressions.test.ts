import { describe, expect, it } from "vitest";

import {
  FerricStoreClient,
  type ClaimedItem,
  type Codec,
  type CommandArgument,
  type CommandExecutor
} from "../src/index.js";

describe("Flow batch admission snapshots", () => {
  it("captures nested create items and shared options before awaiting the first chunk", async () => {
    const firstRequest = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) await firstRequest.promise;
        return Buffer.from("OK");
      }
    };
    const codec = new CountingJsonCodec();
    const client = new FerricStoreClient(executor, {
      codec,
      flowManyBatchLimit: 1
    });
    const sharedValue = { version: "shared-original" };
    const attribute = Buffer.from("attribute-original");
    const stateMeta = Buffer.from("meta-original");
    const items = [
      createItem("flow-1", "payload-1", "item-1", "ref-1"),
      createItem("flow-2", "payload-2", "item-2", "ref-2")
    ];
    const options = {
      attributes: { binary: attribute },
      independent: true,
      nowMs: 1,
      stateMeta: { binary: stateMeta },
      type: "order",
      valueRefs: { sharedRef: "shared-ref-original" },
      values: { shared: sharedValue }
    };

    const execution = client.createMany("tenant-a", items, options);
    await waitFor(() => calls.length === 1);
    const second = items[1];
    if (second == null) throw new Error("expected second create item");
    second.payload.version = "payload-mutated";
    second.values.item.version = "item-mutated";
    if (second.valueRefs != null) second.valueRefs.itemRef = "item-ref-mutated";
    sharedValue.version = "shared-mutated";
    options.valueRefs.sharedRef = "shared-ref-mutated";
    attribute.fill(0x78);
    stateMeta.fill(0x78);
    firstRequest.resolve();

    await execution;
    const secondArgs = calls[1] ?? [];
    expect(secondArgs).toEqual(expect.arrayContaining([
      Buffer.from('{"version":"payload-2"}'),
      Buffer.from('{"version":"item-2"}'),
      Buffer.from('{"version":"shared-original"}'),
      Buffer.from("attribute-original"),
      Buffer.from("meta-original"),
      "item-ref-original-ref-2",
      "shared-ref-original"
    ]));
    expect(commandText(secondArgs)).not.toContain("mutated");
    expect(codec.encodeCalls).toBe(5);
  });

  it("captures nested mutation options before awaiting the first chunk", async () => {
    const firstRequest = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) await firstRequest.promise;
        return Buffer.from("OK");
      }
    };
    const codec = new CountingJsonCodec();
    const client = new FerricStoreClient(executor, {
      codec,
      flowManyBatchLimit: 1
    });
    const result = { version: "result-original" };
    const payload = { version: "payload-original" };
    const namedValue = { version: "value-original" };
    const attribute = Buffer.from("attribute-original");
    const stateMeta = Buffer.from("meta-original");
    const options = {
      attributesDelete: ["attribute-delete-original"],
      attributesMerge: { binary: attribute },
      dropValues: ["drop-original"],
      independent: true,
      nowMs: 1,
      overrideValues: ["override-original"],
      payload,
      result,
      stateMeta: { binary: stateMeta },
      valueRefs: { namedRef: "named-ref-original" },
      values: { named: namedValue }
    };

    const execution = client.completeMany("tenant-a", claimedItems(), options);
    await waitFor(() => calls.length === 1);
    result.version = "result-mutated";
    payload.version = "payload-mutated";
    namedValue.version = "value-mutated";
    options.valueRefs.namedRef = "named-ref-mutated";
    options.dropValues[0] = "drop-mutated";
    options.overrideValues[0] = "override-mutated";
    options.attributesDelete[0] = "attribute-delete-mutated";
    attribute.fill(0x78);
    stateMeta.fill(0x78);
    firstRequest.resolve();

    await execution;
    const secondArgs = calls[1] ?? [];
    expect(secondArgs).toEqual(expect.arrayContaining([
      Buffer.from('{"version":"result-original"}'),
      Buffer.from('{"version":"payload-original"}'),
      Buffer.from('{"version":"value-original"}'),
      Buffer.from("attribute-original"),
      Buffer.from("meta-original"),
      "named-ref-original",
      "drop-original",
      "override-original",
      "attribute-delete-original"
    ]));
    expect(commandText(secondArgs)).not.toContain("mutated");
    expect(codec.encodeCalls).toBe(3);
  });

  it("encodes each shared mutation value once while preserving request count", async () => {
    const codec = new CountingJsonCodec();
    let requests = 0;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        requests += 1;
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, { codec, flowManyBatchLimit: 1 });

    await client.completeMany("tenant-a", claimedItems(), {
      independent: true,
      result: { ok: true },
      values: { receipt: { accepted: true } }
    });

    expect(requests).toBe(2);
    expect(codec.encodeCalls).toBe(2);
  });

  it("validates valueMGet against the request cardinality captured before dispatch", async () => {
    const response = deferred();
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        await response.promise;
        return [Buffer.from('{"index":1}'), Buffer.from('{"index":2}')];
      }
    };
    const client = new FerricStoreClient(executor, { codec: new CountingJsonCodec() });
    const refs = ["ref-1", "ref-2"];

    const values = client.valueMGet(refs);
    refs.pop();
    response.resolve();

    await expect(values).resolves.toEqual([{ index: 1 }, { index: 2 }]);
  });
});

class CountingJsonCodec implements Codec {
  encodeCalls = 0;

  decode(value: Buffer | null | undefined): unknown {
    return value == null ? null : JSON.parse(value.toString("utf8"));
  }

  encode(value: unknown): Buffer {
    this.encodeCalls += 1;
    return Buffer.from(JSON.stringify(value ?? null));
  }
}

function createItem(id: string, payload: string, value: string, ref: string) {
  return {
    id,
    payload: { version: payload },
    valueRefs: { itemRef: `item-ref-original-${ref}` },
    values: { item: { version: value } }
  };
}

function claimedItems(): ClaimedItem[] {
  return [1, 2].map((index) => ({
    fencingToken: index,
    id: `flow-${index}`,
    leaseToken: Buffer.from(`lease-${index}`),
    partitionKey: "tenant-a",
    state: "running"
  }));
}

function commandText(args: readonly CommandArgument[]): string {
  return args.flatMap((arg) => {
    if (Buffer.isBuffer(arg)) return [arg.toString("utf8")];
    return typeof arg === "string" ? [arg] : [];
  }).join(" ");
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
