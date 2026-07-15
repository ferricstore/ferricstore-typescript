import type { AddressInfo, Socket } from "node:net";
import { createServer } from "node:net";
import { describe, expect, it } from "vitest";

import type { CommandExecutor } from "../src/adapters.js";
import { snapshotNativeClientOptions } from "../src/client-native-options.js";
import { FerricStoreError, OverloadedError } from "../src/errors.js";
import {
  FerricStoreClient,
  JsonCodec,
  QueueClient,
  ReconnectingExecutor,
  retry,
  type ClaimedItem,
  type CommandArgument
} from "../src/index.js";
import { sleep } from "../src/internal.js";
import { TopologyAdapterRegistry } from "../src/topology-adapter-registry.js";

describe("remaining SDK review areas", () => {
  it("captures invocation authorization context before awaiting connection readiness", async () => {
    const ready = deferred();
    let captured: readonly CommandArgument[] | undefined;
    const reconnecting = new ReconnectingExecutor(async () => {
      await ready.promise;
      return {
        async executeCommand(...args: CommandArgument[]): Promise<unknown> {
          captured = args;
          return new Map<unknown, unknown>([["ok", true]]);
        }
      };
    });
    const client = new FerricStoreClient(reconnecting);
    const requestContext = {
      scopes: ["invocation:create:tenant-a"],
      subject: "proxy-a",
      tenant: "tenant-a"
    };

    const invocation = client.invocationCreate("send-email", { recipient: "user@example.com" }, {
      requestContext
    });
    requestContext.scopes[0] = "invocation:create:*";
    requestContext.subject = "proxy-b";
    requestContext.tenant = "tenant-b";
    ready.resolve();

    try {
      await invocation;
      expect(captured?.at(-1)).toEqual({
        scopes: ["invocation:create:tenant-a"],
        subject: "proxy-a",
        tenant: "tenant-a"
      });
    } finally {
      await client.close();
    }
  });

  it("captures every independent Flow item before the first chunk is awaited", async () => {
    const firstRequest = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) await firstRequest.promise;
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, { flowManyBatchLimit: 1 });
    const items: ClaimedItem[] = [
      claimedItem("flow-1", "lease-1", 1),
      claimedItem("flow-2", "lease-2", 2)
    ];

    const completion = client.completeMany("tenant-a", items, {
      independent: true,
      returnOkOnSuccess: true
    });
    await waitFor(() => calls.length === 1);
    const secondItem = items[1];
    if (secondItem == null) throw new Error("expected a second claimed item");
    secondItem.id = "different-flow";
    secondItem.leaseToken.fill(0x78);
    secondItem.fencingToken = 99;
    firstRequest.resolve();

    await completion;
    const second = calls[1] ?? [];
    const itemsIndex = second.indexOf("ITEMS");
    expect(second.slice(itemsIndex + 1)).toEqual([
      "flow-2",
      Buffer.from("lease-2"),
      2
    ]);
  });

  it("aborts an in-progress topology adapter bootstrap during close", async () => {
    const accepted = deferred();
    const sockets = new Set<Socket>();
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
      accepted.resolve();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address() as AddressInfo;
    let closed = false;
    const registry = new TopologyAdapterRegistry(
      () => {
        if (closed) throw new FerricStoreError("registry closed");
      },
      () => closed
    );
    const creation = registry.get(
      "silent",
      `ferric://127.0.0.1:${address.port}`,
      { timeoutMs: 2_000 }
    );
    void creation.catch(() => undefined);
    await accepted.promise;
    closed = true;
    const closing = registry.close(1);
    const closeOutcome = await Promise.race([
      closing.then(() => "closed" as const),
      sleep(250).then(() => "pending" as const)
    ]);

    for (const socket of sockets) socket.destroy();
    await Promise.allSettled([creation, closing]);
    await closeServer(server);
    expect(closeOutcome).toBe("closed");
  });

  it("renews over-returned continuous-worker leases before handler admission", async () => {
    const controller = new AbortController();
    const firstHandler = deferred();
    const releaseFirst = deferred();
    const secondHandler = deferred();
    const calls: { readonly args: CommandArgument[]; readonly at: number }[] = [];
    const jobs = [flowResponse("job-1", "lease-1", 1), flowResponse("job-2", "lease-2", 2)];
    let claimed = false;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push({ args, at: performance.now() });
        if (args[0] === "FLOW.CLAIM_DUE") {
          if (claimed) return [];
          claimed = true;
          return jobs;
        }
        if (args[0] === "FLOW.EXTEND_LEASE") {
          if (args.includes("RETURN")) return Buffer.from("OK");
          return args[1] === "job-1" ? jobs[0] : jobs[1];
        }
        return Buffer.from("OK");
      }
    };
    const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
    const running = queue.worker({
      batchSize: 2,
      concurrency: 1,
      leaseMs: 40,
      leaseRenewIntervalMs: 2,
      signal: controller.signal,
      worker: "worker-1"
    }).run(async (job) => {
      if (job.id === "job-1") {
        firstHandler.resolve();
        await releaseFirst.promise;
      } else {
        secondHandler.resolve();
        controller.abort();
      }
      return retry();
    });

    await firstHandler.promise;
    await sleep(20);
    const renewedWhileQueued = calls.some(({ args }) =>
      args[0] === "FLOW.EXTEND_LEASE" && args[1] === "job-2"
    );
    releaseFirst.resolve();
    await secondHandler.promise;
    await running;

    expect(renewedWhileQueued).toBe(true);
  });

  it("deeply captures object-form TLS key material used by reconnects", () => {
    const identity = { buf: Buffer.from("original-pfx"), passphrase: "original-pass" };
    const captured = snapshotNativeClientOptions({ tlsOptions: { pfx: [identity] } });
    const capturedIdentity = (captured.tlsOptions?.pfx as typeof identity[])[0];
    if (capturedIdentity == null) throw new Error("expected a captured PFX identity");

    identity.buf.fill(0x78);
    identity.passphrase = "changed-pass";

    expect(capturedIdentity).not.toBe(identity);
    expect(capturedIdentity.buf).toEqual(Buffer.from("original-pfx"));
    expect(capturedIdentity.passphrase).toBe("original-pass");
    expect(Object.isFrozen(capturedIdentity)).toBe(true);
  });

  it("uses the Flow response plan captured with the dispatched request", async () => {
    const releaseCreate = deferred();
    const calls: CommandArgument[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        if (calls.length === 1) {
          await releaseCreate.promise;
          return Buffer.from("OK");
        }
        return flowResponse("flow-1", "lease-1", 1);
      }
    };
    const client = new FerricStoreClient(executor);
    const options = { partitionKey: "tenant-a", returnRecord: true, type: "email" };

    const creation = client.create("flow-1", options);
    await waitFor(() => calls.length === 1);
    options.partitionKey = "tenant-b";
    options.returnRecord = false;
    releaseCreate.resolve();

    await expect(creation).resolves.toMatchObject({ id: "flow-1", partitionKey: "tenant-a" });
    expect(calls[1]).toEqual(["FLOW.GET", "flow-1", "PARTITION", "tenant-a"]);
  });

  it("uses the GEOSEARCH metadata shape captured before dispatch", async () => {
    const releaseSearch = deferred();
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        await releaseSearch.promise;
        return [[Buffer.from('"alice"'), Buffer.from("1.25")]];
      }
    };
    const client = new FerricStoreClient(executor, { codec: new JsonCodec() });
    const args: CommandArgument[] = ["FROMMEMBER", "alice", "BYRADIUS", 10, "km", "WITHDIST"];

    const search = client.geo.geosearch("places", args);
    args.pop();
    releaseSearch.resolve();

    await expect(search).resolves.toEqual([["alice", "1.25"]]);
  });

  it("cancels producer backpressure sleep when the client closes", async () => {
    const attempted = deferred();
    let attempts = 0;
    let closed = false;
    const executor: CommandExecutor = {
      close(): void {
        closed = true;
      },
      async executeCommand(): Promise<unknown> {
        attempts += 1;
        if (attempts === 1) {
          attempted.resolve();
          throw new OverloadedError("busy", { retryAfterMs: 150 });
        }
        if (closed) throw new FerricStoreError("executor closed");
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, {
      backpressure: { baseDelayMs: 150, jitterPct: 0, maxDelayMs: 150, maxRetries: 1 }
    });
    const creation = client.create("flow-1", { type: "email" }).then(
      () => "fulfilled" as const,
      () => "rejected" as const
    );
    await attempted.promise;
    await client.close();
    const earlyOutcome = await Promise.race([
      creation,
      sleep(30).then(() => "pending" as const)
    ]);
    await creation;

    expect(earlyOutcome).toBe("rejected");
    expect(attempts).toBe(1);
  });
});

function claimedItem(id: string, leaseToken: string, fencingToken: number): ClaimedItem {
  return {
    fencingToken,
    id,
    leaseToken: Buffer.from(leaseToken),
    partitionKey: "tenant-a",
    state: "running",
    type: "email"
  };
}

function flowResponse(id: string, leaseToken: string, fencingToken: number): Map<string, unknown> {
  return new Map<string, unknown>([
    ["fencing_token", fencingToken],
    ["id", Buffer.from(id)],
    ["lease_token", Buffer.from(leaseToken)],
    ["partition_key", Buffer.from("tenant-a")],
    ["state", Buffer.from("running")],
    ["type", Buffer.from("email")],
    ["version", 1]
  ]);
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

async function closeServer(server: ReturnType<typeof createServer>): Promise<void> {
  if (!server.listening) return;
  await new Promise<void>((resolve, reject) => server.close((error) => {
    if (error == null) resolve();
    else reject(error);
  }));
}
