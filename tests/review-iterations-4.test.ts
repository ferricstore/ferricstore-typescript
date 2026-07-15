import net from "node:net";
import { describe, expect, it } from "vitest";

import { FerricStoreClient, type CommandExecutor } from "../src/index.js";
import { expandManyResponse } from "../src/internal.js";
import { connect } from "../src/native-connection.js";
import { encodeValue } from "../src/protocol.js";
import { TopologyScatterExecutor } from "../src/topology-scatter.js";
import { FakeExecutor } from "./fake-executor.js";

describe("fourth ten-pass review regressions", () => {
  it("rejects a missing positional MGET response before returning a sparse result", async () => {
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("first");
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.kv.mget(["first", "second"])).rejects.toThrow(
      "MGET response item 1 is missing"
    );
  });

  it("rejects missing fixed-width module response items", async () => {
    const response = new Array<unknown>(2);
    response[1] = 1;
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.bloom.maddMany("filter", ["first", "second"])).rejects.toThrow(
      "BF.MADD response item 0 is missing"
    );
  });

  it("does not treat a scalar with a matching length as an auto-batch response", async () => {
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        throw new Error("unexpected individual dispatch");
      },
      async executePipeline(): Promise<unknown[]> {
        return "OK" as unknown as unknown[];
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 2, maxDelayMs: 0 }
    });

    const first = client.command("GET", "first");
    const second = client.command("GET", "second");
    await Promise.all([
      expect(first).rejects.toThrow("auto-batch response length mismatch"),
      expect(second).rejects.toThrow("auto-batch response length mismatch")
    ]);
  });

  it("does not silently encode sparse native value arrays as nulls", () => {
    const value = new Array<unknown>(2);
    value[1] = "present";

    expect(() => encodeValue(value)).toThrow("native protocol value arrays must be dense");
  });

  it("removes bootstrap listeners after a native socket connects", async () => {
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (address == null || typeof address === "string") throw new Error("expected TCP address");
    const socket = await connect({
      host: "127.0.0.1",
      port: address.port,
      tls: false
    }, { connectTimeoutMs: 1_000 });

    try {
      expect(socket.listenerCount("error")).toBe(0);
      expect(socket.listenerCount("connect")).toBe(0);
    } finally {
      socket.destroy();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error == null ? resolve() : reject(error));
      });
    }
  });

  it("rejects sparse per-shard array responses before topology merge", async () => {
    const route = {
      endpoint: { host: "node.local", nativePort: 6388, node: "node@local" },
      endpointKey: "node.local:6388",
      laneId: 1,
      leaderNode: "node@local",
      shard: 0
    };
    const response = new Array<unknown>(2);
    response[0] = Buffer.from("first");
    const scatter = new TopologyScatterExecutor({
      concurrency: 1,
      executeOnRoute: async () => response,
      route: () => route
    });

    await expect(scatter.execute(["MGET", "first", "second"])).rejects.toThrow(
      "MGET shard response item 1 is missing"
    );
  });

  it("rejects missing positional Flow bulk response items", () => {
    const response = new Array<unknown>(2);
    response[0] = "first";

    expect(() => expandManyResponse(response, 2)).toThrow(
      "batch response item 1 is missing"
    );
  });

  it("rejects missing compact claims before worker lease handling", async () => {
    const response = new Array<unknown>(1);
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.claimDue("email", {
      jobOnly: true,
      worker: "worker-1"
    })).rejects.toThrow("FLOW.CLAIM_DUE response item 0 is missing");
  });

  it("rejects missing positional FLOW.VALUE.MGET response items", async () => {
    const response = new Array<unknown>(2);
    response[0] = null;
    const client = new FerricStoreClient(new FakeExecutor([response]));

    await expect(client.valueMGet(["first-ref", "second-ref"])).rejects.toThrow(
      "FLOW.VALUE.MGET response item 1 is missing"
    );
  });
});
