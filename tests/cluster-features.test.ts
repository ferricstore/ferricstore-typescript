import { describe, expect, it } from "vitest";
import { COMMAND_OPCODES, FerricStoreClient } from "../src/index.js";
import { FakeExecutor } from "./fake-executor.js";

describe("cluster feature levels", () => {
  it("knows the native opcodes the server advertises for feature levels", () => {
    expect(COMMAND_OPCODES["CLUSTER.FEATURES"]).toBe(0x0311);
    expect(COMMAND_OPCODES["CLUSTER.FEATURES.RAISE"]).toBe(0x0312);
  });

  it("reads and raises the cluster feature level", async () => {
    const reply = "supported: 1\r\nactive: 1\r\nshard_0: 1\r\nshard_1: 1";
    const executor = new FakeExecutor(["supported: 1\r\nactive: 0\r\nshard_0: 0\r\nshard_1: 0", reply]);
    const client = new FerricStoreClient(executor);

    await expect(client.clusterFeatures()).resolves.toEqual({
      active: 0,
      shard_0: 0,
      shard_1: 0,
      supported: 1
    });
    await expect(client.clusterFeaturesRaise(1)).resolves.toMatchObject({ active: 1 });

    expect(executor.calls).toEqual([["CLUSTER.FEATURES"], ["CLUSTER.FEATURES.RAISE", 1]]);
  });
});
