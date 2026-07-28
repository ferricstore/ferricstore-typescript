import { describe, expect, expectTypeOf, it } from "vitest";
import {
  ConnectionClosedError,
  FerricStoreClient,
  FERRICSTORE_MINIMUM_SERVER_VERSION,
  FERRICSTORE_NATIVE_PROTOCOL_VERSION,
  FERRICSTORE_SDK_VERSION,
  ReconnectingExecutor,
  StalePolicyGenerationError,
  WorkflowClient,
  classifyServerError,
  type FlowPolicySnapshot
} from "../src/index.js";
import { buildProtocolCommand, OPCODES } from "../src/protocol.js";
import { FakeExecutor } from "./fake-executor.js";

function policyResponse(overrides: Record<string, unknown> = {}): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["type", "order"],
    ["generation", 7],
    ["version", "policy-v2"],
    ["max_active_ms", 60_000],
    ["retry", new Map<unknown, unknown>([
      ["max_retries", 3],
      ["backoff", new Map<unknown, unknown>([
        ["kind", "exponential"],
        ["base_ms", 1_000],
        ["max_ms", 30_000],
        ["jitter_pct", 20]
      ])],
      ["exhausted_to", "failed"]
    ])],
    ["retention", new Map<unknown, unknown>([
      ["ttl_ms", 604_800_000],
      ["history_max_events", 100_000]
    ])],
    ["indexed_attributes", ["tenant"]],
    ["indexed_state_meta", "schema"],
    ["states", new Map<unknown, unknown>([
      ["queued", new Map<unknown, unknown>([
        ["mode", "fifo"],
        ["max_active_ms", 60_000],
        ["retry", new Map<unknown, unknown>([
          ["max_retries", 3],
          ["backoff", new Map<unknown, unknown>([
            ["kind", "exponential"],
            ["base_ms", 1_000],
            ["max_ms", 30_000],
            ["jitter_pct", 20]
          ])],
          ["exhausted_to", "failed"]
        ])],
        ["retention", new Map<unknown, unknown>([
          ["ttl_ms", 604_800_000],
          ["history_max_events", 100_000]
        ])]
      ])]
    ])],
    ...Object.entries(overrides)
  ]);
}

describe("FerricStore 0.9.1 TypeScript contract", () => {
  it("retains the policy contract under the current package and native protocol v1", () => {
    expect(FERRICSTORE_SDK_VERSION).toBe("0.11.4");
    expect(FERRICSTORE_MINIMUM_SERVER_VERSION).toBe("0.11.4");
    expect(FERRICSTORE_NATIVE_PROTOCOL_VERSION).toBe(1);
  });

  it("sends direct policy patches, replacements, and generation CAS options", async () => {
    const executor = new FakeExecutor([policyResponse()]);
    const client = new FerricStoreClient(executor);

    const snapshot = await client.installPolicy("order", {
      expectedGeneration: 6,
      maxActiveMs: 60_000,
      replace: false,
      states: { queued: { mode: "fifo" } }
    });

    expect(executor.calls).toEqual([[
      "FLOW.POLICY.SET",
      "order",
      "EXPECTED_GENERATION",
      6,
      "REPLACE",
      "false",
      "MAX_ACTIVE_MS",
      60_000,
      "STATE",
      "queued",
      "MODE",
      "FIFO"
    ]]);
    expect(snapshot).toMatchObject({
      generation: 7,
      indexedAttributes: ["tenant"],
      indexedStateMeta: "schema",
      maxActiveMs: 60_000,
      states: { queued: { mode: "fifo" } },
      type: "order",
      version: "policy-v2"
    });
    expectTypeOf(snapshot).toEqualTypeOf<FlowPolicySnapshot>();
    expectTypeOf(snapshot.generation).toEqualTypeOf<number>();
  });

  it("uses the dedicated native opcode and structured policy payload", () => {
    expect(buildProtocolCommand([
      "FLOW.POLICY.SET",
      "order",
      "EXPECTED_GENERATION",
      6,
      "REPLACE",
      "false",
      "MAX_ACTIVE_MS",
      60_000,
      "STATE",
      "queued",
      "MODE",
      "FIFO",
      "MAX_RETRIES",
      2,
      "BASE_MS",
      100
    ])).toEqual({
      opcode: OPCODES.flowPolicySet,
      payload: {
        expected_generation: 6,
        max_active_ms: 60_000,
        replace: false,
        states: {
          queued: {
            mode: "fifo",
            retry: {
              backoff: { base_ms: 100 },
              max_retries: 2
            }
          }
        },
        type: "order"
      }
    });
    expect(buildProtocolCommand([
      "FLOW.POLICY.SET", "order", "MAX_RETRIES", "2", "BASE_MS", "100"
    ])).toMatchObject({
      opcode: OPCODES.flowPolicySet,
      payload: { retry: { backoff: { base_ms: 100 }, max_retries: 2 } }
    });
  });

  it("keeps type retention outside state blocks while preserving explicit state scope", async () => {
    const executor = new FakeExecutor([policyResponse(), policyResponse()]);
    const client = new FerricStoreClient(executor);

    await client.installPolicy("order", {
      retentionTtlMs: 123_456,
      states: { queued: { mode: "fifo" } }
    });
    await client.installPolicy("order", {
      retentionTtlMs: 654_321,
      state: "queued"
    });

    expect(executor.calls[0]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "RETENTION_TTL_MS",
      123_456,
      "STATE",
      "queued",
      "MODE",
      "FIFO"
    ]);
    expect(buildProtocolCommand(executor.calls[0] ?? [])).toEqual({
      opcode: OPCODES.commandExec,
      payload: {
        args: [
          "order",
          "RETENTION_TTL_MS",
          123_456,
          "STATE",
          "queued",
          "MODE",
          "FIFO"
        ],
        command: "FLOW.POLICY.SET"
      }
    });
    expect(executor.calls[1]).toEqual([
      "FLOW.POLICY.SET",
      "order",
      "STATE",
      "queued",
      "RETENTION_TTL_MS",
      654_321
    ]);
    expect(buildProtocolCommand(executor.calls[1] ?? [])).toMatchObject({
      payload: {
        states: { queued: { retention: { ttl_ms: 654_321 } } },
        type: "order"
      }
    });
  });

  it("builds policy objects without reusing polluted prototype fields", () => {
    const descriptor = Object.getOwnPropertyDescriptor(Object.prototype, "retry");
    const inheritedRetry: Record<string, unknown> = { sentinel: true };
    Object.defineProperty(Object.prototype, "retry", {
      configurable: true,
      value: inheritedRetry,
      writable: true
    });

    try {
      const command = buildProtocolCommand([
        "FLOW.POLICY.SET", "order", "MAX_RETRIES", 2
      ]);
      expect(Object.hasOwn(command.payload as Record<string, unknown>, "retry")).toBe(true);
      expect(command).toMatchObject({
        payload: { retry: { max_retries: 2 }, type: "order" }
      });
      expect(inheritedRetry).toEqual({ sentinel: true });
    } finally {
      if (descriptor == null) {
        Reflect.deleteProperty(Object.prototype, "retry");
      } else {
        Object.defineProperty(Object.prototype, "retry", descriptor);
      }
    }
  });

  it("returns typed policy snapshots from reads", async () => {
    const client = new FerricStoreClient(new FakeExecutor([policyResponse()]));

    const snapshot = await client.policyGet("order");

    expect(snapshot.generation).toBe(7);
    expect(snapshot.retry).toMatchObject({
      backoff: { baseMs: 1_000, jitterPct: 20, kind: "exponential", maxMs: 30_000 },
      exhaustedTo: "failed",
      maxRetries: 3
    });
    expect(snapshot.retention).toEqual({ historyMaxEvents: 100_000, ttlMs: 604_800_000 });
    expectTypeOf(snapshot).toEqualTypeOf<FlowPolicySnapshot>();
  });

  it.each([
    -1,
    0.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1
  ])("rejects invalid expected generation %s before dispatch", async (expectedGeneration) => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.installPolicy("order", { expectedGeneration })).rejects.toThrow(
      /expectedGeneration.*nonnegative safe integer/u
    );
    expect(executor.calls).toEqual([]);
  });

  it("accepts both generation range boundaries", async () => {
    const executor = new FakeExecutor([policyResponse({ generation: 1 }), policyResponse()]);
    const client = new FerricStoreClient(executor);

    await expect(client.installPolicy("order", { expectedGeneration: 0 })).resolves.toBeDefined();
    await expect(client.installPolicy("order", {
      expectedGeneration: Number.MAX_SAFE_INTEGER
    })).resolves.toBeDefined();
    expect(executor.calls.map((call) => call.slice(2))).toEqual([
      ["EXPECTED_GENERATION", 0],
      ["EXPECTED_GENERATION", Number.MAX_SAFE_INTEGER]
    ]);
  });

  it("rejects invalid replacement inputs and unsafe snapshot generations", async () => {
    const executor = new FakeExecutor([
      policyResponse({ generation: Number.MAX_SAFE_INTEGER + 1 })
    ]);
    const client = new FerricStoreClient(executor);

    await expect(client.installPolicy("order", {
      replace: "true" as never
    })).rejects.toThrow(/replace.*boolean/u);
    expect(executor.calls).toEqual([]);

    await expect(client.installPolicy("order")).rejects.toThrow(
      /FLOW policy generation returned an unexpected response/u
    );
  });

  it("defaults direct writes to patch and workflow installation to replacement", async () => {
    const executor = new FakeExecutor([policyResponse(), policyResponse(), policyResponse()]);
    const client = new FerricStoreClient(executor);
    const workflow = new WorkflowClient(client).workflow({ type: "order" });
    workflow.state("queued", async () => undefined, { mode: "fifo" });

    await client.installPolicy("order", { maxActiveMs: 10_000 });
    await workflow.installPolicy({ maxActiveMs: 20_000 });
    await workflow.installPolicy({ maxActiveMs: 30_000, replace: false });

    expect(executor.calls[0]).not.toContain("REPLACE");
    expect(executor.calls[1]).toContain("REPLACE");
    expect(executor.calls[1]).toContain("true");
    expect(executor.calls[2]).toEqual(expect.arrayContaining(["REPLACE", "false"]));
  });

  it("maps stale policy generations to a dedicated error", () => {
    const error = classifyServerError("ERR stale flow policy generation");

    expect(error).toBeInstanceOf(StalePolicyGenerationError);
    expect(error.code).toBe("stale_policy_generation");
  });

  it.each(["unsent", "possibly_sent"] as const)(
    "repairs the connection after %s CAS failure without replaying the mutation",
    async (requestDisposition) => {
      let attempts = 0;
      let connections = 0;
      const closed = new ConnectionClosedError(requestDisposition);
      const executor = new ReconnectingExecutor(async () => {
        connections += 1;
        const connection = connections;
        return {
          async executeCommand(): Promise<string> {
            attempts += 1;
            if (connection === 1) throw closed;
            return "OK";
          }
        };
      }, { maxRetries: 3 });
      await executor.ready();

      try {
        await expect(executor.executeCommand(
          "FLOW.POLICY.SET",
          "order",
          "EXPECTED_GENERATION",
          7,
          "MAX_ACTIVE_MS",
          10_000
        )).rejects.toBe(closed);
        expect(attempts).toBe(1);
        expect(connections).toBe(2);

        await expect(executor.executeCommand(
          "FLOW.POLICY.SET",
          "order",
          "EXPECTED_GENERATION",
          7,
          "MAX_ACTIVE_MS",
          10_000
        )).resolves.toBe("OK");
        expect(attempts).toBe(2);
        expect(connections).toBe(2);
      } finally {
        await executor.close();
      }
    }
  );
});
