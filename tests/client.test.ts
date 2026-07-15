import { describe, expect, it, vi } from "vitest";
import {
  FerricStoreClient,
  FerricStoreError,
  RoutingTopology,
  type RoutingRoute
} from "../src/index.js";
import { executeCommandsIndividually, type CommandExecutor } from "../src/adapters.js";
import { FakeExecutor } from "./fake-executor.js";

describe("FerricStoreClient", () => {
  it("rejects sparse FLOW.VALUE.MGET reference arrays before dispatch", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor);

    await expect(client.valueMGet(new Array<string>(1))).rejects.toThrow(
      "FLOW.VALUE.MGET refs must be dense"
    );
    expect(executor.calls).toEqual([]);
  });

  it("keeps a delayed auto-batch flush referenced while commands are pending", async () => {
    const delayMs = 12_345;
    const timerSpy = vi.spyOn(globalThis, "setTimeout");
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("PONG");
      },
      async executePipeline(commands): Promise<unknown[]> {
        return commands.map(() => Buffer.from("value"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxDelayMs: delayMs }
    });
    const pending = client.kv.get("key");

    try {
      const timerCall = timerSpy.mock.calls.findIndex((call) => call[1] === delayMs);
      const timer = timerSpy.mock.results[timerCall]?.value as NodeJS.Timeout | undefined;
      expect(timer?.hasRef()).toBe(true);

      // An excluded command flushes immediately so the test does not wait for the delay.
      await client.ping();
      await expect(pending).resolves.toEqual(Buffer.from("value"));
    } finally {
      await client.close().catch(() => undefined);
      await pending.catch(() => undefined);
      timerSpy.mockRestore();
    }
  });

  it("dispatches large Flow state filters through the array-native path", async () => {
    const states = new Array<string>(150_000).fill("queued");
    const executor = new FakeExecutor([[]]);
    const client = new FerricStoreClient(executor);

    await expect(client.claimDue("email", {
      jobOnly: true,
      states,
      worker: "worker-1"
    })).resolves.toEqual([]);

    expect(executor.calls[0]).toHaveLength(12 + states.length);
    expect(executor.calls[0]?.slice(0, 4)).toEqual(["FLOW.CLAIM_DUE", "email", "STATES", states.length]);
  });

  it("orders state-changing controls when a command-only executor backs pipeline", async () => {
    let authenticated = false;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "AUTH") {
          await new Promise((resolve) => setImmediate(resolve));
          authenticated = true;
          return Buffer.from("OK");
        }
        return Buffer.from(authenticated ? "after-auth" : "before-auth");
      }
    };
    const client = new FerricStoreClient(executor);

    await expect(client.pipeline([
      ["AUTH", "default", "secret"],
      ["GET", "protected-key"]
    ])).resolves.toEqual([Buffer.from("OK"), Buffer.from("after-auth")]);
  });

  it("honors public ordered pipeline options with a command-only executor", async () => {
    let value = Buffer.from("before-set");
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "SET") {
          await new Promise((resolve) => setImmediate(resolve));
          const nextValue = args[2];
          if (typeof nextValue !== "string") throw new TypeError("expected string SET value");
          value = Buffer.from(nextValue);
          return Buffer.from("OK");
        }
        return value;
      }
    };
    const client = new FerricStoreClient(executor);

    await expect(client.pipeline([
      ["SET", "dependent", "after-set"],
      ["GET", "dependent"]
    ], { ordered: true })).resolves.toEqual([Buffer.from("OK"), Buffer.from("after-set")]);
  });

  it("auto-batches concurrent safe commands when enabled", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("SET", "auto-batch:a", "1"),
      client.command("SET", "auto-batch:b", "2")
    ]);

    expect(executor.pipelineCalls).toEqual([
      [
        ["SET", "auto-batch:a", "1"],
        ["SET", "auto-batch:b", "2"]
      ]
    ]);
  });

  it("orders same-key auto-batch fallback dependencies while overlapping independent keys", async () => {
    let value = Buffer.from("before-set");
    let releaseWrite: (() => void) | undefined;
    const writeGate = new Promise<void>((resolve) => { releaseWrite = resolve; });
    let dependentReadStarted = false;
    let independentReadStarted = false;
    const executeCommand: CommandExecutor["executeCommand"] = async (...args) => {
      if (args[0] === "SET") {
        await writeGate;
        if (args[2] !== "after-set") throw new Error("unexpected SET value");
        value = Buffer.from(args[2]);
        return Buffer.from("OK");
      }
      if (args[1] === "dependent") {
        dependentReadStarted = true;
        return value;
      }
      independentReadStarted = true;
      return Buffer.from("independent");
    };
    const executor: CommandExecutor = {
      executeCommand,
      async executePipeline(commands, options): Promise<unknown[]> {
        return await executeCommandsIndividually(executeCommand, commands, options);
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const resultsPromise = Promise.all([
      client.command("SET", "dependent", "after-set"),
      client.command("GET", "dependent"),
      client.command("GET", "independent")
    ]);
    await new Promise((resolve) => setImmediate(resolve));
    const startsBeforeWriteFinished = { dependentReadStarted, independentReadStarted };
    releaseWrite?.();
    const results = await resultsPromise;

    expect(startsBeforeWriteFinished).toEqual({
      dependentReadStarted: false,
      independentReadStarted: true
    });
    expect(results).toEqual([
      Buffer.from("OK"),
      Buffer.from("after-set"),
      Buffer.from("independent")
    ]);
  });

  it("keeps every supported Flow auto-batch footprint explicit", async () => {
    let pipelineOptions: Parameters<NonNullable<CommandExecutor["executePipeline"]>>[1];
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands, options): Promise<unknown[]> {
        pipelineOptions = options;
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });
    const lease = Buffer.from("lease");
    const commands = [
      ["FLOW.CREATE_MANY", "tenant-a", "TYPE", "order", "ITEMS", "create-1", Buffer.from("payload")],
      ["FLOW.COMPLETE_MANY", "tenant-a", "ITEMS", "complete-1", lease, 1],
      ["FLOW.TRANSITION_MANY", "tenant-a", "from", "to", "ITEMS", "transition-1", 1, lease],
      ["FLOW.RETRY_MANY", "tenant-a", "ITEMS", "retry-1", lease, 1],
      ["FLOW.FAIL_MANY", "tenant-a", "ITEMS", "fail-1", lease, 1],
      ["FLOW.CANCEL_MANY", "tenant-a", "ITEMS", "cancel-1", 1],
      ["FLOW.RUN_STEPS_MANY", "TYPE", "order", "ITEMS", [{ id: "steps-1" }]],
      ["FLOW.VALUE.MGET", "ref-1", "ref-2", "MAX_BYTES", 1_024],
      ["FLOW.VALUE.PUT", Buffer.from("one"), "OWNER_FLOW_ID", "owner-1", "NAME", "profile"],
      ["FLOW.VALUE.PUT", Buffer.from("two"), "OWNER_FLOW_ID", "owner-2", "NAME", "profile"]
    ] as const;

    await Promise.all(commands.map(async (command) => await client.command(...command)));

    expect(pipelineOptions?.ordered).not.toBe(true);
    expect(pipelineOptions?.fallbackDependencies).toHaveLength(commands.length);
    expect(pipelineOptions?.fallbackDependencies?.every((dependencies) => dependencies.length === 0)).toBe(true);
  });

  it("orders named value puts for the same owner while keeping other owners independent", async () => {
    let dependencies: readonly (readonly number[])[] | undefined;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands, options): Promise<unknown[]> {
        dependencies = options?.fallbackDependencies;
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("FLOW.VALUE.PUT", Buffer.from("one"), "OWNER_FLOW_ID", "owner-a", "NAME", "profile"),
      client.command("FLOW.VALUE.PUT", Buffer.from("two"), "OWNER_FLOW_ID", "owner-a", "NAME", "profile"),
      client.command("FLOW.VALUE.PUT", Buffer.from("three"), "OWNER_FLOW_ID", "owner-b", "NAME", "profile")
    ]);

    expect(dependencies).toEqual([[], [0], []]);
  });

  it("flushes queued auto-batch work before an excluded direct command", async () => {
    const order: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        order.push(`direct:${args[0] as string}`);
        return 1;
      },
      async executePipeline(commands): Promise<unknown[]> {
        order.push(`pipeline:${commands[0]?.[0] as string}`);
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxDelayMs: 1_000 }
    });

    await Promise.all([
      client.command("SET", "ordered", "1"),
      client.command("INCR", "ordered")
    ]);

    expect(order).toEqual(["pipeline:SET", "direct:INCR"]);
  });

  it("releases the ordering barrier after dispatching a blocking command", async () => {
    let releasePop: (() => void) | undefined;
    let markPopStarted: (() => void) | undefined;
    const popGate = new Promise<void>((resolve) => { releasePop = resolve; });
    const popStarted = new Promise<void>((resolve) => { markPopStarted = resolve; });
    let pushStarted = false;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "BLPOP") {
          markPopStarted?.();
          await popGate;
        }
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        pushStarted = commands.some((command) => command[0] === "RPUSH");
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const pop = client.command("BLPOP", "queue", 0);
    await popStarted;
    const push = client.command("RPUSH", "queue", "value");
    await new Promise((resolve) => setImmediate(resolve));
    const pushStartedBeforePopFinished = pushStarted;

    releasePop?.();
    await Promise.all([pop, push]);
    expect(pushStartedBeforePopFinished).toBe(true);
  });

  it("closes the underlying executor while a blocking command is pending", async () => {
    let releasePop: (() => void) | undefined;
    let markPopStarted: (() => void) | undefined;
    const popGate = new Promise<void>((resolve) => { releasePop = resolve; });
    const popStarted = new Promise<void>((resolve) => { markPopStarted = resolve; });
    let closeCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[0] === "BLPOP") {
          markPopStarted?.();
          await popGate;
        }
        return Buffer.from("OK");
      },
      async executePipeline(): Promise<unknown[]> {
        return [];
      },
      async close(): Promise<void> {
        closeCalls += 1;
        releasePop?.();
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const pop = client.command("BLPOP", "queue", 0);
    await popStarted;
    const closing = client.close();
    await new Promise((resolve) => setImmediate(resolve));
    const closeReachedExecutorBeforePopFinished = closeCalls === 1;

    releasePop?.();
    await Promise.allSettled([pop, closing]);
    expect(closeReachedExecutorBeforePopFinished).toBe(true);
  });

  it("keeps an explicit pipeline behind an in-flight excluded command", async () => {
    let releaseDirect: (() => void) | undefined;
    let markDirectStarted: (() => void) | undefined;
    const directGate = new Promise<void>((resolve) => { releaseDirect = resolve; });
    const directStarted = new Promise<void>((resolve) => { markDirectStarted = resolve; });
    const order: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        order.push("direct");
        markDirectStarted?.();
        await directGate;
        return 1;
      },
      async executePipeline(commands): Promise<unknown[]> {
        order.push("pipeline");
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });
    const direct = client.command("INCR", "ordered");
    await directStarted;
    const explicit = client.pipeline([["GET", "ordered"]]);

    await new Promise((resolve) => setImmediate(resolve));
    expect(order).toEqual(["direct"]);

    releaseDirect?.();
    await Promise.all([direct, explicit]);
    expect(order).toEqual(["direct", "pipeline"]);
  });

  it("waits for an in-flight auto-batch before sending an explicit pipeline", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const pipelineCalls: string[][] = [];
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        pipelineCalls.push(commands.map((command) => {
          const key = command[1];
          return typeof key === "string" ? key : Buffer.isBuffer(key) ? key.toString("utf8") : "";
        }));
        if (pipelineCalls.length === 1) {
          markFirstStarted?.();
          await firstGate;
        }
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });
    const first = client.command("SET", "first", "1");
    await firstStarted;
    const explicit = client.pipeline([["GET", "second"]]);

    await new Promise((resolve) => setImmediate(resolve));
    expect(pipelineCalls).toEqual([["first"]]);

    releaseFirst?.();
    await Promise.all([first, explicit]);
    expect(pipelineCalls).toEqual([["first"], ["second"]]);
  });

  it("does not let a later auto-batch overtake an earlier write", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const pipelineCalls: string[][] = [];
    let stored = "initial";
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        const values = commands.map((command) => {
          const value = command[2];
          return typeof value === "string"
            ? value
            : Buffer.isBuffer(value)
              ? value.toString("utf8")
              : "";
        });
        pipelineCalls.push(values);
        if (pipelineCalls.length === 1) {
          markFirstStarted?.();
          await firstGate;
        }
        for (const value of values) stored = value;
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command("SET", "shared", "first");
    await firstStarted;
    const second = client.command("SET", "shared", "second");
    await new Promise((resolve) => setImmediate(resolve));

    expect(pipelineCalls).toEqual([["first"]]);
    releaseFirst?.();
    await Promise.all([first, second]);
    expect(pipelineCalls).toEqual([["first"], ["second"]]);
    expect(stored).toBe("second");
  });

  it("keeps disjoint auto-batch keys concurrent", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const secondStarted = new Promise<void>((resolve) => { markSecondStarted = resolve; });
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        if (commands[0]?.[1] === "first-key") {
          markFirstStarted?.();
          await firstGate;
        } else {
          markSecondStarted?.();
        }
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command("SET", "first-key", "1");
    await firstStarted;
    const second = client.command("SET", "second-key", "2");
    const secondStartedBeforeFirstFinished = await Promise.race([
      secondStarted.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 50))
    ]);

    releaseFirst?.();
    await Promise.all([first, second]);
    expect(secondStartedBeforeFirstFinished).toBe(true);
  });

  it("keeps disjoint Flow ids concurrent across auto-batch flushes", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const secondStarted = new Promise<void>((resolve) => { markSecondStarted = resolve; });
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        if (commands[0]?.[1] === "flow-1") {
          markFirstStarted?.();
          await firstGate;
        } else {
          markSecondStarted?.();
        }
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command("FLOW.SIGNAL", "flow-1", "SIGNAL", "go");
    await firstStarted;
    const second = client.command("FLOW.SIGNAL", "flow-2", "SIGNAL", "go");
    const overlapped = await Promise.race([
      secondStarted.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 50))
    ]);

    releaseFirst?.();
    await Promise.all([first, second]);
    expect(overlapped).toBe(true);
  });

  it("keeps disjoint lease renewals concurrent across auto-batch flushes", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const secondStarted = new Promise<void>((resolve) => { markSecondStarted = resolve; });
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args[1] === "flow-1") {
          markFirstStarted?.();
          await firstGate;
        } else {
          markSecondStarted?.();
        }
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        if (commands[0]?.[1] === "flow-1") {
          markFirstStarted?.();
          await firstGate;
        } else {
          markSecondStarted?.();
        }
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command(
      "FLOW.EXTEND_LEASE", "flow-1", Buffer.from("lease-1"),
      "FENCING", 1, "LEASE_MS", 30_000, "RETURN", "OK_ON_SUCCESS"
    );
    await firstStarted;
    const second = client.command(
      "FLOW.EXTEND_LEASE", "flow-2", Buffer.from("lease-2"),
      "FENCING", 2, "LEASE_MS", 30_000, "RETURN", "OK_ON_SUCCESS"
    );
    const overlapped = await Promise.race([
      secondStarted.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 50))
    ]);

    releaseFirst?.();
    await Promise.all([first, second]);
    expect(overlapped).toBe(true);
  });

  it("requests an OK-only response for lightweight lease renewal", async () => {
    const executor = new FakeExecutor([Buffer.from("OK")]);
    const client = new FerricStoreClient(executor);

    await expect(client.extendLease("flow-1", {
      fencingToken: 7,
      leaseMs: 30_000,
      leaseToken: Buffer.from("lease-1"),
      returnOkOnSuccess: true
    })).resolves.toBe(true);

    expect(executor.calls).toHaveLength(1);
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[0]).toContain("OK_ON_SUCCESS");
  });

  it("falls back once when an older server does not support OK-only lease renewal", async () => {
    const legacyRecord = {
      id: "flow-1",
      partition_key: "tenant-a",
      state: "running",
      type: "job"
    };
    const executor = new FakeExecutor([
      new Error("ERR syntax error"),
      legacyRecord,
      legacyRecord
    ]);
    const client = new FerricStoreClient(executor);
    const options = {
      fencingToken: 7,
      leaseMs: 30_000,
      leaseToken: Buffer.from("lease-1"),
      partitionKey: "tenant-a",
      returnOkOnSuccess: true as const
    };

    await expect(client.extendLease("flow-1", options)).resolves.toBe(true);
    await expect(client.extendLease("flow-1", options)).resolves.toBe(true);

    expect(executor.calls).toHaveLength(3);
    expect(executor.calls[0]).toContain("RETURN");
    expect(executor.calls[1]).not.toContain("RETURN");
    expect(executor.calls[2]).not.toContain("RETURN");
  });

  it("single-flights the OK-only capability probe without serializing lease renewals", async () => {
    let releaseProbe: (() => void) | undefined;
    let markProbeStarted: (() => void) | undefined;
    const probeGate = new Promise<void>((resolve) => { releaseProbe = resolve; });
    const probeStarted = new Promise<void>((resolve) => { markProbeStarted = resolve; });
    let probeCalls = 0;
    let legacyCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        if (args.includes("RETURN")) {
          probeCalls += 1;
          markProbeStarted?.();
          await probeGate;
          throw new Error("ERR syntax error");
        }
        legacyCalls += 1;
        const id = args[1];
        if (typeof id !== "string") throw new TypeError("expected string flow id");
        return {
          id,
          partition_key: "tenant-a",
          state: "running",
          type: "job"
        };
      }
    };
    const client = new FerricStoreClient(executor);
    const renew = (id: string): Promise<boolean> => client.extendLease(id, {
      fencingToken: 7,
      leaseMs: 30_000,
      leaseToken: Buffer.from(`lease-${id}`),
      partitionKey: "tenant-a",
      returnOkOnSuccess: true
    });

    const first = renew("flow-0");
    await probeStarted;
    const rest = Array.from({ length: 5 }, (_, index) => renew(`flow-${index + 1}`));
    await new Promise((resolve) => setImmediate(resolve));
    const renewalsStartedWhileProbeWasPending = legacyCalls;

    releaseProbe?.();
    await Promise.all([first, ...rest]);
    expect(renewalsStartedWhileProbeWasPending).toBe(5);
    expect(probeCalls).toBe(1);
    expect(legacyCalls).toBe(6);
  });

  it("orders same-id Flow writes while allowing unrelated KV work", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    let markKvStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const kvStarted = new Promise<void>((resolve) => { markKvStarted = resolve; });
    const started: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        const name = commands[0]?.[0];
        const key = commands[0]?.[1];
        started.push(`${name as string}:${key as string}`);
        if (name === "FLOW.SIGNAL" && started.length === 1) {
          markFirstStarted?.();
          await firstGate;
        } else if (name === "SET") {
          markKvStarted?.();
        }
        return commands.map(() => Buffer.from("OK"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command("FLOW.SIGNAL", "flow-1", "SIGNAL", "first");
    await firstStarted;
    const sameId = client.command("FLOW.SIGNAL", "flow-1", "SIGNAL", "second");
    const kv = client.command("SET", "kv-key", "value");
    const kvOverlapped = await Promise.race([
      kvStarted.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 50))
    ]);

    expect(started).toEqual(["FLOW.SIGNAL:flow-1", "SET:kv-key"]);
    releaseFirst?.();
    await Promise.all([first, sameId, kv]);
    expect(kvOverlapped).toBe(true);
    expect(started).toEqual([
      "FLOW.SIGNAL:flow-1",
      "SET:kv-key",
      "FLOW.SIGNAL:flow-1"
    ]);
  });

  it("keeps same-key read-only auto-batches concurrent", async () => {
    let releaseFirst: (() => void) | undefined;
    let markFirstStarted: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const secondStarted = new Promise<void>((resolve) => { markSecondStarted = resolve; });
    let calls = 0;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("value");
      },
      async executePipeline(commands): Promise<unknown[]> {
        calls += 1;
        if (calls === 1) {
          markFirstStarted?.();
          await firstGate;
        } else {
          markSecondStarted?.();
        }
        return commands.map(() => Buffer.from("value"));
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });

    const first = client.command("GET", "shared-key");
    await firstStarted;
    const second = client.command("GET", "shared-key");
    const overlapped = await Promise.race([
      secondStarted.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 50))
    ]);

    releaseFirst?.();
    await Promise.all([first, second]);
    expect(overlapped).toBe(true);
  });

  it("waits for in-flight auto-batches before closing the executor", async () => {
    let releaseBatch: (() => void) | undefined;
    let markBatchStarted: (() => void) | undefined;
    const batchGate = new Promise<void>((resolve) => { releaseBatch = resolve; });
    const batchStarted = new Promise<void>((resolve) => { markBatchStarted = resolve; });
    let executorClosed = false;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(commands): Promise<unknown[]> {
        markBatchStarted?.();
        await batchGate;
        return commands.map(() => Buffer.from("OK"));
      },
      async close(): Promise<void> {
        executorClosed = true;
      }
    };
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, maxCommands: 1, maxDelayMs: 0 }
    });
    const request = client.command("SET", "first", "1");
    await batchStarted;
    let closeFinished = false;
    const closing = client.close().then(() => { closeFinished = true; });

    await new Promise((resolve) => setImmediate(resolve));
    expect(closeFinished).toBe(false);
    expect(executorClosed).toBe(false);

    releaseBatch?.();
    await Promise.all([request, closing]);
    expect(executorClosed).toBe(true);
  });

  it("rejects fused operations admitted after auto-batch close starts", async () => {
    const events: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executeFusedPipeline(): Promise<unknown[]> {
        events.push("fused");
        return [Buffer.from("OK"), []];
      },
      async close(): Promise<void> {
        events.push("close");
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await client.close();
    await expect(client.completeJobsAndClaimJobs(
      [{ id: "flow-1", leaseToken: Buffer.from("lease"), fencingToken: 1, type: "order", state: "running" }],
      "order",
      { jobOnly: true, state: "queued", worker: "worker-1" }
    )).rejects.toThrow("client is closed");
    expect(events).toEqual(["close"]);
  });

  it("waits for admitted topology helpers before auto-batch close", async () => {
    let markRouteStarted: (() => void) | undefined;
    let releaseRoute: (() => void) | undefined;
    const routeStarted = new Promise<void>((resolve) => { markRouteStarted = resolve; });
    const routeGate = new Promise<void>((resolve) => { releaseRoute = resolve; });
    const expected = RoutingTopology.build({
      ranges: [{
        endpoint: { host: "node.local", native_port: 6388, node: "node@local" },
        first_slot: 0,
        lane_id: 1,
        last_slot: 1023,
        shard: 0
      }],
      shard_count: 1
    }).routeKey("key");
    let executorClosed = false;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async route(): Promise<RoutingRoute> {
        markRouteStarted?.();
        await routeGate;
        return expected;
      },
      async close(): Promise<void> {
        executorClosed = true;
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });
    const route = client.route("key");
    await routeStarted;
    let closeFinished = false;
    const closing = client.close().then(() => { closeFinished = true; });

    try {
      await new Promise((resolve) => setImmediate(resolve));
      expect(closeFinished).toBe(false);
      expect(executorClosed).toBe(false);

      releaseRoute?.();
      await expect(route).resolves.toBe(expected);
      await closing;
      expect(executorClosed).toBe(true);
    } finally {
      releaseRoute?.();
      await closing;
    }
  });

  it("joins concurrent close callers to one auto-batch shutdown", async () => {
    let releaseClose: (() => void) | undefined;
    let markCloseStarted: (() => void) | undefined;
    const closeGate = new Promise<void>((resolve) => { releaseClose = resolve; });
    const closeStarted = new Promise<void>((resolve) => { markCloseStarted = resolve; });
    let closeCalls = 0;
    const executor: CommandExecutor = {
      async executeCommand(): Promise<unknown> {
        return Buffer.from("OK");
      },
      async executePipeline(): Promise<unknown[]> {
        return [];
      },
      async close(): Promise<void> {
        closeCalls += 1;
        markCloseStarted?.();
        await closeGate;
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const first = client.close();
    await closeStarted;
    let secondFinished = false;
    const second = client.close().then(() => { secondFinished = true; });
    await new Promise((resolve) => setImmediate(resolve));

    expect(closeCalls).toBe(1);
    expect(secondFinished).toBe(false);
    releaseClose?.();
    await Promise.all([first, second]);
  });

  it("does not auto-batch blocking claim commands", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("FLOW.CLAIM_DUE", "email", "WORKER", "worker-1"),
      client.command("SET", "auto-batch:claim-safe", "1")
    ]);

    expect(executor.calls[0]).toEqual(["FLOW.CLAIM_DUE", "email", "WORKER", "worker-1"]);
    expect(executor.pipelineCalls).toEqual([[["SET", "auto-batch:claim-safe", "1"]]]);
  });

  it("auto-batches nonblocking fused Flow commands in safe mode", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await Promise.all([
      client.command("FLOW.STEP_CONTINUE", "flow-1", "lease-1", "created", "charged"),
      client.command("FLOW.RUN_STEPS_MANY", "TYPE", "order", "ITEMS", [])
    ]);

    expect(executor.pipelineCalls).toEqual([[
      ["FLOW.STEP_CONTINUE", "flow-1", "lease-1", "created", "charged"],
      ["FLOW.RUN_STEPS_MANY", "TYPE", "order", "ITEMS", []]
    ]]);
  });

  it("never auto-batches blocking or session commands in all mode", async () => {
    const executor = new FakeExecutor();
    const client = new FerricStoreClient(executor, {
      autoBatch: { enabled: true, mode: "all" }
    });

    await Promise.all([
      client.command("XREADGROUP", "GROUP", "workers", "worker-1", "STREAMS", "jobs", ">"),
      client.command("BLMOVE", "source", "destination", "LEFT", "RIGHT", 1),
      client.command("BRPOPLPUSH", "source", "destination", 1),
      client.command("BLMPOP", 1, 1, "jobs", "LEFT"),
      client.command("WAIT", 1, 100),
      client.command("WAITAOF", 1, 1, 100),
      client.command("PING", "health"),
      client.command("OPTIONS"),
      client.command("SSUBSCRIBE", "shard-events"),
      client.command("SUNSUBSCRIBE", "shard-events"),
      client.command("SANDBOX", "tenant-a"),
      client.command("COMMAND_EXEC", "SSUBSCRIBE", "wrapped-events"),
      client.command("COMMAND_EXEC", "SANDBOX", "wrapped-tenant"),
      client.command("COMMAND_EXEC", "BLPOP", "wrapped-queue", 0),
      client.command("FLOW.SCHEDULE.FIRE_DUE", "WORKER", "scheduler-1", "BLOCK", 100)
    ]);

    expect(executor.pipelineCalls).toEqual([]);
    expect(executor.calls.map((call) => call[0])).toEqual([
      "XREADGROUP",
      "BLMOVE",
      "BRPOPLPUSH",
      "BLMPOP",
      "WAIT",
      "WAITAOF",
      "PING",
      "OPTIONS",
      "SSUBSCRIBE",
      "SUNSUBSCRIBE",
      "SANDBOX",
      "COMMAND_EXEC",
      "COMMAND_EXEC",
      "COMMAND_EXEC",
      "FLOW.SCHEDULE.FIRE_DUE"
    ]);
  });

  it("delegates topology helpers through client wrappers", async () => {
    const executor = new FakeExecutor() as FakeExecutor & {
      refreshTopology: () => Promise<RoutingTopology>;
      route: (key: string) => Promise<{
        endpoint: { host: string; nativePort: number; node: string };
        endpointKey: string;
        key: string;
        laneId: number;
        leaderNode: string;
        shard: number;
      }>;
    };
    const topology = RoutingTopology.empty();
    executor.refreshTopology = async () => topology;
    executor.route = async (key: string) => ({
      endpoint: { host: "127.0.0.1", nativePort: 6388, node: "node@local" },
      endpointKey: "127.0.0.1:6388",
      key,
      laneId: 1,
      leaderNode: "node@local",
      shard: 0
    });
    const client = new FerricStoreClient(executor, { autoBatch: true });

    await expect(client.refreshTopology()).resolves.toBe(topology);
    await expect(client.route("tenant-key")).resolves.toMatchObject({ key: "tenant-key", shard: 0 });
  });

  it("rejects only the failed promise for auto-batched item errors", async () => {
    const itemError = new Error("ERR item failed");
    const executor: CommandExecutor = {
      async executeCommand() {
        return Buffer.from("OK");
      },
      async executePipeline() {
        return [Buffer.from("OK"), itemError];
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const [first, second] = await Promise.allSettled([
      client.command("SET", "auto-batch:ok", "1"),
      client.command("SET", "auto-batch:error", "2")
    ]);

    expect(first).toMatchObject({ status: "fulfilled", value: Buffer.from("OK") });
    expect(second).toMatchObject({ status: "rejected" });
    if (second.status === "rejected") {
      const reason = second.reason as unknown;
      expect(reason).toBeInstanceOf(FerricStoreError);
      expect((reason as Error).message).toBe("ERR item failed");
    }
  });

  it("preserves successful auto-batch results when a command-only executor item fails", async () => {
    const applied: string[] = [];
    const executor: CommandExecutor = {
      async executeCommand(...args) {
        const keyArg = args[1];
        const key = typeof keyArg === "string"
          ? keyArg
          : Buffer.isBuffer(keyArg)
            ? keyArg.toString("utf8")
            : "";
        applied.push(key);
        if (key === "auto-batch:error") {
          throw new Error("ERR item failed");
        }
        return Buffer.from("OK");
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const [first, second] = await Promise.allSettled([
      client.command("SET", "auto-batch:ok", "1"),
      client.command("SET", "auto-batch:error", "2")
    ]);

    expect(applied).toEqual(["auto-batch:ok", "auto-batch:error"]);
    expect(first).toMatchObject({ status: "fulfilled", value: Buffer.from("OK") });
    expect(second).toMatchObject({ status: "rejected" });
  });

  it("preserves arbitrary rejection reasons through command-only auto-batches", async () => {
    const reasons: readonly unknown[] = [null, "plain rejection", { code: "custom_rejection" }];
    const executor: CommandExecutor = {
      async executeCommand(...args): Promise<unknown> {
        const index = Number(args[1]);
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- JavaScript promises may reject with any value
        return await Promise.reject(reasons[index]);
      }
    };
    const client = new FerricStoreClient(executor, { autoBatch: true });

    const settled = await Promise.allSettled(reasons.map(async (_reason, index) =>
      await client.command("SET", index, "value")
    ));

    settled.forEach((result, index) => {
      expect(result.status).toBe("rejected");
      if (result.status === "rejected") expect(result.reason).toBe(reasons[index]);
    });
  });

  it("normalizes non-finite auto-batch and backpressure controls", () => {
    const client = new FerricStoreClient(new FakeExecutor(), {
      autoBatch: {
        maxCommands: Number.NaN,
        maxDelayMs: Number.NaN
      },
      backpressure: {
        baseDelayMs: Number.NaN,
        jitterPct: Number.NaN,
        maxDelayMs: Number.NaN,
        maxRetries: Number.NaN
      }
    });

    expect(client.backpressure).toEqual({
      baseDelayMs: 25,
      jitterPct: 20,
      maxDelayMs: 1_000,
      maxRetries: 8
    });
    expect(
      (client.executor as unknown as {
        options: { maxCommands: number; maxDelayMs: number };
      }).options
    ).toMatchObject({ maxCommands: 512, maxDelayMs: 0 });
  });

});
