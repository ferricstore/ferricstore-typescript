import { describe, expect, it } from "vitest";
import {
  COMMAND_OPCODES,
  FerricStoreClient,
  JsonCodec,
  NativeAdapter,
  RawCodec,
  StalePolicyGenerationError,
  type CommandArgument
} from "../../src/index.js";
import {
  binaryStateMeta,
  claimOne,
  createAndClaim,
  createManyAndClaim,
  deletePrefixedKeys,
  eventually,
  expectStateMeta,
  expectSupportedOrKnownServerError,
  fenced,
  field,
  isReadonlyArray,
  httpIntegration,
  integrationClient,
  integrationExecutor,
  suffix,
  text,
  url,
  waitForAclProjection
} from "./live-support.js";

const nativeProtocolCommands = new Set<string>(
  `
    ACL APPEND AUTH BF.ADD BF.CARD BF.EXISTS BF.INFO BF.MADD BF.MEXISTS BF.RESERVE
    BGSAVE BITCOUNT BITOP BITPOS BLMOVE BLMPOP BLPOP BRPOP CAS CF.ADD CF.ADDNX
    CF.COUNT CF.DEL CF.EXISTS CF.INFO CF.MEXISTS CF.RESERVE CLIENT CLUSTER.DEMOTE
    CLUSTER.FAILOVER CLUSTER.HEALTH CLUSTER.JOIN CLUSTER.KEYSLOT CLUSTER.LEAVE
    CLUSTER.PROMOTE CLUSTER.ROLE CLUSTER.SLOTS CLUSTER.STATS CLUSTER.STATUS
    CMS.INCRBY CMS.INFO CMS.INITBYDIM CMS.INITBYPROB CMS.MERGE CMS.QUERY COMMAND
    CONFIG COPY DBSIZE DEBUG DECR DECRBY DEL DISCARD ECHO EXEC EXISTS EXPIRE
    EXPIREAT EXPIRETIME EXTEND FERRICSTORE.BLOBGC FERRICSTORE.CAPABILITIES
    FERRICSTORE.CONFIG FERRICSTORE.DOCTOR FERRICSTORE.HOTNESS FERRICSTORE.KEY_INFO
    FERRICSTORE.METRICS FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FERRICSTORE.TELEMETRY
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_ERROR FETCH_OR_COMPUTE_RESULT
    FLOW.APPROVAL.APPROVE FLOW.APPROVAL.GET FLOW.APPROVAL.LIST FLOW.APPROVAL.REJECT
    FLOW.APPROVAL.REQUEST FLOW.ATTRIBUTES FLOW.ATTRIBUTE_VALUES FLOW.BUDGET.COMMIT
    FLOW.BUDGET.GET FLOW.BUDGET.LIST FLOW.BUDGET.RELEASE FLOW.BUDGET.RESERVE
    FLOW.CANCEL FLOW.CANCEL_MANY FLOW.CIRCUIT.CLOSE FLOW.CIRCUIT.GET
    FLOW.CIRCUIT.OPEN FLOW.CLAIM_DUE
    FLOW.COMPLETE FLOW.COMPLETE_MANY FLOW.CREATE FLOW.CREATE_MANY
    FLOW.EFFECT.COMPENSATE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.GET
    FLOW.EFFECT.RESERVE FLOW.EXTEND_LEASE FLOW.FAIL FLOW.FAIL_MANY
    FLOW.GET FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW FLOW.HISTORY
    FLOW.INFO FLOW.LIMIT.GET FLOW.LIMIT.LEASE FLOW.LIMIT.LIST FLOW.LIMIT.RELEASE
    FLOW.LIMIT.SPEND FLOW.POLICY.GET FLOW.POLICY.SET FLOW.QUERY FLOW.QUERY.INDEXES FLOW.RECLAIM
    FLOW.RETENTION_CLEANUP FLOW.RETRY FLOW.RETRY_MANY FLOW.REWIND
    FLOW.RUN_STEPS_MANY FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE
    FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.FIRE_DUE FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST
    FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME FLOW.SIGNAL FLOW.SPAWN_CHILDREN
    FLOW.START_AND_CLAIM FLOW.STATS FLOW.STEP_CONTINUE
    FLOW.TRANSITION FLOW.TRANSITION_MANY FLOW.VALUE.PUT FLUSHALL FLUSHDB GEOADD
    GEODIST GEOHASH GEOPOS GEOSEARCH GEOSEARCHSTORE GET GETBIT GETDEL GETEX
    GETRANGE GETSET HDEL HELLO HEXISTS HEXPIRE HEXPIRETIME HGET HGETALL HGETDEL
    HGETEX HINCRBY HINCRBYFLOAT HKEYS HLEN HMGET HPERSIST HPEXPIRE HPTTL
    HRANDFIELD HSCAN HSET HSETEX HSETNX HSTRLEN HTTL HVALS INCR INCRBY
    INCRBYFLOAT INFO INVOCATION.CREATE INVOCATION.DEFINITION.GET
    INVOCATION.DEFINITION.LIST INVOCATION.DEFINITION.PUT INVOCATION.GET
    INVOCATION.PARTITION.LIST KEY_INFO KEYS LASTSAVE LINDEX LINSERT LLEN LMOVE LOCK LOLWUT
    LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM MEMORY MGET MODULE MSET MSETNX
    MULTI OBJECT PERSIST PEXPIRE PEXPIREAT PEXPIRETIME PFADD PFCOUNT PFMERGE PING
    PSETEX PSUBSCRIBE PTTL PUBLISH PUBSUB PUNSUBSCRIBE QUIT RANDOMKEY RATELIMIT.ADD
    RENAME RENAMENX RESET RPOP RPOPLPUSH RPUSH RPUSHX SADD SANDBOX SAVE SCAN SCARD
    SDIFF SDIFFSTORE SELECT SET SETBIT SETEX SETNX SETRANGE SINTER SINTERCARD
    SINTERSTORE SISMEMBER SLOWLOG SMEMBERS SMISMEMBER SMOVE SPOP SRANDMEMBER SREM
    SSCAN STRLEN SUBSCRIBE SUNION SUNIONSTORE TDIGEST.ADD TDIGEST.BYRANK
    TDIGEST.BYREVRANK TDIGEST.CDF TDIGEST.CREATE TDIGEST.INFO TDIGEST.MAX
    TDIGEST.MERGE TDIGEST.MIN TDIGEST.QUANTILE TDIGEST.RANK TDIGEST.RESET
    TDIGEST.REVRANK TDIGEST.TRIMMED_MEAN TOPK.ADD TOPK.COUNT TOPK.INCRBY TOPK.INFO
    TOPK.LIST TOPK.QUERY TOPK.RESERVE TTL TYPE UNLINK UNLOCK UNSUBSCRIBE UNWATCH
    WAIT WAITAOF WATCH XACK XADD XDEL XGROUP XINFO XLEN XRANGE XREAD XREADGROUP
    XREVRANGE XTRIM ZADD ZCARD ZCOUNT ZINCRBY ZMSCORE ZPOPMAX ZPOPMIN
    ZRANDMEMBER ZRANGE ZRANGEBYSCORE ZRANK ZREM ZREVRANGE ZREVRANGEBYSCORE
    ZREVRANK ZSCAN ZSCORE
  `
    .trim()
    .split(/\s+/)
);

const nativeProtocolSharedIntegrationExcluded = {
  ACL: "requires protected/security-mode fixture, not the default open integration server",
  AUTH: "requires protected/security-mode fixture, not the default open integration server",
  BGSAVE: "admin persistence command; not part of normal SDK app command coverage",
  "CLUSTER.DEMOTE": "mutates cluster topology",
  "CLUSTER.FAILOVER": "mutates cluster topology",
  "CLUSTER.JOIN": "mutates cluster topology",
  "CLUSTER.LEAVE": "mutates cluster topology",
  "CLUSTER.PROMOTE": "mutates cluster topology",
  DEBUG: "debug/admin command, not normal SDK app surface",
  FLUSHALL: "destructive for shared integration state",
  FLUSHDB: "destructive for shared integration state",
  "FERRICSTORE.NAMESPACE": "management command requires namespace control-plane support",
  "FERRICSTORE.QUOTA": "management command requires quota control-plane support",
  HELLO: "connection handshake command",
  LASTSAVE: "admin persistence command; not part of normal SDK app command coverage",
  LOLWUT: "diagnostic compatibility command, not SDK app surface",
  MODULE: "admin module command; FerricStore does not load modules through SDK tests",
  QUIT: "connection lifecycle command",
  RESET: "connection lifecycle command",
  SANDBOX: "debug/admin command, not normal SDK app surface",
  SAVE: "admin persistence command; not part of normal SDK app command coverage",
  SELECT: "single-database compatibility command, not normal SDK app surface"
} as const;

const nativeProtocolSharedIntegrationExcludedNames = new Set<string>(
  Object.keys(nativeProtocolSharedIntegrationExcluded)
);

const nativeProtocolIntegrationExercised = new Set<string>(
  [...nativeProtocolCommands].filter((command) => !nativeProtocolSharedIntegrationExcludedNames.has(command))
);

function commandCatalogNames(value: unknown): Set<string> {
  const names = new Set<string>();
  if (!isReadonlyArray(value)) {
    return names;
  }

  for (const item of value) {
    if (isReadonlyArray(item) && item.length > 0) {
      names.add(text(item[0]).toUpperCase());
    }
  }

  return names;
}

function opcodeValue(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  return Number.parseInt(text(value), 0);
}

function optionsOpcodeTable(value: unknown): Record<string, number> {
  const rawOpcodes = field(value, "opcodes");
  if (!isReadonlyArray(rawOpcodes)) {
    throw new Error(`OPTIONS response does not contain opcodes list: ${JSON.stringify(value)}`);
  }

  const table: Record<string, number> = {};
  for (const item of rawOpcodes) {
    let name: unknown;
    let opcode: unknown;
    if (isReadonlyArray(item) && item.length >= 2) {
      name = item[0];
      opcode = item[1];
    } else {
      name = field(item, "name");
      opcode = field(item, "opcode");
    }
    if (name == null || opcode == null) {
      throw new Error(`OPTIONS opcode entry missing name/opcode: ${JSON.stringify(item)}`);
    }
    table[text(name).toUpperCase()] = opcodeValue(opcode);
  }
  return table;
}

function optionsCompactResponseOpcodes(value: unknown): Record<string, number[]> | undefined {
  const codecs = field(value, "response_codecs");
  const raw = field(codecs, "compact_response_opcodes");
  const entries = raw instanceof Map
    ? [...raw.entries()]
    : typeof raw === "object" && raw != null
      ? Object.entries(raw as Record<string, unknown>)
      : [];
  if (entries.length === 0) {
    return undefined;
  }

  const table: Record<string, number[]> = {};
  for (const [kind, opcodes] of entries) {
    if (!isReadonlyArray(opcodes)) {
      throw new Error(`OPTIONS compact response entry is not an opcode list: ${JSON.stringify(opcodes)}`);
    }
    table[text(kind)] = opcodes.map(opcodeValue);
  }
  return table;
}

function setDifference(left: ReadonlySet<string>, right: ReadonlySet<string>): string[] {
  return [...left].filter((item) => !right.has(item)).sort();
}


describe("FerricStore integration", () => {
  it("rejects connection-pinned transactions before any mutation is sent", async () => {
    const flow = await integrationClient();
    const key = `ts-sdk:unsupported-transaction:${suffix()}`;

    try {
      await expect(flow.command("MULTI")).rejects.toThrow(/MULTI.*(?:pinned connection|persistent native TCP session)/i);
      await expect(flow.pipeline([
        ["MULTI"],
        ["SET", key, "value"],
        ["EXEC"]
      ])).rejects.toThrow(/MULTI.*(?:pinned connection|persistent native TCP session)/i);
      await expect(flow.command("GET", key)).resolves.toBeNull();
    } finally {
      await flow.command("DEL", key).catch(() => undefined);
      await flow.close();
    }
  });

  it("keeps native control commands out of explicit and automatic pipelines", async () => {
    const flow = await integrationClient({
      autoBatch: { enabled: true, mode: "all" }
    });

    try {
      await expect(Promise.all([flow.ping("one"), flow.ping("two")])).resolves.toEqual([
        Buffer.from("one"),
        Buffer.from("two")
      ]);
      await expect(flow.pipeline([
        ["PING", "three"],
        ["PING", "four"]
      ])).resolves.toEqual([Buffer.from("three"), Buffer.from("four")]);
    } finally {
      await flow.close();
    }
  });

  it("completes acknowledged jobs and claims replacements in one native pipeline", async () => {
    const flow = await integrationClient({ codec: new RawCodec() });
    const runId = suffix();
    const now = Date.now();
    const type = `ts-sdk:fused-refill:${runId}`;
    const partitionKey = `${type}:partition`;
    const ids = Array.from({ length: 3 }, (_, index) => `${type}:${index}`);

    try {
      await flow.createMany(partitionKey, ids.map((id) => ({ id })), {
        nowMs: now,
        runAtMs: now,
        state: "queued",
        type
      });
      const initial = await flow.claimJobs(type, {
        limit: 2,
        nowMs: now,
        partitionKey,
        state: "queued",
        worker: "ts-sdk-fused-worker"
      });
      expect(initial).toHaveLength(2);

      const result = await flow.completeJobsAndClaimJobs(initial, type, {
        jobOnly: true,
        limit: 1,
        nowMs: now + 1,
        partitionKey,
        state: "queued",
        worker: "ts-sdk-fused-worker"
      }, { nowMs: now + 1 });

      expect(result).toMatchObject({ fused: true });
      expect(result.completionError).toBeUndefined();
      expect(result.claimError).toBeUndefined();
      expect(result.claimed).toHaveLength(1);
      await expect(flow.completeJobs(result.claimed, { nowMs: now + 2 })).resolves.toBeDefined();

      const records = await Promise.all(ids.map(async (id) => await flow.get(id, { partitionKey })));
      expect(records.map((record) => record?.state)).toEqual(["completed", "completed", "completed"]);
    } finally {
      await flow.close();
    }
  });

  it("keeps native protocol command catalog integration coverage classified", async () => {
    const flow = await integrationClient({ codec: new RawCodec() });

    try {
      const catalogNames = commandCatalogNames(await flow.command("COMMAND"));
      expect(setDifference(catalogNames, nativeProtocolCommands)).toEqual([]);
      expect(setDifference(nativeProtocolSharedIntegrationExcludedNames, nativeProtocolCommands)).toEqual([]);

      const catalogNotExercised = new Set<string>(
        setDifference(catalogNames, nativeProtocolIntegrationExercised)
      );
      expect(setDifference(catalogNotExercised, nativeProtocolSharedIntegrationExcludedNames)).toEqual([]);

      const unclassifiedKnownCommands = new Set<string>(
        setDifference(nativeProtocolCommands, nativeProtocolIntegrationExercised)
      );
      expect(setDifference(unclassifiedKnownCommands, nativeProtocolSharedIntegrationExcludedNames)).toEqual([]);
    } finally {
      await flow.close();
    }
  });

  it.skipIf(httpIntegration())("matches the live native OPTIONS opcode table", async () => {
    const flow = await integrationClient({ codec: new RawCodec() });

    try {
      expect(optionsOpcodeTable(await flow.command("OPTIONS"))).toEqual(COMMAND_OPCODES);
    } finally {
      await flow.close();
    }
  });

  it.skipIf(httpIntegration())("receives a well-formed compact response compatibility matrix from OPTIONS", async () => {
    const flow = await integrationClient({ codec: new RawCodec() });

    try {
      const advertised = optionsCompactResponseOpcodes(await flow.command("OPTIONS"));
      if (advertised != null) {
        expect(Object.keys(advertised)).not.toHaveLength(0);
        for (const opcodes of Object.values(advertised)) {
          expect(opcodes.every((opcode) => Number.isInteger(opcode) && opcode >= 0 && opcode <= 0xffff)).toBe(true);
        }
      }
    } finally {
      await flow.close();
    }
  });

  it.skipIf(httpIntegration())("receives policy replacement and generation fields in HELLO capabilities", async () => {
    const adapter = await NativeAdapter.fromUrl(url());

    try {
      const hello = await adapter.executeCommand("HELLO", "client_name", "typescript-contract-test");
      const capabilities = field(hello, "capabilities");
      const schemas = field(capabilities, "schemas");
      const policySchema = field(schemas, "FLOW.POLICY.SET");
      const fields = field(policySchema, "fields");
      expect(isReadonlyArray(fields)).toBe(true);
      expect((fields as readonly unknown[]).map(text)).toEqual(expect.arrayContaining([
        "expected_generation",
        "replace"
      ]));
    } finally {
      await adapter.close();
    }
  });

  it("uses KV helpers and a full Flow claim/complete cycle", async () => {
    const flow = await integrationClient({
      codec: new JsonCodec()
    });

    const runId = suffix();
    const key = `ts-sdk:kv:${runId}`;
    const id = `ts-sdk:flow:${runId}`;
    const type = "ts-sdk-integration";
    const now = Date.now();

    try {
      await flow.kv.set(key, { ok: true }, { px: 60_000 });
      await expect(flow.kv.get(key)).resolves.toEqual({ ok: true });

      await flow.create(id, {
        idempotent: true,
        nowMs: now,
        partitionKey: id,
        payload: { hello: "world" },
        runAtMs: now,
        state: "queued",
        type
      });

      const jobs = await flow.claimDue(type, {
        leaseMs: 30_000,
        limit: 1,
        nowMs: now + 1,
        partitionKey: id,
        payload: true,
        state: "queued",
        worker: "ts-sdk-integration-worker"
      });

      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      if (job == null || !("leaseToken" in job)) {
        throw new Error("expected an integration job");
      }
      expect(job).toMatchObject({
        id,
        partitionKey: id,
        payload: { hello: "world" },
        state: "running",
        type
      });

      await flow.complete(job.id, {
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        partitionKey: job.partitionKey,
        result: { ok: true }
      });

      const record = await flow.get(id, { partitionKey: id });
      expect(record?.state).toBe("completed");
    } finally {
      await flow.kv.del(key);
      await flow.close();
    }
  });

  it("claims full records across multiple states in one command response", async () => {
    const adapter = await integrationExecutor();
    const calls: CommandArgument[][] = [];
    const flow = new FerricStoreClient({
      async close(): Promise<void> {
        await adapter.close();
      },
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        calls.push(args);
        return await adapter.executeCommand(...args);
      }
    }, { codec: new RawCodec() });
    const runId = suffix();
    const type = `ts-sdk-multi-state-${runId}`;
    const partitionKey = `ts-sdk:multi-state:${runId}`;
    const createdId = `${partitionKey}:created`;
    const chargedId = `${partitionKey}:charged`;
    const ids = [createdId, chargedId];
    const now = Date.now();

    try {
      await flow.create(createdId, {
        nowMs: now,
        partitionKey,
        payload: Buffer.from("created-payload"),
        runAtMs: now,
        state: "created",
        type
      });
      await flow.create(chargedId, {
        nowMs: now,
        partitionKey,
        payload: Buffer.from("charged-payload"),
        runAtMs: now,
        state: "charged",
        type
      });

      const jobs = await flow.claimDue(type, {
        leaseMs: 30_000,
        limit: 2,
        nowMs: now + 1,
        partitionKey,
        states: ["created", "charged"],
        worker: "ts-sdk-multi-state-worker"
      });

      expect(jobs.map((job) => job.id).sort()).toEqual([...ids].sort());
      expect(
        jobs.map((job) => (job.payload as Buffer).toString("utf8")).sort()
      ).toEqual(["charged-payload", "created-payload"]);
      expect(calls.filter((call) => call[0] === "FLOW.CLAIM_DUE")).toHaveLength(1);
      expect(calls.some((call) => call[0] === "FLOW.GET")).toBe(false);
      for (const job of jobs) {
        await flow.complete(job.id, {
          fencingToken: job.fencingToken,
          leaseToken: job.leaseToken,
          nowMs: now + 2,
          partitionKey
        });
      }
    } finally {
      await flow.close();
    }
  });

  it("stores state metadata and policy indexed state metadata", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-state-meta-${runId}`;
    const id = `ts-sdk:state-meta:${runId}`;
    const partitionKey = `${id}:partition`;
    const now = Date.now();

    try {
      await expect(flow.installPolicy(type, { indexedStateMeta: "version" })).resolves.toBeDefined();
      const policy = await flow.policyGet(type);
      expect(policy.indexedStateMeta).toBe("version");

      await expect(flow.create(id, {
        idempotent: true,
        nowMs: now,
        partitionKey,
        runAtMs: now,
        state: "accept",
        stateMeta: { owner: "risk", version: "1" },
        type
      })).resolves.toBeDefined();

      await expect(flow.get(id, { partitionKey })).resolves.toMatchObject({
        indexedStateMeta: "version",
        stateMeta: {
          accept: binaryStateMeta({ owner: "risk", version: "1" })
        }
      });

      await eventually(
        () => flow.search(type, {
          partitionKey,
          state: "accept",
          stateMeta: { version: "1" }
        }),
        (records) => records.some((record) => record.id === id),
        "FLOW.QUERY state metadata projection did not become ready"
      );

      const job = await claimOne(flow, type, "accept", partitionKey, { nowMs: now + 1 });
      await expect(flow.complete(id, {
        fencingToken: job.fencingToken,
        leaseToken: job.leaseToken,
        nowMs: now + 2,
        partitionKey,
        stateMeta: { version: "3" }
      })).resolves.toBeDefined();

      await expect(flow.get(id, { partitionKey })).resolves.toMatchObject({
        stateMeta: {
          accept: binaryStateMeta({ owner: "risk", version: "1" }),
          completed: binaryStateMeta({ version: "3" })
        }
      });
    } finally {
      await flow.close();
    }
  });

  it("patches, replaces, and generation-fences policies atomically", async () => {
    const flow = await integrationClient();
    const type = `ts-sdk-policy-cas-${suffix()}`;

    try {
      const initial = await flow.installPolicy(type, {
        maxActiveMs: 1_000,
        retentionTtlMs: 86_400_123,
        states: { queued: { mode: "fifo" } }
      });
      expect(initial.retention.ttlMs).toBe(86_400_123);
      expect(initial.states?.queued?.retention.ttlMs).toBe(86_400_123);
      const patched = await flow.installPolicy(type, {
        expectedGeneration: initial.generation,
        maxActiveMs: 2_000
      });
      expect(patched).toMatchObject({
        generation: initial.generation + 1,
        maxActiveMs: 2_000,
        states: { queued: { mode: "fifo" } }
      });

      await expect(flow.installPolicy(type, {
        expectedGeneration: initial.generation,
        maxActiveMs: 3_000
      })).rejects.toBeInstanceOf(StalePolicyGenerationError);
      await expect(flow.policyGet(type)).resolves.toMatchObject({
        generation: patched.generation,
        maxActiveMs: 2_000,
        states: { queued: { mode: "fifo" } }
      });

      const replaced = await flow.installPolicy(type, {
        expectedGeneration: patched.generation,
        replace: true
      });
      expect(replaced.generation).toBe(patched.generation + 1);
      expect(replaced.maxActiveMs).toBeUndefined();
      expect(replaced.states).toEqual({});
    } finally {
      await flow.close();
    }
  });

  it("enforces FIFO state policy edges on the real server", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const parallelType = `ts-sdk-fifo-default-${runId}`;
    const fifoType = `ts-sdk-fifo-policy-${runId}`;
    const partition = `ts-sdk:fifo:${runId}:partition`;
    const now = Date.now();

    try {
      for (const name of ["first", "second"]) {
        await expect(flow.create(`ts-sdk:fifo-default:${runId}:${name}`, {
          nowMs: now,
          partitionKey: partition,
          payload: { name },
          priority: 1,
          runAtMs: now,
          state: "queued",
          type: parallelType
        })).resolves.toBeDefined();
      }

      await expect(flow.claimJobs(parallelType, {
        limit: 2,
        nowMs: now + 1,
        partitionKey: partition,
        priority: 1,
        state: "queued",
        worker: "ts-sdk-default-parallel-worker"
      })).resolves.toHaveLength(2);

      await expect(flow.installPolicy(fifoType, {
        states: {
          queued: { mode: "fifo" },
          start: { mode: "parallel" }
        }
      })).resolves.toBeDefined();
      expect((await flow.policyGet(fifoType, { state: "queued" })).mode).toBe("fifo");
      expect((await flow.policyGet(fifoType, { state: "start" })).mode).toBe("parallel");

      const fifoPartitions = [`${partition}:a`, `${partition}:b`];
      const firstIds = fifoPartitions.map((_key, index) => `ts-sdk:fifo:${runId}:${index}:first`);
      const secondIds = fifoPartitions.map((_key, index) => `ts-sdk:fifo:${runId}:${index}:second`);
      for (let index = 0; index < fifoPartitions.length; index += 1) {
        const partitionKey = fifoPartitions[index];
        const firstId = firstIds[index];
        const secondId = secondIds[index];
        if (partitionKey == null || firstId == null || secondId == null) throw new Error("missing FIFO fixture");
        await flow.create(firstId, {
          nowMs: now + 2,
          partitionKey,
          runAtMs: now + 2,
          state: "queued",
          type: fifoType
        });
        await flow.create(secondId, {
          nowMs: now + 3,
          partitionKey,
          runAtMs: now + 3,
          state: "queued",
          type: fifoType
        });
      }
      const firstWave = await flow.claimJobs(fifoType, {
        limit: 4,
        nowMs: now + 4,
        partitionKeys: fifoPartitions,
        state: "queued",
        worker: "ts-sdk-fifo-wave-one"
      });
      expect(firstWave.map((job) => job.id).sort()).toEqual([...firstIds].sort());
      for (const job of firstWave) {
        await flow.complete(job.id, {
          fencingToken: job.fencingToken,
          leaseToken: job.leaseToken,
          nowMs: now + 5,
          partitionKey: job.partitionKey
        });
      }
      const secondWave = await flow.claimJobs(fifoType, {
        limit: 4,
        nowMs: now + 6,
        partitionKeys: fifoPartitions,
        state: "queued",
        worker: "ts-sdk-fifo-wave-two"
      });
      expect(secondWave.map((job) => job.id).sort()).toEqual([...secondIds].sort());

      await expect(flow.create(`ts-sdk:fifo-no-partition:${runId}`, {
        nowMs: now + 10,
        payload: { bad: "missing-partition" },
        runAtMs: now + 10,
        state: "queued",
        type: fifoType
      })).rejects.toThrow(/partition_key is required for fifo state/i);

      await expect(flow.create(`ts-sdk:fifo-priority:${runId}`, {
        nowMs: now + 11,
        partitionKey: partition,
        payload: { bad: "priority" },
        priority: 1,
        runAtMs: now + 11,
        state: "queued",
        type: fifoType
      })).rejects.toThrow(/priority is not supported for fifo state/i);

      const transitionId = `ts-sdk:fifo-transition:${runId}`;
      await flow.create(transitionId, {
        nowMs: now + 20,
        payload: { step: "start" },
        runAtMs: now + 20,
        state: "start",
        type: fifoType
      });
      const startJobs = await flow.claimJobs(fifoType, {
        limit: 1,
        nowMs: now + 21,
        state: "start",
        worker: "ts-sdk-fifo-transition-worker"
      });
      expect(startJobs).toHaveLength(1);
      const startJob = startJobs[0];
      if (startJob == null) {
        throw new Error("expected start job");
      }
      expect(startJob.partitionKey).toBeDefined();

      await expect(flow.transition(transitionId, {
        fencingToken: startJob.fencingToken,
        fromState: startJob.state,
        leaseToken: startJob.leaseToken,
        nowMs: now + 22,
        runAtMs: now + 22,
        toState: "queued"
      })).rejects.toThrow(/partition_key is required for fifo state/i);

      await expect(flow.transition(transitionId, {
        fencingToken: startJob.fencingToken,
        fromState: startJob.state,
        leaseToken: startJob.leaseToken,
        nowMs: now + 23,
        partitionKey: startJob.partitionKey,
        runAtMs: now + 23,
        toState: "queued"
      })).resolves.toBeDefined();

      const queued = await flow.claimJobs(fifoType, {
        limit: 1,
        nowMs: now + 24,
        partitionKey: startJob.partitionKey,
        state: "queued",
        worker: "ts-sdk-fifo-queued-worker"
      });
      expect(queued.map((job) => job.id)).toEqual([transitionId]);
    } finally {
      await flow.close();
    }
  });

  it("stores state metadata for every flow mutation command", async () => {
    const flow = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const type = `ts-sdk-state-meta-all-${runId}`;
    const now = Date.now();

    try {
      await flow.installPolicy(type, { indexedStateMeta: "version" });

      const createId = `ts-sdk:state-meta-create:${runId}`;
      const createPartition = `${createId}:partition`;
      await flow.create(createId, {
        nowMs: now,
        partitionKey: createPartition,
        runAtMs: now,
        state: "created",
        stateMeta: { version: "1" },
        type
      });
      expectStateMeta(await flow.get(createId, { partitionKey: createPartition }), "created", { version: "1" });

      const transitionJob = await createAndClaim(flow, type, runId, "state-meta-transition", { nowMs: now, state: "transition-start" });
      await flow.transition(transitionJob.id, {
        fencingToken: transitionJob.job.fencingToken,
        fromState: transitionJob.job.state,
        leaseToken: transitionJob.job.leaseToken,
        nowMs: now + 1,
        partitionKey: transitionJob.partitionKey,
        stateMeta: { version: "2" },
        toState: "transition-next"
      });
      expectStateMeta(await flow.get(transitionJob.id, { partitionKey: transitionJob.partitionKey }), "transition-next", { version: "2" });

      const completeJob = await createAndClaim(flow, type, runId, "state-meta-complete", { nowMs: now, state: "complete-start" });
      await flow.complete(completeJob.id, {
        fencingToken: completeJob.job.fencingToken,
        leaseToken: completeJob.job.leaseToken,
        nowMs: now + 2,
        partitionKey: completeJob.partitionKey,
        stateMeta: { version: "3" }
      });
      expectStateMeta(await flow.get(completeJob.id, { partitionKey: completeJob.partitionKey }), "completed", { version: "3" });

      const retryJob = await createAndClaim(flow, type, runId, "state-meta-retry", { nowMs: now, state: "retry-start" });
      await flow.retry(retryJob.id, {
        fencingToken: retryJob.job.fencingToken,
        leaseToken: retryJob.job.leaseToken,
        nowMs: now + 3,
        partitionKey: retryJob.partitionKey,
        runAtMs: now + 3,
        stateMeta: { version: "4" }
      });
      expectStateMeta(await flow.get(retryJob.id, { partitionKey: retryJob.partitionKey }), "retry-start", { version: "4" });

      const failJob = await createAndClaim(flow, type, runId, "state-meta-fail", { nowMs: now, state: "fail-start" });
      await flow.fail(failJob.id, {
        fencingToken: failJob.job.fencingToken,
        leaseToken: failJob.job.leaseToken,
        nowMs: now + 4,
        partitionKey: failJob.partitionKey,
        stateMeta: { version: "5" }
      });
      expectStateMeta(await flow.get(failJob.id, { partitionKey: failJob.partitionKey }), "failed", { version: "5" });

      const cancelJob = await createAndClaim(flow, type, runId, "state-meta-cancel", { nowMs: now, state: "cancel-start" });
      await flow.cancel(cancelJob.id, {
        fencingToken: cancelJob.job.fencingToken,
        leaseToken: cancelJob.job.leaseToken,
        nowMs: now + 5,
        partitionKey: cancelJob.partitionKey,
        stateMeta: { version: "6" }
      });
      expectStateMeta(await flow.get(cancelJob.id, { partitionKey: cancelJob.partitionKey }), "cancelled", { version: "6" });

      const createManyIds = [`ts-sdk:state-meta-create-many:${runId}:a`, `ts-sdk:state-meta-create-many:${runId}:b`];
      const createManyPartition = `ts-sdk:state-meta-create-many:${runId}:partition`;
      await flow.createMany(createManyPartition, createManyIds.map((id) => ({ id })), {
        nowMs: now,
        runAtMs: now,
        state: "create-many",
        stateMeta: { version: "7" },
        type
      });
      for (const id of createManyIds) {
        expectStateMeta(await flow.get(id, { partitionKey: createManyPartition }), "create-many", { version: "7" });
      }

      const transitionMany = await createManyAndClaim(flow, type, runId, "state-meta-transition-many", "transition-many-start", now);
      const transitionManyState = transitionMany.jobs[0]?.state;
      if (transitionManyState == null) {
        throw new Error("expected transitionMany state");
      }
      await flow.transitionMany(transitionMany.partitionKey, {
        fromState: transitionManyState,
        items: transitionMany.jobs.map(fenced),
        nowMs: now + 6,
        stateMeta: { version: "8" },
        toState: "transition-many-next"
      });
      for (const id of transitionMany.ids) {
        expectStateMeta(await flow.get(id, { partitionKey: transitionMany.partitionKey }), "transition-many-next", { version: "8" });
      }

      const completeMany = await createManyAndClaim(flow, type, runId, "state-meta-complete-many", "complete-many-start", now);
      await flow.completeMany(completeMany.partitionKey, completeMany.jobs, {
        nowMs: now + 7,
        stateMeta: { version: "9" }
      });
      for (const id of completeMany.ids) {
        expectStateMeta(await flow.get(id, { partitionKey: completeMany.partitionKey }), "completed", { version: "9" });
      }

      const retryMany = await createManyAndClaim(flow, type, runId, "state-meta-retry-many", "retry-many-start", now);
      await flow.retryMany(retryMany.partitionKey, retryMany.jobs, {
        nowMs: now + 8,
        runAtMs: now + 8,
        stateMeta: { version: "10" }
      });
      for (const id of retryMany.ids) {
        expectStateMeta(await flow.get(id, { partitionKey: retryMany.partitionKey }), "retry-many-start", { version: "10" });
      }

      const failMany = await createManyAndClaim(flow, type, runId, "state-meta-fail-many", "fail-many-start", now);
      await flow.failMany(failMany.partitionKey, failMany.jobs, {
        nowMs: now + 9,
        stateMeta: { version: "11" }
      });
      for (const id of failMany.ids) {
        expectStateMeta(await flow.get(id, { partitionKey: failMany.partitionKey }), "failed", { version: "11" });
      }

      const cancelManyIds = [`ts-sdk:state-meta-cancel-many:${runId}:a`, `ts-sdk:state-meta-cancel-many:${runId}:b`];
      const cancelManyPartition = `ts-sdk:state-meta-cancel-many:${runId}:partition`;
      await flow.createMany(cancelManyPartition, cancelManyIds.map((id) => ({ id })), {
        nowMs: now,
        runAtMs: now,
        state: "cancel-many-start",
        type
      });
      await flow.cancelMany(cancelManyPartition, cancelManyIds.map((id) => ({ fencingToken: 0, id, partitionKey: cancelManyPartition })), {
        nowMs: now + 10,
        stateMeta: { version: "12" }
      });
      for (const id of cancelManyIds) {
        expectStateMeta(await flow.get(id, { partitionKey: cancelManyPartition }), "cancelled", { version: "12" });
      }
    } finally {
      await flow.close();
    }
  }, 20_000);

  it("covers native helpers and read-only diagnostics", async () => {
    let flow = await integrationClient({ codec: new JsonCodec() });
    const runId = suffix();
    const prefix = `ts-sdk:native:${runId}:`;
    const key = `${prefix}cas`;
    const lockKey = `${prefix}lock`;
    const rateKey = `${prefix}rate`;
    const cacheKey = `${prefix}cache`;

    try {
      expect(await flow.ping()).toSatisfy((value: unknown) => value === true || text(value) === "PONG");
      expect(text(await flow.echo("hello"))).toBe("hello");
      const pipeline = await flow.pipeline([
        ["SET", key, flow.codec.encode("old")],
        ["GET", key]
      ]);
      expect(flow.codec.decode(pipeline[1] as Buffer)).toBe("old");

      const mgetKeys = Array.from({ length: 4 }, (_, index) => `${prefix}mget:${index}`);
      await Promise.all(mgetKeys.map(async (mgetKey, index) => {
        await flow.kv.set(mgetKey, { index });
      }));
      const customPayloadPipeline = await flow.pipeline([
        ["MGET", mgetKeys[0], mgetKeys[1]],
        ["MGET", mgetKeys[2], mgetKeys[3]]
      ]);
      expect((customPayloadPipeline[0] as Buffer[]).map((value) => flow.codec.decode(value))).toEqual([
        { index: 0 },
        { index: 1 }
      ]);
      expect((customPayloadPipeline[1] as Buffer[]).map((value) => flow.codec.decode(value))).toEqual([
        { index: 2 },
        { index: 3 }
      ]);

      const bigintKey = `${prefix}bigint`;
      await flow.command("SET", bigintKey, "9007199254740992");
      await expect(flow.command("INCR", bigintKey)).resolves.toBe(9_007_199_254_740_993n);
      const typedBigintKey = `${prefix}typed-bigint`;
      await flow.command("SET", typedBigintKey, "9007199254740992");
      await expect(flow.kv.incr(typedBigintKey)).resolves.toBe(9_007_199_254_740_993n);

      await expect(flow.cas(key, "old", "new")).resolves.toBe(true);
      await expect(flow.cas(key, "old", "newer")).resolves.toBe(false);
      await expect(flow.cas(`${prefix}missing-cas`, "old", "new")).resolves.toBe(false);
      expect(flow.codec.decode((await flow.command("GET", key)) as Buffer)).toBe("new");

      await expect(flow.lock(lockKey, "owner-a", 30_000)).resolves.toBe(true);
      await expect(flow.extendLock(lockKey, "owner-a", 30_000)).resolves.toBe(1);
      await expect(flow.unlock(lockKey, "owner-a")).resolves.toBe(1);

      const rate = await flow.rateLimitAdd(rateKey, { count: 2, max: 5, windowMs: 60_000 });
      expect(rate.count).toBeGreaterThanOrEqual(1);
      expect(rate.remaining).toBeGreaterThanOrEqual(0);

      const keyInfo = await flow.keyInfo(key);
      expect(keyInfo).toMatchObject({
        hotCacheStatus: "hot",
        ttlMs: -1,
        type: "string"
      });
      expect(keyInfo.valueSize).toBeGreaterThan(0);
      expect(keyInfo.lastWriteShard).toBeGreaterThanOrEqual(0);

      if (!httpIntegration()) {
        const first = await flow.fetchOrCompute(cacheKey, { hint: "integration", ttlMs: 60_000 });
        expect(first.shouldCompute).toBe(true);
        if (!first.shouldCompute) throw new Error("expected a fetch-or-compute lease");
        await expect(flow.fetchOrComputeResult(cacheKey, { computed: true }, {
          computeToken: first.computeToken,
          ttlMs: 60_000
        })).resolves.toBe(true);
        const cached = await flow.fetchOrCompute<{ computed: boolean }>(cacheKey, { ttlMs: 60_000 });
        expect(cached.hit).toBe(true);
        if (!cached.hit) throw new Error("expected a cached fetch-or-compute result");
        expect(cached.value).toEqual({ computed: true });

        const errorKey = `${prefix}cache-error`;
        const firstError = await flow.fetchOrCompute(errorKey, { ttlMs: 60_000 });
        expect(firstError.shouldCompute).toBe(true);
        if (!firstError.shouldCompute) throw new Error("expected a fetch-or-compute lease");
        await expect(flow.fetchOrComputeError(errorKey, "boom", {
          computeToken: firstError.computeToken
        })).resolves.toBe(true);
      }

      await expect(flow.serverInfo("server")).resolves.toContain("#");
      await expectSupportedOrKnownServerError(flow.configGet("*"));
      await expectSupportedOrKnownServerError(flow.configGetLocal("protected-mode"));
      await expectSupportedOrKnownServerError(flow.configSet(`ts-sdk-${runId}-unknown`, "1"));
      await expectSupportedOrKnownServerError(flow.configResetStat());
      await expectSupportedOrKnownServerError(flow.configRewrite());
      await expectSupportedOrKnownServerError(flow.slowlogGet(10));
      await expectSupportedOrKnownServerError(flow.slowlogLen());
      await expectSupportedOrKnownServerError(flow.slowlogReset());
      await expectSupportedOrKnownServerError(flow.commandMetadata());
      await expect(flow.commandCount()).resolves.toBeGreaterThan(0);
      await expect(flow.commandList()).resolves.not.toHaveLength(0);
      await expect(flow.commandInfo("get")).resolves.toHaveLength(1);
      await expect(flow.commandDocs("get")).resolves.toBeDefined();
      expect((await flow.commandGetKeys(["GET", key])).map(text)).toContain(key);
      if (!httpIntegration()) {
        await expect(flow.clientId()).resolves.toBeGreaterThan(0);
        await expect(flow.clientSetName(`ts-sdk-${runId}`)).rejects.toThrow(/stable single connection/i);
        const stableClient = await FerricStoreClient.fromUrl(url(), {
          codec: new JsonCodec(),
          reconnect: false
        });
        try {
          await expect(stableClient.clientSetName(`ts-sdk-${runId}`)).resolves.toBe(true);
          await expect(stableClient.clientGetName()).resolves.toBe(`ts-sdk-${runId}`);
          await expectSupportedOrKnownServerError(stableClient.auth("bad-password"));
        } finally {
          await stableClient.close();
        }
        await expect(flow.clientInfo()).resolves.toContain("id=");
        await expect(flow.clientList()).resolves.toContain("id=");
        await expect(flow.clientTracking("ON", { optin: true })).rejects.toThrow(/not supported/i);
        await expect(flow.clientTrackingInfo()).resolves.toBeDefined();
        await expect(flow.clientGetRedir()).resolves.toBeGreaterThanOrEqual(0);
        await expect(flow.clientCaching("NO")).rejects.toThrow(/not supported/i);
        await expect(flow.clientTracking("OFF")).rejects.toThrow(/not supported/i);
      }
      await expectSupportedOrKnownServerError(flow.save());
      await expectSupportedOrKnownServerError(flow.bgsave());
      await expect(flow.lastsave()).resolves.toBeGreaterThanOrEqual(0);
      await expectSupportedOrKnownServerError(flow.lolwut());
      await expect(flow.moduleList()).resolves.toEqual([]);
      await expect(flow.publish(`${prefix}channel`, "hello")).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.pubsubChannels()).resolves.toBeDefined();
      await expect(flow.pubsubNumSub(`${prefix}channel`)).resolves.toBeDefined();
      await expect(flow.pubsubNumPat()).resolves.toBeGreaterThanOrEqual(0);
      await expectSupportedOrKnownServerError(flow.aclSetUser(`ts-sdk-${runId}`, ["off"]));
      await expectSupportedOrKnownServerError(flow.aclGetUser("default"));
      await expectSupportedOrKnownServerError(flow.aclList());
      if (!httpIntegration()) {
        const aclWhoami = await expectSupportedOrKnownServerError(
          flow.aclWhoami(),
          /unsupported|unknown|not supported|not enabled|invalid/i
        );
        if (aclWhoami != null) expect(aclWhoami).toBe("default");
      }
      await expectSupportedOrKnownServerError(flow.aclSave());
      if (!httpIntegration()) {
        const aclLoad = await expectSupportedOrKnownServerError(
          flow.aclLoad(),
          /unsupported|unknown|not supported|not enabled|invalid|no config file|connection closed/i
        );
        if (aclLoad != null) {
          await flow.close();
          flow = await integrationClient({ codec: new JsonCodec() });
          await expect(waitForAclProjection(async () => await flow.aclWhoami())).resolves.toBe("default");
        }
      }
      await expectSupportedOrKnownServerError(
        waitForAclProjection(async () => await flow.aclDelUser(`ts-sdk-${runId}`))
      );
      if (!httpIntegration()) {
        await expect(flow.auth("bad-password")).rejects.toThrow(/stable single connection/i);
      }
      await expect(flow.clusterHealth()).resolves.toBeTypeOf("object");
      await expect(flow.clusterStats()).resolves.toBeTypeOf("object");
      await expect(flow.clusterKeyslot(key)).resolves.toBeGreaterThanOrEqual(0);
      await expect(flow.clusterSlots()).resolves.toBeDefined();
      await expect(flow.clusterStatus()).resolves.toBeTypeOf("object");
      await expect(flow.clusterRole()).resolves.toBeDefined();
      await expectSupportedOrKnownServerError(flow.clusterJoin("invalid-node"));
      await expectSupportedOrKnownServerError(flow.clusterFailover(9_999, "invalid-node"));
      await expectSupportedOrKnownServerError(flow.clusterPromote("invalid-node"));
      await expectSupportedOrKnownServerError(flow.clusterDemote("invalid-node"));
      await expect(flow.ferricstoreConfig("GET", "*")).resolves.toBeDefined();
      await expect(flow.ferricstoreMetrics()).resolves.toContain("ferricstore_");
      await expect(flow.ferricstoreHotness()).resolves.toBeTypeOf("object");
      await expect(flow.ferricstoreBlobgc()).resolves.toBeDefined();
      await expect(flow.ferricstoreDoctor("CHECK", "SCOPE", "BITCASK")).resolves.toBeDefined();
      await expectSupportedOrKnownServerError(flow.invocationDefinitionPut({ name: `send-email-${runId}` }));
      await expectSupportedOrKnownServerError(flow.invocationDefinitionGet(`send-email-${runId}`));
      await expectSupportedOrKnownServerError(flow.invocationDefinitionList());
      await expectSupportedOrKnownServerError(flow.invocationCreate(`send-email-${runId}`, { source: "typescript-sdk-integration" }));
      await expectSupportedOrKnownServerError(flow.invocationGet(`invocation-${runId}`));
      await expectSupportedOrKnownServerError(flow.invocationPartitionList(`send-email-${runId}`));
    } finally {
      await deletePrefixedKeys(flow, prefix);
      await flow.close();
    }
  });
});
