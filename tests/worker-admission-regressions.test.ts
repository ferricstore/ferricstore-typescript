import { describe, expect, it } from "vitest";

import {
  FerricStoreClient,
  QueueClient,
  WorkflowClient,
  complete,
  retry,
  type CommandArgument,
  type CommandExecutor
} from "../src/index.js";

describe("worker lease admission", () => {
  it.each(["runOnce", "continuous"] as const)(
    "does not admit a queued Queue job after lease renewal fails (%s)",
    async (mode) => {
      const firstStarted = deferred();
      const releaseFirst = deferred();
      const queuedRenewalFailed = deferred();
      const jobs = [queueFlow("email-1", 1), queueFlow("email-2", 2)];
      const terminalIds: unknown[] = [];
      let claimed = false;
      let secondHandled = false;
      const executor: CommandExecutor = {
        async executeCommand(...args: CommandArgument[]): Promise<unknown> {
          if (args[0] === "FLOW.CLAIM_DUE") {
            if (claimed) return [];
            claimed = true;
            return jobs;
          }
          if (args[0] === "FLOW.EXTEND_LEASE") {
            if (args[1] === "email-2") {
              queuedRenewalFailed.resolve();
              throw new Error("queued renewal rejected");
            }
            return Buffer.from("OK");
          }
          if (typeof args[0] === "string" && isTerminalCommand(args[0])) {
            terminalIds.push(args[1]);
          }
          return Buffer.from("OK");
        }
      };
      const queue = new QueueClient(new FerricStoreClient(executor)).queue("email");
      const worker = queue.worker({
        batchSize: 2,
        concurrency: 1,
        leaseMs: 100,
        leaseRenewIntervalMs: 1,
        refillStrategy: mode === "continuous" ? "continuous" : "wave",
        worker: "worker-1"
      });
      const running = (mode === "continuous"
        ? worker.run(handler)
        : worker.runOnce(handler)
      ).then(
        () => undefined,
        (error: unknown) => error
      );

      await firstStarted.promise;
      await queuedRenewalFailed.promise;
      await nextTurn();
      releaseFirst.resolve();

      const error = await running;
      expect(error).toMatchObject({ message: "FerricStore lease renewal failed" });
      expect(secondHandled).toBe(false);
      expect(terminalIds).not.toContain("email-2");

      async function handler(job: { readonly id: string }): Promise<ReturnType<typeof retry>> {
        if (job.id === "email-1") {
          firstStarted.resolve();
          await releaseFirst.promise;
        } else {
          secondHandled = true;
        }
        return retry();
      }
    }
  );

  it("does not admit a queued Workflow job after lease renewal fails", async () => {
    const firstStarted = deferred();
    const releaseFirst = deferred();
    const queuedRenewalFailed = deferred();
    const jobs = [workflowFlow("order-1", 1), workflowFlow("order-2", 2)];
    const terminalIds: unknown[] = [];
    let claimed = false;
    let secondHandled = false;
    const executor: CommandExecutor = {
      async executeCommand(...args: CommandArgument[]): Promise<unknown> {
        if (args[0] === "FLOW.CLAIM_DUE") {
          if (claimed) return [];
          claimed = true;
          return jobs;
        }
        if (args[0] === "FLOW.EXTEND_LEASE") {
          if (args[1] === "order-2") {
            queuedRenewalFailed.resolve();
            throw new Error("queued renewal rejected");
          }
          return Buffer.from("OK");
        }
        if (typeof args[0] === "string" && isTerminalCommand(args[0])) {
          terminalIds.push(args[1]);
        }
        return Buffer.from("OK");
      }
    };
    const workflow = new WorkflowClient(new FerricStoreClient(executor)).workflow({ type: "order" });
    workflow.state("charged", async (ctx) => {
      if (ctx.id === "order-1") {
        firstStarted.resolve();
        await releaseFirst.promise;
      } else {
        secondHandled = true;
      }
      return complete();
    });
    const running = workflow.worker({
      batchSize: 2,
      concurrency: 1,
      leaseMs: 100,
      leaseRenewIntervalMs: 1,
      states: ["charged"],
      worker: "worker-1"
    }).runOnce().then(
      () => undefined,
      (error: unknown) => error
    );

    await firstStarted.promise;
    await queuedRenewalFailed.promise;
    await nextTurn();
    releaseFirst.resolve();

    const error = await running;
    expect(error).toMatchObject({ message: "FerricStore lease renewal failed" });
    expect(secondHandled).toBe(false);
    expect(terminalIds).not.toContain("order-2");
  });
});

function isTerminalCommand(command: string): boolean {
  return command === "FLOW.COMPLETE" || command === "FLOW.COMPLETE_MANY" ||
    command === "FLOW.RETRY" || command === "FLOW.FAIL" || command === "FLOW.TRANSITION";
}

function queueFlow(id: string, fencingToken: number): Map<unknown, unknown> {
  return leasedFlow(id, "email", "queued", fencingToken);
}

function workflowFlow(id: string, fencingToken: number): Map<unknown, unknown> {
  return leasedFlow(id, "order", "charged", fencingToken);
}

function leasedFlow(
  id: string,
  type: string,
  state: string,
  fencingToken: number
): Map<unknown, unknown> {
  return new Map<unknown, unknown>([
    ["fencing_token", fencingToken],
    ["id", id],
    ["lease_token", Buffer.from(`lease-${fencingToken}`)],
    ["partition_key", "tenant-a"],
    ["state", state],
    ["type", type]
  ]);
}

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve;
  });
  return { promise, resolve: () => resolvePromise?.() };
}

async function nextTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}
