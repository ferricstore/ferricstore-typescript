import type { ClaimedItem, FlowRecord } from "./types.js";
import type { Workflow } from "./workflow.js";
import { WorkflowContext } from "./workflow-context.js";

/** @internal Coordinates lease renewal around an atomic lease-rotating write. */
export interface MutationCoordinator {
  pause(): Promise<void>;
  resume(job: ClaimedItem): void;
}

/** @internal Worker-only context that coordinates lease renewal around durable mutations. */
export class WorkerWorkflowContext extends WorkflowContext {
  constructor(
    workflow: Workflow,
    job: FlowRecord | ClaimedItem,
    stateName: string,
    leaseJob: ClaimedItem,
    mutationCoordinator: MutationCoordinator
  ) {
    super(workflow, job, stateName);
    this.configureWorkerMutation(leaseJob, mutationCoordinator);
  }
}
