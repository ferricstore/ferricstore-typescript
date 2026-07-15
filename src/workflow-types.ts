import type { FlowStateMode } from "./client.js";
import type { Outcome } from "./outcomes.js";
import type {
  ClaimedItem,
  ExceptionPolicy,
  FlowRecord,
  RetryPolicy,
  ValueConfig
} from "./types.js";
import type { WorkflowContext } from "./workflow-context.js";

export interface ContinuousWorkflowJob {
  job: FlowRecord | ClaimedItem;
  leaseMs: number;
  registration: StateRegistration;
}

export type WorkflowHandler = (
  ctx: WorkflowContext
) => Promise<Outcome | void | unknown> | Outcome | void | unknown;

export interface StateOptions {
  mode?: FlowStateMode;
  leaseMs?: number;
  claimPayload?: boolean;
  claimRecord?: boolean;
  claimValues?: readonly string[];
  valueMaxBytes?: number;
  exceptionPolicy?: ExceptionPolicy;
  retryPolicy?: RetryPolicy;
  returnRecord?: boolean;
}

export interface WorkflowOptions {
  type: string;
  initialState?: string;
  valueConfig?: ValueConfig;
  worker?: string;
}

export interface WorkflowWorkerResult {
  claimed: number;
  applied: number;
  claimCalls: number;
  emptyClaims: number;
}

export interface StateRegistration {
  readonly claimValues?: readonly string[];
  readonly claimPayload: boolean;
  readonly claimRecord: boolean;
  readonly exceptionPolicy: ExceptionPolicy;
  readonly handler: WorkflowHandler;
  readonly leaseMs: number;
  readonly mode?: FlowStateMode;
  readonly name: string;
  readonly returnRecord: boolean;
  readonly retryPolicy?: Readonly<RetryPolicy>;
  readonly valueMaxBytes?: number;
}
