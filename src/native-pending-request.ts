import type { LongTimer } from "./internal.js";
import type { CompactClaimMode } from "./protocol.js";

export interface PendingRequest {
  readonly compactClaimMode?: CompactClaimMode;
  discardedResponseBytes?: number;
  discardedResponseFrames?: number;
  readonly hasFlowControlCredit: boolean;
  readonly indefinite: boolean;
  lateResponseTimer?: LongTimer;
  readonly laneId: number;
  readonly opcode: number;
  readonly pipelineClaimModes?: readonly (CompactClaimMode | undefined)[];
  readonly reject: (reason: unknown) => void;
  readonly resolve: (value: unknown) => void;
  timedOut?: boolean;
}
