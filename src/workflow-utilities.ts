import type { FlowStateMode } from "./client.js";
import type { StateRegistration } from "./workflow-types.js";

export interface ResolvedWorkflowState {
  registration: StateRegistration;
  stateName: string;
}

export function resolveWorkflowStates(
  configured: readonly string[],
  registrationFor: (stateName: string) => StateRegistration | undefined
): ResolvedWorkflowState[] {
  const stateNames = [...new Set(configured)];
  if (stateNames.length === 0) throw new Error("Workflow worker requires at least one state");
  return stateNames.map((stateName) => {
    const registration = registrationFor(stateName);
    if (registration == null) {
      throw new Error(`No handler registered for workflow state '${stateName}'`);
    }
    return { registration, stateName };
  });
}

export function normalizeStateMode(mode: FlowStateMode): FlowStateMode {
  if (mode === "fifo" || mode === "parallel") return mode;
  throw new Error("state mode must be 'fifo' or 'parallel'");
}

export function valueRefToString(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  if (typeof value === "object" && value != null && Object.hasOwn(value, "ref")) {
    const ref = (value as { ref?: unknown }).ref;
    return typeof ref === "string" ? ref : Buffer.isBuffer(ref) ? ref.toString("utf8") : undefined;
  }
  return undefined;
}
