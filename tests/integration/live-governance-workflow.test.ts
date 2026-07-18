import { describe } from "vitest";
import { registerGovernanceWorkflowIntegrationTests } from "./live-governance-workflow-cases.js";

describe("FerricStore governance and workflow integration", () => {
  registerGovernanceWorkflowIntegrationTests();
});
