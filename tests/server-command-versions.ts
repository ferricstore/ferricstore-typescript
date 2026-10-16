import { expect } from "vitest";
import { COMMAND_OPCODES } from "../src/protocol-constants.js";

// Native commands added in FerricStore 0.12.0. The older servers this SDK is
// still tested against (the 0.11.4 floor and the pinned core revision) neither
// advertise nor accept them. Every other opcode must match exactly.
export const commandsAddedInServer0_12 = new Set<string>(["CLUSTER.FEATURES", "CLUSTER.FEATURES.RAISE"]);

// The SDK opcode table must equal the server's, apart from 0.12.0 commands the
// server does not have.
export function expectSdkOpcodesMatchServer(serverOpcodes: Readonly<Record<string, number>>): void {
  const sdkOpcodes = Object.fromEntries(
    Object.entries(COMMAND_OPCODES).filter(
      ([name]) => name in serverOpcodes || !commandsAddedInServer0_12.has(name)
    )
  );
  expect(sdkOpcodes).toEqual(serverOpcodes);
}
