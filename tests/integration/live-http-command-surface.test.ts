import { describe, expect, it } from "vitest";
import { COMMAND_OPCODES, httpCommandDisposition, RawCodec } from "../../src/index.js";
import { integrationClient, url } from "./live-support.js";

const httpIntegration = /^https?:\/\//u.test(url());

describe.runIf(httpIntegration)("FerricStore HTTP command integration", () => {
  it("accepts the complete typed Flow command catalog", async () => {
    expect(url()).toMatch(/^https?:\/\//u);
    const commands = [...Object.keys(COMMAND_OPCODES), "FLOW.QUERY.INDEXES"]
      .filter((command, index, all) => command.startsWith("FLOW.") && all.indexOf(command) === index)
      .sort();
    expect(commands).toHaveLength(67);
    expect(commands.every((command) => httpCommandDisposition(command) === "supported")).toBe(true);
    const client = await integrationClient({ codec: new RawCodec() });
    try {
      for (const command of commands) {
        try {
          await client.command(command);
        } catch (error) {
          expect(error).toBeInstanceOf(Error);
          expect((error as Error).message, command).not.toMatch(/unsupported over stateless http|native TCP session/i);
        }
      }
    } finally {
      await client.close();
    }
  });
});
