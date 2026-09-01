import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));

describe("public durable workflow documentation", () => {
  it("covers replay, external idempotency, waiting handoff, and migration", () => {
    const readme = readFileSync(`${repositoryRoot}/README.md`, "utf8").replace(
      /\s+/gu,
      " "
    );

    for (const required of [
      "TypeScript SDK `0.13.2` requires FerricStore server",
      "The step name must remain stable across retries",
      "external systems still need the same stable provider idempotency key",
      "A waiting workflow does not occupy a worker",
      "any available worker can acquire a fresh lease",
      "`stepContinue()` remains available only as a deprecated low-level migration API"
    ]) {
      expect(readme).toContain(required);
    }
  });
});
