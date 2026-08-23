import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

function repositoryFile(path: string): string {
  return readFileSync(new URL(`../${path}`, import.meta.url), "utf8");
}

function expectImmutableFerricStoreImages(contents: string): void {
  for (const line of contents.split("\n")) {
    if (line.includes("quay.io/ferricstore/ferricstore:")) expect(line).toContain("@sha256:");
  }
}

describe("TLS HTTP release gate", () => {
  it("is owned by CI, release, and user documentation", () => {
    const runner = repositoryFile("scripts/run-http-integration.sh");
    expectImmutableFerricStoreImages(runner);
    for (const required of [
      "FERRICSTORE_HTTP_TLS_ENABLED=true",
      "FERRICSTORE_USERNAME",
      "FERRICSTORE_PASSWORD",
      "FERRICSTORE_CA_FILE",
      "@sha256:",
      "chmod 700",
      "chmod 600",
      'rm -f "$tls_dir/ca.key"',
      "source=$tls_dir/server.key,target=/tls/server.key,readonly",
      "sdk-http-denied",
      "ACL authorization probe unexpectedly allowed SET",
      "unauthenticated HTTP request returned",
      "vitest run tests/integration"
    ]) {
      expect(runner).toContain(required);
    }

    for (const workflow of [".github/workflows/test.yml", ".github/workflows/release.yml"]) {
      const contents = repositoryFile(workflow);
      expect(contents).toContain("npm run test:integration:http");
      expect(contents).toContain("@sha256:");
      expectImmutableFerricStoreImages(contents);
    }

    const readme = repositoryFile("README.md");
    expect(readme).toContain("test:integration:http");
    expect(readme).toContain("FERRICSTORE_CA_FILE");
  });
});
