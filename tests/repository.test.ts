import { existsSync, readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const testedServerVersion = "0.11.6";

function workflowJob(source: string, name: string): string {
  const lines = source.split("\n");
  const start = lines.findIndex((line) => line === `  ${name}:`);
  if (start < 0) return "";
  const end = lines.findIndex((line, index) => index > start && /^ {2}[a-zA-Z0-9_-]+:$/.test(line));
  return lines.slice(start, end < 0 ? undefined : end).join("\n");
}

function testFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = `${directory}/${entry.name}`;
    if (entry.isDirectory()) return testFiles(path);
    return entry.isFile() && entry.name.endsWith(".ts") ? [path] : [];
  });
}

describe("release workflow", () => {
  it("gates npm publishing on live FerricStore integration tests", () => {
    const source = readFileSync(`${repositoryRoot}/.github/workflows/release.yml`, "utf8");
    const isolated = readFileSync(
      `${repositoryRoot}/scripts/run-isolated-integration.mjs`,
      "utf8"
    );
    const integration = workflowJob(source, "integration");
    const npm = workflowJob(source, "npm");

    expect(integration).toContain("npm run test:integration:isolated");
    expect(integration).toMatch(/- if: always\(\)\s+run: npm run integration:down/);
    expect(isolated).toContain("npm, [\"run\", \"integration:up\"]");
    expect(isolated).toContain("npm, [\"run\", \"integration:down\"]");
    for (const file of [
      "tests/integration/live.test.ts",
      "tests/integration/live-pubsub-pipeline.test.ts",
      "tests/integration/live-store-flow.test.ts",
      "tests/integration/live-governance-workflow.test.ts"
    ]) {
      expect(isolated).toContain(file);
    }
    expect(npm).toMatch(/needs:\s*\[integration, authenticated-integration\]/u);
    expect(npm).toContain("npm publish --provenance --access public");
  });

  it("pins third-party actions and grants write permissions only to the job that needs them", () => {
    const source = readFileSync(`${repositoryRoot}/.github/workflows/release.yml`, "utf8");
    const preJobs = source.slice(0, source.indexOf("jobs:"));
    const integration = workflowJob(source, "integration");
    const npm = workflowJob(source, "npm");
    const release = workflowJob(source, "release");
    const actions = [...source.matchAll(/^\s*- uses:\s*([^\s#]+)/gm)].flatMap((match) =>
      match[1] == null ? [] : [match[1]]
    );

    expect(preJobs).not.toMatch(/^permissions:/m);
    expect(integration).toMatch(/permissions:\s+contents: read/);
    expect(integration).not.toContain("write");
    expect(npm).toMatch(/permissions:\s+contents: read\s+id-token: write/);
    expect(npm).not.toContain("action-gh-release");
    expect(release).toMatch(/needs:\s+npm/);
    expect(release).toMatch(/permissions:\s+contents: write/);
    expect(release).not.toContain("id-token: write");
    expect(release).toContain("softprops/action-gh-release@");
    expect(actions.length).toBeGreaterThan(0);
    expect(actions.every((action) => /^[^@]+@[0-9a-f]{40}$/.test(action))).toBe(true);
  });
});

describe("core compatibility CI", () => {
  it("declares the 0.11 server contract while retaining native wire v1", () => {
    const metadata = JSON.parse(readFileSync(`${repositoryRoot}/package.json`, "utf8")) as {
      ferricstore?: { minimumServerVersion?: string; nativeProtocolVersion?: number };
      version?: string;
    };
    const manifest = JSON.parse(
      readFileSync(`${repositoryRoot}/src/native-protocol-manifest.json`, "utf8")
    ) as { magic?: string; requestVersion?: number };

    expect(metadata.version).toBe("0.11.6");
    expect(metadata.ferricstore).toEqual({
      minimumServerVersion: "0.11.4",
      nativeProtocolVersion: 1
    });
    expect(manifest).toMatchObject({ magic: "FSNP", requestVersion: 1 });
  });

  it("tests against the latest patch without raising the compatible server floor", () => {
    const testWorkflow = readFileSync(
      `${repositoryRoot}/.github/workflows/test.yml`,
      "utf8"
    );
    const releaseWorkflow = readFileSync(
      `${repositoryRoot}/.github/workflows/release.yml`,
      "utf8"
    );

    for (const source of [testWorkflow, releaseWorkflow]) {
      expect(source).toContain("release-0.11.4");
      expect(source).toMatch(
        /ghcr\.io\/ferricstore\/ferricstore:0\.11\.4@sha256:[0-9a-f]{64}/u
      );
    }
  });

  it("compares generated TypeDoc output without depending on zlib bytes", () => {
    const metadata = JSON.parse(readFileSync(`${repositoryRoot}/package.json`, "utf8")) as {
      scripts?: Record<string, string>;
    };
    const guardPath = `${repositoryRoot}/scripts/check-generated-docs.mjs`;

    expect(metadata.scripts?.["docs:check"]).toBe(
      "typedoc --logLevel Warn && node scripts/check-generated-docs.mjs"
    );
    expect(existsSync(guardPath)).toBe(true);
    if (!existsSync(guardPath)) return;

    const guard = readFileSync(guardPath, "utf8");
    expect(guard).toContain("inflateSync");
    expect(guard).toContain("git show");
    for (const asset of ["hierarchy.js", "navigation.js", "search.js"]) {
      expect(guard).toContain(asset);
    }
  });

  it("runs routing and native ABI parity against a pinned core checkout and cannot silently skip", () => {
    const source = readFileSync(`${repositoryRoot}/.github/workflows/test.yml`, "utf8");
    const parityJob = workflowJob(source, "core-routing-parity");
    const parityTest = readFileSync(`${repositoryRoot}/tests/core-routing-parity.test.ts`, "utf8");
    const abiTestPath = `${repositoryRoot}/tests/core-native-abi-parity.test.ts`;

    expect(parityJob).toContain("repository: ferricstore/ferricstore");
    expect(parityJob).toMatch(/ref:\s*[0-9a-f]{40}/u);
    expect(parityJob).toContain("path: ferricstore-core");
    expect(parityJob).toContain("FERRICSTORE_CORE_REQUIRED: \"1\"");
    expect(parityJob).toContain("tests/core-routing-parity.test.ts");
    expect(parityJob).toContain("tests/core-native-abi-parity.test.ts");
    expect(parityTest).toContain("FERRICSTORE_CORE_REQUIRED");
    expect(existsSync(abiTestPath)).toBe(true);
    if (existsSync(abiTestPath)) {
      expect(readFileSync(abiTestPath, "utf8")).toContain("FERRICSTORE_CORE_REQUIRED");
    }
  });

  it("pins parity and pinned-core integration to the reviewed OSS revision", () => {
    const revision = "014997467189c292f7a15b7ca605d514adcd8df2";
    const image =
      `ghcr.io/ferricstore/ferricstore:ci-${revision}` +
      "@sha256:2dc91388959a308947e762081bc447feaa2a5f6846d56de9e05aca17ce2f57f7";
    const testWorkflow = readFileSync(`${repositoryRoot}/.github/workflows/test.yml`, "utf8");
    const releaseWorkflow = readFileSync(`${repositoryRoot}/.github/workflows/release.yml`, "utf8");

    expect(workflowJob(testWorkflow, "core-routing-parity")).toContain(`ref: ${revision}`);
    expect(workflowJob(testWorkflow, "integration")).toContain(image);
    expect(workflowJob(releaseWorkflow, "integration")).toContain(image);
  });

  it("pins live server images immutably and tests the same pinned core revision", () => {
    const testWorkflow = readFileSync(`${repositoryRoot}/.github/workflows/test.yml`, "utf8");
    const releaseWorkflow = readFileSync(`${repositoryRoot}/.github/workflows/release.yml`, "utf8");
    const parityJob = workflowJob(testWorkflow, "core-routing-parity");
    const integrationJob = workflowJob(testWorkflow, "integration");
    const releaseIntegrationJob = workflowJob(releaseWorkflow, "integration");
    const compose = readFileSync(`${repositoryRoot}/docker-compose.yml`, "utf8");
    const coreRevision = /ref:\s*([0-9a-f]{40})/u.exec(parityJob)?.[1];
    const immutableImage = /quay\.io\/ferricstore\/ferricstore:[^\s}"']+@sha256:[0-9a-f]{64}/gu;
    const releaseImage = new RegExp(
      `quay\\.io/ferricstore/ferricstore:${testedServerVersion.replaceAll(".", "\\.")}@sha256:[0-9a-f]{64}`,
      "u"
    ).exec(compose)?.[0];

    expect(coreRevision).toBe("014997467189c292f7a15b7ca605d514adcd8df2");
    expect(releaseImage).toBeDefined();
    expect(compose).toMatch(immutableImage);
    expect(integrationJob).toMatch(immutableImage);
    expect(integrationJob).toContain(releaseImage ?? "missing-release-image");
    expect(integrationJob).toContain(`ci-${coreRevision ?? "missing"}@sha256:`);
    expect(releaseIntegrationJob).toMatch(immutableImage);
    expect(releaseIntegrationJob).toContain(releaseImage ?? "missing-release-image");
    expect(releaseIntegrationJob).toContain(`ci-${coreRevision ?? "missing"}@sha256:`);
  });

  it("shares readiness framing constants with the SDK protocol manifest", () => {
    const protocol = readFileSync(`${repositoryRoot}/src/protocol-constants.ts`, "utf8");
    const readiness = readFileSync(`${repositoryRoot}/scripts/wait-for-ferricstore.mjs`, "utf8");

    expect(protocol).toContain('from "./native-protocol-manifest.json"');
    expect(readiness).toContain('from "../src/native-protocol-manifest.json"');
  });

  it("grants native bootstrap controls to the scoped query integration user", () => {
    const deployment = readFileSync(
      `${repositoryRoot}/tests/integration/deployment.test.ts`,
      "utf8"
    );

    for (const command of ["shards", "subscribe_events", "flow.query", "flow.query.explain"]) {
      expect(deployment).toContain(`"+${command}"`);
    }
  });

  it("executes the scoped query ACL case in an authenticated integration job", () => {
    const workflow = readFileSync(`${repositoryRoot}/.github/workflows/test.yml`, "utf8");
    const releaseWorkflow = readFileSync(
      `${repositoryRoot}/.github/workflows/release.yml`,
      "utf8"
    );
    const job = workflowJob(workflow, "authenticated-integration");
    const releaseJob = workflowJob(releaseWorkflow, "authenticated-integration");
    const bootstrap = readFileSync(
      `${repositoryRoot}/scripts/bootstrap-integration-auth.mjs`,
      "utf8"
    );
    const releaseImage = new RegExp(
      `quay\\.io/ferricstore/ferricstore:${testedServerVersion.replaceAll(".", "\\.")}@sha256:[0-9a-f]{64}`,
      "u"
    ).exec(readFileSync(`${repositoryRoot}/docker-compose.yml`, "utf8"))?.[0];

    for (const authenticatedJob of [job, releaseJob]) {
      expect(releaseImage).toBeDefined();
      expect(authenticatedJob).toContain(releaseImage ?? "missing-release-image");
      expect(authenticatedJob).toContain("node scripts/bootstrap-integration-auth.mjs");
      expect(authenticatedJob).toContain("npm run test:integration:deployment");
      expect(authenticatedJob).toContain("FERRICSTORE_AUTH_URL:");
      expect(authenticatedJob).toMatch(/- if: always\(\)\s+run: npm run integration:down/u);
    }
    expect(bootstrap).toContain('await bootstrapClient.aclSetUser("default"');
    expect(bootstrap).toContain('nativeOptions: { password, username: "default" }');
    expect(bootstrap).toContain("await authenticatedClient.ping()");
    expect(bootstrap).toContain("await authenticatedClient.aclSave()");
  });
});

describe("workflow supply-chain security", () => {
  it("rejects high-severity dependency advisories in the package job", () => {
    const testWorkflow = readFileSync(
      `${repositoryRoot}/.github/workflows/test.yml`,
      "utf8"
    );
    const releaseWorkflow = readFileSync(
      `${repositoryRoot}/.github/workflows/release.yml`,
      "utf8"
    );

    const pinnedAudit = "npx --yes npm@11.6.2 audit --audit-level=high";
    expect(workflowJob(testWorkflow, "package")).toContain(pinnedAudit);
    expect(workflowJob(releaseWorkflow, "npm")).toContain(pinnedAudit);
  });

  it("pins every third-party action in every workflow", () => {
    const workflowDirectory = `${repositoryRoot}/.github/workflows`;
    const mutableActions = readdirSync(workflowDirectory)
      .filter((name) => /\.ya?ml$/u.test(name))
      .flatMap((name) => {
        const source = readFileSync(`${workflowDirectory}/${name}`, "utf8");
        return [...source.matchAll(/^\s*- uses:\s*([^\s#]+)/gmu)].flatMap((match) => {
          const action = match[1];
          return action == null || /^[^@]+@[0-9a-f]{40}$/u.test(action)
            ? []
            : [{ action, workflow: name }];
        });
      });

    expect(mutableActions).toEqual([]);
  });
});

describe("source architecture", () => {
  it("does not construct reserved Flow storage keys for topology routing", () => {
    const routing = readFileSync(`${repositoryRoot}/src/topology-routing.ts`, "utf8");
    expect(routing).not.toMatch(/`f:\{(?:f|fa):/u);
  });

  it("keeps hand-maintained modules at or below 450 lines", () => {
    const sourceDirectory = `${repositoryRoot}/src`;
    const oversized = readdirSync(sourceDirectory)
      .filter((name) => name.endsWith(".ts"))
      .map((name) => ({
        lines: readFileSync(`${sourceDirectory}/${name}`, "utf8").split(/\r?\n/u).length,
        name
      }))
      .filter(({ lines }) => lines > 450);

    expect(oversized).toEqual([]);
  });

  it("keeps test modules at or below 1,200 lines", () => {
    const testDirectory = `${repositoryRoot}/tests`;
    const oversized = testFiles(testDirectory)
      .map((path) => ({
        lines: readFileSync(path, "utf8").split(/\r?\n/u).length,
        name: path.slice(testDirectory.length + 1)
      }))
      .filter(({ lines }) => lines > 1_200);

    expect(oversized).toEqual([]);
  });

  it("keeps Flow protocol codecs below the core protocol dependency boundary", () => {
    const flowProtocol = readFileSync(`${repositoryRoot}/src/protocol-flow.ts`, "utf8");

    expect(flowProtocol).not.toMatch(/from\s+["']\.\/protocol\.js["']/u);
  });

  it("keeps cross-cutting command classifications in one catalog", () => {
    const catalogPath = `${repositoryRoot}/src/command-metadata.ts`;
    expect(existsSync(catalogPath)).toBe(true);
    if (!existsSync(catalogPath)) return;

    const catalog = readFileSync(catalogPath, "utf8");
    const client = readFileSync(`${repositoryRoot}/src/client.ts`, "utf8");
    const protocol = readFileSync(`${repositoryRoot}/src/protocol.ts`, "utf8");
    const topology = readFileSync(`${repositoryRoot}/src/topology.ts`, "utf8");
    for (const name of [
      "safeAutoBatchCommands",
      "connectionPinnedCommands",
      "connectionBlockingCommands",
      "firstKeyCommands",
      "typeScopedFlowCommands"
    ]) {
      expect(catalog, name).toContain(`export const ${name}`);
      expect(client, name).not.toContain(`const ${name}`);
      expect(protocol, name).not.toContain(`const ${name}`);
      expect(topology, name).not.toContain(`const ${name}`);
    }
  });
});
