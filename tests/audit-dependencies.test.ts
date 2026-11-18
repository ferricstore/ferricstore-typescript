import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const script = `${repositoryRoot}/scripts/audit-dependencies.mjs`;

const picomatch = {
  name: "picomatch",
  title: "picomatch vulnerable to ReDoS through single-branch repeated extglobs",
  url: "https://github.com/advisories/GHSA-fc5q-39xc-vjpf",
  severity: "high",
  range: "<=4.0.7"
};
const minimatch = {
  name: "minimatch",
  title: "minimatch has potential denial of service via globparts amplification",
  url: "https://github.com/advisories/GHSA-g3hv-75wg-m249",
  severity: "moderate",
  range: ">=7.0.0 <11.0.1"
};
const picomatchException = {
  url: picomatch.url,
  package: "picomatch",
  expires: "2026-12-18",
  reason: "No fixed release; only test and build tooling use it."
};

type Advisory = typeof picomatch;

function report(...advisories: Advisory[]): object {
  return {
    auditReportVersion: 2,
    vulnerabilities: Object.fromEntries(
      advisories.flatMap((advisory): [string, object][] => [
        [advisory.name, { name: advisory.name, severity: advisory.severity, via: [advisory] }],
        [`${advisory.name}-dependent`, { severity: advisory.severity, via: [advisory.name] }]
      ])
    )
  };
}

function audit(options: {
  runtime: object;
  full: object;
  exceptions?: object[];
  today?: string;
}): { status: number | null; output: string } {
  const directory = mkdtempSync(join(tmpdir(), "ferricstore-audit-"));
  const write = (name: string, value: object): string => {
    const path = join(directory, name);
    writeFileSync(path, JSON.stringify(value));
    return path;
  };
  const result = spawnSync(
    process.execPath,
    [
      script,
      "--runtime-report",
      write("runtime.json", options.runtime),
      "--full-report",
      write("full.json", options.full),
      "--exceptions",
      write("exceptions.json", { advisories: options.exceptions ?? [] }),
      "--today",
      options.today ?? "2026-11-18"
    ],
    { encoding: "utf8" }
  );
  return { status: result.status, output: `${result.stdout}${result.stderr}` };
}

describe("dependency audit", () => {
  it("passes when no advisory is high or critical", () => {
    const result = audit({ runtime: report(), full: report(minimatch) });

    expect(result.status).toBe(0);
  });

  it("rejects a high advisory in development dependencies without an exception", () => {
    const result = audit({ runtime: report(), full: report(picomatch) });

    expect(result.status).toBe(1);
    expect(result.output).toContain(picomatch.url);
  });

  it("allows a listed development advisory until its exception expires", () => {
    const result = audit({
      runtime: report(),
      full: report(picomatch),
      exceptions: [picomatchException]
    });

    expect(result.status).toBe(0);
    expect(result.output).toContain(`allowed until 2026-12-18: ${picomatch.url}`);
  });

  it("never excepts an advisory that reaches runtime dependencies", () => {
    const result = audit({
      runtime: report(picomatch),
      full: report(picomatch),
      exceptions: [picomatchException]
    });

    expect(result.status).toBe(1);
    expect(result.output).toContain(`runtime dependency picomatch: ${picomatch.url}`);
  });

  it("rejects an expired exception", () => {
    const result = audit({
      runtime: report(),
      full: report(picomatch),
      exceptions: [picomatchException],
      today: "2026-12-19"
    });

    expect(result.status).toBe(1);
    expect(result.output).toContain(`exception expired on 2026-12-18: ${picomatch.url}`);
  });

  it("rejects an exception that no longer matches an advisory", () => {
    const result = audit({
      runtime: report(),
      full: report(),
      exceptions: [picomatchException]
    });

    expect(result.status).toBe(1);
    expect(result.output).toContain(`exception matches no advisory, remove it: ${picomatch.url}`);
  });

  it("rejects a report npm could not produce", () => {
    const result = audit({
      runtime: report(),
      full: { error: { code: "ENOAUDIT", summary: "audit endpoint returned an error" } }
    });

    expect(result.status).toBe(1);
    expect(result.output).toContain("ENOAUDIT");
  });
});
