// Fails on high or critical npm advisories. Runtime dependencies get no
// exceptions. A development-only advisory may be listed in
// scripts/audit-exceptions.json with a reason and an expiry date, for cases
// where no fixed release exists yet.
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

const blockingSeverities = new Set(["high", "critical"]);

const { values: options } = parseArgs({
  options: {
    "runtime-report": { type: "string" },
    "full-report": { type: "string" },
    exceptions: { type: "string", default: "scripts/audit-exceptions.json" },
    today: { type: "string", default: new Date().toISOString().slice(0, 10) }
  }
});

const runtimeAdvisories = blockingAdvisories(
  loadReport(options["runtime-report"], ["--omit=dev"])
);
const fullAdvisories = blockingAdvisories(loadReport(options["full-report"], []));
const exceptions = JSON.parse(readFileSync(options.exceptions, "utf8")).advisories;

const failures = [];
for (const advisory of runtimeAdvisories.values()) {
  failures.push(`runtime dependency ${advisory.name}: ${advisory.url} (${advisory.title})`);
}

const activeExceptions = new Map();
for (const exception of exceptions) {
  if (exception.expires < options.today) {
    failures.push(`exception expired on ${exception.expires}: ${exception.url}`);
  } else if (!fullAdvisories.has(exception.url)) {
    failures.push(`exception matches no advisory, remove it: ${exception.url}`);
  } else {
    activeExceptions.set(exception.url, exception);
  }
}

for (const advisory of fullAdvisories.values()) {
  if (runtimeAdvisories.has(advisory.url)) continue;
  const exception = activeExceptions.get(advisory.url);
  if (exception == null) {
    failures.push(`${advisory.severity} advisory in ${advisory.name}: ${advisory.url} (${advisory.title})`);
  } else {
    console.log(`allowed until ${exception.expires}: ${advisory.url} (${exception.reason})`);
  }
}

if (failures.length > 0) {
  for (const failure of failures) console.error(failure);
  process.exit(1);
}

function loadReport(path, auditArgs) {
  const source =
    path == null
      ? spawnSync("npx", ["--yes", "npm@11.6.2", "audit", "--json", ...auditArgs], {
          encoding: "utf8",
          maxBuffer: 64 * 1024 * 1024
        }).stdout
      : readFileSync(path, "utf8");
  const report = JSON.parse(source);
  if (report.error != null) {
    throw new Error(`npm audit failed: ${report.error.code} ${report.error.summary}`);
  }
  return report;
}

function blockingAdvisories(report) {
  const advisories = new Map();
  for (const vulnerability of Object.values(report.vulnerabilities ?? {})) {
    for (const via of vulnerability.via) {
      if (typeof via === "object" && blockingSeverities.has(via.severity)) {
        advisories.set(via.url, via);
      }
    }
  }
  return advisories;
}
