import { spawn } from "node:child_process";

const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const groups = [
  ["agent framework persistence", "tests/integration/live-agent-persistence.test.ts"],
  ["core SDK", "tests/integration/live.test.ts"],
  ["compact Stream multi-topic pipeline", "tests/integration/live-stream-pipeline.test.ts"],
  ["compact Pub/Sub pipeline", "tests/integration/live-pubsub-pipeline.test.ts"],
  ["Flow retention cleanup", "tests/integration/live-retention.test.ts"],
  ["Flow query planner", "tests/integration/live-query.test.ts"],
  [
    "typed stores",
    "tests/integration/live-store-flow.test.ts",
    "covers typed native store families"
  ],
  [
    "probabilistic stores",
    "tests/integration/live-store-flow.test.ts",
    "covers native probabilistic helpers"
  ],
  [
    "Flow repair and indexes",
    "tests/integration/live-store-flow.test.ts",
    "covers Flow state-machine repair and index commands"
  ],
  [
    "Flow governance",
    "tests/integration/live-governance-workflow.test.ts",
    "covers fused Flow, schedule, query, and governance helpers"
  ],
  [
    "queue and workflow",
    "tests/integration/live-governance-workflow.test.ts",
    "covers queue and workflow wrappers against the live server"
  ],
  [
    "automatic batching",
    "tests/integration/live-governance-workflow.test.ts",
    "auto-batches concurrent safe API calls over the native protocol"
  ]
];

await run(npm, ["run", "integration:down"], true);
try {
  for (const [name, file, testName] of groups) {
    process.stdout.write(`\nRunning ${name} integration against a fresh FerricStore volume\n`);
    await run(npm, ["run", "integration:up"]);
    try {
      await run(npm, [
        "exec",
        "--",
        "vitest",
        "run",
        file,
        ...(testName == null ? [] : ["-t", testName])
      ]);
    } catch (error) {
      await run("docker", ["compose", "logs", "--no-color", "ferricstore"], true);
      throw error;
    } finally {
      await run(npm, ["run", "integration:down"], true);
    }
  }
} finally {
  await run(npm, ["run", "integration:down"], true);
}

function run(command, args, allowFailure = false) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env: process.env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0 || allowFailure) {
        resolve();
        return;
      }
      reject(new Error(
        `${command} ${args.join(" ")} failed${signal == null ? ` with exit code ${code}` : ` from ${signal}`}`
      ));
    });
  });
}
