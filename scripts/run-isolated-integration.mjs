import { spawn } from "node:child_process";

const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const groups = [
  ["core SDK", "tests/integration/live.test.ts"],
  ["store and Flow", "tests/integration/live-store-flow.test.ts"],
  ["governance and workflow", "tests/integration/live-governance-workflow.test.ts"]
];

await run(npm, ["run", "integration:down"], true);
try {
  for (const [name, file] of groups) {
    process.stdout.write(`\nRunning ${name} integration against a fresh FerricStore volume\n`);
    await run(npm, ["run", "integration:up"]);
    try {
      await run(npm, ["exec", "--", "vitest", "run", file]);
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
