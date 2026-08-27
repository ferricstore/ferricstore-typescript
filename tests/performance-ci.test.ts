import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const root = fileURLToPath(new URL("..", import.meta.url));

describe("native response throughput guard", () => {
  it("supports a fixed request count and fails below a configured throughput floor", () => {
    const benchmark = readFileSync(`${root}/benchmarks/kv-benchmark.mjs`, "utf8");

    expect(benchmark).toContain("min-throughput");
    expect(benchmark).toContain("throughput regression");
    expect(benchmark).toContain("requestedRequests");
  });

  it("runs the direct-response smoke guard in test and release integration CI", () => {
    for (const workflow of ["test.yml", "release.yml"]) {
      const source = readFileSync(`${root}/.github/workflows/${workflow}`, "utf8");
      expect(source).toContain("Acknowledged response throughput smoke");
      expect(source).toContain("--request-mode direct");
      expect(source).toContain("--requests 1000");
      expect(source).toContain("--pipeline 1");
      expect(source).toContain("--min-throughput 100");
    }
  });
});
