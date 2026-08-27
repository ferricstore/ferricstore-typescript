import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { relative, resolve, sep } from "node:path";
import { inflateSync } from "node:zlib";
import { isDeepStrictEqual } from "node:util";

const root = process.cwd();
const docsRoots = ["docs/api", "docs/agent-api"];
const compressedAssets = docsRoots.flatMap((path) => [
  `${path}/assets/hierarchy.js`,
  `${path}/assets/navigation.js`,
  `${path}/assets/search.js`
]);

const generatedFiles = docsRoots.flatMap((path) => listFiles(resolve(root, path))).sort();
const trackedFiles = execFileSync("git", ["ls-files", "--", ...docsRoots], {
  encoding: "utf8"
}).trim().split("\n").filter(Boolean).sort();

if (!isDeepStrictEqual(generatedFiles, trackedFiles)) {
  throw new Error("generated TypeDoc file set differs from the Git index");
}

const diff = spawnSync(
  "git",
  [
    "diff",
    "--exit-code",
    "--",
    ...docsRoots,
    ...compressedAssets.map((path) => `:(exclude)${path}`)
  ],
  { stdio: "inherit" }
);
if (diff.error != null) throw diff.error;
if (diff.status !== 0) process.exit(diff.status ?? 1);

for (const path of compressedAssets) {
  const generated = compressedJson(readFileSync(resolve(root, path), "utf8"), path);
  const indexed = compressedJson(
    execFileSync("git", ["show", `:${path}`], { encoding: "utf8" }),
    `${path} from git show`
  );
  if (!isDeepStrictEqual(generated, indexed)) {
    throw new Error(`${path} contains stale generated TypeDoc data`);
  }
}

function listFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) return listFiles(path);
    return entry.isFile() ? [relative(root, path).split(sep).join("/")] : [];
  });
}

function compressedJson(source, context) {
  const match = /=\s*"([A-Za-z0-9+/=]+)"\s*;?\s*$/u.exec(source.trim());
  if (match?.[1] == null) throw new Error(`${context} is not a compressed TypeDoc asset`);
  return JSON.parse(inflateSync(Buffer.from(match[1], "base64")).toString("utf8"));
}
