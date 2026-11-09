// Builds the browser examples into ./site/<name>/, ready to be served:
//
//   cargo build --release --target wasm32-unknown-unknown --features browser --example <name>
//   wasm-bindgen --target web --out-dir site/<name>/pkg <name>.wasm
//   + the example's index.html and any data files, copied beside it.
//
// The output is kept out of the examples' own folders, so running the tests
// never leaves build products in the source tree.
//
// Needs `wasm-bindgen` (the CLI) on PATH, at the same version as the
// `wasm-bindgen` crate in Cargo.lock.

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const examples = [
  "browser",
  "browser_debounce",
  "browser_fetch",
  "browser_mock_clock",
  "browser_yield",
];

const here = path.dirname(fileURLToPath(import.meta.url));
const crateDir = path.resolve(here, "..");
const repoRoot = path.resolve(crateDir, "../..");
export const siteDir = path.join(here, "site");

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", cwd: repoRoot, ...options });
  if (result.error?.code === "ENOENT") {
    fail(`\`${command}\` was not found on PATH.`);
  }
  if (result.status !== 0) {
    fail(`\`${command} ${args.join(" ")}\` failed (exit ${result.status}).`);
  }
}

function fail(message) {
  console.error(`build.mjs: ${message}`);
  process.exit(1);
}

function capture(command, args) {
  const result = spawnSync(command, args, { encoding: "utf8", cwd: repoRoot });
  if (result.error?.code === "ENOENT") {
    fail(`\`${command}\` was not found on PATH.`);
  }
  return result.stdout.trim();
}

// The CLI and the crate must match, or wasm-bindgen refuses the module.
function checkWasmBindgenVersion() {
  const lock = fs.readFileSync(path.join(repoRoot, "Cargo.lock"), "utf8");
  const wanted = lock.match(/name = "wasm-bindgen"\nversion = "([^"]+)"/)?.[1];
  const have = capture("wasm-bindgen", ["--version"]).split(" ")[1];
  if (wanted !== have) {
    fail(
      `wasm-bindgen CLI is ${have}, but Cargo.lock has ${wanted}. ` +
        `Install the matching one: cargo install wasm-bindgen-cli --version ${wanted} --locked`,
    );
  }
}

// Honors CARGO_TARGET_DIR and `build.target-dir`.
function targetDir() {
  const metadata = capture("cargo", ["metadata", "--format-version", "1", "--no-deps"]);
  return JSON.parse(metadata).target_directory;
}

export function build() {
  checkWasmBindgenVersion();

  run("cargo", [
    "build",
    "--release",
    "-p",
    "spooly",
    "--target",
    "wasm32-unknown-unknown",
    "--features",
    "browser",
    ...examples.flatMap((name) => ["--example", name]),
  ]);

  const wasmDir = path.join(targetDir(), "wasm32-unknown-unknown/release/examples");
  fs.rmSync(siteDir, { recursive: true, force: true });
  for (const name of examples) {
    const source = path.join(crateDir, "examples", name);
    const out = path.join(siteDir, name);
    // The page and its data files; not the Rust source or a stale `pkg`.
    fs.cpSync(source, out, {
      recursive: true,
      filter: (file) => !file.endsWith(".rs") && path.basename(file) !== "pkg",
    });
    run("wasm-bindgen", [
      "--target",
      "web",
      "--out-dir",
      path.join(out, "pkg"),
      path.join(wasmDir, `${name}.wasm`),
    ]);
  }
  console.log(`built ${examples.length} examples into ${siteDir}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  build();
}
