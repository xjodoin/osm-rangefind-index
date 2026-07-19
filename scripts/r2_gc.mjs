#!/usr/bin/env node

import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, posix, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  collectManifestProtections,
  createProtections,
  generationManifestPath,
  updateCandidateState
} from "./lib/r2_gc.mjs";

const execFileAsync = promisify(execFile);
const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(argv) {
  const args = {
    apply: false,
    graceDays: 7,
    remote: process.env.R2_REMOTE || "",
    statePath: join(projectRoot, "work/r2-gc-state.json"),
    reportPath: join(projectRoot, "work/r2-gc-last-report.json")
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--apply") args.apply = true;
    else if (arg === "--grace-days") args.graceDays = Number(argv[++i]);
    else if (arg === "--remote") args.remote = argv[++i];
    else if (arg === "--state") args.statePath = resolve(argv[++i]);
    else if (arg === "--report") args.reportPath = resolve(argv[++i]);
    else throw new Error(`Unknown argument: ${arg}`);
  }
  if (!args.remote) throw new Error("Set R2_REMOTE or pass --remote.");
  if (!Number.isFinite(args.graceDays) || args.graceDays < 1) throw new Error("--grace-days must be at least 1.");
  return args;
}

function loadJson(path, fallback) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return fallback;
  }
}

async function rcloneCat(remote, path) {
  const { stdout } = await execFileAsync("rclone", ["cat", `${remote}/${path}`], { maxBuffer: 20 * 1024 * 1024 });
  return stdout;
}

async function fetchManifest(remote, path) {
  return JSON.parse(await rcloneCat(remote, path));
}

async function parallelMap(values, concurrency, fn) {
  const results = new Array(values.length);
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    while (cursor < values.length) {
      const index = cursor++;
      results[index] = await fn(values[index], index);
    }
  }));
  return results;
}

async function loadProtections(remote) {
  const protections = createProtections();
  const rootPath = "manifest.min.json";
  const rootText = await rcloneCat(remote, rootPath);
  const root = JSON.parse(rootText);
  collectManifestProtections(root, rootPath, protections);
  const shards = Array.isArray(root.shards) ? root.shards : [];

  await parallelMap(shards, 8, async shard => {
    const shardBase = String(shard.path || `shards/${shard.id}/`).replace(/\/?$/u, "/");
    const manifestPath = posix.normalize(posix.join(shardBase, shard.manifest || "manifest.min.json"));
    const manifest = await fetchManifest(remote, manifestPath);
    collectManifestProtections(manifest, manifestPath, protections);
    for (const generation of manifest.generations || []) {
      const childPath = generationManifestPath(shardBase, generation);
      if (!childPath) throw new Error(`Unsafe generation manifest path in ${manifestPath}`);
      const child = await fetchManifest(remote, childPath);
      collectManifestProtections(child, childPath, protections);
    }
  });

  return {
    protections,
    rootHash: createHash("sha256").update(rootText).digest("hex"),
    shards: shards.length
  };
}

async function listObjectsAt(remote, prefix, maxDepth = null) {
  const target = prefix ? `${remote}/${prefix.replace(/\/$/u, "")}` : remote;
  const argv = [
    "lsf", target, "--files-only", "--format", "pst", "--separator", "\t"
  ];
  if (maxDepth) argv.push("--max-depth", String(maxDepth));
  else argv.push("--recursive", "--fast-list");
  const child = spawn("rclone", [
    ...argv
  ], { stdio: ["ignore", "pipe", "inherit"] });
  const exit = new Promise((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", resolveExit);
  });
  const objects = [];
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  for await (const line of lines) {
    const [path, sizeText, modTime] = line.split("\t");
    const size = Number(sizeText);
    if (path && Number.isFinite(size)) {
      objects.push({ path: prefix ? posix.join(prefix, path) : path, size, modTime });
    }
  }
  const code = await exit;
  if (code !== 0) throw new Error(`rclone lsf exited with code ${code}`);
  return objects;
}

async function listDirectories(remote, prefix = "") {
  const target = prefix ? `${remote}/${prefix.replace(/\/$/u, "")}` : remote;
  const { stdout } = await execFileAsync("rclone", [
    "lsf", target, "--dirs-only", "--max-depth", "1"
  ], { maxBuffer: 20 * 1024 * 1024 });
  return stdout.split("\n").map(value => value.trim()).filter(Boolean);
}

async function listObjects(remote) {
  const topLevel = await listDirectories(remote);
  const shardDirectory = topLevel.find(path => path.replace(/\/$/u, "") === "shards");
  const shardPrefixes = shardDirectory
    ? (await listDirectories(remote, "shards")).map(path => posix.join("shards", path))
    : [];
  const otherPrefixes = topLevel
    .filter(path => path !== shardDirectory)
    .map(path => path.replace(/\/$/u, ""));
  const rootObjects = await listObjectsAt(remote, "", 1);
  const batches = await parallelMap([...shardPrefixes, ...otherPrefixes], 8, prefix => listObjectsAt(remote, prefix));
  return [...rootObjects, ...batches.flat()];
}

function formatBytes(bytes) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let unit = units.shift();
  while (value >= 1024 && units.length) {
    value /= 1024;
    unit = units.shift();
  }
  return `${value.toFixed(value >= 10 || unit === "B" ? 0 : 1)} ${unit}`;
}

async function deleteObjects(remote, paths) {
  if (!paths.length) return;
  const listPath = join(tmpdir(), `osm-rangefind-r2-gc-${process.pid}.txt`);
  try {
    writeFileSync(listPath, paths.map(path => `${path}\n`).join(""));
    const { stdout, stderr } = await execFileAsync("rclone", [
      "delete", remote, "--files-from-raw", listPath, "--no-traverse"
    ], { maxBuffer: 20 * 1024 * 1024 });
    if (stdout) process.stdout.write(stdout);
    if (stderr) process.stderr.write(stderr);
  } finally {
    rmSync(listPath, { force: true });
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const now = new Date().toISOString();
  console.log(`[${now}] R2 GC ${args.apply ? "apply" : "dry-run"}: collecting live manifest references.`);
  const live = await loadProtections(args.remote);
  console.log(`[${new Date().toISOString()}] Protected ${live.protections.paths.size.toLocaleString()} exact paths, ${live.protections.basenames.size.toLocaleString()} object names, and ${live.protections.prefixes.size.toLocaleString()} directory prefixes across ${live.shards} shards.`);
  const objects = await listObjects(args.remote);
  const prior = loadJson(args.statePath, { version: 1, candidates: {} });
  const planned = updateCandidateState({
    objects,
    protections: live.protections,
    previous: prior.candidates || {},
    now,
    graceMs: args.graceDays * 86400_000
  });
  const rootHashAfter = createHash("sha256").update(await rcloneCat(args.remote, "manifest.min.json")).digest("hex");
  if (rootHashAfter !== live.rootHash) throw new Error("Root manifest changed during the GC scan; refusing to continue.");

  const report = {
    version: 1,
    scannedAt: now,
    mode: args.apply ? "apply" : "dry-run",
    graceDays: args.graceDays,
    remote: args.remote,
    rootHash: live.rootHash,
    shards: live.shards,
    listedObjects: objects.length,
    ...planned.summary,
    deletedObjects: 0,
    deletedBytes: 0
  };

  if (args.apply) {
    mkdirSync(dirname(args.statePath), { recursive: true });
    writeFileSync(args.statePath, JSON.stringify({ version: 1, updatedAt: now, candidates: planned.candidates }, null, 2));
    await deleteObjects(args.remote, planned.eligible);
    for (const path of planned.eligible) delete planned.candidates[path];
    writeFileSync(args.statePath, JSON.stringify({ version: 1, updatedAt: now, candidates: planned.candidates }, null, 2));
    report.deletedObjects = planned.summary.eligibleObjects;
    report.deletedBytes = planned.summary.eligibleBytes;
  }

  mkdirSync(dirname(args.reportPath), { recursive: true });
  writeFileSync(args.reportPath, JSON.stringify(report, null, 2));
  console.log(`[${new Date().toISOString()}] R2 GC: ${report.protectedObjects.toLocaleString()} live immutable objects (${formatBytes(report.protectedBytes)}), ${report.pendingObjects.toLocaleString()} awaiting grace (${formatBytes(report.pendingBytes)}), ${report.eligibleObjects.toLocaleString()} eligible (${formatBytes(report.eligibleBytes)}).`);
  if (!args.apply) console.log("Dry-run only; candidate state and R2 were not changed.");
  else console.log(`Deleted ${report.deletedObjects.toLocaleString()} objects (${formatBytes(report.deletedBytes)}).`);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
