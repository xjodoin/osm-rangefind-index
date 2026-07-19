#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectManifestProtections,
  createProtections,
  generationManifestPath,
  updateCandidateState
} from "./lib/r2_gc.mjs";
import { createR2Store } from "./lib/r2_store.mjs";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(argv) {
  const args = {
    apply: false,
    graceDays: 7,
    statePath: join(projectRoot, "work/r2-gc-state.json"),
    reportPath: join(projectRoot, "work/r2-gc-last-report.json")
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--apply") args.apply = true;
    else if (arg === "--grace-days") args.graceDays = Number(argv[++i]);
    else if (arg === "--state") args.statePath = resolve(argv[++i]);
    else if (arg === "--report") args.reportPath = resolve(argv[++i]);
    else throw new Error(`Unknown argument: ${arg}`);
  }
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

async function fetchManifest(store, path) {
  return JSON.parse(await store.getText(path));
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

async function loadProtections(store) {
  const protections = createProtections();
  const rootPath = "manifest.min.json";
  const rootText = await store.getText(rootPath);
  const root = JSON.parse(rootText);
  collectManifestProtections(root, rootPath, protections);
  const shards = Array.isArray(root.shards) ? root.shards : [];

  await parallelMap(shards, 8, async shard => {
    const shardBase = String(shard.path || `shards/${shard.id}/`).replace(/\/?$/u, "/");
    const manifestPath = posix.normalize(posix.join(shardBase, shard.manifest || "manifest.min.json"));
    const manifest = await fetchManifest(store, manifestPath);
    collectManifestProtections(manifest, manifestPath, protections);
    for (const generation of manifest.generations || []) {
      const childPath = generationManifestPath(shardBase, generation);
      if (!childPath) throw new Error(`Unsafe generation manifest path in ${manifestPath}`);
      const child = await fetchManifest(store, childPath);
      collectManifestProtections(child, childPath, protections);
    }
  });

  return {
    protections,
    rootHash: createHash("sha256").update(rootText).digest("hex"),
    shards: shards.length
  };
}

async function listObjects(store) {
  const topLevel = await store.listLevel("");
  const shardDirectory = topLevel.prefixes.find(path => path.replace(/\/$/u, "") === "shards");
  const shardPrefixes = shardDirectory ? await store.listCommonPrefixes("shards/") : [];
  const otherPrefixes = topLevel.prefixes.filter(path => path !== shardDirectory);
  const batches = await parallelMap([...shardPrefixes, ...otherPrefixes], 8, prefix => store.listObjects(prefix));
  return [...topLevel.objects, ...batches.flat()];
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const store = createR2Store();
  const now = new Date().toISOString();
  console.log(`[${now}] R2 GC ${args.apply ? "apply" : "dry-run"}: collecting live manifest references.`);
  const live = await loadProtections(store);
  console.log(`[${new Date().toISOString()}] Protected ${live.protections.paths.size.toLocaleString()} exact paths, ${live.protections.basenames.size.toLocaleString()} object names, and ${live.protections.prefixes.size.toLocaleString()} directory prefixes across ${live.shards} shards.`);
  const objects = await listObjects(store);
  const prior = loadJson(args.statePath, { version: 1, candidates: {} });
  const planned = updateCandidateState({
    objects,
    protections: live.protections,
    previous: prior.candidates || {},
    now,
    graceMs: args.graceDays * 86400_000
  });
  const rootHashAfter = createHash("sha256").update(await store.getText("manifest.min.json")).digest("hex");
  if (rootHashAfter !== live.rootHash) throw new Error("Root manifest changed during the GC scan; refusing to continue.");

  const report = {
    version: 1,
    scannedAt: now,
    mode: args.apply ? "apply" : "dry-run",
    graceDays: args.graceDays,
    bucket: store.bucket,
    prefix: store.prefix,
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
    await store.deleteObjects(planned.eligible);
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
  store.close();
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
