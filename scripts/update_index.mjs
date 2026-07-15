#!/usr/bin/env node

// Idle-window OSM index updater.
//
// Designed to run on a server that is only free at night and on weekends:
// every invocation makes as much progress as fits before --deadline, then
// stops cleanly. All heavy steps are incremental and resumable, so a run
// killed mid-build simply continues the next night:
//
//   1. refresh    — download a region's Geofabrik PBF only when upstream
//                   changed (Last-Modified) or nothing local remains.
//   2. extract    — PBF → places JSONL, only when the PBF changed.
//   3. stats      — regenerate the frozen scoring-stats artifact only when
//                   the region set changes or the corpus drifts beyond
//                   statsDriftRatio (regenerating forces a full rebuild of
//                   every shard, by design — scores must stay comparable).
//   4. build      — INCREMENTAL by default: the fresh corpus is diffed
//                   against the snapshot the shard was built from, and the
//                   changed documents ship as a generational delta
//                   (`build --update` against the same frozen stats — proven
//                   identical to a full rebuild). Full rebuilds happen only
//                   when the delta is too large, deletions accumulate past
//                   maxDeletedRatio (deltas cannot remove documents), the
//                   generation count hits maxGenerations, or the stats
//                   artifact changed. Interrupted full builds resume from
//                   rangefind's stage checkpoints; interrupted deltas re-run.
//   5. publish    — rewrite the sharded root manifest over the shards that
//                   are currently built, then rclone-sync to R2: packs
//                   first, manifests last, so readers never see a manifest
//                   that references missing objects. rclone compares against
//                   the remote listing, so only changed objects upload —
//                   nothing is ever downloaded back from R2.
//   6. cleanup    — after a shard is uploaded, reclaim the space: drop the
//                   PBF and extractor caches, gzip the corpus JSONL, and gut
//                   the local index copy down to its manifests. Steady-state
//                   disk per region is just the compressed corpus; the next
//                   update re-materializes only what it needs.
//
// Usage:
//   node scripts/update_index.mjs [--deadline HH:MM] [--max-hours N]
//     [--regions id,id] [--no-upload] [--force-stats] [--prune]
//     [--keep-artifacts] [--status]
//
// Environment (see .env.example): R2_REMOTE (rclone remote:bucket/prefix).

import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { dirname, join, resolve } from "node:path";
import { availableParallelism, hostname } from "node:os";
import { pipeline } from "node:stream/promises";
import { createGunzip, createGzip } from "node:zlib";
import { fileURLToPath } from "node:url";
import { collectScoringStats, loadScoringStats } from "rangefind/scoring-stats";
import { writeShardedRootManifest } from "rangefind/shards";
import { readConfig } from "rangefind/config";
import { createOsmIndexConfig } from "rangefind/osm/node";
import { extractOsmPlaces } from "rangefind/osm/extract";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const WORK = join(projectRoot, "work");
const OUT = join(WORK, "public/rangefind");
const STATE_PATH = join(WORK, "state.json");
const LOCK_PATH = join(WORK, ".lock");
const STATS_DIR = join(WORK, "scoring-stats");

function parseArgs(argv) {
  const args = {
    deadline: null,
    maxHours: 0,
    regions: null,
    upload: true,
    forceStats: false,
    prune: false,
    status: false,
    keepArtifacts: false,
    partial: false
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--deadline") args.deadline = argv[++i];
    else if (arg === "--max-hours") args.maxHours = Number(argv[++i]) || 0;
    else if (arg === "--regions") args.regions = argv[++i].split(",").map(s => s.trim()).filter(Boolean);
    else if (arg === "--no-upload") args.upload = false;
    else if (arg === "--force-stats") args.forceStats = true;
    else if (arg === "--prune") args.prune = true;
    else if (arg === "--status") args.status = true;
    else if (arg === "--keep-artifacts") args.keepArtifacts = true;
    else if (arg === "--partial") args.partial = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function deadlineMs(args) {
  if (args.maxHours > 0) return Date.now() + args.maxHours * 3600_000;
  if (!args.deadline) return Infinity;
  const match = args.deadline.match(/^(\d{1,2}):(\d{2})$/u);
  if (!match) throw new Error(`--deadline expects HH:MM, got "${args.deadline}"`);
  const target = new Date();
  target.setHours(Number(match[1]), Number(match[2]), 0, 0);
  if (target.getTime() <= Date.now()) target.setDate(target.getDate() + 1);
  return target.getTime();
}

function log(line) {
  console.log(`[${new Date().toISOString()}] ${line}`);
}

function loadJson(path, fallback) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return fallback;
  }
}

function saveState(state) {
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

function loadRegions(args) {
  const config = loadJson(join(projectRoot, "regions.json"), null);
  if (!config?.regions?.length) throw new Error("regions.json has no regions.");
  const regions = config.regions
    .map(region => ({
      id: String(region.id || region.geofabrik?.split("/").pop() || "").trim(),
      geofabrik: region.geofabrik || "",
      pbf: region.pbf ? resolve(projectRoot, region.pbf) : join(WORK, "regions", String(region.id), `${region.id}-latest.osm.pbf`),
      pinned: Boolean(region.pbf),
      groups: Array.isArray(region.groups) ? region.groups.map(String) : [],
      overrides: region.overrides || null
    }))
    .filter(region => region.id && (!args.regions || args.regions.includes(region.id)));
  if (!regions.length) throw new Error("No regions selected.");
  return {
    regions,
    statsDriftRatio: Number(config.statsDriftRatio || 0.1),
    workerCount: Number(config.workerCount || 0),
    acquisitionConcurrency: Math.max(1, Math.min(4, Number(config.acquisitionConcurrency || 1))),
    largePbfBytes: Math.max(1, Number(config.largePbfBytes || 1024 ** 3)),
    publisher: String(config.publisher || ""),
    // Delta policy: past any of these, the region gets a full rebuild.
    maxGenerations: Math.max(1, Number(config.maxGenerations || 6)),
    maxDeletedRatio: Number(config.maxDeletedRatio ?? 0.005),
    maxDeltaRatio: Number(config.maxDeltaRatio ?? 0.3)
  };
}

function createWeightedLimiter(capacity) {
  let used = 0;
  const queue = [];
  const drain = () => {
    while (queue.length && used + queue[0].weight <= capacity) {
      const next = queue.shift();
      used += next.weight;
      next.resolve();
    }
  };
  return async (requestedWeight, fn) => {
    const weight = Math.max(1, Math.min(capacity, requestedWeight));
    await new Promise(resolveSlot => {
      queue.push({ weight, resolve: resolveSlot });
      drain();
    });
    try {
      return await fn();
    } finally {
      used -= weight;
      drain();
    }
  };
}

// --- locking ---------------------------------------------------------------

function acquireLock() {
  mkdirSync(WORK, { recursive: true });
  const stale = loadJson(LOCK_PATH, null);
  if (stale?.pid) {
    try {
      process.kill(stale.pid, 0);
      throw new Error(`Another run is active (pid ${stale.pid}); exiting.`);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
      log(`Removing stale lock from pid ${stale.pid}`);
    }
  }
  writeFileSync(LOCK_PATH, JSON.stringify({ pid: process.pid, started: new Date().toISOString() }));
  process.on("exit", () => rmSync(LOCK_PATH, { force: true }));
}

// --- corpus files ------------------------------------------------------------

function regionWorkRoot(region) {
  return join(WORK, "regions", region.id);
}

function regionJsonl(region) {
  return join(regionWorkRoot(region), "data/osm-places.jsonl");
}

function regionJsonlGz(region) {
  return `${regionJsonl(region)}.gz`;
}

function hasCorpus(region) {
  return existsSync(regionJsonl(region)) || existsSync(regionJsonlGz(region));
}

// Re-materializes the plain JSONL from its compressed snapshot when a build
// or stats pass needs it after cleanup.
async function ensurePlainJsonl(region) {
  const plain = regionJsonl(region);
  if (existsSync(plain)) return plain;
  const gz = regionJsonlGz(region);
  if (!existsSync(gz)) throw new Error(`${region.id}: no corpus (neither JSONL nor .gz) — needs refresh/extract first.`);
  log(`${region.id}: decompressing corpus snapshot`);
  await pipeline(createReadStream(gz), createGunzip(), createWriteStream(`${plain}.tmp`));
  renameSync(`${plain}.tmp`, plain);
  return plain;
}

async function compressJsonl(region) {
  const plain = regionJsonl(region);
  if (!existsSync(plain)) return;
  const gz = regionJsonlGz(region);
  log(`${region.id}: compressing corpus snapshot`);
  await pipeline(createReadStream(plain), createGzip({ level: 6 }), createWriteStream(`${gz}.tmp`));
  renameSync(`${gz}.tmp`, gz);
  rmSync(plain, { force: true });
}

// extractOsmPlaces keeps resumable stage outputs beside the final corpus.
// Once osm-places exists those stage outputs are no longer needed: future
// builds consume the corpus, while a changed PBF can regenerate them. Keep
// delta/build inputs and metadata outside these known extractor prefixes.
function cleanupExtractionScratch(region) {
  const dataDir = join(regionWorkRoot(region), "data");
  if (!existsSync(dataDir)) return;
  const prefixes = [
    "osm-node-docs.",
    "osm-way-candidates.",
    "osm-way-anchor-coords.",
    "osm-way-anchors."
  ];
  let files = 0;
  let bytes = 0;
  for (const name of readdirSync(dataDir)) {
    if (!prefixes.some(prefix => name.startsWith(prefix))) continue;
    const path = join(dataDir, name);
    try { bytes += statSync(path).size; } catch { /* already gone */ }
    rmSync(path, { recursive: true, force: true });
    files++;
  }
  if (files) {
    log(`${region.id}: cleaned ${files} extractor scratch file(s) (${(bytes / 1024 / 1024).toFixed(1)} MiB)`);
  }
}

// Identity of a region's current upstream corpus — stable across
// gzip/gunzip cycles and PBF deletion, so cleanup never causes rebuilds.
function pbfIdentity(region, state) {
  if (!region.pinned) return state.regions[region.id]?.pbfLastModified || "";
  const stat = statSync(region.pbf);
  return `${stat.size}:${Math.floor(stat.mtimeMs)}`;
}

// --- step 1: refresh PBFs ----------------------------------------------------

async function refreshPbf(region, state) {
  if (region.pinned) {
    if (!existsSync(region.pbf)) throw new Error(`${region.id}: pinned PBF missing at ${region.pbf}`);
    return { bytes: statSync(region.pbf).size };
  }
  const url = `https://download.geofabrik.de/${region.geofabrik}-latest.osm.pbf`;
  const head = await fetch(url, { method: "HEAD" });
  if (!head.ok) throw new Error(`${region.id}: HEAD ${url} → ${head.status}`);
  const lastModified = head.headers.get("last-modified") || "";
  const bytes = Math.max(0, Number(head.headers.get("content-length") || 0));
  const entry = state.regions[region.id] || {};
  // The PBF gets deleted after a successful publish; re-download only when
  // upstream changed, or when extraction still needs it (stale/lost
  // extraction state) and the file is gone.
  const current = lastModified && lastModified === entry.pbfLastModified;
  const extractionCurrent = entry.extractIdentity === lastModified && hasCorpus(region);
  if (current && (existsSync(region.pbf) || extractionCurrent)) return { bytes: bytes || entry.pbfBytes || 0 };

  log(`${region.id}: downloading ${url} (${lastModified || "unknown date"})`);
  mkdirSync(dirname(region.pbf), { recursive: true });
  const tmp = `${region.pbf}.download`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${region.id}: GET ${url} → ${response.status}`);
  const file = createWriteStream(tmp);
  await new Promise((resolveDone, rejectDone) => {
    const reader = response.body.getReader();
    const pump = () => reader.read().then(({ done, value }) => {
      if (done) {
        file.end(resolveDone);
        return;
      }
      if (!file.write(Buffer.from(value))) file.once("drain", pump);
      else pump();
    }).catch(rejectDone);
    pump();
  });
  renameSync(tmp, region.pbf);
  state.regions[region.id] = { ...state.regions[region.id], pbfLastModified: lastModified, pbfBytes: bytes };
  return { bytes };
}

// --- step 2: extract JSONL ---------------------------------------------------

async function extractJsonl(region, state) {
  const entry = state.regions[region.id] || (state.regions[region.id] = {});
  const identity = pbfIdentity(region, state);
  if (entry.extractIdentity === identity && hasCorpus(region)) return false;
  if (!existsSync(region.pbf)) {
    throw new Error(`${region.id}: corpus is stale but the PBF is missing (refresh failed?)`);
  }
  // The compressed snapshot stays: it is the corpus the built shard
  // reflects and the base the delta diff runs against. Cleanup replaces it
  // only after the shard is rebuilt/updated and uploaded.
  const meta = await extractOsmPlaces({
    region: region.id,
    pbf: region.pbf,
    root: regionWorkRoot(region),
    rqa: false
  });
  entry.docs = Number(meta.docs || 0);
  entry.extractIdentity = identity;
  entry.overrides = region.overrides || null;
  return true;
}

// --- corpus diff ---------------------------------------------------------------

function lineDocId(line) {
  const match = line.match(/"id":"([^"]*)"/u);
  return match ? match[1] : "";
}

async function eachLineOf(stream, fn) {
  const rl = createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line) fn(line);
  }
}

// Diffs the fresh extraction against the snapshot the shard was built from.
// Added and changed documents become the delta corpus; deleted ids are only
// counted — generational deltas cannot remove documents, so deletions
// accumulate in state until they force a full rebuild.
// Memory: one hash entry per snapshot doc (~100B) — ~600MB for a 6M-doc
// region; oversized regions fall back to full rebuilds via maxDeltaRatio.
async function computeDelta(region) {
  const old = new Map();
  await eachLineOf(createReadStream(regionJsonlGz(region)).pipe(createGunzip()), line => {
    old.set(lineDocId(line), createHash("sha1").update(line).digest("base64"));
  });
  const deltaPath = join(regionWorkRoot(region), "data/delta.jsonl");
  const writer = createWriteStream(`${deltaPath}.tmp`);
  let added = 0;
  let changed = 0;
  let fresh = 0;
  await eachLineOf(createReadStream(regionJsonl(region)), line => {
    fresh++;
    const id = lineDocId(line);
    const known = old.get(id);
    if (known !== undefined) {
      old.delete(id);
      if (known === createHash("sha1").update(line).digest("base64")) return;
      changed++;
    } else {
      added++;
    }
    writer.write(line + "\n");
  });
  await new Promise(resolveEnd => writer.end(resolveEnd));
  renameSync(`${deltaPath}.tmp`, deltaPath);
  return { deltaPath, added, changed, deleted: old.size, fresh };
}

// --- step 3: scoring stats ---------------------------------------------------

function statsPath() {
  return join(STATS_DIR, "scoring-stats.json");
}

async function ensureScoringStats(regions, options, state, force, allowRegen = true) {
  const current = existsSync(statsPath()) ? loadScoringStats(statsPath()) : null;
  const wantedIds = regions.map(region => region.id).sort();
  const currentIds = (current?.inputs || []).map(input => input.id).sort();
  const totalDocs = regions.reduce((sum, region) => sum + (state.regions[region.id]?.docs || 0), 0);
  const drift = current ? Math.abs(totalDocs - current.total) / Math.max(1, current.total) : 1;
  const reason = force ? "forced"
    : !current ? "missing"
    : JSON.stringify(wantedIds) !== JSON.stringify(currentIds) ? "region set changed"
    : drift > options.statsDriftRatio ? `corpus drift ${(drift * 100).toFixed(1)}%`
    : null;
  if (!reason) return;
  if (!allowRegen) {
    // A --regions-scoped run must never regenerate the artifact: it would
    // freeze statistics over the subset and break cross-shard scoring.
    throw new Error(`scoring stats need regeneration (${reason}) — run without --regions.`);
  }

  log(`scoring-stats: regenerating (${reason}) — this invalidates every shard build`);
  for (const region of regions) await ensurePlainJsonl(region);
  const templatePath = join(WORK, "configs/_stats-template.json");
  mkdirSync(dirname(templatePath), { recursive: true });
  writeFileSync(templatePath, JSON.stringify(shardConfig(regions[0], options, "", null, state)));
  const templateConfig = await readConfig(templatePath);
  await collectScoringStats({
    config: templateConfig,
    inputs: regions.map(region => ({ id: region.id, input: regionJsonl(region) })),
    outDir: STATS_DIR,
    log: line => log(line)
  });
}

// --- step 4: shard builds ----------------------------------------------------

function shardConfig(region, options, scoringStatsPath, input = null, state = null) {
  const workerCount = options.workerCount > 0 ? options.workerCount : Math.max(1, availableParallelism() - 1);
  const entry = state?.regions?.[region.id] || {};
  return createOsmIndexConfig({
    workerCount,
    input: input || regionJsonl(region),
    output: join(OUT, "shards", region.id),
    buildProgressLogMs: 60000,
    // Provenance stamped into the shard manifest on top of the OSM
    // attribution defaults: who built it, from which upstream file, and the
    // data vintage (Geofabrik Last-Modified — distinct from built_at).
    meta: {
      generator: "osm-rangefind-index",
      generated_by: options.publisher || hostname(),
      region: region.id,
      ...(region.geofabrik ? { source_url: `https://download.geofabrik.de/${region.geofabrik}-latest.osm.pbf` } : {}),
      ...(entry.pbfLastModified ? { data_version: entry.pbfLastModified } : {})
    },
    overrides: {
      ...(region.overrides || {}),
      ...(scoringStatsPath ? { scoringStats: scoringStatsPath } : {})
    }
  });
}

function statsFingerprint() {
  const stats = statSync(statsPath());
  return `${stats.size}:${Math.floor(stats.mtimeMs)}`;
}

// What a built shard depends on: the upstream corpus version and the frozen
// stats artifact — deliberately not file mtimes, so compressing/deleting
// local artifacts never triggers a rebuild.
function shardFingerprint(region, state) {
  const entry = state.regions[region.id] || {};
  return `${entry.extractIdentity || "?"}:${entry.docs || 0}:${statsFingerprint()}:${JSON.stringify(region.overrides || null)}`;
}

function shardDir(region) {
  return join(OUT, "shards", region.id);
}

function shardGenerationCount(region) {
  const manifest = loadJson(join(shardDir(region), "manifest.json"), null);
  return Array.isArray(manifest?.generations) ? manifest.generations.length : (manifest ? 1 : 0);
}

// Decides how to bring a stale shard up to date: a generational delta of the
// changed documents (the default — uploads only the delta generation), or a
// full rebuild when deltas can no longer carry the change.
async function planShardBuild(region, options, state) {
  const entry = state.regions[region.id] || {};
  const full = reason => ({ update: false, reason });
  if (!entry.builtFingerprint || !existsSync(join(shardDir(region), "manifest.json"))) return full("no base shard");
  if (entry.builtStats !== statsFingerprint()) return full("stats artifact changed");
  if (!existsSync(regionJsonlGz(region))) return full("no corpus snapshot to diff against");
  if (!existsSync(regionJsonl(region))) return full("no fresh extraction");
  const generations = shardGenerationCount(region);
  if (generations + 1 > options.maxGenerations) return full(`generation cap (${generations})`);

  const diff = await computeDelta(region);
  const docs = Math.max(1, entry.docs || diff.fresh);
  const deletedPending = (entry.deletedPending || 0) + diff.deleted;
  log(`${region.id}: delta vs snapshot — +${diff.added.toLocaleString()} added, ~${diff.changed.toLocaleString()} changed, -${diff.deleted.toLocaleString()} deleted`);
  if (diff.added + diff.changed === 0 && diff.deleted === 0) return { update: false, reason: "", noop: true };
  if ((diff.added + diff.changed) / docs > options.maxDeltaRatio) return full("delta too large");
  if (deletedPending / docs > options.maxDeletedRatio) return full(`deletions pending ${deletedPending}`);
  return { update: true, input: diff.deltaPath, deletedPending };
}

async function buildShard(region, options, budgetMs, plan, state) {
  if (!plan.update) await ensurePlainJsonl(region);
  const configPath = join(WORK, "configs", `${region.id}${plan.update ? ".delta" : ""}.json`);
  mkdirSync(dirname(configPath), { recursive: true });
  writeFileSync(configPath, JSON.stringify(shardConfig(region, options, statsPath(), plan.update ? plan.input : null, state), null, 2));
  log(`${region.id}: ${plan.update ? "applying delta generation" : "full shard build"} (budget ${budgetMs === Infinity ? "unbounded" : `${(budgetMs / 60000).toFixed(0)}m`})`);
  return new Promise(resolveDone => {
    const child = spawn(
      process.execPath,
      [join(projectRoot, "scripts/build_shard.mjs"), configPath, ...(plan.update ? ["--update"] : [])],
      { stdio: "inherit" }
    );
    const timer = budgetMs < Infinity
      ? setTimeout(() => {
          log(`${region.id}: deadline reached — stopping build (resumes next run)`);
          child.kill("SIGTERM");
        }, Math.max(0, budgetMs))
      : null;
    child.on("exit", code => {
      if (timer) clearTimeout(timer);
      resolveDone(code === 0);
    });
  });
}

// --- step 5: publish ---------------------------------------------------------

function rclone(argv) {
  log(`rclone ${argv.join(" ")}`);
  execFileSync("rclone", argv, { stdio: "inherit" });
}

function uploadShard(region, remote, prune) {
  const local = join(OUT, "shards", region.id);
  const target = `${remote}/shards/${region.id}`;
  const excludes = ["--exclude", "_build/**", "--exclude", "manifest*.json"];
  // Packs are content-addressed and immutable: size comparison against the
  // remote listing suffices, so unchanged objects are never re-uploaded (and
  // never downloaded). Manifests go last.
  rclone(["copy", local, target, ...excludes, "--size-only", "--transfers", "8"]);
  rclone(["copy", local, target, "--include", "manifest*.json", "--transfers", "4"]);
  if (prune) {
    // Removes remote packs no longer referenced. Only valid right after a
    // fresh build while the local copy is complete; run occasionally — a
    // reader mid-query on the previous manifest may still fetch old objects
    // for a short while.
    rclone(["sync", local, target, ...excludes, "--size-only", "--transfers", "8"]);
    rclone(["copy", local, target, "--include", "manifest*.json"]);
  }
}

function uploadRoot(remote) {
  for (const name of ["manifest.json", "manifest.min.json"]) {
    rclone(["copyto", join(OUT, name), `${remote}/${name}`]);
  }
}

function statusSnapshot(regions, state) {
  const rows = regions.map(region => {
    const entry = state.regions[region.id] || {};
    const acquired = hasCorpus(region);
    const built = Boolean(entry.builtFingerprint)
      && existsSync(join(shardDir(region), "manifest.min.json"));
    const uploaded = built
      && entry.uploadedFingerprint === entry.builtFingerprint;
    return { region, entry, acquired, built, uploaded };
  });
  const totalRegions = rows.length;
  const acquiredRegions = rows.filter(row => row.acquired).length;
  const builtShards = rows.filter(row => row.built).length;
  const uploadedShards = rows.filter(row => row.uploaded).length;
  const fallbackPublished = rows.filter(row => row.uploaded);
  const publishedRegionIds = state.publishedRoot?.regionIds
    || fallbackPublished.map(row => row.region.id);
  const publishedIdSet = new Set(publishedRegionIds);
  const publishedShards = state.publishedRoot?.shards ?? publishedRegionIds.length;
  const publishedDocuments = Number(state.publishedRoot?.documents ?? fallbackPublished
    .filter(row => row.uploaded)
    .reduce((sum, row) => sum + (row.entry.docs || 0), 0));
  const latestDataAt = rows
    .filter(row => publishedIdSet.has(row.region.id))
    .map(row => row.entry.pbfLastModified)
    .filter(Boolean)
    .map(value => new Date(value))
    .filter(value => !Number.isNaN(value.getTime()))
    .sort((a, b) => b - a)[0]?.toISOString() || null;
  const phase = state.run?.status === "failed" ? "failed"
    : acquiredRegions < totalRegions ? "acquiring"
    : builtShards < totalRegions ? "building"
    : publishedShards < totalRegions ? "publishing"
    : "ready";
  const percent = value => totalRegions
    ? Math.round((value / totalRegions) * 1000) / 10
    : 0;

  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    run: {
      state: state.run?.status || "idle",
      startedAt: state.run?.startedAt || null,
      completedAt: state.run?.completedAt || null,
      deadline: state.run?.deadline || null,
      selectedRegions: state.run?.selectedRegions || null,
      progress: state.run?.progress || null,
      lastSuccessfulAt: state.lastSuccessfulRunAt || null,
      error: state.run?.error || null
    },
    index: {
      phase,
      totalRegions,
      acquiredRegions,
      builtShards,
      uploadedShards,
      publishedShards,
      publishedDocuments,
      acquisitionPercent: percent(acquiredRegions),
      publicationPercent: percent(publishedShards),
      latestDataAt,
      lastPublishedAt: state.rootPublishedAt || null,
      nextPendingRegions: rows
        .filter(row => !row.acquired)
        .slice(0, 10)
        .map(row => row.region.id)
    },
    endpoints: {
      manifest: "manifest.min.json",
      status: "status.json"
    }
  };
}

function writeStatusArtifacts(regions, state) {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "index.html"), readFileSync(join(projectRoot, "public/index.html")));
  writeFileSync(join(OUT, "status.json"), `${JSON.stringify(statusSnapshot(regions, state), null, 2)}\n`);
}

function publishStatusArtifacts(regions, state, remote, upload, includePage = false) {
  try {
    writeStatusArtifacts(regions, state);
    if (upload) {
      const names = includePage ? ["index.html", "status.json"] : ["status.json"];
      for (const name of names) {
        rclone(["copyto", join(OUT, name), `${remote}/${name}`]);
      }
    }
  } catch (error) {
    log(`Status page update failed — ${error.message}`);
  }
}

// The state records what was uploaded, but the remote is the truth: a wiped
// bucket or a changed R2_REMOTE must not be trusted-through. One cheap
// listing call per shard per run.
function remoteHasShard(remote, region) {
  try {
    const out = execFileSync("rclone", ["lsf", `${remote}/shards/${region.id}/manifest.min.json`], {
      stdio: ["ignore", "pipe", "ignore"]
    });
    return out.toString().trim().length > 0;
  } catch {
    return false;
  }
}

// --- step 6: cleanup -----------------------------------------------------------

// Local files a FUTURE generational delta needs from the built shard:
// every manifest (root, generation-scoped, per-generation) plus each
// generation's id-map (tombstone bookkeeping). Everything else — packs,
// directories, doc payloads — lives on R2 and is never read back.
function shardKeepPaths(region) {
  const keep = new Set(["manifest.json", "manifest.min.json", "manifest.full.json"]);
  const manifest = loadJson(join(shardDir(region), "manifest.json"), null);
  if (!manifest) return keep;
  const normalize = (base, path) => join(base || ".", path).replace(/^\.\//u, "");
  if (manifest.id_map) keep.add(normalize("", manifest.id_map));
  for (const generation of manifest.generations || []) {
    if (generation.manifest) {
      keep.add(normalize("", generation.manifest));
      keep.add(normalize("", generation.manifest).replace(".min.json", ".json"));
    }
    for (const name of ["manifest.json", "manifest.min.json"]) {
      keep.add(normalize(generation.path, name));
    }
    if (generation.id_map) keep.add(normalize(generation.path, generation.id_map));
  }
  return keep;
}

function pruneShardDir(region) {
  const root = shardDir(region);
  if (!existsSync(root)) return;
  const keep = shardKeepPaths(region);
  const walk = relative => {
    const absolute = join(root, relative);
    for (const name of readdirSync(absolute, { withFileTypes: true })) {
      const rel = relative ? join(relative, name.name) : name.name;
      if (name.isDirectory()) {
        walk(rel);
        try {
          if (!readdirSync(join(root, rel)).length) rmSync(join(root, rel), { recursive: true, force: true });
        } catch { /* raced */ }
      } else if (!keep.has(rel)) {
        rmSync(join(root, rel), { force: true });
      }
    }
  };
  walk("");
}

// After a shard is safely on R2, local disk keeps only what the next update
// needs: the compressed corpus snapshot (diff base + stats regeneration
// input), the extraction meta, shard manifests, and generation id-maps.
// PBFs re-download only when Geofabrik publishes a new version.
async function cleanupRegion(region, state) {
  if (!region.pinned) rmSync(region.pbf, { force: true });
  await compressJsonl(region);
  const dataDir = join(regionWorkRoot(region), "data");
  const keep = new Set(["osm-places.jsonl.gz", "osm-places.meta.json"]);
  if (existsSync(dataDir)) {
    for (const name of readdirSync(dataDir)) {
      if (!keep.has(name)) rmSync(join(dataDir, name), { recursive: true, force: true });
    }
  }
  pruneShardDir(region);
  state.regions[region.id].cleaned = true;
  state.regions[region.id].localComplete = false;
  log(`${region.id}: cleaned local artifacts (kept compressed corpus, manifests, id-maps)`);
}

async function uploadAndCleanupShard(region, state, remote, args) {
  const entry = state.regions[region.id];
  uploadShard(region, remote, args.prune && entry.localComplete === true);
  entry.uploadedFingerprint = entry.builtFingerprint;
  saveState(state);
  log(`${region.id}: shard uploaded to R2.`);
  if (!args.keepArtifacts) {
    await cleanupRegion(region, state);
    saveState(state);
  }
}

// --- status ------------------------------------------------------------------

function printStatus(regions, state) {
  for (const region of regions) {
    const entry = state.regions[region.id] || {};
    const built = entry.builtFingerprint && existsSync(join(OUT, "shards", region.id, "manifest.min.json"));
    console.log([
      region.id.padEnd(16),
      (entry.docs || 0).toLocaleString().padStart(12),
      built ? `built(gen ${shardGenerationCount(region)})` : "PENDING",
      entry.uploadedFingerprint === entry.builtFingerprint && entry.builtFingerprint ? "uploaded" : "upload-pending",
      entry.cleaned ? "cleaned" : "artifacts-on-disk",
      `del-pending ${entry.deletedPending || 0}`,
      entry.pbfLastModified || ""
    ].join("  "));
  }
}

// --- main --------------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const loaded = loadRegions(args);
  const { regions } = loaded;
  const allRegions = args.regions
    ? loadRegions({ ...args, regions: null }).regions
    : regions;
  const options = loaded;
  mkdirSync(WORK, { recursive: true });
  const state = loadJson(STATE_PATH, { regions: {} });
  state.regions = state.regions || {};

  if (args.status) {
    printStatus(regions, state);
    return;
  }

  const remote = process.env.R2_REMOTE || "";
  if (args.upload && !remote) throw new Error("Set R2_REMOTE (e.g. r2:osm-index) or pass --no-upload.");
  acquireLock();
  const stopAt = deadlineMs(args);
  const remaining = () => stopAt - Date.now();
  const outOfTime = (needMs = 5 * 60_000) => remaining() < needMs;
  log(`Run starts; deadline ${stopAt === Infinity ? "none" : new Date(stopAt).toISOString()}`);
  state.run = {
    status: "running",
    startedAt: new Date().toISOString(),
    completedAt: null,
    deadline: stopAt === Infinity ? null : new Date(stopAt).toISOString(),
    selectedRegions: args.regions,
    progress: null,
    error: null
  };
  saveState(state);
  publishStatusArtifacts(allRegions, state, remote, args.upload, true);
  const updateProgress = (stage, region = null, completed = 0, total = regions.length, extra = {}) => {
    state.run.progress = {
      stage,
      region: region?.id || null,
      completed,
      total,
      ...extra,
      updatedAt: new Date().toISOString()
    };
    saveState(state);
    publishStatusArtifacts(allRegions, state, remote, args.upload);
  };
  let runError = null;

  try {

  // 1 + 2: refresh and extract selected regions. Downloads may overlap with
  // extraction; normal extracts share the configured lanes, while a large
  // PBF takes every lane so two memory-heavy regions can never overlap.
  const acquisitionConcurrency = Math.min(options.acquisitionConcurrency, regions.length);
  const withExtractionCapacity = createWeightedLimiter(acquisitionConcurrency);
  const activeRegions = new Set();
  let acquisitionCursor = 0;
  let acquisitionCompleted = 0;
  const reportAcquisition = region => updateProgress(
    "acquiring",
    region,
    acquisitionCompleted,
    regions.length,
    { regions: [...activeRegions] }
  );
  const acquireRegions = async () => {
    while (!outOfTime()) {
      const regionIndex = acquisitionCursor++;
      if (regionIndex >= regions.length) return;
      const region = regions[regionIndex];
      activeRegions.add(region.id);
      reportAcquisition(region);
      try {
        const source = await refreshPbf(region, state);
        const large = source.bytes >= options.largePbfBytes;
        if (large) {
          log(`${region.id}: large PBF (${(source.bytes / 1024 / 1024 / 1024).toFixed(1)} GiB) — extracting exclusively`);
        }
        const extracted = await withExtractionCapacity(
          large ? acquisitionConcurrency : 1,
          () => extractJsonl(region, state)
        );
        if (extracted) {
          log(`${region.id}: corpus refreshed (${(state.regions[region.id].docs || 0).toLocaleString()} docs)`);
          if (!state.regions[region.id].builtFingerprint) {
            // Bring-up acquisition: no shard exists yet, so the corpus is not
            // a diff base — compress it now and drop the PBF, keeping the
            // acquisition footprint near the gzipped corpus total instead of
            // hundreds of GB of PBFs + plain JSONL.
            await compressJsonl(region);
            if (!region.pinned) rmSync(region.pbf, { force: true });
          }
        }
        cleanupExtractionScratch(region);
        saveState(state);
      } catch (error) {
        log(`${region.id}: refresh/extract failed — ${error.message} (continuing)`);
      } finally {
        activeRegions.delete(region.id);
        acquisitionCompleted++;
      }
    }
  };
  log(`Acquisition concurrency: ${acquisitionConcurrency} lane(s); PBFs >= ${(options.largePbfBytes / 1024 / 1024 / 1024).toFixed(1)} GiB run exclusively.`);
  await Promise.all(Array.from({ length: acquisitionConcurrency }, () => acquireRegions()));
  reportAcquisition(null);

  // 3: frozen stats (regenerating cascades a full rebuild via fingerprints).
  const ready = regions.filter(region => hasCorpus(region));
  if (!ready.length) throw new Error("No region has an extracted corpus yet.");
  // Bring-up gate: stats must cover the FULL region set before any shard
  // builds, or every night's newly acquired corpora would change the region
  // set, regenerate the stats, and invalidate all previously built shards.
  if (ready.length < regions.length && !args.partial) {
    log(`Acquisition phase: ${ready.length}/${regions.length} corpora present — builds start when all are acquired (pass --partial to build with a subset).`);
    return;
  }
  // Region-scoped runs normally preserve the planet-wide artifact. An
  // explicit --partial run is the deliberate exception used for bring-up
  // and smoke tests; the next full run will regenerate stats for all regions.
  updateProgress("preparing", null, ready.length, regions.length);
  await ensureScoringStats(ready, options, state, args.forceStats, !args.regions || args.partial);

  // 4: rebuild stale shards until the deadline.
  const stale = ready.filter(region => {
    try {
      return shardFingerprint(region, state) !== state.regions[region.id]?.builtFingerprint;
    } catch {
      return true;
    }
  });
  log(`${stale.length}/${ready.length} shard(s) need building: ${stale.map(r => r.id).join(", ") || "none"}`);
  for (const [regionIndex, region] of stale.entries()) {
    if (outOfTime(10 * 60_000)) {
      log("Deadline near — stopping before next shard build.");
      break;
    }
    updateProgress("building", region, regionIndex, stale.length);
    const entry = state.regions[region.id];
    const plan = await planShardBuild(region, options, state);
    if (plan.noop) {
      // Upstream churn without any place-document change (metadata-only OSM
      // edits): mark current without touching the index.
      entry.builtFingerprint = shardFingerprint(region, state);
      entry.builtStats = statsFingerprint();
      saveState(state);
      log(`${region.id}: corpus unchanged — shard already current.`);
      continue;
    }
    if (!plan.update) {
      log(`${region.id}: full rebuild (${plan.reason})`);
      // A fresh full rebuild starts from a clean slate so no stale
      // generations linger — but never wipe an in-progress build's
      // checkpoints (same fingerprint = same build resuming).
      const fingerprint = shardFingerprint(region, state);
      if (entry.buildStartedFingerprint !== fingerprint) {
        rmSync(shardDir(region), { recursive: true, force: true });
        entry.buildStartedFingerprint = fingerprint;
        saveState(state);
      }
    }
    const ok = await buildShard(region, options, remaining() - 60_000, plan, state);
    if (ok) {
      entry.builtFingerprint = shardFingerprint(region, state);
      entry.builtStats = statsFingerprint();
      entry.deletedPending = plan.update ? plan.deletedPending : 0;
      // Deltas leave the local copy partial when cleanup already ran; only
      // a full rebuild guarantees a complete local mirror (prune-safe).
      if (!plan.update) entry.localComplete = true;
      entry.cleaned = false;
      saveState(state);
      log(`${region.id}: shard ${plan.update ? "delta applied" : "built"} (${shardGenerationCount(region)} generation(s)).`);
      if (args.upload && !outOfTime(2 * 60_000)) {
        updateProgress("publishing", region, regionIndex + 1, stale.length);
        await uploadAndCleanupShard(region, state, remote, args);
      } else if (args.upload) {
        log(`${region.id}: shard ready; upload deferred because the deadline is near.`);
      }
    } else {
      log(`${region.id}: build incomplete (will resume next run).`);
      break;
    }
  }

  // 5 + 6: publish everything built and consistent, then reclaim disk.
  const built = ready.filter(region =>
    state.regions[region.id]?.builtFingerprint
    && existsSync(join(OUT, "shards", region.id, "manifest.min.json")));
  if (!built.length) {
    log("Nothing built yet; skipping publish.");
    return;
  }
  const stats = loadScoringStats(statsPath());
  const rootManifest = writeShardedRootManifest({
    outDir: OUT,
    shards: built.map(region => ({
      id: region.id,
      path: `shards/${region.id}/`,
      bbox: stats.inputs.find(input => input.id === region.id)?.bbox || null,
      groups: region.groups
    })),
    scoringStats: stats,
    extra: {
      // Root-level provenance: the OSM attribution block without any
      // region-specific fields; per-shard manifests carry source URLs and
      // data versions.
      meta: {
        ...createOsmIndexConfig({}).meta,
        generator: "osm-rangefind-index",
        generated_by: options.publisher || hostname()
      }
    }
  });
  log(`Root manifest: ${rootManifest.shards.length} shard(s), ${rootManifest.total.toLocaleString()} docs.`);

  if (args.upload) {
    for (const [regionIndex, region] of built.entries()) {
      if (outOfTime(2 * 60_000)) {
        log("Deadline near — remaining uploads next run.");
        break;
      }
      updateProgress("publishing", region, regionIndex, built.length);
      const entry = state.regions[region.id];
      if (entry.uploadedFingerprint === entry.builtFingerprint) {
        if (remoteHasShard(remote, region)) {
          // Already published; reclaim disk if a previous run kept artifacts.
          if (!args.keepArtifacts && !entry.cleaned) await cleanupRegion(region, state);
          continue;
        }
        if (entry.localComplete !== true) {
          // Remote lost the shard and local artifacts were reclaimed: only a
          // full rebuild can restore it. Clearing the fingerprint schedules
          // that; the shard drops from the root manifest until then.
          log(`${region.id}: shard missing on remote and local copy incomplete — scheduling full rebuild.`);
          entry.builtFingerprint = "";
          entry.uploadedFingerprint = "";
          saveState(state);
          continue;
        }
        log(`${region.id}: shard missing on remote — re-uploading from local copy.`);
      }
      // Prune requires a complete local mirror (fresh full rebuild): a
      // sync-with-delete from a partial local copy would delete live packs.
      await uploadAndCleanupShard(region, state, remote, args);
    }
    const allUploaded = built.every(region => {
      const entry = state.regions[region.id];
      return Boolean(entry.builtFingerprint) && entry.uploadedFingerprint === entry.builtFingerprint;
    });
    if (allUploaded) {
      updateProgress("publishing", null, built.length, built.length);
      uploadRoot(remote);
      state.rootPublishedAt = new Date().toISOString();
      state.publishedRoot = {
        shards: rootManifest.shards.length,
        documents: rootManifest.total,
        regionIds: rootManifest.shards.map(shard => shard.id)
      };
      saveState(state);
      log("Publish complete.");
    } else {
      log("Some shards not uploaded yet; root manifest NOT updated remotely (stays consistent).");
    }
  }
  log("Run finished.");
  } catch (error) {
    runError = error;
    throw error;
  } finally {
    const completedAt = new Date().toISOString();
    state.run = {
      ...state.run,
      status: runError ? "failed" : "idle",
      completedAt,
      progress: null,
      error: runError ? String(runError.message || runError).slice(0, 500) : null
    };
    if (!runError) state.lastSuccessfulRunAt = completedAt;
    saveState(state);
    publishStatusArtifacts(allRegions, state, remote, args.upload);
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
