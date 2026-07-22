#!/usr/bin/env node

// Idle-window OSM index updater.
//
// Designed to run on a server that is only free at night and on weekends:
// every invocation makes as much progress as fits before --deadline, then
// stops cleanly. All heavy steps are incremental and resumable, so a run
// killed mid-build simply continues the next night:
//
//   0. bootstrap  — once every corpus exists, finish the initial build and
//                   publication before checking for newer upstream PBFs.
//                   This prevents daily Geofabrik updates from starving the
//                   initial shard build indefinitely.
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
//   5. publish    — queue each completed shard for direct background R2 upload
//                   while the next shard builds, then rewrite the sharded
//                   root manifest after the bounded queue drains. Packs go
//                   first and manifests last, so readers never see a manifest
//                   that references missing objects. A shared S3 request pool
//                   batches immutable object PUTs across completed shards.
//   6. cleanup    — after a shard is uploaded, reclaim the space: drop the
//                   PBF and extractor caches, gzip the corpus JSONL, and gut
//                   the local index copy down to its manifests. Steady-state
//                   disk per region is just the compressed corpus; the next
//                   update re-materializes only what it needs.
//
// Usage:
//   node scripts/update_index.mjs [--deadline HH:MM] [--max-hours N]
//     [--regions id,id] [--no-upload] [--force-stats] [--prune]
//     [--keep-artifacts] [--finalize-only] [--status]
//
// Environment (see .env.example): direct Cloudflare R2/S3 credentials.

import { fork, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { availableParallelism, hostname } from "node:os";
import { pipeline } from "node:stream/promises";
import { createGunzip, createGzip } from "node:zlib";
import { fileURLToPath } from "node:url";
import { collectScoringStats, loadScoringStats } from "rangefind/scoring-stats";
import { writeShardedRootManifest } from "rangefind/shards";
// Namespace import for feature detection: writeShardTermSet /
// writeTextRoutingIndex only exist on rangefind > 0.3.1; older versions
// publish a fan-out root (no text_routing block) instead of failing.
import * as rangefindShards from "rangefind/shards";
import { readConfig } from "rangefind/config";
import { createOsmIndexConfig } from "rangefind/osm/node";
import { extractOsmPlaces } from "rangefind/osm/extract";
import { createR2Store, listLocalFiles } from "./lib/r2_store.mjs";
import { acquireProcessLock } from "./lib/process_lock.mjs";
import { createTaskQueue } from "./lib/serial_task_queue.mjs";
import {
  DEFAULT_PUBLIC_BASE_URL,
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "./lib/category_lexicon.mjs";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const WORK = join(projectRoot, "work");
const OUT = join(WORK, "public/rangefind");
const STATE_PATH = join(WORK, "state.json");
const LOCK_PATH = join(WORK, ".lock");
const STATS_DIR = join(WORK, "scoring-stats");
const CORPUS_DELTA_WORKER = join(projectRoot, "scripts/compute_delta_worker.mjs");
// Rangefind 0.3.6 locality enrichment changed normalized OSM documents.
// Keep this in the orchestrator identity so a package upgrade cannot reuse a
// corpus produced by an older extractor before extractOsmPlaces sees it.
const OSM_EXTRACTION_SCHEMA_VERSION = 9;

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
    finalizeOnly: false,
    partial: false,
    textRouting: true,
    suggestRouting: true,
    categoryLexicon: true
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
    else if (arg === "--finalize-only") args.finalizeOnly = true;
    else if (arg === "--partial") args.partial = true;
    else if (arg === "--no-text-routing") args.textRouting = false;
    else if (arg === "--no-suggest-routing") args.suggestRouting = false;
    else if (arg === "--no-category-lexicon") args.categoryLexicon = false;
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
  acquireProcessLock(LOCK_PATH, { label: "Another index or root-refresh run", log });
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

function bootstrapPublicationPending(regions, state, upload) {
  if (!regions.every(hasCorpus)) return false;
  const published = new Set(state.publishedRoot?.regionIds || []);
  return regions.some(region => {
    const entry = state.regions[region.id] || {};
    return !entry.builtFingerprint || (upload && !published.has(region.id));
  });
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
  const extractionCurrent = entry.extractIdentity === lastModified
    && entry.extractSchema === OSM_EXTRACTION_SCHEMA_VERSION
    && hasCorpus(region);
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
  if (entry.extractIdentity === identity
    && entry.extractSchema === OSM_EXTRACTION_SCHEMA_VERSION
    && hasCorpus(region)) return false;
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
  if (Number(meta.schemaVersion) !== OSM_EXTRACTION_SCHEMA_VERSION) {
    throw new Error(`${region.id}: Rangefind OSM extraction schema ${meta.schemaVersion || "unknown"}; expected ${OSM_EXTRACTION_SCHEMA_VERSION}`);
  }
  entry.docs = Number(meta.docs || 0);
  entry.extractIdentity = identity;
  entry.extractSchema = OSM_EXTRACTION_SCHEMA_VERSION;
  entry.overrides = region.overrides || null;
  return true;
}

// Diffs the fresh extraction against the snapshot the shard was built from.
// Added and changed documents become the delta corpus; deleted ids are only
// counted — generational deltas cannot remove documents, so deletions
// accumulate in state until they force a full rebuild.
// The hash map lives in an isolated high-heap child: large regions need
// several GiB, but the long-running orchestrator returns to baseline after
// every comparison instead of retaining that expanded V8 heap.
async function computeDelta(region) {
  const deltaPath = join(regionWorkRoot(region), "data/delta.jsonl");
  const heapMb = Math.max(4096, Math.min(24576, Number(process.env.CORPUS_DIFF_HEAP_MB || 16384) || 16384));
  return runIpcWorker(
    CORPUS_DELTA_WORKER,
    [regionJsonlGz(region), regionJsonl(region), deltaPath],
    heapMb
  );
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
  return `${entry.extractIdentity || "?"}:${entry.extractSchema || 0}:${entry.docs || 0}:${statsFingerprint()}:${JSON.stringify(region.overrides || null)}`;
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

function isManifestFile(path) {
  return /^manifest.*\.json$/u.test(path.split("/").pop());
}

function partitionPublishFiles(root) {
  const files = listLocalFiles(root).filter(file => !file.relative.startsWith("_build/"));
  const content = files.filter(file => !isManifestFile(file.relative));
  const manifests = files.filter(file => isManifestFile(file.relative));
  const rootNames = new Set(["manifest.json", "manifest.min.json", "manifest.full.json"]);
  return {
    files,
    content,
    dependencyManifests: manifests.filter(file => !rootNames.has(file.relative)),
    rootManifests: manifests.filter(file => rootNames.has(file.relative))
  };
}

async function uploadShard(region, store, prune) {
  const local = join(OUT, "shards", region.id);
  const target = `shards/${region.id}`;
  const publish = partitionPublishFiles(local);
  const started = Date.now();
  const contentResult = await store.putFiles(publish.content, target);
  await store.putFiles(publish.dependencyManifests, target);
  // Stable shard manifests flip only after every immutable object and
  // generation-scoped manifest they reference is durable in R2.
  await store.putFiles(publish.rootManifests, target);
  if (prune) {
    const keep = new Set(publish.files.map(file => `${target}/${file.relative}`));
    const stale = (await store.listObjects(`${target}/`))
      .map(object => object.path)
      .filter(path => !keep.has(path) && !path.includes("/_build/"));
    await store.deleteObjects(stale);
    if (stale.length) log(`${region.id}: pruned ${stale.length.toLocaleString()} superseded R2 object(s).`);
  }
  log(`${region.id}: direct R2 upload ${contentResult.files.toLocaleString()} immutable file(s), ${(contentResult.bytes / 1024 / 1024).toFixed(1)} MiB in ${Math.round((Date.now() - started) / 1000)}s.`);
}

async function uploadRoot(store, args) {
  // Routing artifacts are content-addressed, so copying them before the
  // manifests flip keeps R2 consistent at every instant. Old files linger
  // until a --prune run deletes them in S3 batches.
  const staleByPrefix = [];
  for (const prefix of ["text-routing", "authority"]) {
    const dir = join(OUT, prefix);
    if (!existsSync(dir)) continue;
    const files = listLocalFiles(dir);
    await store.putFiles(files, prefix);
    if (args?.prune) {
      const keep = new Set(files.map(file => `${prefix}/${file.relative}`));
      staleByPrefix.push(...(await store.listObjects(`${prefix}/`))
        .map(object => object.path)
        .filter(path => !keep.has(path)));
    }
  }
  for (const name of ["manifest.json", "manifest.min.json"]) {
    await store.putFile(join(OUT, name), name);
  }
  // Only retire routing objects after both stable root manifests have flipped.
  await store.deleteObjects(staleByPrefix);
  if (staleByPrefix.length) log(`Routing artifacts: pruned ${staleByPrefix.length.toLocaleString()} superseded R2 object(s).`);
}

// --- text routing --------------------------------------------------------------

// Term-set sidecars survive shard cleanup: routing rebuilds merge these small
// files instead of re-reading (possibly reclaimed) shard term directories.
const TERM_SETS_DIR = join(WORK, "term-sets");
const TEXT_ROUTING_BLOCK_PATH = join(WORK, "text-routing-block.json");
const SUGGEST_SETS_DIR = join(WORK, "suggest-sets");
const SUGGEST_ROUTING_BLOCK_PATH = join(WORK, "suggest-routing-block.json");
const TEXT_ROUTING_WORKER = join(projectRoot, "scripts/text_routing_worker.mjs");

function termSetPath(region) {
  return join(TERM_SETS_DIR, `${region.id.replaceAll("/", "-")}.terms.gz`);
}

function suggestSetPath(region) {
  return join(SUGGEST_SETS_DIR, `${region.id.replaceAll("/", "-")}.suggest.gz`);
}

function runIpcWorker(worker, args, heapMb) {
  return new Promise((resolveDone, rejectDone) => {
    let result;
    const child = fork(worker, args, {
      execArgv: [`--max-old-space-size=${heapMb}`],
      stdio: ["ignore", "inherit", "inherit", "ipc"]
    });
    child.on("message", message => {
      if (message?.type === "result") result = message.value;
    });
    child.on("error", rejectDone);
    child.on("exit", (code, signal) => {
      if (code === 0 && result) resolveDone(result);
      else rejectDone(new Error(`text routing worker failed (${signal || `exit ${code}`})`));
    });
  });
}

function runTextRoutingWorker(args, heapMb = 4096) {
  return runIpcWorker(TEXT_ROUTING_WORKER, args, heapMb);
}

async function writeRegionTermSet(region, state) {
  if (typeof rangefindShards.writeShardTermSet !== "function") return false;
  const entry = state.regions[region.id];
  try {
    const started = Date.now();
    const written = await runTextRoutingWorker([
      "term-set",
      join(OUT, "shards", region.id),
      termSetPath(region)
    ]);
    entry.termSetFingerprint = entry.builtFingerprint;
    saveState(state);
    log(`${region.id}: term set written (${written.terms.toLocaleString()} terms, ${Math.round((Date.now() - started) / 1000)}s).`);
    return true;
  } catch (error) {
    log(`${region.id}: term set failed (${error.message}) — text routing skipped until it succeeds.`);
    return false;
  }
}

// Published-then-cleaned shards have no local term directory; pull just the
// manifests + terms of the remote copy to regenerate the sidecar once.
async function backfillRegionTermSet(region, state, store) {
  const tempDir = join(WORK, "term-backfill", region.id.replaceAll("/", "-"));
  rmSync(tempDir, { recursive: true, force: true });
  try {
    const prefix = `shards/${region.id}/`;
    await store.downloadPrefix(prefix, tempDir, relative => (
      /^manifest[^/]*\.json$/u.test(relative)
      || relative.startsWith("terms/")
      || /^gen-[^/]+\/manifest[^/]*\.json$/u.test(relative)
      || /^gen-[^/]+\/terms\//u.test(relative)
    ));
    const started = Date.now();
    const written = await runTextRoutingWorker(["term-set", tempDir, termSetPath(region)]);
    const entry = state.regions[region.id];
    entry.termSetFingerprint = entry.uploadedFingerprint || entry.builtFingerprint;
    saveState(state);
    log(`${region.id}: term set backfilled from remote (${written.terms.toLocaleString()} terms, ${Math.round((Date.now() - started) / 1000)}s).`);
    return true;
  } catch (error) {
    log(`${region.id}: term set backfill failed (${error.message}).`);
    return false;
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}

async function ensureRegionTermSet(region, state, store) {
  const entry = state.regions[region.id];
  const fresh = entry.termSetFingerprint === entry.builtFingerprint && existsSync(termSetPath(region));
  if (fresh) return true;
  const localManifest = join(OUT, "shards", region.id, "manifest.min.json");
  if (!entry.cleaned && existsSync(localManifest) && await writeRegionTermSet(region, state)) return true;
  return store ? backfillRegionTermSet(region, state, store) : false;
}

// Backfill existing published shards before new shard builds consume the
// nightly window. Sidecars checkpoint independently, so an interrupted pass
// resumes at the first missing shard on the next run.
async function prepareTextRoutingTermSets(built, state, store, args, outOfTime, reserveMs) {
  if (!args.textRouting) return true;
  if (typeof rangefindShards.writeShardTermSet !== "function") return false;
  for (const region of built) {
    const entry = state.regions[region.id];
    const fresh = entry.termSetFingerprint === entry.builtFingerprint && existsSync(termSetPath(region));
    if (fresh) continue;
    if (outOfTime(reserveMs)) {
      log("Text routing: deadline reserve reached — remaining term sets continue next run.");
      return false;
    }
    if (!await ensureRegionTermSet(region, state, store)) return false;
  }
  return true;
}

// Builds (or reuses) the root text-routing directory covering exactly the
// published shard set. Any missing piece downgrades to null — the root then
// ships without a text_routing block and clients fan out as before.
async function buildTextRoutingArtifact(built, state, store, args, outOfTime) {
  if (!args.textRouting) return null;
  if (typeof rangefindShards.writeTextRoutingIndex !== "function") {
    log("Text routing: installed rangefind lacks writeTextRoutingIndex — root stays fan-out.");
    return null;
  }
  if (!await prepareTextRoutingTermSets(built, state, store, args, outOfTime, 10 * 60_000)) return null;
  const fingerprint = createHash("sha1")
    .update(JSON.stringify(built.map(region => [region.id, state.regions[region.id]?.builtFingerprint || ""])))
    .digest("hex");
  const existing = loadJson(TEXT_ROUTING_BLOCK_PATH, null);
  if (state.textRoutingFingerprint === fingerprint && existing && existsSync(join(OUT, "text-routing"))) {
    return existing;
  }
  if (outOfTime(10 * 60_000)) {
    log("Text routing: deadline near — merge deferred to next run, root stays fan-out.");
    return null;
  }
  try {
    const started = Date.now();
    rmSync(join(OUT, "text-routing"), { recursive: true, force: true });
    const workerConfig = join(WORK, "text-routing-worker.json");
    writeFileSync(workerConfig, JSON.stringify({
      outDir: OUT,
      shards: built.map(region => ({ id: region.id, termSet: termSetPath(region) }))
    }));
    const routingHeapMb = Math.max(4096, Math.min(24576, Number(process.env.TEXT_ROUTING_HEAP_MB || 12288) || 12288));
    const block = await runTextRoutingWorker(["routing", workerConfig], routingHeapMb);
    writeFileSync(TEXT_ROUTING_BLOCK_PATH, JSON.stringify(block));
    state.textRoutingFingerprint = fingerprint;
    saveState(state);
    log(`Text routing: ${block.term_count.toLocaleString()} terms over ${built.length} shard(s) in ${Math.round((Date.now() - started) / 1000)}s.`);
    return block;
  } catch (error) {
    log(`Text routing build failed (root stays fan-out): ${error.message}`);
    return null;
  }
}

// --- suggest routing -----------------------------------------------------------

// Suggest-set sidecars mirror term sets: each shard's authority autocomplete
// lexicon survives local cleanup as a small gzipped JSONL file, so the root
// suggest artifact merges sidecars instead of whole shards.
async function writeRegionSuggestSet(region, state) {
  if (typeof rangefindShards.writeShardSuggestSet !== "function") return false;
  const entry = state.regions[region.id];
  try {
    const started = Date.now();
    const written = await runTextRoutingWorker([
      "suggest-set",
      join(OUT, "shards", region.id),
      suggestSetPath(region)
    ]);
    entry.suggestSetFingerprint = entry.builtFingerprint;
    saveState(state);
    log(`${region.id}: suggest set written (${written.keys.toLocaleString()} keys, ${Math.round((Date.now() - started) / 1000)}s).`);
    return true;
  } catch (error) {
    log(`${region.id}: suggest set failed (${error.message}) — suggest routing skipped until it succeeds.`);
    return false;
  }
}

// Published-then-cleaned shards have no local authority sidecar; pull just
// the manifests + authority files of the remote copy to regenerate it once.
async function backfillRegionSuggestSet(region, state, store) {
  const tempDir = join(WORK, "suggest-backfill", region.id.replaceAll("/", "-"));
  rmSync(tempDir, { recursive: true, force: true });
  try {
    const prefix = `shards/${region.id}/`;
    await store.downloadPrefix(prefix, tempDir, relative => (
      /^manifest[^/]*\.json$/u.test(relative)
      || relative.startsWith("authority/")
      || /^gen-[^/]+\/manifest[^/]*\.json$/u.test(relative)
      || /^gen-[^/]+\/authority\//u.test(relative)
    ));
    const started = Date.now();
    const written = await runTextRoutingWorker(["suggest-set", tempDir, suggestSetPath(region)]);
    const entry = state.regions[region.id];
    entry.suggestSetFingerprint = entry.uploadedFingerprint || entry.builtFingerprint;
    saveState(state);
    log(`${region.id}: suggest set backfilled from remote (${written.keys.toLocaleString()} keys, ${Math.round((Date.now() - started) / 1000)}s).`);
    return true;
  } catch (error) {
    log(`${region.id}: suggest set backfill failed (${error.message}).`);
    return false;
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}

async function ensureRegionSuggestSet(region, state, store) {
  const entry = state.regions[region.id];
  const fresh = entry.suggestSetFingerprint === entry.builtFingerprint && existsSync(suggestSetPath(region));
  if (fresh) return true;
  const localManifest = join(OUT, "shards", region.id, "manifest.min.json");
  if (!entry.cleaned && existsSync(localManifest) && await writeRegionSuggestSet(region, state)) return true;
  return store ? backfillRegionSuggestSet(region, state, store) : false;
}

async function prepareSuggestSets(built, state, store, args, outOfTime, reserveMs) {
  if (!args.suggestRouting) return true;
  if (typeof rangefindShards.writeShardSuggestSet !== "function") return false;
  for (const region of built) {
    const entry = state.regions[region.id];
    const fresh = entry.suggestSetFingerprint === entry.builtFingerprint && existsSync(suggestSetPath(region));
    if (fresh) continue;
    if (outOfTime(reserveMs)) {
      log("Suggest routing: deadline reserve reached — remaining suggest sets continue next run.");
      return false;
    }
    if (!await ensureRegionSuggestSet(region, state, store)) return false;
  }
  return true;
}

// Builds (or reuses) the root suggest artifact (merged authority lexicon at
// <root>/authority/) covering exactly the published shard set. Any missing
// piece downgrades to null — the root then ships without a suggest_routing
// block and clients fan out per keystroke as before.
async function buildSuggestRoutingArtifact(built, state, store, args, outOfTime) {
  if (!args.suggestRouting) return null;
  if (typeof rangefindShards.writeSuggestRoutingIndex !== "function") {
    log("Suggest routing: installed rangefind lacks writeSuggestRoutingIndex — suggest stays fan-out.");
    return null;
  }
  if (!await prepareSuggestSets(built, state, store, args, outOfTime, 10 * 60_000)) return null;
  const fingerprint = createHash("sha1")
    .update(JSON.stringify(built.map(region => [region.id, state.regions[region.id]?.builtFingerprint || ""])))
    .digest("hex");
  const existing = loadJson(SUGGEST_ROUTING_BLOCK_PATH, null);
  if (state.suggestRoutingFingerprint === fingerprint && existing && existsSync(join(OUT, "authority"))) {
    return existing;
  }
  if (outOfTime(10 * 60_000)) {
    log("Suggest routing: deadline near — merge deferred to next run, suggest stays fan-out.");
    return null;
  }
  try {
    const started = Date.now();
    rmSync(join(OUT, "authority"), { recursive: true, force: true });
    const workerConfig = join(WORK, "suggest-routing-worker.json");
    // `writeSuggestRoutingIndex` streams the shard sidecars, but it groups
    // adjacent keys before recursively partitioning them. At the library
    // default depth ("s|" plus two normalized characters), planet-scale
    // prefixes can still retain millions of entries. Start the physical
    // partitions six characters deeper; lookup remains compatible because
    // the resulting depth is recorded in the root manifest.
    const baseShardDepth = Math.max(4, Math.min(12,
      Number(process.env.SUGGEST_ROUTING_BASE_SHARD_DEPTH || 10) || 10));
    const maxShardDepth = Math.max(baseShardDepth, Math.min(16,
      Number(process.env.SUGGEST_ROUTING_MAX_SHARD_DEPTH || 14) || 14));
    writeFileSync(workerConfig, JSON.stringify({
      outDir: OUT,
      shards: built.map(region => ({ id: region.id, suggestSet: suggestSetPath(region) })),
      baseShardDepth,
      maxShardDepth
    }));
    const heapMb = Math.max(4096, Math.min(24576, Number(process.env.SUGGEST_ROUTING_HEAP_MB || 12288) || 12288));
    const block = await runTextRoutingWorker(["suggest-routing", workerConfig], heapMb);
    writeFileSync(SUGGEST_ROUTING_BLOCK_PATH, JSON.stringify(block));
    state.suggestRoutingFingerprint = fingerprint;
    saveState(state);
    log(`Suggest routing: ${block.keys.toLocaleString()} lexicon keys over ${built.length} shard(s) in ${Math.round((Date.now() - started) / 1000)}s.`);
    return block;
  } catch (error) {
    log(`Suggest routing build failed (suggest stays fan-out): ${error.message}`);
    return null;
  }
}

// Category lexicon: the merged `type` facet vocabulary across every shard,
// joined with the rangefind alias table and embedded in the root manifest —
// the query planner then gates bare category words ("cinema", "boulangerie")
// on the corpus's own vocabulary. Feature-detected like text/suggest
// routing: a rangefind without the builder publishes a root without the
// block, and the browser bundle falls back to its bundled vocabulary.
async function buildCategoryLexiconRootArtifact(built, state, args, outOfTime) {
  if (!args.categoryLexicon) return null;
  const lexiconModule = await loadCategoryLexiconModule();
  if (!lexiconModule) {
    log("Category lexicon: rangefind lacks the artifact builder — root published without it.");
    return null;
  }
  try {
    const merged = await mergeShardTypeVocabulary({
      shards: built.map(region => ({
        id: region.id,
        cacheKey: state.regions[region.id]?.builtFingerprint || "",
        // Reclaimed shards keep only manifests locally; the merge falls
        // back to the published copy for their facet dictionaries.
        localDir: shardDir(region)
      })),
      cachePath: join(WORK, "category-lexicon-cache.json"),
      remoteBase: process.env.OSM_PUBLIC_BASE_URL || DEFAULT_PUBLIC_BASE_URL,
      log,
      shouldStop: () => outOfTime(60_000)
    });
    return merged ? lexiconModule.buildCategoryLexiconArtifact(merged) : null;
  } catch (error) {
    log(`Category lexicon build failed (root published without it): ${error.message}`);
    return null;
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

let statusUploadTail = Promise.resolve();
let statusUploadRunning = false;
let statusUploadRequested = false;
let statusPageRequested = false;

function startStatusUpload(store) {
  if (statusUploadRunning) return statusUploadTail;
  statusUploadRunning = true;
  statusUploadTail = (async () => {
    while (statusUploadRequested) {
      statusUploadRequested = false;
      const includePage = statusPageRequested;
      statusPageRequested = false;
      const names = includePage ? ["index.html", "status.json"] : ["status.json"];
      try {
        for (const name of names) await store.putFile(join(OUT, name), name);
      } catch (error) {
        log(`Status page upload failed — ${error.message}`);
      }
    }
  })().finally(() => {
    statusUploadRunning = false;
    if (statusUploadRequested) startStatusUpload(store);
  });
  return statusUploadTail;
}

async function flushStatusUploads(store) {
  while (statusUploadRunning || statusUploadRequested) {
    if (!statusUploadRunning && statusUploadRequested) startStatusUpload(store);
    await statusUploadTail;
  }
}

function publishStatusArtifacts(regions, state, store, upload, includePage = false) {
  try {
    writeStatusArtifacts(regions, state);
    if (upload) {
      statusUploadRequested = true;
      statusPageRequested ||= includePage;
      startStatusUpload(store);
    }
  } catch (error) {
    log(`Status page update failed — ${error.message}`);
  }
  return statusUploadTail;
}

// The state records what was uploaded, but the remote is the truth: a wiped
// bucket or changed R2 credentials must not be trusted-through. One cheap
// HEAD call per shard per run.
async function remoteHasShard(store, region) {
  try {
    return await store.exists(`shards/${region.id}/manifest.min.json`);
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

async function uploadAndCleanupShard(region, state, store, args) {
  const entry = state.regions[region.id];
  await uploadShard(region, store, args.prune && entry.localComplete === true);
  entry.uploadedFingerprint = entry.builtFingerprint;
  saveState(state);
  log(`${region.id}: shard uploaded to R2.`);
  // A delta build may not retain the old generations' term packs locally.
  // Once the new manifest and delta are durable, regenerate its term-set
  // sidecar from the complete remote shard before local cleanup.
  const termSetFresh = entry.termSetFingerprint === entry.builtFingerprint
    && existsSync(termSetPath(region));
  if (args.textRouting && !termSetFresh) {
    await backfillRegionTermSet(region, state, store);
  }
  const suggestSetFresh = entry.suggestSetFingerprint === entry.builtFingerprint
    && existsSync(suggestSetPath(region));
  if (args.suggestRouting && !suggestSetFresh && typeof rangefindShards.writeShardSuggestSet === "function") {
    await backfillRegionSuggestSet(region, state, store);
  }
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

  const store = args.upload ? createR2Store() : null;
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
  publishStatusArtifacts(allRegions, state, store, args.upload, true);
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
    publishStatusArtifacts(allRegions, state, store, args.upload);
  };
  const configuredUploadQueueDepth = Number(process.env.R2_UPLOAD_QUEUE_DEPTH || 2);
  const uploadQueueDepth = Number.isInteger(configuredUploadQueueDepth) && configuredUploadQueueDepth > 0
    ? Math.min(configuredUploadQueueDepth, 8)
    : 2;
  const configuredUploadLanes = Number(process.env.R2_UPLOAD_LANES || 2);
  const uploadLanes = Number.isInteger(configuredUploadLanes) && configuredUploadLanes > 0
    ? Math.min(configuredUploadLanes, uploadQueueDepth)
    : Math.min(2, uploadQueueDepth);
  const uploadQueue = args.upload
    ? createTaskQueue({ maxPending: uploadQueueDepth, concurrency: uploadLanes })
    : null;
  const queueShardUpload = async region => {
    await uploadQueue.enqueue(async () => {
      log(`${region.id}: background upload started.`);
      try {
        await uploadAndCleanupShard(region, state, store, args);
        publishStatusArtifacts(allRegions, state, store, true);
      } catch (error) {
        log(`${region.id}: background upload failed — ${error.message}`);
        throw error;
      }
    });
    log(`${region.id}: queued for background upload (${uploadQueue.pending}/${uploadQueue.capacity} pending).`);
  };
  if (uploadQueue) {
    log(`Direct R2 uploads: ${uploadQueue.concurrency} shard lane(s), up to ${uploadQueue.capacity} shard(s) pending, ${process.env.R2_REQUEST_CONCURRENCY || 16} total S3 requests.`);
  }
  let runError = null;

  try {

  // 1 + 2: refresh and extract selected regions. Once every corpus exists,
  // the initial build takes priority until every selected shard has been
  // built (and, for uploaded runs, included in the published root). Without
  // this gate, daily upstream changes can consume every nightly window and
  // starve the initial build forever.
  const buildFirst = !args.regions
    && !args.partial
    && bootstrapPublicationPending(regions, state, args.upload);
  if (args.finalizeOnly) {
    log("Finalize-only: skipping upstream acquisition and shard builds.");
  } else if (buildFirst) {
    const published = new Set(state.publishedRoot?.regionIds || []);
    const builtCount = regions.filter(region => state.regions[region.id]?.builtFingerprint).length;
    const publishedCount = regions.filter(region => published.has(region.id)).length;
    log(`Bootstrap build-first: ${builtCount}/${regions.length} built, ${publishedCount}/${regions.length} published — skipping upstream refresh.`);
  } else {
    // Downloads may overlap with extraction; normal extracts share the
    // configured lanes, while a large PBF takes every lane so two
    // memory-heavy regions can never overlap.
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
              // Bring-up acquisition: no shard exists yet, so the corpus is
              // not a diff base — compress it now and drop the PBF, keeping
              // the acquisition footprint near the gzipped corpus total
              // instead of hundreds of GB of PBFs + plain JSONL.
              await compressJsonl(region);
            }
          }
          // The completed corpus is the only downstream build input. Keeping
          // all ~79 GiB of downloaded PBFs until 310 shards publish can exhaust
          // the planet-build disk while fresh JSONL and old gz snapshots
          // coexist, so reclaim each non-pinned source immediately.
          if (!region.pinned) rmSync(region.pbf, { force: true });
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
  }

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
  if (args.finalizeOnly) {
    if (!existsSync(statsPath())) {
      throw new Error("Finalize-only requires an existing scoring-stats artifact.");
    }
  } else {
    await ensureScoringStats(ready, options, state, args.forceStats, !args.regions || args.partial);
  }

  // Existing cleaned shards need one remote term-set backfill for federated
  // routing. Do that before builds so a full nightly build window cannot
  // starve the backfill indefinitely.
  const builtBeforeBuild = ready.filter(region =>
    state.regions[region.id]?.builtFingerprint
    && existsSync(join(OUT, "shards", region.id, "manifest.min.json")));
  const textRoutingAvailable = args.textRouting
    && typeof rangefindShards.writeShardTermSet === "function"
    && typeof rangefindShards.writeTextRoutingIndex === "function";
  const suggestRoutingAvailable = args.suggestRouting
    && typeof rangefindShards.writeShardSuggestSet === "function"
    && typeof rangefindShards.writeSuggestRoutingIndex === "function";
  if (textRoutingAvailable && builtBeforeBuild.length) {
    updateProgress("routing", null, 0, builtBeforeBuild.length);
    await prepareTextRoutingTermSets(
      builtBeforeBuild,
      state,
      store,
      args,
      outOfTime,
      30 * 60_000
    );
  }
  if (suggestRoutingAvailable && builtBeforeBuild.length) {
    updateProgress("routing", null, 0, builtBeforeBuild.length);
    await prepareSuggestSets(
      builtBeforeBuild,
      state,
      store,
      args,
      outOfTime,
      30 * 60_000
    );
  }

  // 4: rebuild stale shards until the deadline.
  const stale = args.finalizeOnly ? [] : ready.filter(region => {
    try {
      return shardFingerprint(region, state) !== state.regions[region.id]?.builtFingerprint;
    } catch {
      return true;
    }
  });
  log(`${stale.length}/${ready.length} shard(s) need building: ${stale.map(r => r.id).join(", ") || "none"}`);
  // Leave enough time to finish routing and atomically publish the new root.
  // Interrupted Rangefind builds retain their stage checkpoints.
  const finalizationReserveMs = textRoutingAvailable ? 30 * 60_000 : 10 * 60_000;
  for (const [regionIndex, region] of stale.entries()) {
    if (outOfTime(finalizationReserveMs)) {
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
    const ok = await buildShard(region, options, remaining() - finalizationReserveMs, plan, state);
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
      // Full builds capture their term set before cleanup. Delta indexes
      // reference old generation term packs that were reclaimed
      // locally. Their sidecar is regenerated from the complete remote shard
      // immediately after upload instead.
      if (args.textRouting && (!plan.update || !args.upload)) {
        await writeRegionTermSet(region, state);
      }
      if (args.suggestRouting && (!plan.update || !args.upload) && typeof rangefindShards.writeShardSuggestSet === "function") {
        await writeRegionSuggestSet(region, state);
      }
      if (args.upload && !outOfTime(2 * 60_000)) {
        await queueShardUpload(region);
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
  const textRouting = await buildTextRoutingArtifact(built, state, store, args, outOfTime);
  const suggestRouting = await buildSuggestRoutingArtifact(built, state, store, args, outOfTime);
  // Category vocabulary may need the remote facet dictionaries after shard
  // cleanup. Ensure every queued shard is durable before those reads so a
  // local cleanup race can never fall back to the previous remote version.
  if (args.upload) await uploadQueue.drain();
  const categoryLexicon = await buildCategoryLexiconRootArtifact(built, state, args, outOfTime);
  const rootManifest = writeShardedRootManifest({
    outDir: OUT,
    shards: built.map(region => ({
      id: region.id,
      path: `shards/${region.id}/`,
      bbox: stats.inputs.find(input => input.id === region.id)?.bbox || null,
      groups: region.groups
    })),
    scoringStats: stats,
    textRouting,
    suggestRouting,
    extra: {
      ...(categoryLexicon ? { category_lexicon: categoryLexicon } : {}),
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
    // Builds run ahead of the bounded multi-lane queue. It was drained before
    // the category merge; now verify remote completeness before the root flip.
    for (const [regionIndex, region] of built.entries()) {
      if (outOfTime(2 * 60_000)) {
        log("Deadline near — remaining uploads next run.");
        break;
      }
      updateProgress("publishing", region, regionIndex, built.length);
      const entry = state.regions[region.id];
      if (entry.uploadedFingerprint === entry.builtFingerprint) {
        if (await remoteHasShard(store, region)) {
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
      // Prune requires a complete local mirror (fresh full rebuild): deleting
      // remote extras from a partial local copy would delete live packs.
      await uploadAndCleanupShard(region, state, store, args);
    }
    const allUploaded = built.every(region => {
      const entry = state.regions[region.id];
      return Boolean(entry.builtFingerprint) && entry.uploadedFingerprint === entry.builtFingerprint;
    });
    if (allUploaded) {
      updateProgress("publishing", null, built.length, built.length);
      await uploadRoot(store, args);
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
    publishStatusArtifacts(allRegions, state, store, args.upload);
    if (args.upload) await flushStatusUploads(store);
    store?.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
