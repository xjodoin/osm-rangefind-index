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
//     [--keep-artifacts] [--finalize-only] [--roads-only] [--status]
//
// Environment (see .env.example): direct Cloudflare R2/S3 credentials.

import { fork, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { closeSync, createReadStream, createWriteStream, existsSync, mkdirSync, openSync, readFileSync, readSync, readdirSync, renameSync, rmSync, statfsSync, statSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { basename, dirname, join, resolve } from "node:path";
import { availableParallelism, hostname } from "node:os";
import { pipeline } from "node:stream/promises";
import { setTimeout as delay } from "node:timers/promises";
import { createGunzip, createGzip } from "node:zlib";
import { fileURLToPath } from "node:url";
import { collectScoringStats, loadScoringStats } from "rangefind/scoring-stats";
import { writeShardedRootManifest } from "rangefind/shards";
// Namespace import for feature detection: writeShardTermSet /
// writeTextRoutingIndex only exist on rangefind > 0.3.1; older versions
// publish a fan-out root (no text_routing block) instead of failing.
import * as rangefindShards from "rangefind/shards";
import { readConfig } from "rangefind/config";
import * as rangefindOsmNode from "rangefind/osm/node";
import { createR2Store, listLocalFiles } from "./lib/r2_store.mjs";
import {
  createAdaptiveStageLimiter,
  pipelineStageWeight,
  systemHeadroom,
  waitForSystemHeadroom
} from "./lib/adaptive_stage_limiter.mjs";
import { acquireProcessLock } from "./lib/process_lock.mjs";
import { appendStaleObjectPaths } from "./lib/root_artifacts.mjs";
import {
  buildDiscoveryDocument,
  DISCOVERY_PATH,
  rootDiscoveryEndpoints
} from "./lib/discovery.mjs";
import {
  ROOT_ROUTING_ARTIFACTS,
  rootRoutingArtifactIsPublished
} from "./lib/root_publish.mjs";
import { createTaskQueue } from "./lib/serial_task_queue.mjs";
import {
  createDiskAdmissionController,
  DiskHeadroomError
} from "./lib/disk_admission.mjs";
import { fetchSource } from "./lib/source_fetch.mjs";
import {
  acquisitionSessionSignature,
  openAcquisitionSession,
  recordAcquisitionFailure,
  recordAcquisitionSuccess
} from "./lib/acquisition_resume.mjs";
import {
  buildContentFingerprint,
  buildShardFingerprint,
  previouslyBuiltBuilderVersion,
  previouslyBuiltContentFingerprint,
  selectRootCandidates,
  shouldReuseFrozenStats
} from "./lib/build_identity.mjs";
import {
  DEFAULT_PUBLIC_BASE_URL,
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "./lib/category_lexicon.mjs";
import {
  additionalSourceMetadata,
  addressSourceAdapterOptions,
  addressSourcesForRegion,
  loadAddressSourcesConfig,
  partitionAddressSourceSpatially,
  prepareAddressSource,
  regionAddressSourceIdentity,
  regionRoutingMetadata,
  spatialPartitionForRegion
} from "./lib/address_sources.mjs";
import {
  buildRoadCatalog,
  normalizeRoadIndexConfig,
  planRoadObjectPrune,
  roadFederationNeighbors,
  roadIndexesCurrent,
  roadProfileIdentity
} from "./lib/road_indexes.mjs";

const { createOsmIndexConfig } = rangefindOsmNode;

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ADDRESS_SOURCES = loadAddressSourcesConfig(projectRoot);
const taskRequire = createRequire(import.meta.url);
const RANGEFIND_VERSION = taskRequire("rangefind/package.json").version;
// Runtime-only releases must not invalidate every published shard. Keep this
// at the newest Rangefind release that changed builder output or analysis
// semantics, and bump it deliberately when artifacts really must be rebuilt.
const RANGEFIND_BUILDER_VERSION = "0.4.13";
// Route artifacts are versioned independently from search artifacts. Rangefind
// 0.4.13 changes authority/suggestion output; 0.4.14 optimizes extraction,
// 0.4.15 streams serialization, 0.4.16 bounds SCC materialization, 0.4.17
// streams large graph reads, 0.4.18 compacts overlay materialization, and
// 0.4.19 bounds turn expansion and compacts SCC output in place. 0.4.20 adds
// shared-OSM-id portals. 0.4.21 makes them per-neighbor range blocks and
// separates geometry packs for low-request rendering, deliberately
// invalidating road artifacts while keeping rfroutegraph-v1 compatible.
// 0.5.0 adds vehicle/lane access flags and road numbering schemes, and bumps
// the route root/cell codecs; old graphs remain readable but must be rebuilt
// to expose the new semantics.
const RANGEFIND_ROAD_BUILDER_VERSION = "0.5.0";
const WORK = join(projectRoot, "work");
const OUT = join(WORK, "public/rangefind");
const STATE_PATH = join(WORK, "state.json");
const LOCK_PATH = join(WORK, ".lock");
const STATS_DIR = join(WORK, "scoring-stats");
const CORPUS_DELTA_WORKER = join(projectRoot, "scripts/compute_delta_worker.mjs");
const ROAD_INDEX_WORKER = join(projectRoot, "scripts/road_index_worker.mjs");
const OSM_EXTRACT_WORKER = join(projectRoot, "scripts/osm_extract_worker.mjs");
const ADDRESS_ENRICHMENT_WORKER = join(projectRoot, "scripts/address_enrichment_worker.mjs");
const SOURCE_REQUEST_TIMEOUT_MS = Math.max(
  1_000,
  Number(process.env.SOURCE_REQUEST_TIMEOUT_MS || 30_000)
);
const PBF_DOWNLOAD_TIMEOUT_MS = Math.max(
  SOURCE_REQUEST_TIMEOUT_MS,
  Number(process.env.PBF_DOWNLOAD_TIMEOUT_MS || 30 * 60_000)
);
const ACQUISITION_MAX_ATTEMPTS = Math.max(
  1,
  Math.min(10, Number(process.env.ACQUISITION_MAX_ATTEMPTS || 4))
);
const ACQUISITION_RETRY_BASE_MS = Math.max(
  1_000,
  Number(process.env.ACQUISITION_RETRY_BASE_MS || 10_000)
);
// Keep this synchronized with Rangefind's PBF extraction schema so a package
// upgrade cannot reuse a corpus produced by an older extractor before
// extractOsmPlaces sees it. Schema 11 adds normalized alternate names and
// fallback identities.
const OSM_EXTRACTION_SCHEMA_VERSION = 11;

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
    categoryLexicon: true,
    roadIndexes: true,
    roadsOnly: false
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
    else if (arg === "--no-road-indexes") args.roadIndexes = false;
    else if (arg === "--roads-only") args.roadsOnly = true;
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
  const tmp = `${STATE_PATH}.tmp`;
  rmSync(tmp, { force: true });
  writeFileSync(tmp, JSON.stringify(state, null, 2));
  renameSync(tmp, STATE_PATH);
}

function loadState() {
  if (!existsSync(STATE_PATH)) return { regions: {} };
  try {
    return JSON.parse(readFileSync(STATE_PATH, "utf8"));
  } catch (error) {
    throw new Error(`State file is invalid at ${STATE_PATH}; refusing to discard resumable build state: ${error.message}`);
  }
}

function loadRegions(args) {
  const config = loadJson(join(projectRoot, "regions.json"), null);
  if (!config?.regions?.length) throw new Error("regions.json has no regions.");
  const regions = config.regions
    .map(region => {
      const bbox = region.bbox == null
        ? null
        : Array.isArray(region.bbox)
          ? region.bbox.map(Number)
          : [];
      if (
        bbox
        && (
          bbox.length !== 4
          || bbox.some(value => !Number.isFinite(value))
          || bbox[0] < -90
          || bbox[2] > 90
          || bbox[0] > bbox[2]
          || bbox[1] < -180
          || bbox[1] > 180
          || bbox[3] < -180
          || bbox[3] > 180
        )
      ) {
        throw new Error(`Invalid coverage bbox for region ${region.id}.`);
      }
      return {
        id: String(region.id || region.geofabrik?.split("/").pop() || "").trim(),
        geofabrik: region.geofabrik || "",
        pbf: region.pbf ? resolve(projectRoot, region.pbf) : join(WORK, "regions", String(region.id), `${region.id}-latest.osm.pbf`),
        pinned: Boolean(region.pbf),
        groups: Array.isArray(region.groups) ? region.groups.map(String) : [],
        bbox,
        ...regionRoutingMetadata(region),
        overrides: region.overrides || null,
        addressSources: addressSourcesForRegion(ADDRESS_SOURCES.sources, {
          id: String(region.id),
          groups: Array.isArray(region.groups) ? region.groups.map(String) : []
        })
      };
    })
    .filter(region => region.id && (!args.regions || args.regions.includes(region.id)));
  if (!regions.length) throw new Error("No regions selected.");
  return {
    regions,
    statsDriftRatio: Number(config.statsDriftRatio || 0.1),
    workerCount: Number(config.workerCount || 0),
    // Reducers share the compact hot-column preload. Keep a separate cap from
    // scan parallelism, but use enough workers to saturate continent reducers.
    partitionReducerWorkers: Math.max(1, Number(config.partitionReducerWorkers) || 8),
    // The production OSM schema has enough filter columns that a large shard's
    // code store can exceed Rangefind's conservative 1.5 GiB default. A shared
    // preload avoids hundreds of millions of tiny random reads and is allocated
    // once for the reducer pool, not once per worker.
    codeStoreWorkerPreloadMaxBytes: Number.isFinite(Number(config.codeStoreWorkerPreloadMaxBytes))
      ? Math.max(0, Number(config.codeStoreWorkerPreloadMaxBytes))
      : 3 * 1024 ** 3,
    acquisitionConcurrency: Math.max(1, Math.min(8, Number(config.acquisitionConcurrency || 1))),
    acquisitionPipelineWorkers: Math.max(1, Math.min(
      16,
      Number(config.acquisitionPipelineWorkers || (Number(config.acquisitionConcurrency || 1) + 2))
    )),
    largePbfBytes: Math.max(1, Number(config.largePbfBytes || 1024 ** 3)),
    roadIndexes: args.roadIndexes
      ? normalizeRoadIndexConfig(config.roadIndexes)
      : normalizeRoadIndexConfig(null),
    minFreeBytes: Math.max(1, Number(process.env.INDEX_MIN_FREE_GIB || config.minFreeGiB || 24)) * 1024 ** 3,
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

function regionOsmJsonl(region) {
  return join(regionWorkRoot(region), "data/osm-places.jsonl");
}

function regionOsmCorpusInput(region) {
  const plain = regionOsmJsonl(region);
  if (existsSync(plain)) return plain;
  const compressed = `${plain}.gz`;
  if (existsSync(compressed)) return compressed;
  return plain;
}

function regionEnrichedJsonl(region) {
  return join(regionWorkRoot(region), "data/osm-enriched-places.jsonl");
}

function regionJsonl(region) {
  if (Array.isArray(region.preparedAddressSources)) {
    return region.preparedAddressSources.length ? regionEnrichedJsonl(region) : regionOsmJsonl(region);
  }
  const enriched = regionEnrichedJsonl(region);
  if (existsSync(enriched) || existsSync(`${enriched}.gz`) || existsSync(`${enriched}.meta.json`)) return enriched;
  const raw = regionOsmJsonl(region);
  if (existsSync(raw) || existsSync(`${raw}.gz`)) return raw;
  return region.addressSources?.length ? enriched : raw;
}

function regionJsonlGz(region) {
  return `${regionJsonl(region)}.gz`;
}

function hasCorpus(region) {
  return existsSync(regionJsonl(region)) || existsSync(regionJsonlGz(region));
}

function regionCorpusInput(region) {
  const plain = regionJsonl(region);
  if (existsSync(plain)) return plain;
  const compressed = regionJsonlGz(region);
  if (existsSync(compressed)) return compressed;
  throw new Error(`${region.id}: no corpus (neither JSONL nor .gz) — needs refresh/extract first.`);
}

function roadProfileRoot(region, profile) {
  return join(regionWorkRoot(region), "roads", profile);
}

function roadSourcePath(region, profile) {
  return join(roadProfileRoot(region, profile), "graph.bin");
}

function roadIndexDir(region, profile) {
  return join(OUT, "routes", profile, region.id);
}

function roadIdentityRegion(region, state) {
  return region.pinned
    ? { ...region, pbfIdentity: pbfIdentity(region, state) }
    : region;
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
  const rawOsm = regionOsmJsonl(region);
  if (rawOsm !== regionJsonl(region) && hasCorpus(region)) {
    rmSync(rawOsm, { force: true });
    rmSync(`${rawOsm}.gz`, { force: true });
  }
}

function cleanupFailedAcquisition(region) {
  const dataDir = join(regionWorkRoot(region), "data");
  for (const path of [
    `${region.pbf}.download`,
    `${regionJsonl(region)}.partial`,
    `${regionJsonl(region)}.tmp`,
    `${regionJsonlGz(region)}.tmp`
  ]) rmSync(path, { force: true });
  if (existsSync(dataDir)) {
    for (const name of readdirSync(dataDir)) {
      if (name.endsWith(".partial")
          || name.endsWith(".download")
          || name.startsWith("address-enrichment.sqlite")) {
        rmSync(join(dataDir, name), { recursive: true, force: true });
      }
    }
  }
  const roadsDir = join(regionWorkRoot(region), "roads");
  if (existsSync(roadsDir)) {
    for (const profile of readdirSync(roadsDir)) {
      rmSync(join(roadsDir, profile, "graph.bin.partial"), { force: true });
      rmSync(`${roadIndexDir(region, profile)}.partial`, { recursive: true, force: true });
    }
  }
  cleanupExtractionScratch(region);
}

function hydrateStateFromLocalArtifacts(regions, state) {
  for (const region of regions) {
    const entry = state.regions[region.id] || (state.regions[region.id] = {});
    const manifest = loadJson(join(OUT, "shards", region.id, "manifest.json"), null);
    const osmMeta = loadJson(`${regionOsmJsonl(region)}.meta.json`, null);
    const enrichmentMeta = loadJson(`${regionJsonl(region)}.meta.json`, null);
    if (!entry.pbfLastModified && manifest?.meta?.data_version) {
      entry.pbfLastModified = String(manifest.meta.data_version);
    }
    if (!entry.pbfBytes && Number(osmMeta?.pbfBytes) > 0) {
      entry.pbfBytes = Number(osmMeta.pbfBytes);
    }
    if (!entry.docs) {
      entry.docs = Number(enrichmentMeta?.totalDocs ?? osmMeta?.docs ?? manifest?.total ?? 0);
    }
  }
}

function diskWorkingBytes(region, sourceBytes = 0) {
  return Math.max(
    region.addressSources?.length ? 16 * 1024 ** 3 : 8 * 1024 ** 3,
    Number(sourceBytes || 0) * 6
  );
}

function diskFreeBytes() {
  const disk = statfsSync(WORK);
  return Number(disk.bavail) * Number(disk.bsize);
}

function recoverCompletedEnrichedCorpus(region, state) {
  if (!region.preparedAddressSources?.length || !hasCorpus(region)) return false;
  const entry = state.regions[region.id] || (state.regions[region.id] = {});
  const identity = pbfIdentity(region, state);
  const enrichmentIdentity = region.enrichmentIdentity || "";
  const enriched = regionEnrichedJsonl(region);
  const meta = loadJson(`${enriched}.meta.json`, null);
  const osmMeta = loadJson(`${regionOsmJsonl(region)}.meta.json`, null);
  const pbf = existsSync(region.pbf) ? statSync(region.pbf) : null;
  const currentPbf = pbf
    ? Number(osmMeta?.pbfBytes) === pbf.size && Math.floor(Number(osmMeta?.pbfMtimeMs)) === Math.floor(pbf.mtimeMs)
    : Boolean(identity && Number(osmMeta?.pbfBytes) === Number(entry.pbfBytes));
  const currentSources = Array.isArray(meta?.sources)
    && regionAddressSourceIdentity(meta.sources) === enrichmentIdentity;
  const validOutput = existsSync(enriched)
    ? statSync(enriched).size === Number(meta?.bytes)
    : existsSync(`${enriched}.gz`);
  const rejectionReasons = [
    Number(meta?.schemaVersion) === 1 ? "" : "metadata schema",
    Number(meta?.totalDocs) > 0 ? "" : "document count",
    currentPbf ? "" : "PBF identity",
    currentSources ? "" : "address-source identity",
    validOutput ? "" : "output file"
  ].filter(Boolean);
  if (rejectionReasons.length) {
    log(`${region.id}: completed enriched corpus cannot be recovered (${rejectionReasons.join(", ")})`);
    return false;
  }
  entry.docs = Number(meta.totalDocs);
  entry.extractIdentity = identity;
  entry.extractSchema = OSM_EXTRACTION_SCHEMA_VERSION;
  entry.enrichmentIdentity = enrichmentIdentity;
  entry.additionalSources = additionalSourceMetadata(region.preparedAddressSources);
  entry.overrides = region.overrides || null;
  log(`${region.id}: recovered completed enriched corpus from durable metadata`);
  return true;
}

function extractedCorpusCurrent(region, state) {
  const entry = state.regions[region.id] || {};
  return entry.extractIdentity === pbfIdentity(region, state)
    && entry.extractSchema === OSM_EXTRACTION_SCHEMA_VERSION
    && (entry.enrichmentIdentity || "") === (region.enrichmentIdentity || "")
    && hasCorpus(region);
}

// Identity of a region's current upstream corpus — stable across
// gzip/gunzip cycles and PBF deletion, so cleanup never causes rebuilds.
function pbfIdentity(region, state) {
  if (!region.pinned) return state.regions[region.id]?.pbfLastModified || "";
  const stat = statSync(region.pbf);
  return `${stat.size}:${Math.floor(stat.mtimeMs)}`;
}

// --- step 1: refresh PBFs ----------------------------------------------------

const addressSourcePreparations = new Map();
const addressSourcePartitions = new Map();

async function prepareRegionAddressSources(region, regions, { reuseCached = false } = {}) {
  if (!region.addressSources?.length) {
    region.preparedAddressSources = [];
    region.enrichmentIdentity = "";
    return;
  }
  const prepared = await Promise.all(region.addressSources.map(source => {
    if (!addressSourcePreparations.has(source.id)) {
      const preparation = prepareAddressSource(source, {
        root: join(WORK, "address-sources"),
        fetchSource,
        timeoutMs: SOURCE_REQUEST_TIMEOUT_MS,
        reuseCached,
        log
      }).catch(error => {
        addressSourcePreparations.delete(source.id);
        throw error;
      });
      addressSourcePreparations.set(source.id, preparation);
    }
    return addressSourcePreparations.get(source.id);
  }));
  region.preparedAddressSources = (await Promise.all(prepared.map(async source => {
    if (source.partition?.mode !== "spatial") return source;
    if (typeof rangefindOsmNode.createDelimitedAddressSource !== "function"
        || typeof rangefindOsmNode.normalizeExternalAddressRecord !== "function") {
      throw new Error(`${region.id}: spatial address partitioning requires the current Rangefind address-enrichment API.`);
    }
    if (!addressSourcePartitions.has(source.id)) {
      const sourceRegions = regions.filter(candidate => (
        candidate.addressSources?.some(candidateSource => candidateSource.id === source.id)
      ));
      const adapter = typeof source.batches === "function"
        ? source
        : rangefindOsmNode.createDelimitedAddressSource(
          addressSourceAdapterOptions(source, region)
        );
      const partition = partitionAddressSourceSpatially(adapter, {
        root: join(WORK, "address-sources", source.id, "partitions"),
        regions: sourceRegions,
        normalizeRecord: rangefindOsmNode.normalizeExternalAddressRecord,
        log
      }).catch(error => {
        addressSourcePartitions.delete(source.id);
        throw error;
      });
      addressSourcePartitions.set(source.id, partition);
    }
    return spatialPartitionForRegion(source, await addressSourcePartitions.get(source.id), region);
  }))).filter(Boolean);
  region.enrichmentIdentity = regionAddressSourceIdentity(region.preparedAddressSources);
}

async function refreshPbf(region, state, { roadIndexes = null, requireRoadUpload = true } = {}) {
  if (region.pinned) {
    if (!existsSync(region.pbf)) throw new Error(`${region.id}: pinned PBF missing at ${region.pbf}`);
    return { bytes: statSync(region.pbf).size };
  }
  const url = `https://download.geofabrik.de/${region.geofabrik}-latest.osm.pbf`;
  const head = await fetchSource(
    url,
    { method: "HEAD" },
    { timeoutMs: SOURCE_REQUEST_TIMEOUT_MS }
  );
  if (!head.ok) throw new Error(`${region.id}: HEAD ${url} → ${head.status}`);
  const lastModified = head.headers.get("last-modified") || "";
  const bytes = Math.max(0, Number(head.headers.get("content-length") || 0));
  const entry = state.regions[region.id] || {};
  // The PBF gets deleted after a successful publish; re-download only when
  // upstream changed, or when extraction still needs it (stale/lost
  // extraction state) and the file is gone.
  const current = lastModified && lastModified === entry.pbfLastModified;
  const enrichmentCurrent = !region.addressSources?.length
    || (entry.enrichmentIdentity || "") === (region.enrichmentIdentity || "");
  const extractionCurrent = entry.extractIdentity === lastModified
    && entry.extractSchema === OSM_EXTRACTION_SCHEMA_VERSION
    && enrichmentCurrent
    && hasCorpus(region);
  const roadsCurrent = !roadIndexes?.enabled || roadIndexesCurrent({
    region,
    state,
    config: roadIndexes,
    rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION,
    requireUploaded: requireRoadUpload
  });
  if (current && (existsSync(region.pbf) || (extractionCurrent && roadsCurrent))) {
    return { bytes: bytes || entry.pbfBytes || 0 };
  }

  log(`${region.id}: downloading ${url} (${lastModified || "unknown date"})`);
  mkdirSync(dirname(region.pbf), { recursive: true });
  const tmp = `${region.pbf}.download`;
  const response = await fetchSource(
    url,
    {},
    { timeoutMs: PBF_DOWNLOAD_TIMEOUT_MS }
  );
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

async function extractOsmCorpus(region, state) {
  if (!existsSync(region.pbf)) {
    throw new Error(`${region.id}: corpus is stale but the PBF is missing (refresh failed?)`);
  }
  // The compressed snapshot stays: it is the corpus the built shard
  // reflects and the base the delta diff runs against. Cleanup replaces it
  // only after the shard is rebuilt/updated and uploaded.
  const priorOsmMeta = loadJson(`${regionOsmJsonl(region)}.meta.json`, null);
  const pbf = statSync(region.pbf);
  const reusableOsm = Number(priorOsmMeta?.schemaVersion) === OSM_EXTRACTION_SCHEMA_VERSION
    && existsSync(regionOsmCorpusInput(region))
    && Number(priorOsmMeta?.pbfBytes) === pbf.size
    && Math.floor(Number(priorOsmMeta?.pbfMtimeMs)) === Math.floor(pbf.mtimeMs);
  const osmMeta = reusableOsm ? priorOsmMeta : await runIpcWorker(
    OSM_EXTRACT_WORKER,
    [writeWorkerConfig(`osm-extract-${region.id}`, {
      region: region.id,
      pbf: region.pbf,
      root: regionWorkRoot(region),
      rqa: false
    })],
    Math.max(2048, Math.min(16384, Number(process.env.OSM_EXTRACT_HEAP_MB || 8192) || 8192)),
    `osm extract ${region.id}`
  );
  if (reusableOsm && regionOsmCorpusInput(region).endsWith(".gz")) {
    log(`${region.id}: reusing compressed OSM base corpus`);
  }
  if (Number(osmMeta.schemaVersion) !== OSM_EXTRACTION_SCHEMA_VERSION) {
    throw new Error(`${region.id}: Rangefind OSM extraction schema ${osmMeta.schemaVersion || "unknown"}; expected ${OSM_EXTRACTION_SCHEMA_VERSION}`);
  }
  return { meta: osmMeta, extracted: !reusableOsm };
}

async function enrichOsmCorpus(region, osmMeta) {
  if (!region.preparedAddressSources?.length) return { meta: osmMeta, enriched: false };
  if (typeof rangefindOsmNode.augmentOsmWithAddressSources !== "function"
      || typeof rangefindOsmNode.createDelimitedAddressSource !== "function"
      || typeof rangefindOsmNode.createJsonlAddressSource !== "function") {
    throw new Error(`${region.id}: configured address enrichment requires a Rangefind release with augmentOsmWithAddressSources().`);
  }
  const result = await runIpcWorker(
    ADDRESS_ENRICHMENT_WORKER,
    [writeWorkerConfig(`address-enrichment-${region.id}`, {
      region: region.id,
      regionConfig: {
        id: region.id,
        groups: region.groups,
        countryCodes: region.countryCodes,
        subdivisionCodes: region.subdivisionCodes
      },
      root: regionWorkRoot(region),
      osmPath: regionOsmCorpusInput(region),
      outputPath: regionJsonl(region),
      sources: region.preparedAddressSources,
      osmDocs: Number(osmMeta.docs || 0)
    })],
    Math.max(2048, Math.min(16384, Number(process.env.ADDRESS_ENRICHMENT_HEAP_MB || 8192) || 8192)),
    `address enrichment ${region.id}`
  );
  return { meta: result.meta, enriched: true };
}

function commitExtractedCorpus(region, state, meta) {
  const entry = state.regions[region.id] || (state.regions[region.id] = {});
  const identity = pbfIdentity(region, state);
  const enrichmentIdentity = region.enrichmentIdentity || "";
  entry.docs = Number(meta.totalDocs ?? meta.docs ?? 0);
  entry.extractIdentity = identity;
  entry.extractSchema = OSM_EXTRACTION_SCHEMA_VERSION;
  entry.enrichmentIdentity = enrichmentIdentity;
  entry.additionalSources = additionalSourceMetadata(region.preparedAddressSources || []);
  entry.overrides = region.overrides || null;
}

function addressEnrichmentWorkload(region) {
  let bytes = 0;
  let records = 0;
  for (const source of region.preparedAddressSources || []) {
    try { bytes += statSync(source.path).size; } catch { /* worker reports a useful missing-source error */ }
    records += Math.max(0, Number(source.identity?.records || 0));
  }
  return { bytes, records };
}

async function waitForRegionStages(stages) {
  const results = await Promise.allSettled(stages);
  const failure = results.find(result => result.status === "rejected");
  if (failure) throw failure.reason;
  return results.map(result => result.value);
}

// --- regional road indexes --------------------------------------------------

function runRoadWorker(config, budgetMs = Infinity) {
  const configPath = join(
    WORK,
    "configs",
    `roads-${config.region}-${config.profile}-${config.mode}.json`
  );
  mkdirSync(dirname(configPath), { recursive: true });
  writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`);
  const heapMb = Math.max(2048, Math.min(24576, Number(process.env.ROAD_INDEX_HEAP_MB || 16384) || 16384));
  return new Promise((resolveDone, rejectDone) => {
    const child = spawn(
      process.execPath,
      [`--max-old-space-size=${heapMb}`, ROAD_INDEX_WORKER, configPath],
      { stdio: "inherit" }
    );
    const timer = budgetMs < Infinity
      ? setTimeout(() => child.kill("SIGTERM"), Math.max(0, budgetMs))
      : null;
    child.on("error", rejectDone);
    child.on("exit", (code, signal) => {
      if (timer) clearTimeout(timer);
      if (code === 0) resolveDone();
      else rejectDone(new Error(`road-index ${config.mode} worker failed (${signal || `exit ${code}`})`));
    });
  });
}

function roadSourceHeader(path) {
  const handle = openSync(path, "r");
  const chunks = [];
  let total = 0;
  try {
    while (total < 16 * 1024 * 1024) {
      const chunk = Buffer.allocUnsafe(64 * 1024);
      const bytesRead = readSync(handle, chunk, 0, chunk.length, total);
      if (!bytesRead) break;
      chunks.push(chunk.subarray(0, bytesRead));
      total += bytesRead;
      const bytes = Buffer.concat(chunks);
      const newline = bytes.indexOf(0x0a);
      if (newline >= 0) return JSON.parse(bytes.subarray(0, newline).toString("utf8"));
    }
  } finally {
    closeSync(handle);
  }
  throw new Error(`Road graph source has no bounded JSON header: ${path}`);
}

async function uploadRoadIndex(region, profile, store) {
  const local = roadIndexDir(region, profile);
  const target = `routes/${profile}/${region.id}`;
  const publish = partitionPublishFiles(local);
  const content = publish.content.filter(file => file.relative !== "node-order.bin");
  const started = Date.now();
  const result = await store.putFiles(content, target);
  await store.putFiles(publish.dependencyManifests, target);
  await store.putFiles(publish.rootManifests, target);
  log(`${region.id}/${profile}: uploaded ${result.files.toLocaleString()} immutable road file(s), ${(result.bytes / 1024 / 1024).toFixed(1)} MiB in ${Math.round((Date.now() - started) / 1000)}s.`);
  return new Set([
    ...content,
    ...publish.dependencyManifests,
    ...publish.rootManifests
  ].map(file => `${target}/${file.relative}`));
}

async function pruneRoadIndex(region, profile, profileState, keep, store) {
  const target = `routes/${profile}/${region.id}`;
  const configuredDays = Number(process.env.ROAD_OBJECT_PRUNE_GRACE_DAYS || 7);
  const graceDays = Number.isFinite(configuredDays) ? Math.max(1, configuredDays) : 7;
  const planned = planRoadObjectPrune({
    objects: await store.listObjects(`${target}/`),
    keep,
    previous: profileState.pruneCandidates || {},
    now: new Date().toISOString(),
    graceMs: graceDays * 86400_000
  });
  await store.deleteObjects(planned.eligible);
  profileState.pruneCandidates = planned.candidates;
  if (planned.eligible.length) {
    log(`${region.id}/${profile}: pruned ${planned.eligible.length.toLocaleString()} road object(s) after ${graceDays}-day grace (${(planned.eligibleBytes / 1024 / 1024).toFixed(1)} MiB).`);
  }
  if (Object.keys(planned.candidates).length) {
    log(`${region.id}/${profile}: deferred ${Object.keys(planned.candidates).length.toLocaleString()} superseded road object(s) for reader-safe cleanup (${(planned.pendingBytes / 1024 / 1024).toFixed(1)} MiB).`);
  }
}

async function ensureRoadIndexes(region, state, options, store, args, remaining) {
  const config = options.roadIndexes;
  if (!config.enabled) return;
  const entry = state.regions[region.id] || (state.regions[region.id] = {});
  entry.roadIndexes ||= {};
  for (const profile of config.profiles) {
    const identity = roadProfileIdentity({
      region: roadIdentityRegion(region, state),
      state,
      config,
      profile,
      rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION
    });
    if (!identity) throw new Error(`${region.id}/${profile}: source identity is unavailable.`);
    const profileState = entry.roadIndexes[profile] || (entry.roadIndexes[profile] = {});
    const output = roadIndexDir(region, profile);
    let locallyCurrent = profileState.builtFingerprint === identity.fingerprint
      && existsSync(join(roadIndexDir(region, profile), "manifest.json"));
    const recoveredMarker = loadJson(join(output, "_build/identity.json"), null);
    if (!locallyCurrent && recoveredMarker?.fingerprint === identity.fingerprint) {
      const recoveredManifest = loadJson(join(output, "manifest.json"), null);
      if (recoveredManifest?.format === "rfroutegraph-v1" && recoveredManifest?.profile === profile) {
        profileState.builtFingerprint = identity.fingerprint;
        profileState.builtRangefindVersion = RANGEFIND_ROAD_BUILDER_VERSION;
        profileState.manifest = recoveredManifest;
        locallyCurrent = true;
        saveState(state);
        log(`${region.id}/${profile}: recovered completed route graph from its build identity.`);
      }
    }
    const remotelyCurrent = profileState.uploadedFingerprint === identity.fingerprint;
    if ((args.upload && remotelyCurrent) || (!args.upload && locallyCurrent)) continue;
    if (!existsSync(region.pbf)) {
      throw new Error(`${region.id}/${profile}: road index is stale but the PBF is missing.`);
    }
    if (remaining() < 5 * 60_000) throw new Error(`${region.id}/${profile}: deadline reserve reached before road build.`);

    const source = roadSourcePath(region, profile);
    const recoveredSource = loadJson(`${source}.identity.json`, null);
    if (profileState.sourceFingerprint !== identity.sourceFingerprint
        && recoveredSource?.fingerprint === identity.sourceFingerprint
        && existsSync(source)) {
      profileState.sourceFingerprint = identity.sourceFingerprint;
      saveState(state);
      log(`${region.id}/${profile}: recovered completed OSM road extraction from its source identity.`);
    }
    if (profileState.sourceFingerprint !== identity.sourceFingerprint || !existsSync(source)) {
      log(`${region.id}/${profile}: extracting OSM road graph.`);
      await runRoadWorker({
        mode: "extract",
        region: region.id,
        profile,
        pbf: region.pbf,
        source,
        turnCosts: config.turnCosts,
        portalRegions: region.federationNeighbors || [],
        sourceFingerprint: identity.sourceFingerprint,
        rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION
      }, remaining() - 2 * 60_000);
      profileState.sourceFingerprint = identity.sourceFingerprint;
      profileState.builtFingerprint = "";
      profileState.uploadedFingerprint = "";
      profileState.manifest = null;
      saveState(state);
    }
    const header = await roadSourceHeader(source);
    if (Number(header.nodes || 0) < 2 || Number(header.edges || 0) < 1) {
      profileState.builtFingerprint = identity.fingerprint;
      profileState.uploadedFingerprint = identity.fingerprint;
      profileState.manifest = null;
      profileState.unavailable = `No connected ${profile} road network in this extract.`;
      rmSync(source, { force: true });
      rmSync(`${source}.identity.json`, { force: true });
      saveState(state);
      log(`${region.id}/${profile}: no connected road network; catalog entry omitted.`);
      continue;
    }
    if (!locallyCurrent) {
      log(`${region.id}/${profile}: building ${identity.buildOptions.shards}-shard range-addressed route graph.`);
      await runRoadWorker({
        mode: "build",
        region: region.id,
        profile,
        source,
        output,
        fingerprint: identity.fingerprint,
        rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION,
        buildOptions: identity.buildOptions
      }, remaining() - 2 * 60_000);
      const manifest = loadJson(join(output, "manifest.json"), null);
      if (manifest?.format !== "rfroutegraph-v1" || manifest?.profile !== profile) {
        throw new Error(`${region.id}/${profile}: route graph manifest failed validation.`);
      }
      profileState.builtFingerprint = identity.fingerprint;
      profileState.builtRangefindVersion = RANGEFIND_ROAD_BUILDER_VERSION;
      profileState.manifest = manifest;
      profileState.unavailable = null;
      saveState(state);
    }
    if (args.upload) {
      const keep = await uploadRoadIndex(region, profile, store);
      profileState.uploadedFingerprint = identity.fingerprint;
      saveState(state);
      if (args.prune) {
        await pruneRoadIndex(region, profile, profileState, keep, store);
        saveState(state);
      }
      if (!args.keepArtifacts) {
        rmSync(output, { recursive: true, force: true });
        rmSync(source, { force: true });
        rmSync(`${source}.identity.json`, { force: true });
      }
    }
  }
}

async function publishRoadCatalog(regions, state, options, store, upload) {
  if (!options.roadIndexes.enabled) return;
  const catalog = buildRoadCatalog({
    regions,
    state,
    config: options.roadIndexes,
    requireUploaded: upload
  });
  const path = join(OUT, "routes", "catalog.json");
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(catalog, null, 2)}\n`);
  if (upload && (catalog.indexes.length || state.roadCatalogPublishedCount)) {
    await store.putFile(path, "routes/catalog.json");
    state.roadCatalogPublishedCount = catalog.indexes.length;
    state.roadCatalogPublishedAt = new Date().toISOString();
    saveState(state);
  }
  log(`Road catalog: ${catalog.indexes.length}/${regions.length * options.roadIndexes.profiles.length} regional profile index(es).`);
}

async function runRoadOnly({ regions, allRegions, state, options, store, args, remaining, outOfTime, updateProgress }) {
  if (!options.roadIndexes.enabled) throw new Error("--roads-only requires roadIndexes.enabled in regions.json.");
  let catalogPublishTail = Promise.resolve();
  const publishCatalog = () => {
    const publication = catalogPublishTail.then(() => (
      publishRoadCatalog(allRegions, state, options, store, args.upload)
    ));
    catalogPublishTail = publication.catch(() => {});
    return publication;
  };
  const current = region => roadIndexesCurrent({
    region: roadIdentityRegion(region, state),
    state,
    config: options.roadIndexes,
    rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION,
    requireUploaded: args.upload
  });
  let pending = regions.filter(region => !current(region));
  const total = pending.length;
  let completed = 0;
  // A resumed backfill may already have durable profiles that were uploaded
  // before its previous stop. Publish those immediately instead of waiting
  // for another region (or the whole planet) to finish.
  await publishCatalog();
  if (!pending.length) {
    log("Road-only: every selected regional profile is current.");
    return true;
  }

  const concurrency = Math.min(options.acquisitionConcurrency, pending.length);
  const withCapacity = createWeightedLimiter(concurrency);
  const diskAdmission = createDiskAdmissionController({
    minFreeBytes: options.minFreeBytes,
    freeBytes: diskFreeBytes,
    workingBytes: diskWorkingBytes,
    pollMs: Math.max(250, Number(process.env.INDEX_DISK_ADMISSION_POLL_MS || 2_000))
  });
  log(`Road-only: ${pending.length} region(s) need route graphs; ${concurrency} extraction lane(s).`);
  let lastFailures = [];
  let haltError = null;
  for (let attempt = 1; pending.length && attempt <= ACQUISITION_MAX_ATTEMPTS; attempt++) {
    if (outOfTime()) break;
    if (attempt > 1) {
      const waitMs = Math.min(5 * 60_000, ACQUISITION_RETRY_BASE_MS * (3 ** (attempt - 2)));
      log(`Road-only: retrying ${pending.length} region(s), attempt ${attempt}/${ACQUISITION_MAX_ATTEMPTS}, in ${(waitMs / 1000).toFixed(0)}s.`);
      await delay(waitMs);
    }
    let cursor = 0;
    const failures = [];
    const attempted = new Set();
    const work = async () => {
      while (!outOfTime() && !haltError) {
        const index = cursor++;
        if (index >= pending.length) return;
        const region = pending[index];
        attempted.add(region.id);
        updateProgress("road-indexing", region, completed, total, {
          profiles: options.roadIndexes.profiles
        });
        let diskLease = null;
        try {
          diskLease = await diskAdmission.acquire({
            region,
            shouldStop: outOfTime,
            onWait: pressure => log(
              `${region.id}: road-only waiting for disk admission (`
              + `${(pressure.availableBytes / 1024 ** 3).toFixed(1)} GiB free, `
              + `${(pressure.requiredBytes / 1024 ** 3).toFixed(1)} GiB required, `
              + `${pressure.leases} active cleanup reservation(s))`
            )
          });
          if (!diskLease) return;
          const source = await refreshPbf(region, state, {
            roadIndexes: options.roadIndexes,
            requireRoadUpload: args.upload
          });
          const requiredReservation = diskWorkingBytes(region, source.bytes);
          if (requiredReservation > diskLease.bytes) {
            const resized = await diskLease.resize(source.bytes, {
              shouldStop: outOfTime,
              onWait: pressure => log(
                `${region.id}: road-only waiting for extraction disk admission (`
                + `${(pressure.availableBytes / 1024 ** 3).toFixed(1)} GiB free, `
                + `${(pressure.requiredBytes / 1024 ** 3).toFixed(1)} GiB required, `
                + `${pressure.leases} active cleanup reservation(s))`
              )
            });
            if (!resized) return;
          }
          const large = source.bytes >= options.largePbfBytes;
          await withCapacity(large ? concurrency : 1, async () => {
            await ensureRoadIndexes(region, state, options, store, args, remaining);
            // Region workers run concurrently, but mutable catalog writes
            // must remain ordered so an older snapshot cannot overwrite a
            // newer one. Each publication re-reads current durable state.
            await publishCatalog();
          });
          if (!region.pinned) rmSync(region.pbf, { force: true });
          completed++;
          updateProgress("road-indexing", region, completed, total, {
            profiles: options.roadIndexes.profiles
          });
        } catch (error) {
          failures.push({ region, error });
          if (error instanceof DiskHeadroomError) {
            haltError ||= error;
            log(`Road-only paused for disk safety — ${error.message}`);
          } else {
            log(`${region.id}: road indexing failed — ${error.message} (attempt ${attempt}/${ACQUISITION_MAX_ATTEMPTS})`);
          }
        } finally {
          diskLease?.release();
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, () => work()));
    await publishCatalog();
    if (haltError) throw haltError;
    lastFailures = failures;
    pending = [
      ...failures.map(failure => failure.region),
      ...pending.filter(region => !attempted.has(region.id))
    ];
  }
  if (lastFailures.length && !outOfTime()) {
    throw new Error(
      `Road-only indexing incomplete after ${ACQUISITION_MAX_ATTEMPTS} attempt(s): `
      + `${lastFailures.length} region(s) failed; first was ${lastFailures[0].region.id}: ${lastFailures[0].error.message}`
    );
  }
  await publishCatalog();
  return regions.every(current);
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
  const templatePath = join(WORK, "configs/_stats-template.json");
  mkdirSync(dirname(templatePath), { recursive: true });
  writeFileSync(templatePath, JSON.stringify(shardConfig(regions[0], options, "", null, state)));
  const templateConfig = await readConfig(templatePath);
  await collectScoringStats({
    config: templateConfig,
    inputs: regions.map(region => ({ id: region.id, input: regionCorpusInput(region) })),
    outDir: STATS_DIR,
    log: line => log(line)
  });
}

// --- step 4: shard builds ----------------------------------------------------

function shardConfig(region, options, scoringStatsPath, input = null, state = null) {
  const workerCount = options.workerCount > 0 ? options.workerCount : Math.max(1, availableParallelism() - 1);
  const partitionReducerWorkers = Math.min(workerCount, options.partitionReducerWorkers);
  const entry = state?.regions?.[region.id] || {};
  return createOsmIndexConfig({
    workerCount,
    input: input || regionCorpusInput(region),
    output: join(OUT, "shards", region.id),
    buildProgressLogMs: 60000,
    additionalSources: entry.additionalSources || [],
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
      partitionReducerWorkers,
      codeStoreWorkerPreloadMaxBytes: options.codeStoreWorkerPreloadMaxBytes,
      ...(region.overrides || {}),
      ...(scoringStatsPath ? { scoringStats: scoringStatsPath } : {})
    }
  });
}

function statsFingerprint() {
  const stats = statSync(statsPath());
  return `${stats.size}:${Math.floor(stats.mtimeMs)}`;
}

// The logical corpus identity intentionally excludes the Rangefind package:
// root term/suggest routing remains reusable across encoding-only upgrades.
// Bump a separate routing schema when a library release changes analysis or
// authority semantics.
function shardContentFingerprint(region, state) {
  const entry = state.regions[region.id] || {};
  return buildContentFingerprint({
    entry,
    statsFingerprint: statsFingerprint(),
    overrides: region.overrides || null
  });
}

// A built shard also depends on the exact Rangefind builder. Without this
// identity an encoding fix can be incorrectly accepted as a no-op because
// the extracted OSM documents themselves did not change.
function shardFingerprint(region, state) {
  return buildShardFingerprint({
    rangefindVersion: RANGEFIND_VERSION,
    builderVersion: RANGEFIND_BUILDER_VERSION,
    contentFingerprint: shardContentFingerprint(region, state)
  });
}

function builtContentFingerprint(entry) {
  // Existing state predates this field and stored the content-only identity
  // directly in builtFingerprint. Preserve that value during migration so an
  // encoding-only rebuild does not regenerate planet-scale routing artifacts.
  return previouslyBuiltContentFingerprint(entry);
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
  const builtBuilderVersion = previouslyBuiltBuilderVersion(entry);
  if (builtBuilderVersion !== RANGEFIND_BUILDER_VERSION) {
    return full(`Rangefind builder changed (${builtBuilderVersion || "unknown"} -> ${RANGEFIND_BUILDER_VERSION})`);
  }
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
  const localManifest = loadJson(join(OUT, "manifest.min.json"), null);
  let remoteManifest = null;
  try {
    remoteManifest = JSON.parse(await store.getText("manifest.min.json"));
  } catch (error) {
    log(`Root manifest comparison unavailable (${error.message}) — routing artifacts will be uploaded.`);
  }
  for (const { prefix, manifestKey } of ROOT_ROUTING_ARTIFACTS) {
    const dir = join(OUT, prefix);
    if (!existsSync(dir)) continue;
    const files = listLocalFiles(dir);
    if (rootRoutingArtifactIsPublished(localManifest, remoteManifest, manifestKey)) {
      const bytes = files.reduce((sum, file) => sum + statSync(file.path).size, 0);
      log(`Root ${prefix}: unchanged — skipped ${files.length.toLocaleString()} file(s), ${(bytes / 1024 / 1024).toFixed(1)} MiB.`);
    } else {
      await store.putFiles(files, prefix);
    }
    if (args?.prune) {
      const keep = new Set(files.map(file => `${prefix}/${file.relative}`));
      appendStaleObjectPaths(
        staleByPrefix,
        await store.listObjects(`${prefix}/`),
        keep
      );
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
// Bump when a Rangefind release changes which document terms are emitted even
// though the logical OSM/PBF content fingerprint is unchanged. Schema-v2 OSM
// documents add named-road classes and richer searchable metadata.
const TEXT_ROUTING_SCHEMA_VERSION = 2;
// Bump whenever the root suggest artifact layout changes independently of
// shard content. The fingerprint must invalidate the checkpointed manifest
// block so a finalize-only run rebuilds routing from the existing sidecars.
const SUGGEST_ROUTING_SCHEMA_VERSION = 3;

function termSetPath(region) {
  return join(TERM_SETS_DIR, `${region.id.replaceAll("/", "-")}.terms.gz`);
}

function suggestSetPath(region) {
  return join(SUGGEST_SETS_DIR, `${region.id.replaceAll("/", "-")}.suggest.gz`);
}

function writeWorkerConfig(name, value) {
  const path = join(WORK, "configs", `${name}.json`);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
  return path;
}

function runIpcWorker(worker, args, heapMb, label = "worker") {
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
      else rejectDone(new Error(`${label} failed (${signal || `exit ${code}`})`));
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
    .update(`text-routing-schema:${TEXT_ROUTING_SCHEMA_VERSION}\n`)
    .update(JSON.stringify(built.map(region => [region.id, builtContentFingerprint(state.regions[region.id] || {})])))
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
    .update(`suggest-routing-schema:${SUGGEST_ROUTING_SCHEMA_VERSION}\n`)
    .update(JSON.stringify(built.map(region => [region.id, builtContentFingerprint(state.regions[region.id] || {})])))
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

function statusSnapshot(regions, state, roadConfig = { enabled: false, profiles: [] }) {
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

  const roadRows = roadConfig.enabled
    ? regions.flatMap(region => roadConfig.profiles.map(profile => ({
        region,
        profile,
        state: state.regions[region.id]?.roadIndexes?.[profile] || {}
      })))
    : [];
  const builtRoadIndexes = roadRows.filter(row => row.state.builtFingerprint && row.state.manifest).length;
  const uploadedRoadIndexes = roadRows.filter(row => (
    row.state.manifest
    && row.state.uploadedFingerprint === row.state.builtFingerprint
  )).length;
  const unavailableRoadIndexes = roadRows.filter(row => row.state.builtFingerprint && !row.state.manifest).length;

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
    roadIndexes: {
      enabled: roadConfig.enabled,
      profiles: roadConfig.profiles,
      total: roadRows.length,
      built: builtRoadIndexes,
      uploaded: uploadedRoadIndexes,
      unavailable: unavailableRoadIndexes,
      catalogEntries: Number(state.roadCatalogPublishedCount || 0),
      catalogPublishedAt: state.roadCatalogPublishedAt || null,
      nextPending: roadRows
        .filter(row => !row.state.builtFingerprint)
        .slice(0, 10)
        .map(row => `${row.region.id}/${row.profile}`)
    },
    endpoints: {
      manifest: "manifest.min.json",
      status: "status.json",
      ...(roadConfig.enabled ? { routeCatalog: "routes/catalog.json" } : {})
    }
  };
}

function writeStatusArtifacts(regions, state, roadConfig) {
  mkdirSync(OUT, { recursive: true });
  mkdirSync(dirname(join(OUT, DISCOVERY_PATH)), { recursive: true });
  writeFileSync(join(OUT, "index.html"), readFileSync(join(projectRoot, "public/index.html")));
  writeFileSync(join(OUT, "status.json"), `${JSON.stringify(statusSnapshot(regions, state, roadConfig), null, 2)}\n`);
  writeFileSync(join(OUT, DISCOVERY_PATH), `${JSON.stringify(buildDiscoveryDocument(roadConfig), null, 2)}\n`);
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
      const names = includePage ? ["index.html", "status.json", DISCOVERY_PATH] : ["status.json"];
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

function publishStatusArtifacts(regions, state, store, upload, includePage = false, roadConfig = null) {
  try {
    writeStatusArtifacts(regions, state, roadConfig || { enabled: false, profiles: [] });
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
  const keep = new Set([`${basename(regionJsonl(region))}.gz`, "osm-places.meta.json"]);
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

function printStatus(regions, state, roadConfig = { enabled: false, profiles: [] }) {
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
  if (roadConfig.enabled) {
    const rows = regions.flatMap(region => roadConfig.profiles.map(profile => (
      state.regions[region.id]?.roadIndexes?.[profile] || {}
    )));
    const built = rows.filter(entry => entry.builtFingerprint && entry.manifest).length;
    const uploaded = rows.filter(entry => entry.manifest && entry.uploadedFingerprint === entry.builtFingerprint).length;
    const unavailable = rows.filter(entry => entry.builtFingerprint && !entry.manifest).length;
    console.log(`roads            ${built}/${rows.length} built  ${uploaded}/${rows.length} uploaded  ${unavailable} unavailable`);
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
  const federationNeighbors = roadFederationNeighbors(allRegions);
  for (const region of [...allRegions, ...regions]) {
    region.federationNeighbors = federationNeighbors.get(region.id) || [];
  }
  const options = loaded;
  mkdirSync(WORK, { recursive: true });
  const state = loadState();
  state.regions = state.regions || {};
  hydrateStateFromLocalArtifacts(allRegions, state);

  if (args.status) {
    printStatus(regions, state, options.roadIndexes);
    return;
  }

  const store = args.upload ? createR2Store() : null;
  acquireLock();
  let acquisitionSession = state.acquisitionSession || null;
  let resumedAcquisition = false;
  if (!args.finalizeOnly && !args.roadsOnly) {
    const signature = acquisitionSessionSignature({
      extractionSchema: OSM_EXTRACTION_SCHEMA_VERSION,
      rangefindBuilder: RANGEFIND_BUILDER_VERSION,
      regions: regions.map(region => ({
        id: region.id,
        geofabrik: region.geofabrik,
        pinned: region.pinned,
        bbox: region.bbox,
        addressSources: region.addressSources
      })),
      addressSources: ADDRESS_SOURCES.sources
    });
    const opened = openAcquisitionSession(
      state.acquisitionSession,
      signature,
      regions.map(region => region.id)
    );
    acquisitionSession = opened.session;
    resumedAcquisition = opened.resumed;
    state.acquisitionSession = acquisitionSession;
  }
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
    error: null,
    resumedAcquisition
  };
  saveState(state);
  if (resumedAcquisition) {
    log(`Resuming acquisition cycle: ${acquisitionSession.completedRegionIds.length}/${regions.length} region(s) already complete; cached address-source snapshot frozen.`);
  }
  publishStatusArtifacts(allRegions, state, store, args.upload, true, options.roadIndexes);
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
    publishStatusArtifacts(allRegions, state, store, args.upload, false, options.roadIndexes);
  };
  const configuredUploadQueueDepth = Number(process.env.R2_UPLOAD_QUEUE_DEPTH || 2);
  const uploadQueueDepth = Number.isInteger(configuredUploadQueueDepth) && configuredUploadQueueDepth > 0
    ? Math.min(configuredUploadQueueDepth, 8)
    : 2;
  const configuredUploadLanes = Number(process.env.R2_UPLOAD_LANES || 2);
  const uploadLanes = Number.isInteger(configuredUploadLanes) && configuredUploadLanes > 0
    ? Math.min(configuredUploadLanes, uploadQueueDepth)
    : Math.min(2, uploadQueueDepth);
  const uploadQueue = args.upload && !args.roadsOnly
    ? createTaskQueue({ maxPending: uploadQueueDepth, concurrency: uploadLanes })
    : null;
  const queueShardUpload = async region => {
    await uploadQueue.enqueue(async () => {
      log(`${region.id}: background upload started.`);
      try {
        await uploadAndCleanupShard(region, state, store, args);
        publishStatusArtifacts(allRegions, state, store, true, false, options.roadIndexes);
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
  let pipelineComplete = false;

  try {

  if (args.roadsOnly) {
    pipelineComplete = await runRoadOnly({
      regions,
      allRegions,
      state,
      options,
      store,
      args,
      remaining,
      outOfTime,
      updateProgress
    });
    log("Road-only run finished.");
    return;
  }

  // 1 + 2: refresh and extract selected regions. Once every corpus exists,
  // the initial build takes priority until every selected shard has been
  // built (and, for uploaded runs, included in the published root). Without
  // this gate, daily upstream changes can consume every nightly window and
  // starve the initial build forever.
  const buildFirst = !args.forceStats
    && !args.regions
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
    // Downloads, isolated place extraction, road builds, and uploads flow
    // independently. Weighted lanes keep large stages from colliding while
    // leaving capacity for useful work from another region.
    const acquisitionConcurrency = Math.min(options.acquisitionConcurrency, regions.length);
    const acquisitionWorkers = Math.min(
      Math.max(acquisitionConcurrency, options.acquisitionPipelineWorkers),
      regions.length
    );
    const withDownloadCapacity = createWeightedLimiter(acquisitionConcurrency);
    const minPipelineAvailableBytes = Math.max(
      1,
      Number(process.env.INDEX_PIPELINE_MIN_AVAILABLE_GIB || 6)
    ) * 1024 ** 3;
    const maxPipelineMemoryPsi = Math.max(
      0,
      Number(process.env.INDEX_PIPELINE_MAX_MEMORY_PSI || 2)
    );
    const stageLimiter = createAdaptiveStageLimiter({
      capacity: acquisitionConcurrency,
      beforeStart: ({ region, stage }) => waitForSystemHeadroom({
        sample: () => systemHeadroom({
          minAvailableBytes: minPipelineAvailableBytes,
          maxMemoryPsiAvg10: maxPipelineMemoryPsi
        }),
        onPressure: pressure => log(
          `${region}/${stage}: waiting for system headroom (`
          + `${pressure.availableBytes == null ? "unknown" : `${(pressure.availableBytes / 1024 ** 3).toFixed(1)} GiB available`}, `
          + `memory PSI ${pressure.memoryPsiAvg10 ?? "unknown"})`
        )
      }),
      onChange: event => log(
        `Pipeline ${event.meta.region}/${event.meta.stage}: ${event.type} `
        + `(weight ${event.weight}, lanes ${event.used}/${event.capacity}, pending ${event.pending}`
        + `${event.type === "start" ? `, queued ${event.queuedMs}ms` : `, elapsed ${(event.elapsedMs / 1000).toFixed(1)}s`})`
      )
    });
    const activeRegions = new Set();
    let acquisitionHalted = false;
    let acquisitionHaltReason = null;
    const diskAdmission = createDiskAdmissionController({
      minFreeBytes: options.minFreeBytes,
      freeBytes: diskFreeBytes,
      workingBytes: diskWorkingBytes,
      pollMs: Math.max(250, Number(process.env.INDEX_DISK_ADMISSION_POLL_MS || 2_000))
    });
    let acquisitionCatalogPublishTail = Promise.resolve();
    const publishAcquisitionCatalog = () => {
      const publication = acquisitionCatalogPublishTail.then(() => (
        publishRoadCatalog(allRegions, state, options, store, args.upload)
      ));
      acquisitionCatalogPublishTail = publication.catch(() => {});
      return publication;
    };
    const completedRegionIds = new Set(acquisitionSession.completedRegionIds.filter(id => {
      const region = regions.find(candidate => candidate.id === id);
      return region && hasCorpus(region);
    }));
    if (completedRegionIds.size !== acquisitionSession.completedRegionIds.length) {
      log(`Resume validation: ${acquisitionSession.completedRegionIds.length - completedRegionIds.size} completed region(s) lost their local corpus and will be reacquired.`);
      acquisitionSession.completedRegionIds = [...completedRegionIds];
      saveState(state);
    }
    const reportAcquisition = region => updateProgress(
      "acquiring",
      region,
      completedRegionIds.size,
      regions.length,
      {
        regions: [...activeRegions],
        failed: Object.keys(acquisitionSession.failures).length
      }
    );
    log(
      `Adaptive acquisition pipeline: ${acquisitionWorkers} region worker(s), `
      + `${acquisitionConcurrency} weighted stage lane(s); large PBF place/road weights `
      + `${pipelineStageWeight({ stage: "places", sourceBytes: options.largePbfBytes, largePbfBytes: options.largePbfBytes, capacity: acquisitionConcurrency })}/`
      + `${pipelineStageWeight({ stage: "roads", sourceBytes: options.largePbfBytes, largePbfBytes: options.largePbfBytes, capacity: acquisitionConcurrency })}.`
    );
    let pending = regions.filter(region => !completedRegionIds.has(region.id));
    let finalFailures = [];
    for (let attempt = 1; pending.length && attempt <= ACQUISITION_MAX_ATTEMPTS; attempt++) {
      if (outOfTime() || acquisitionHalted) break;
      if (attempt > 1) {
        const waitMs = Math.min(5 * 60_000, ACQUISITION_RETRY_BASE_MS * (3 ** (attempt - 2)));
        log(`Retrying ${pending.length} failed acquisition region(s), attempt ${attempt}/${ACQUISITION_MAX_ATTEMPTS}, in ${(waitMs / 1000).toFixed(0)}s: ${pending.map(region => region.id).join(", ")}`);
        await delay(waitMs);
      }
      let acquisitionCursor = 0;
      const failures = [];
      const acquireRegions = async () => {
        while (!outOfTime() && !acquisitionHalted) {
          const regionIndex = acquisitionCursor++;
          if (regionIndex >= pending.length) return;
          const region = pending[regionIndex];
          activeRegions.add(region.id);
          reportAcquisition(region);
          let diskLease = null;
          try {
            diskLease = await diskAdmission.acquire({
              region,
              shouldStop: () => outOfTime() || acquisitionHalted,
              onWait: pressure => log(
                `${region.id}: waiting for download disk admission (`
                + `${(pressure.availableBytes / 1024 ** 3).toFixed(1)} GiB free, `
                + `${(pressure.requiredBytes / 1024 ** 3).toFixed(1)} GiB required, `
                + `${pressure.leases} active cleanup reservation(s))`
              )
            });
            if (!diskLease) return;
            // Global spatial partitions are keyed by the complete production
            // region topology even during a --regions repair run. Using the
            // selected subset here would invalidate and rebuild the planet
            // partition for a one-shard operation.
            await prepareRegionAddressSources(region, allRegions, { reuseCached: resumedAcquisition });
            const source = await withDownloadCapacity(1, () => refreshPbf(region, state, {
              roadIndexes: options.roadIndexes,
              requireRoadUpload: args.upload
            }));
            const recovered = extractedCorpusCurrent(region, state)
              || recoverCompletedEnrichedCorpus(region, state);
            const large = source.bytes >= options.largePbfBytes;
            if (large) {
              log(`${region.id}: large PBF (${(source.bytes / 1024 / 1024 / 1024).toFixed(1)} GiB) — using weighted pipeline capacity`);
            }
            const requiredReservation = diskWorkingBytes(region, source.bytes);
            if (requiredReservation > diskLease.bytes) {
              const resized = await diskLease.resize(source.bytes, {
                shouldStop: () => outOfTime() || acquisitionHalted,
                onWait: pressure => log(
                  `${region.id}: waiting for extraction disk admission (`
                  + `${(pressure.availableBytes / 1024 ** 3).toFixed(1)} GiB free, `
                  + `${(pressure.requiredBytes / 1024 ** 3).toFixed(1)} GiB required, `
                  + `${pressure.leases} active cleanup reservation(s))`
                )
              });
              if (!resized) return;
            }
            let extracted = recovered;
            if (!recovered) {
              const osm = await stageLimiter.run(
                pipelineStageWeight({
                  stage: "places",
                  sourceBytes: source.bytes,
                  largePbfBytes: options.largePbfBytes,
                  capacity: acquisitionConcurrency
                }),
                () => extractOsmCorpus(region, state),
                { region: region.id, stage: "places" }
              );
              const workload = addressEnrichmentWorkload(region);
              const enrichment = region.preparedAddressSources?.length
                ? stageLimiter.run(
                  pipelineStageWeight({
                    stage: "enrichment",
                    sourceBytes: source.bytes,
                    largePbfBytes: options.largePbfBytes,
                    capacity: acquisitionConcurrency,
                    addressBytes: workload.bytes,
                    addressRecords: workload.records
                  }),
                  () => enrichOsmCorpus(region, osm.meta),
                  { region: region.id, stage: "enrichment" }
                )
                : Promise.resolve({ meta: osm.meta, enriched: false });
              const roads = stageLimiter.run(
                pipelineStageWeight({
                  stage: "roads",
                  sourceBytes: source.bytes,
                  largePbfBytes: options.largePbfBytes,
                  capacity: acquisitionConcurrency
                }),
                () => ensureRoadIndexes(region, state, options, store, args, remaining),
                { region: region.id, stage: "roads" }
              );
              const [enriched] = await waitForRegionStages([enrichment, roads]);
              commitExtractedCorpus(region, state, enriched.meta);
              extracted = osm.extracted || enriched.enriched;
            } else {
              await stageLimiter.run(
                pipelineStageWeight({
                  stage: "roads",
                  sourceBytes: source.bytes,
                  largePbfBytes: options.largePbfBytes,
                  capacity: acquisitionConcurrency
                }),
                () => ensureRoadIndexes(region, state, options, store, args, remaining),
                { region: region.id, stage: "roads" }
              );
            }
            // Region workers finish out of order. Serialize mutable catalog
            // flips and rebuild each snapshot from current durable state so
            // every completed graph becomes live without an older snapshot
            // racing over a newer one.
            await publishAcquisitionCatalog();
            if (extracted || recovered) {
              log(`${region.id}: corpus refreshed (${(state.regions[region.id].docs || 0).toLocaleString()} docs)`);
              const builtBuilderVersion = previouslyBuiltBuilderVersion(state.regions[region.id]);
              if (!state.regions[region.id].builtFingerprint
                  || args.forceStats
                  || builtBuilderVersion !== RANGEFIND_BUILDER_VERSION) {
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
            completedRegionIds.add(region.id);
            recordAcquisitionSuccess(acquisitionSession, region.id);
            saveState(state);
          } catch (error) {
            cleanupFailedAcquisition(region);
            if (error instanceof DiskHeadroomError) {
              acquisitionHalted = true;
              acquisitionHaltReason ||= error;
              log(`Acquisition paused for disk safety — ${error.message}`);
            } else {
              failures.push({ region, error });
              recordAcquisitionFailure(acquisitionSession, region.id, error, attempt);
              saveState(state);
              log(`${region.id}: refresh/extract failed — ${error.message} (attempt ${attempt}/${ACQUISITION_MAX_ATTEMPTS})`);
            }
          } finally {
            // Reservations cover compression, source deletion, and scratch
            // cleanup—not just extraction—so a waiting region cannot enter
            // during the temporary disk peak between those phases.
            diskLease?.release();
            activeRegions.delete(region.id);
            reportAcquisition(region);
          }
        }
      };
      await Promise.all(Array.from({ length: acquisitionWorkers }, () => acquireRegions()));
      await publishAcquisitionCatalog();
      finalFailures = failures;
      pending = failures.map(failure => failure.region);
    }
    reportAcquisition(null);
    if (acquisitionHaltReason) throw acquisitionHaltReason;
    if (completedRegionIds.size === regions.length) {
      acquisitionSession.completedAt = new Date().toISOString();
      acquisitionSession.failures = {};
      saveState(state);
    }
    if (args.forceStats && finalFailures.length && !outOfTime()) {
      const first = finalFailures[0];
      throw new Error(
        `Forced acquisition incomplete after ${ACQUISITION_MAX_ATTEMPTS} attempt(s): ${finalFailures.length} region(s) failed; `
        + `first was ${first.region.id}: ${first.error.message}`
      );
    }
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
  } else if (shouldReuseFrozenStats({
    regionScoped: Boolean(args.regions),
    partial: args.partial
  })) {
    if (!existsSync(statsPath())) {
      throw new Error("Region-scoped builds require the existing planet scoring-stats artifact.");
    }
    log("Region-scoped build: reusing frozen planet scoring stats.");
  } else {
    const forceStatsNow = args.forceStats && !acquisitionSession?.forcedStatsCompleted;
    await ensureScoringStats(ready, options, state, forceStatsNow, !args.regions || args.partial);
    if (forceStatsNow) {
      acquisitionSession.forcedStatsCompleted = true;
      acquisitionSession.forcedStatsCompletedAt = new Date().toISOString();
      saveState(state);
    }
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
      entry.builtContentFingerprint = shardContentFingerprint(region, state);
      entry.builtRangefindVersion = RANGEFIND_VERSION;
      entry.builtRangefindBuilderVersion = RANGEFIND_BUILDER_VERSION;
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
      entry.builtContentFingerprint = shardContentFingerprint(region, state);
      entry.builtRangefindVersion = RANGEFIND_VERSION;
      entry.builtRangefindBuilderVersion = RANGEFIND_BUILDER_VERSION;
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
  // A region-scoped production rebuild must never replace the planet root
  // with only the selected shard. Keep every existing built shard in the
  // publication set; --partial remains the explicit isolated bring-up mode.
  const rootCandidates = selectRootCandidates({
    selected: ready,
    all: allRegions,
    regionScoped: Boolean(args.regions),
    partial: args.partial
  });
  const built = rootCandidates.filter(region =>
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
      // Geofabrik geometry is the routing coverage. Document-derived extrema
      // can contain bad OSM coordinates and make an otherwise regional shard
      // look transcontinental. Custom/pinned regions without configured
      // coverage retain the legacy stats fallback.
      bbox: region.bbox || stats.inputs.find(input => input.id === region.id)?.bbox || null,
      groups: region.groups
    })),
    scoringStats: stats,
    textRouting,
    suggestRouting,
    extra: {
      ...(categoryLexicon ? { category_lexicon: categoryLexicon } : {}),
      endpoints: rootDiscoveryEndpoints(options.roadIndexes),
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
    const allUploaded = built.length === rootCandidates.length && built.every(region => {
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
      pipelineComplete = true;
    } else {
      log("Some shards not uploaded yet; root manifest NOT updated remotely (stays consistent).");
    }
  } else {
    pipelineComplete = ready.every(region => {
      try {
        return shardFingerprint(region, state) === state.regions[region.id]?.builtFingerprint;
      } catch {
        return false;
      }
    });
  }
  await publishRoadCatalog(allRegions, state, options, store, args.upload);
  const roadPipelineComplete = regions.every(region => roadIndexesCurrent({
    region: roadIdentityRegion(region, state),
    state,
    config: options.roadIndexes,
    rangefindVersion: RANGEFIND_ROAD_BUILDER_VERSION,
    requireUploaded: args.upload
  }));
  pipelineComplete = pipelineComplete && roadPipelineComplete;
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
    if (!runError && pipelineComplete && !args.roadsOnly) delete state.acquisitionSession;
    saveState(state);
    publishStatusArtifacts(allRegions, state, store, args.upload, false, options.roadIndexes);
    if (args.upload) await flushStatusUploads(store);
    store?.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
