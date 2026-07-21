#!/usr/bin/env node

// Refresh the published root manifest with the category lexicon artifact —
// no shard rebuilds and no pipeline cycle.
//
// Dry runs read the public manifests and only stage output. Uploads hold the
// same process lock as the indexer, read the authoritative R2 objects, and
// replace both roots with conditional If-Match writes. A changed root aborts
// the refresh; a failure after the first write rolls that write back.
//
//   node scripts/refresh_root_lexicon.mjs
//   node scripts/refresh_root_lexicon.mjs --upload
//   node scripts/refresh_root_lexicon.mjs --base-url https://.../ --regions quebec,ontario

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DEFAULT_PUBLIC_BASE_URL,
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "./lib/category_lexicon.mjs";
import { acquireProcessLock } from "./lib/process_lock.mjs";
import { createR2Store } from "./lib/r2_store.mjs";

const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const STAGE = join(projectRoot, "work", "root-refresh");
const LOCK_PATH = join(projectRoot, "work", ".lock");
const ROOT_NAMES = ["manifest.json", "manifest.min.json"];

function requiredValue(argv, index, flag) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new Error(`${flag} requires a value.`);
  return value;
}

export function parseArgs(argv) {
  const args = { baseUrl: DEFAULT_PUBLIC_BASE_URL, upload: false, regions: null };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--base-url") args.baseUrl = requiredValue(argv, i++, arg);
    else if (arg === "--upload") args.upload = true;
    else if (arg === "--regions") {
      args.regions = requiredValue(argv, i++, arg).split(",").map(value => value.trim()).filter(Boolean);
      if (!args.regions.length) throw new Error("--regions requires at least one region id.");
      if (new Set(args.regions).size !== args.regions.length) throw new Error("--regions contains duplicate ids.");
    } else throw new Error(`Unknown argument: ${arg}`);
  }
  const parsedBase = new URL(args.baseUrl);
  if (!/^https?:$/u.test(parsedBase.protocol)) throw new Error("--base-url must use http or https.");
  if (!args.baseUrl.endsWith("/")) args.baseUrl += "/";
  if (args.upload && args.regions) throw new Error("--upload with --regions would publish a partial vocabulary; run without --regions.");
  return args;
}

function log(line) {
  console.log(`Refresh: ${line}`);
}

function parseSnapshot(name, snapshot) {
  if (!snapshot?.text) throw new Error(`${name} is empty.`);
  let manifest;
  try {
    manifest = JSON.parse(snapshot.text);
  } catch (error) {
    throw new Error(`${name} is not valid JSON: ${error.message}`);
  }
  if (!Array.isArray(manifest.shards) || !manifest.shards.length) throw new Error(`${name} has no shards.`);
  return { ...snapshot, name, manifest };
}

function validateRootPair(snapshots) {
  const full = snapshots["manifest.json"];
  const min = snapshots["manifest.min.json"];
  for (const snapshot of [full, min]) {
    if (!snapshot) throw new Error("Both root manifests are required.");
  }
  for (const field of ["built_at", "total"]) {
    if (full.manifest[field] !== min.manifest[field]) throw new Error(`Root manifests disagree on ${field}.`);
  }
  if (JSON.stringify(full.manifest.shards) !== JSON.stringify(min.manifest.shards)) {
    throw new Error("Root manifests disagree on the published shard set.");
  }
}

async function loadPublicSnapshots(baseUrl, fetchImpl = fetch) {
  const snapshots = {};
  for (const name of ROOT_NAMES) {
    const response = await fetchImpl(new URL(name, baseUrl));
    if (!response.ok) throw new Error(`${name} fetch failed: ${response.status}`);
    snapshots[name] = parseSnapshot(name, {
      text: await response.text(),
      etag: response.headers.get("etag")
    });
  }
  validateRootPair(snapshots);
  return snapshots;
}

async function loadR2Snapshots(store) {
  const snapshots = {};
  for (const name of ROOT_NAMES) {
    const snapshot = await store.getTextWithMetadata(name);
    if (!snapshot.etag) throw new Error(`${name} has no R2 ETag; conditional publication is unavailable.`);
    snapshots[name] = parseSnapshot(name, snapshot);
  }
  validateRootPair(snapshots);
  return snapshots;
}

function snapshotChanged(before, after) {
  return before.etag !== after.etag || before.text !== after.text;
}

export function buildRootPayloads(snapshots, artifact) {
  return Object.fromEntries(ROOT_NAMES.map(name => {
    // Preserve built_at and every other root field: refreshing an embedded
    // vocabulary is not an index rebuild.
    const updated = { ...snapshots[name].manifest, category_lexicon: artifact };
    return [name, JSON.stringify(updated)];
  }));
}

async function rollbackWrites(store, baseline, payloads, names) {
  const failures = [];
  for (const name of [...names].reverse()) {
    try {
      const current = await store.getTextWithMetadata(name);
      if (current.text === baseline[name].text) continue;
      if (current.text !== payloads[name]) {
        failures.push(`${name} changed again before rollback`);
        continue;
      }
      if (!current.etag) throw new Error("missing current ETag");
      await store.putBytes(name, baseline[name].text, { ifMatch: current.etag });
    } catch (error) {
      failures.push(`${name}: ${error.message}`);
    }
  }
  return failures;
}

// Compare-and-swap both stable roots. The process lock prevents the local
// indexer from entering its publish path; ETags also protect against any
// other host or operator publishing during the refresh.
export async function publishRootManifests({ store, baseline, payloads }) {
  const current = await loadR2Snapshots(store);
  for (const name of ROOT_NAMES) {
    if (snapshotChanged(baseline[name], current[name])) {
      throw new Error(`${name} changed while the lexicon was being built; refusing to overwrite a newer root.`);
    }
  }

  const written = [];
  let inFlight = null;
  try {
    for (const name of ROOT_NAMES) {
      inFlight = name;
      await store.putBytes(name, payloads[name], { ifMatch: baseline[name].etag });
      written.push(name);
      inFlight = null;
    }
    const verified = await loadR2Snapshots(store);
    for (const name of ROOT_NAMES) {
      if (verified[name].text !== payloads[name]) throw new Error(`${name} verification mismatch after upload.`);
    }
  } catch (error) {
    // A network failure can arrive after R2 committed the conditional write
    // but before the response reached us. Inspect that in-flight object and
    // roll it back when it contains our exact payload. A 412 guarantees the
    // conditional write did not happen, so only acknowledged predecessors
    // belong in the rollback set.
    const ambiguous = inFlight && Number(error?.$metadata?.httpStatusCode || 0) !== 412 ? [inFlight] : [];
    const rollbackFailures = await rollbackWrites(store, baseline, payloads, [...written, ...ambiguous]);
    const suffix = rollbackFailures.length ? ` Rollback also failed: ${rollbackFailures.join("; ")}.` : " Prior writes were rolled back.";
    throw new Error(`Root lexicon publication failed: ${error.message}.${suffix}`);
  }
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const releaseLock = args.upload
    ? acquireProcessLock(LOCK_PATH, { label: "Another index or root-refresh run", log })
    : null;
  let store = null;
  try {
    store = args.upload ? createR2Store() : null;
    const lexiconModule = await loadCategoryLexiconModule();
    if (!lexiconModule) throw new Error("No installed rangefind exports buildCategoryLexiconArtifact.");

    const snapshots = store
      ? await loadR2Snapshots(store)
      : await loadPublicSnapshots(args.baseUrl);
    const root = snapshots["manifest.json"].manifest;
    log(`Root: ${root.shards.length} shard(s), ${Number(root.total || 0).toLocaleString()} docs, built ${root.built_at}.`);

    const selected = args.regions
      ? root.shards.filter(shard => args.regions.includes(shard.id))
      : root.shards;
    if (args.regions && selected.length !== args.regions.length) throw new Error("Some --regions are not in the root manifest.");
    const merged = await mergeShardTypeVocabulary({
      shards: selected.map(shard => ({
        id: shard.id,
        expectedTotal: Number(shard.total),
        remoteBase: args.baseUrl
      })),
      cachePath: join(projectRoot, "work", "category-lexicon-cache.json"),
      remoteBase: args.baseUrl,
      log
    });
    if (!merged?.length) throw new Error("No type facet values were collected; refusing to publish an empty lexicon.");

    const artifact = lexiconModule.buildCategoryLexiconArtifact(merged);
    if (!artifact?.types?.length) throw new Error("Rangefind produced an empty category lexicon; refusing to publish it.");
    log(`Artifact: ${artifact.types.length} types, ${Object.keys(artifact.aliases || {}).length} aliases.`);

    mkdirSync(STAGE, { recursive: true });
    if (args.regions) {
      const path = join(STAGE, "category_lexicon.partial.json");
      writeFileSync(path, JSON.stringify(artifact));
      log(`Partial test artifact staged in ${path}; no root manifest was produced.`);
      return;
    }

    const payloads = buildRootPayloads(snapshots, artifact);
    for (const name of ROOT_NAMES) writeFileSync(join(STAGE, name), payloads[name]);
    log(`Staged root manifests in ${STAGE}.`);

    if (!store) {
      log("Dry run — rerun with --upload (and R2_* env) to flip the root manifests.");
      return;
    }
    await publishRootManifests({ store, baseline: snapshots, payloads });
    log("Conditionally uploaded and verified both root manifests.");
  } finally {
    store?.close();
    releaseLock?.();
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(`Refresh: ${error.message}`);
    process.exitCode = 1;
  });
}
