// Refresh the published root manifest with the category lexicon artifact —
// no shard rebuilds, no pipeline cycle.
//
// Reads the production root manifest, merges every shard's `type` facet
// dictionary (a few small range reads per shard, disk-cached by the node
// runtime and by work/category-lexicon-cache.json), joins the alias table,
// and stages manifest.json / manifest.min.json with the `category_lexicon`
// block added. Every other root field is preserved verbatim.
//
//   npm exec node scripts/refresh_root_lexicon.mjs            # dry run: stage only
//   node scripts/refresh_root_lexicon.mjs --upload            # also flip both root manifests in R2
//   node scripts/refresh_root_lexicon.mjs --base-url https://.../ --regions quebec,ontario
//
// Note: the nightly pipeline regenerates the root manifest on every publish.
// Until it runs a rangefind release that carries the lexicon builder (see
// update_index.mjs feature detection), a nightly publish will drop the block
// again — harmless, since the browser bundle falls back to its bundled
// vocabulary, but re-run this script or update the dependency to restore it.

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DEFAULT_PUBLIC_BASE_URL,
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "./lib/category_lexicon.mjs";
import { createR2Store, r2ConfigFromEnv } from "./lib/r2_store.mjs";

const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const STAGE = join(projectRoot, "work", "root-refresh");

function parseArgs(argv) {
  const args = { baseUrl: DEFAULT_PUBLIC_BASE_URL, upload: false, regions: null };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--base-url") args.baseUrl = argv[++i];
    else if (arg === "--upload") args.upload = true;
    else if (arg === "--regions") args.regions = argv[++i].split(",").map(s => s.trim()).filter(Boolean);
    else throw new Error(`Unknown argument: ${arg}`);
  }
  if (!args.baseUrl.endsWith("/")) args.baseUrl += "/";
  return args;
}

function log(line) {
  console.log(`Refresh: ${line}`);
}

const args = parseArgs(process.argv.slice(2));
const lexiconModule = await loadCategoryLexiconModule();
if (!lexiconModule) {
  console.error("Refresh: no rangefind with buildCategoryLexiconArtifact found (npm dependency or ../rangefind checkout).");
  process.exit(1);
}

const rootResponse = await fetch(new URL("manifest.json", args.baseUrl));
if (!rootResponse.ok) throw new Error(`Root manifest fetch failed: ${rootResponse.status}`);
const root = await rootResponse.json();
if (!Array.isArray(root.shards) || !root.shards.length) throw new Error("Root manifest has no shards.");
log(`Root: ${root.shards.length} shard(s), ${Number(root.total || 0).toLocaleString()} docs, built ${root.built_at}.`);
if (root.category_lexicon) {
  log(`Root already carries a category lexicon (${root.category_lexicon.types?.length || 0} types) — refreshing it.`);
}

const selected = args.regions
  ? root.shards.filter(shard => args.regions.includes(shard.id))
  : root.shards;
if (args.regions && selected.length !== args.regions.length) {
  throw new Error("Some --regions are not in the root manifest.");
}
// --regions limits the fetch set for testing; the merged artifact must
// still speak for the whole root before it may be uploaded.
const merged = await mergeShardTypeVocabulary({
  shards: selected.map(shard => ({
    id: shard.id,
    // A shard's total changes whenever it rebuilds or takes a delta —
    // that is exactly when its vocabulary is worth refetching.
    cacheKey: `${shard.total || 0}`,
    remoteBase: args.baseUrl
  })),
  cachePath: join(projectRoot, "work", "category-lexicon-cache.json"),
  remoteBase: args.baseUrl,
  log
});
if (!merged) process.exit(1);

const artifact = lexiconModule.buildCategoryLexiconArtifact(merged);
log(`Artifact: ${artifact.types.length} types, ${Object.keys(artifact.aliases).length} aliases.`);

const updated = { ...root, built_at: new Date().toISOString(), category_lexicon: artifact };
const payload = JSON.stringify(updated);
mkdirSync(STAGE, { recursive: true });
for (const name of ["manifest.json", "manifest.min.json"]) {
  writeFileSync(join(STAGE, name), payload);
}
log(`Staged ${(payload.length / 1024).toFixed(1)} KB root manifests in ${STAGE}.`);

if (!args.upload) {
  log("Dry run — rerun with --upload (and R2_* env) to flip the root manifests in R2.");
  process.exit(0);
}
if (args.regions) {
  throw new Error("--upload with --regions would publish a partial vocabulary; run without --regions.");
}
const store = createR2Store(r2ConfigFromEnv());
for (const name of ["manifest.json", "manifest.min.json"]) {
  await store.putFile(join(STAGE, name), name);
}
log("Uploaded manifest.json and manifest.min.json — the published root now carries the lexicon.");
