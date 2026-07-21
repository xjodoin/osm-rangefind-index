// Category lexicon for the sharded root manifest.
//
// The Rangefind OSM planner gates category words ("cinema", "boulangerie")
// on the corpus's own `type` facet vocabulary. This module merges that
// vocabulary across every shard — reading the local shard artifacts when
// they exist and the published CDN copy when the pipeline has already
// reclaimed them — and joins it with the package's alias table into the
// `category_lexicon` block the root manifest embeds.
//
// Per-shard vocabularies are cached in work/ keyed by the shard's build
// fingerprint (or manifest identity), so steady-state runs only refetch
// shards that actually rebuilt.

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { fileURLToPath } from "node:url";
import { createNodeSearch } from "rangefind/node";

const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

export const DEFAULT_PUBLIC_BASE_URL = "https://osm.rangefind.dev/";

// The artifact builder ships with rangefind releases newer than 0.3.7; a
// sibling checkout covers pre-release runs. Returns null when neither has
// it — callers then publish a root without the lexicon, exactly like the
// text/suggest routing feature detection.
export async function loadCategoryLexiconModule() {
  const candidates = [
    () => import("rangefind/osm"),
    () => import(pathToFileURL(join(projectRoot, "..", "rangefind", "src", "integrations", "osm", "category_lexicon.js")).href)
  ];
  for (const load of candidates) {
    try {
      const module = await load();
      if (typeof module.buildCategoryLexiconArtifact === "function") return module;
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function loadCache(path) {
  try {
    const cache = JSON.parse(readFileSync(path, "utf8"));
    return cache && typeof cache.shards === "object" ? cache : { shards: {} };
  } catch {
    return { shards: {} };
  }
}

async function shardTypeCounts(source) {
  const engine = await createNodeSearch({ source });
  const values = await engine.loadFacetValues("type");
  const counts = {};
  for (const item of values || []) {
    if (!item?.value) continue;
    counts[item.value] = (counts[item.value] || 0) + Number(item.n || 0);
  }
  return counts;
}

// shards: [{ id, cacheKey, localDir?, remoteBase? }] — localDir is tried
// first when given; remoteBase (default DEFAULT_PUBLIC_BASE_URL) serves
// shards whose local artifacts were reclaimed after upload.
// Returns [{ value, n }] merged across shards, or null when shouldStop()
// interrupted the merge (callers publish without the artifact this run).
export async function mergeShardTypeVocabulary({
  shards,
  cachePath,
  remoteBase = DEFAULT_PUBLIC_BASE_URL,
  log = () => {},
  shouldStop = () => false
}) {
  const cache = cachePath ? loadCache(cachePath) : { shards: {} };
  let cacheDirty = false;
  let fetched = 0;
  const merged = new Map();
  for (const shard of shards) {
    if (shouldStop()) {
      log(`Category lexicon: interrupted after ${fetched} shard fetch(es).`);
      return null;
    }
    const cached = cache.shards[shard.id];
    let counts = cached && cached.key === shard.cacheKey ? cached.types : null;
    if (!counts) {
      const sources = [
        ...(shard.localDir ? [shard.localDir] : []),
        new URL(`shards/${shard.id}/`, shard.remoteBase || remoteBase).href
      ];
      for (const source of sources) {
        try {
          counts = await shardTypeCounts(source);
          break;
        } catch (error) {
          log(`Category lexicon: ${shard.id} via ${source} failed (${error?.message || error}).`);
        }
      }
      if (!counts) {
        log(`Category lexicon: ${shard.id} unreadable — its vocabulary is skipped this run.`);
        continue;
      }
      fetched += 1;
      cache.shards[shard.id] = { key: shard.cacheKey, types: counts };
      cacheDirty = true;
    }
    for (const [value, n] of Object.entries(counts)) {
      merged.set(value, (merged.get(value) || 0) + n);
    }
  }
  if (cachePath && cacheDirty) {
    mkdirSync(dirname(cachePath), { recursive: true });
    writeFileSync(cachePath, JSON.stringify(cache));
  }
  log(`Category lexicon: ${merged.size} type value(s) across ${shards.length} shard(s) (${fetched} fetched, rest cached).`);
  return [...merged].map(([value, n]) => ({ value, n }));
}
