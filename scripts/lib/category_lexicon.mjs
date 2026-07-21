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

import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { createNodeSearch } from "rangefind/node";

export const DEFAULT_PUBLIC_BASE_URL = "https://osm.rangefind.dev/";

// Production must use the installed package. A sibling checkout must never
// mask a stale lockfile or an incomplete server deployment.
export async function loadCategoryLexiconModule() {
  try {
    const module = await import("rangefind/osm");
    if (typeof module.buildCategoryLexiconArtifact === "function") return module;
  } catch {
    // Feature detection: callers fail closed or publish no lexicon block.
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

function manifestCacheKey(manifest) {
  return createHash("sha256").update(JSON.stringify(manifest)).digest("hex");
}

async function loadShardVocabulary(source, cached = null) {
  const engine = await createNodeSearch({ source });
  const cacheKey = manifestCacheKey(engine.manifest);
  if (cached?.key === cacheKey && cached.types && typeof cached.types === "object") {
    return { cacheKey, counts: cached.types, fetched: false, total: Number(engine.manifest.total) };
  }
  const values = await engine.loadFacetValues("type");
  const counts = {};
  for (const item of values || []) {
    if (!item?.value) continue;
    counts[item.value] = (counts[item.value] || 0) + Number(item.n || 0);
  }
  return { cacheKey, counts, fetched: true, total: Number(engine.manifest.total) };
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
  shouldStop = () => false,
  readShard = loadShardVocabulary
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
    let counts = shard.cacheKey && cached?.key === shard.cacheKey ? cached.types : null;
    if (!counts) {
      const sources = [
        ...(shard.localDir ? [shard.localDir] : []),
        new URL(`shards/${shard.id}/`, shard.remoteBase || remoteBase).href
      ];
      const failures = [];
      for (const source of sources) {
        try {
          // Pipeline callers provide the state fingerprint and can skip all
          // reads on a cache hit. Root-only refreshes omit it: opening the
          // shard manifest yields a content identity, while a matching cache
          // still avoids loading the facet dictionary itself.
          const loaded = await readShard(source, shard.cacheKey ? null : cached);
          if (Number.isFinite(shard.expectedTotal) && loaded.total !== shard.expectedTotal) {
            throw new Error(`manifest total ${loaded.total} does not match root total ${shard.expectedTotal}`);
          }
          counts = loaded.counts;
          const cacheKey = shard.cacheKey || loaded.cacheKey;
          if (!cacheKey) throw new Error("source did not provide a cache identity");
          cache.shards[shard.id] = { key: cacheKey, types: counts };
          cacheDirty = cacheDirty || loaded.fetched !== false || cached?.key !== cacheKey;
          if (loaded.fetched !== false) fetched += 1;
          break;
        } catch (error) {
          failures.push(error?.message || String(error));
          log(`Category lexicon: ${shard.id} via ${source} failed (${error?.message || error}).`);
        }
      }
      if (!counts) {
        throw new Error(`Category lexicon: ${shard.id} unreadable; refusing a partial artifact (${failures.join("; ")}).`);
      }
    }
    for (const [value, n] of Object.entries(counts)) {
      merged.set(value, (merged.get(value) || 0) + n);
    }
  }
  if (cachePath && cacheDirty) {
    mkdirSync(dirname(cachePath), { recursive: true });
    const temporary = `${cachePath}.${process.pid}.tmp`;
    try {
      writeFileSync(temporary, JSON.stringify(cache));
      renameSync(temporary, cachePath);
    } catch (error) {
      rmSync(temporary, { force: true });
      throw error;
    }
  }
  log(`Category lexicon: ${merged.size} type value(s) across ${shards.length} shard(s) (${fetched} fetched, rest cached).`);
  return [...merged].map(([value, n]) => ({ value, n }));
}
