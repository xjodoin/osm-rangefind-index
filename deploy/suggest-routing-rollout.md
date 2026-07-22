# Rollout: root suggest routing + locality enrichment

Two rangefind features land together in the next planet rebuild. Both are in
rangefind's `Unreleased` changelog; the pipeline wiring (suggest-set
sidecars, `suggest-routing` artifact build, `authority/` root upload) is
already in `scripts/update_index.mjs` behind feature detection, so the
sequence is:

1. Publish the next rangefind release; on ns3222652:
   `git pull && npm install` in this repo.
2. Run the updater with **`--force-stats`**. This is not optional for this
   rollout: locality enrichment adds each municipality's name to every
   document inside it, which shifts document frequencies corpus-wide. A
   stale scoring-stats artifact would give city-name terms wildly inflated
   idf and distort ranking on every enriched shard.
3. Expect a full re-extraction and full rebuild of all 310 shards:
   - extraction schema is now v9, so cached corpora are invalid and PBFs
     re-download (~79 GiB; the refresh path handles reclaimed files);
   - the corpus content change exceeds every delta threshold anyway.
4. First run also writes `work/suggest-sets/<id>.suggest.gz` per shard
   (during the build, before cleanup — no backfill needed on a full
   rebuild) and merges them into the root `authority/` artifact
   (`SUGGEST_ROUTING_BASE_SHARD_DEPTH`, default 8, bounds the largest
   in-memory prefix group; `SUGGEST_ROUTING_HEAP_MB`, default 8192, sizes
   the isolated merge worker).
5. After the root manifest flips, verify:
   - `manifest.json` has a `suggest_routing` block;
   - `node scripts/osm_remote_bench.mjs --lanes=suggest,suggest-address`
     from the rangefind repo shows the suggest lanes answered in a handful
     of requests (`stats.suggestRouting: "root-authority"`), not a fan-out;
   - `node scripts/osm_mobile_bench.mjs` for the phone-side numbers;
   - a brand+town query ("jean coutu rosemère") returns the POI.

If the shard pass completed but root routing finalization was interrupted,
resume with `scripts/nightly.sh --finalize-only --max-hours 8`. This reuses
the 310 checkpointed sidecars and cannot be starved by a new daily PBF cycle.

Escape hatches: `--no-suggest-routing` skips the artifact (suggest falls
back to fan-out); the runtime fails open on any missing/broken artifact.
