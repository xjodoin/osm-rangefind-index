# osm-rangefind-index

Scheduled pipeline that builds a **sharded [Rangefind](../rangefind) OSM
index** (one shard per Geofabrik region, exact cross-shard scoring via a
frozen scoring-stats artifact) and publishes it **incrementally to Cloudflare
R2** — designed to run on a server that is only idle at night and on
weekends.

Every run makes as much progress as fits before its deadline and stops
cleanly; interrupted shard builds resume from rangefind's stage checkpoints
on the next run. Only regions whose upstream OSM data changed are
re-downloaded, re-extracted, rebuilt, and re-uploaded (packs are
content-addressed, so `rclone` skips unchanged objects).

## Setup

```sh
npm install                       # rangefind ^0.3.0 from npm
cp .env.example .env              # fill in R2 credentials / remote
chmod +x scripts/nightly.sh
# edit regions.json — one entry per shard (Geofabrik path)
```

Requires: Node ≥ 22, `rclone` on PATH, disk ≈ the largest single region's
PBF + corpus + built shard (steady state is far smaller — see Disk usage)
(PBFs + JSONL + build temp + index).

## The shard set

`regions.json` ships with the **full planet: 310 shards** (187 countries
covered, ≈78.6 GiB of PBF downloads), generated from Geofabrik's official
region index by `scripts/generate_regions.mjs`. The policy is size-driven:
country-level everywhere, and any region whose PBF exceeds
`EXPAND_PBF_GIB` (2.5 GiB) with available subregions splits into them —
currently the US (53 states), France (27 régions, overseas territories get
their own coverage bboxes), Germany (16 Bundesländer), Canada (13
provinces — Québec is its own shard), and Russia (10 federal districts).
Overlapping combined extracts (dach, alps, britain-and-ireland, US
macro-regions…) are excluded, double-coverage is guarded by an ISO-3166
check, and short-id collisions are parent-qualified (`us-georgia` vs the
country `georgia`). Re-run the generator to refresh the list — it
HEAD-verifies every URL — or trim `regions.json` to a subset any time.

## Initial bring-up

The first runs are an **acquisition phase**: download + extract + compress
each region's corpus (PBFs are dropped immediately; footprint stays near
the gzipped corpus total, ~12–15 GiB for the planet). Builds are gated
until *all* regions have a corpus — otherwise each night's new arrivals
would change the region set, regenerate the stats artifact, and invalidate
every shard already built (`--partial` overrides the gate deliberately).
Once acquisition completes, one stats pass runs, then shards build and
publish region by region — every step deadline-aware and resumable. While the
initial root manifest is incomplete, later runs reuse those acquired snapshots
and resume building before checking Geofabrik again. Daily upstream refreshes
start only after every initial shard has been published, so fresh source files
cannot starve the first complete index.

Acquisition uses `acquisitionConcurrency` lanes (default `2` in the shipped
configuration), so downloads and normal-sized extracts can overlap. A PBF at
or above `largePbfBytes` (default 1 GiB) consumes every lane and extracts
alone to protect memory on the 31 GiB production host. Stats and shard builds
remain sequential; each shard build already uses the configured CPU worker
pool.

Rough planet budget on a modern 12–16-core box: ~78 GiB of downloads
(bandwidth-bound), a few hours of extraction, several hours for the stats
pass, and on the order of 10–15 h of shard builds — i.e. **a weekend run
plus a few nights**, all unattended. Disk: ~60 GiB free is comfortable
(gzipped corpora + the stats pass's transient plain corpora + the largest
region's build); steady state after publish is ~15 GiB.

Adding a region later re-runs the stats pass (region set changed) and
therefore rebuilds all shards — batch additions, and expect that cycle to
take a weekend too.

## Manual runs

```sh
npm run update -- --no-upload          # build everything locally
npm run update -- --deadline 06:15    # stop cleanly before the workday
npm run update -- --regions quebec    # limit to one region
npm run status                         # what's built / uploaded / pending
npm run update -- --prune             # occasional: delete unreferenced packs on R2
```

## Scheduling (cron)

```cron
# Weeknights: start 20:00, hand the server back before 06:15.
0 20 * * 0-4   /usr/bin/env INDEX_LOG_FILE=logs/nightly.log /srv/osm-rangefind-index/scripts/nightly.sh --deadline 06:15

# Weekend: start Saturday 00:15, run up to 54h (until ~Mon 06:15).
15 0 * * 6     /usr/bin/env INDEX_LOG_FILE=logs/weekend.log /srv/osm-rangefind-index/scripts/nightly.sh --max-hours 54
```

A lockfile prevents overlapping runs (a weekend run still going Monday keeps
the nightly one from starting). `nightly.sh` runs everything under
`nice`/`ionice`, so an early return of daytime load mostly just slows the
build. The launcher tees all progress and errors to `logs/indexing.log` by
default; `INDEX_LOG_FILE` selects a different file as shown above.

## How incremental updates work

| Step | Trigger | Cost when unchanged |
|---|---|---|
| PBF download | Geofabrik `Last-Modified` changed | one HEAD request |
| JSONL extract | PBF version changed | skipped |
| scoring stats | region set changed, corpus drift > `statsDriftRatio` (default 10%), or `--force-stats` | none |
| shard update | corpus or stats changed | none |
| upload | built fingerprint ≠ uploaded fingerprint | rclone size-check against the remote listing |

**Changed regions ship as generational deltas, not full rebuilds.** The
fresh corpus is diffed against the snapshot the shard was built from; the
added/changed documents build as a small `--update` generation against the
same frozen stats artifact — proven identical to a full rebuild — so a
typical nightly region refresh uploads kilobytes, not gigabytes, and leaves
CDN caches for every existing pack intact. A **full rebuild** happens only
when: the delta exceeds `maxDeltaRatio` (default 30%), pending deletions
exceed `maxDeletedRatio` (default 0.5% — deltas cannot remove documents, so
deleted places linger until the next full rebuild), the shard reaches
`maxGenerations` (default 6), or the stats artifact was regenerated.

Regenerating the stats artifact intentionally invalidates **all** shards
(BM25 statistics are frozen corpus-wide so shard scores merge exactly);
between regenerations, updated regions stay exactly comparable with
untouched shards. Drift only shifts idf slightly, and 10% corpus growth is
years of OSM edits for most regions.

Publish ordering is reader-safe: shard packs upload before shard manifests,
and the root manifest uploads only after every built shard is fully synced.
Old packs are left in place until a `--prune` run so in-flight readers on the
previous manifest never 404 (prune runs only on freshly rebuilt shards whose
local mirror is complete).

## Disk usage

Nothing is ever downloaded back from R2. After each region publishes, local
artifacts are reclaimed automatically (disable with `--keep-artifacts`):
the PBF and extractor caches are deleted, the corpus JSONL is compressed
(it is the next diff base and the stats-regeneration input), and the local
index copy is gutted to manifests + generation id-maps (what future deltas
need). Steady state per region ≈ the gzipped corpus — e.g. Luxembourg
~17 MB on disk vs a 186 MB published index. Transient acquisition is bounded
by `acquisitionConcurrency` normal regions or one large region. Shards still
build one at a time, so build-time disk should be sized for the largest
region rather than the full corpus.

## Serving

Point a Cloudflare custom domain (or Worker) at the bucket and open it with
rangefind — no server:

```js
import { createSearch } from "rangefind";        // browser
const engine = await createSearch({ baseUrl: "https://osm.example.com/" });
await engine.search({ q: "1234 rue sainte-catherine", size: 5 });
await engine.search({ q: "", geo: { near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
```

`deploy/cloudflare-cache-rules.json` enables a one-year edge and browser TTL
for content-addressed `.bin` and `.bin.gz` objects on `osm.rangefind.dev`.
Those names include a content hash and are immutable. Mutable HTML,
`status.json`, manifests, `.json.gz` metadata, and the root manifest remain
uncached so a newly published index becomes visible atomically. Create the
zone cache ruleset with a token that has Cache Settings Edit access:

```bash
curl --request POST \
  "https://api.cloudflare.com/client/v4/zones/$CLOUDFLARE_ZONE_ID/rulesets" \
  --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
  --json @deploy/cloudflare-cache-rules.json
```
