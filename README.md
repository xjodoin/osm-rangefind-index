# osm-rangefind-index

Scheduled pipeline that builds a **sharded [Rangefind](../rangefind) OSM
index** (one shard per Geofabrik region, exact cross-shard scoring via a
frozen scoring-stats artifact) and publishes it **incrementally to Cloudflare
R2** — designed to run on a server that is only idle at night and on
weekends. The same PBF pass also publishes static, range-addressed road graphs
for client-only routing, travel-time matrices, and itinerary planning.

Every run makes as much progress as fits before its deadline and stops
cleanly; interrupted shard builds resume from rangefind's stage checkpoints
on the next run. Only regions whose upstream OSM data changed are
re-downloaded, re-extracted, rebuilt, and re-uploaded. Immutable packs are
content-addressed and published directly through Cloudflare's S3 API.

## Setup

```sh
npm install                       # installs the pinned Rangefind feature line
cp .env.example .env              # fill in direct R2/S3 credentials
chmod +x scripts/nightly.sh
# edit regions.json — one entry per shard (Geofabrik path)
```

Requires: Node ≥ 22 and disk ≈ the largest single region's PBF + corpus +
built shard (steady state is far smaller — see Disk usage) (PBFs + JSONL +
build temp + index). No external object-storage CLI is required.

## Optional address and postal authorities

The OSM corpus can be augmented by any licensed civic-address or postal-code
authority without adding a server-side search dependency. Copy
`address-sources.example.json` to `address-sources.json` (or set
`ADDRESS_SOURCES_CONFIG`) to enable sources. The included production example
combines current OpenAddresses civic addresses worldwide with GeoNames postal
centroids and its denser Canadian postal file. Create a free OpenAddresses API
token and set `OPENADDRESSES_TOKEN`; the token is used only for authenticated
downloads and is never placed in index state, manifests, or logs.

The configuration is provider-neutral: ordinary files declare a URL, compression,
CSV/TSV field mapping, defaults, attribution, applicable region groups, and an
optional explicit or coordinate-based shard partition. The built-in
`openaddresses-batch` provider consumes the live normalized address catalog as
bounded per-source gzip jobs instead of staging the roughly 72 GB global ZIP.
Four jobs run concurrently by default (`partitionConcurrency`, configurable
from 1–16), each producing isolated shard fragments. Completed fragments are
committed in deterministic source order as concatenated gzip members. A small
write-ahead checkpoint truncates an interrupted append before resume, so a
crash cannot duplicate records and disk use stays close to the compressed
regional output. A worldwide file
downloads once into `work/address-sources/` and is converted in one pass to
canonical per-shard JSONL, avoiding hundreds of rescans. Every matching region
then streams its thin partition through the same enrichment engine. Code adapters can use the lower-level
async-record API for OpenAddresses GeoJSON, a national register, a database,
or an application-owned feed.

Sources are applied in configuration order. The engine:

- suppresses canonical civic duplicates against OSM and earlier providers;
- groups all samples for a country/postcode into one searchable postal result;
- records centroid, bounds, primary locality, aliases, sample/address counts,
  and source provenance;
- keeps residential records out of BM25, autocomplete, and map-browse lanes;
- includes source identity in each shard fingerprint, so a provider refresh
  rebuilds only affected shards;
- publishes every source attribution in the shard manifest.

OpenAddresses is particularly useful as an address provider. The integration
uses every current worldwide `addresses` job, assigns collision-safe IDs,
links each record back to its exact OpenAddresses source definition (where the
original publisher and license are declared), and derives the country from the
source namespace. Sources are refreshed weekly by default so a continuously
running indexer does not repartition the planet every night; change
`refreshIntervalHours` when a different cadence is appropriate. For a thin
postal-only layer, GeoNames remains substantially cheaper. Spatial partitioning
uses Geofabrik ISO country and subdivision metadata before falling back to
coverage boxes. This avoids safe-to-eliminate cross-border and state/province
duplication while retaining bbox overlap for regions without authoritative
codes, including antimeridian-spanning regions.

Run `npm run benchmark:addresses` to compare sequential and four-job partition
throughput with deterministic synthetic country/subdivision overlap. Use
`-- --jobs N --rows-per-job N --parallelism N --network-delay-ms N` to model a
specific host or source profile.

The normal updater runs downloads, isolated OSM place-extraction workers,
isolated address-enrichment workers, regional road builders, and direct R2
uploads as a bounded pipeline. Once the shared OSM place corpus exists,
address enrichment and road construction for that region run independently,
so a large OpenAddresses partition no longer delays the start of its route
graphs. Weighted capacity uses PBF size plus address-partition bytes and record
count: small regions overlap while country-scale enrichment and road graphs
reserve additional lanes. Before starting a new CPU-heavy stage, Linux hosts
check available memory and memory PSI; active work is never killed merely to
admit more concurrency. Configure the hard lane count with
`acquisitionConcurrency`, the number of regions allowed to flow between stages
with `acquisitionPipelineWorkers`, optional worker heaps with
`OSM_EXTRACT_HEAP_MB` and `ADDRESS_ENRICHMENT_HEAP_MB`, and pressure thresholds
with `INDEX_PIPELINE_MIN_AVAILABLE_GIB` and `INDEX_PIPELINE_MAX_MEMORY_PSI`.
Every stage logs its queue delay, elapsed time, weight, and active lane count.
Disk reservations remain held through corpus compression, source deletion, and
scratch cleanup. A region that cannot fit waits for active cleanup leases to
release capacity instead of aborting the planet run; if no active lease can
free space, the updater still fails closed. `INDEX_DISK_ADMISSION_POLL_MS`
controls the wait probe interval.
Completed regional road graphs publish through a serialized catalog lane, so
they become discoverable immediately without mutable-catalog races.

Run `npm run benchmark:acquisition` for real-PBF comparisons of sequential
versus isolated parallel place extraction and sequential versus overlapping
address enrichment/road extraction. It downloads Liechtenstein and Luxembourg,
generates a deterministic address workload, uses the production workers, and
removes all artifacts afterward. Set `ACQUISITION_BENCH_ADDRESS_ROWS` to scale
the enrichment half of the benchmark.

## Road indexes and itinerary planning

`roadIndexes` in `regions.json` enables Rangefind's `rfroutegraph-v1` lane.
For every configured profile, the updater reuses the region PBF while it is
already local, extracts a turn-aware OSM road graph in an isolated process,
builds a CRP/MLD index, uploads its immutable packs, and only then publishes
its `manifest.json`. The source graph, build-only node order, route index, and
PBF are removed after a durable upload. R2 therefore retains only the files a
client can request; local disk remains bounded to the active region.

The shipped production configuration enables `car`, `bike`, and `foot`, with
an independent graph for each travel profile. Turn restrictions, one-way
roads, access tags, roundabouts, conditional/max speeds, junction delays,
road geometry, names, classes, and turn costs come from OSM and the Rangefind
profile. Internal shard count scales with PBF size up to `maxShards`, keeping
large regional graphs range-efficient without burdening small islands with
empty shards.

Each durable graph is discoverable through `routes/catalog.json`. A catalog
entry contains its region bbox and ISO metadata, profile, base URL, source
vintage, route manifest, and exact shared-OSM-node portals to neighboring
regions. The catalog engine opens regional graphs lazily and crosses borders
only where both graphs prove the same OSM node and coordinate; it never
invents a connection from bbox overlap or proximity.

Clients that do not know which capabilities a deployment provides can start
at `/.well-known/rangefind.json`. The descriptor identifies the search
manifest, route catalog, status endpoint, supported travel profiles, and each
format explicitly. The search root also embeds the same relative paths in its
`endpoints` field. Route graphs use `manifest.json` below each catalog entry's
`base`; they are not place shards and do not use `manifest.min.json`.

The route lane is incremental and resumable at region/profile boundaries:

- unchanged source + builder identities do no work;
- an interrupted upload reuses the completed local graph;
- a Rangefind route-format change rebuilds only road indexes, not search;
- regions with no connected network are recorded as unavailable and omitted
  from the catalog rather than blocking the planet run;
- a region can set `roadIndexes: false` to remain fully searchable while being
  excluded from route extraction, federation, catalog totals, and completion;
- the R2 garbage collector protects every catalog-referenced route prefix,
  while update-time `--prune` removes superseded objects inside that prefix.

Run `npm run benchmark:roads` for a real-PBF end-to-end smoke benchmark. It
downloads Liechtenstein into a temporary directory, exercises the production
extract/build worker, opens the result through positional file reads, and
calculates a real route. Set `ROAD_BENCH_PBF_URL` to test another extract.

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

The first runs are an **acquisition phase**: download, extract search and road
artifacts, publish each completed road graph, then compress each region's
corpus (PBFs are dropped immediately; footprint stays near
the gzipped corpus total plus configured address-source partitions). Builds are gated
until *all* regions have a corpus — otherwise each night's new arrivals
would change the region set, regenerate the stats artifact, and invalidate
every shard already built (`--partial` overrides the gate deliberately).
Once acquisition completes, one stats pass runs, then shards build and
publish region by region — every step deadline-aware and resumable. Each
completed shard enters a bounded multi-lane R2 queue, so batched object PUTs
and cleanup overlap the next shard build. Two shard uploads run concurrently
by default (`R2_UPLOAD_LANES`) through one shared 16-request S3 pool
(`R2_REQUEST_CONCURRENCY`); `R2_UPLOAD_QUEUE_DEPTH` bounds temporary disk. While
the initial root manifest is incomplete, later runs reuse those acquired
snapshots and resume building before checking Geofabrik again. Daily upstream
refreshes start only after every initial shard has been published, so fresh
source files cannot starve the first complete index.

Acquisition uses `acquisitionConcurrency` weighted lanes, so downloads and
normal-sized stages can overlap. With four lanes, an ordinary road builder
uses two, a PBF at or above `largePbfBytes` (default 1 GiB) uses three, and a
large address partition uses two. This leaves useful overlap without admitting
two country-scale road graphs at once on the 31 GiB production host. Stats and
shard builds remain sequential; each shard build already uses the configured
CPU worker pool. `partitionReducerWorkers` is capped separately (default `8`) to saturate
continent-scale reduction without tying it to scan parallelism. The shipped
`codeStoreWorkerPreloadMaxBytes` is 3 GiB so large OSM filter stores use one
shared sequential preload instead of per-worker random reads. Rangefind's OSM
schema summarizes only the six high-value category, type, prominence,
population, and coordinate columns; compact facet cells keep that shared set
inside the preload budget. Direct R2
uploads preserve packs-before-generation-manifests-before-root-
manifest ordering for every shard.

Rangefind streams completed `.jsonl.gz` corpora directly during scoring and
full builds, so a planet stats pass no longer materializes every corpus at
once. `INDEX_MIN_FREE_GIB` (default 24) preserves filesystem headroom; each
active extraction also reserves a source-sized allowance, and acquisition
pauses cleanly when the combined requirement cannot be met. Completed
enrichment metadata is recovered after interruption, while failed partial
outputs and SQLite scratch are removed automatically.

Rough planet budget on a modern 12–16-core box: ~78 GiB of downloads
(bandwidth-bound), a few hours of extraction, several hours for the stats
pass, and on the order of 10–15 h of shard builds — i.e. **a weekend run
plus a few nights**, all unattended. Disk must cover the compressed OSM and
configured address-source corpora, the largest active extraction/build, and
the bounded upload queue; the updater enforces the configured safety reserve.

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
npm run update:roads -- --prune       # publish roads without touching search shards or scoring stats
npm run gc:r2                          # dry-run manifest-aware R2 garbage collection
npm run gc:r2 -- --apply              # track/delete objects after the grace period
npm run refresh:root-lexicon           # stage a category-lexicon root refresh
npm run refresh:root-lexicon -- --upload # conditionally publish it to R2
node scripts/update_index.mjs --finalize-only --max-hours 8 # publish existing completed shards and routing
node scripts/update_index.mjs --no-road-indexes # emergency: run only the place-search lane
```

A region-scoped production run rebuilds only the selected regions but
publishes them into the complete existing root; it never replaces the planet
root with a one-shard manifest. `--partial` is the explicit isolated
bring-up mode.

Use `npm run update:roads -- --prune` for the first road-index rollout after a
search index is already complete. It bypasses address enrichment, search
extraction, scoring stats, search builds, and the search root—even if the
normal service command carries `--force-stats`. Later normal update cycles
refresh a changed region's place and road indexes from the same PBF download.

The root-lexicon refresh does not rebuild shards. Upload mode shares the
indexer's process lock, requires every shard vocabulary to be readable, and
uses conditional R2 writes so it aborts rather than overwriting a root that
changed during the merge. Run it on the production host with the normal
`R2_*` environment, never by copying files from `work/root-refresh` manually.

`--finalize-only` is the recovery path after an interrupted root routing
merge. It reuses existing scoring stats, built shard manifests, and
checkpointed term/suggest sidecars; it does not download PBFs or build shard
updates. Normal scheduled runs remain unchanged.

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
| road graph/index | PBF, profile, road options, or Rangefind route builder changed | none |
| scoring stats | region set changed, corpus drift > `statsDriftRatio` (default 10%), or `--force-stats` | none |
| shard update | corpus or stats changed | none |
| upload | built fingerprint ≠ uploaded fingerprint | none |

**Changed regions ship as generational deltas, not full rebuilds.** The
fresh corpus is diffed against the snapshot the shard was built from; the
added/changed documents build as a small `--update` generation against the
same frozen stats artifact — proven identical to a full rebuild — so a
typical nightly region refresh uploads kilobytes, not gigabytes, and leaves
CDN caches for every existing pack intact. A **full rebuild** happens only
when: the installed Rangefind builder version changes, the delta exceeds
`maxDeltaRatio` (default 30%), pending deletions
exceed `maxDeletedRatio` (default 0.5% — deltas cannot remove documents, so
deleted places linger until the next full rebuild), the shard reaches
`maxGenerations` (default 6), or the stats artifact was regenerated.

Regenerating the stats artifact intentionally invalidates **all** shards
(BM25 statistics are frozen corpus-wide so shard scores merge exactly);
between regenerations, updated regions stay exactly comparable with
untouched shards. Drift only shifts idf slightly, and 10% corpus growth is
years of OSM edits for most regions.

Builder identity and logical corpus identity are tracked separately. An
encoding-only Rangefind upgrade forces correct shard rewrites while retaining
unchanged root term/suggest routing artifacts.

Publish ordering is reader-safe: shard packs upload before generation
manifests, stable shard manifests publish last, and the root manifest uploads
only after every built shard is fully durable.
Old packs are left in place until a `--prune` run so in-flight readers on the
previous manifest never 404. Regional road namespaces go one step further:
`--prune` records superseded objects and deletes them only after they remain
continuously unreferenced for seven days (`ROAD_OBJECT_PRUNE_GRACE_DAYS` can
raise that window). This keeps R2 thin without breaking a catalog or graph
that a long-lived client opened before the manifest flip.

`scripts/r2_gc.sh` handles leftovers after local shard cleanup. It takes the
same launcher lock as the indexer, marks immutable objects referenced by the
live root/shard/generation manifests, and tracks everything else in
`work/r2-gc-state.json`. An object must remain continuously unreferenced for
seven days before `--apply` deletes it; its upload age alone is never enough.
Binary autocomplete lexicon roots reference content-addressed hot lists
transitively, so every live lexicon's sibling `authority/hot/` namespace is
also protected even though those object names do not appear in manifest JSON.
The weekly systemd timer runs Friday at 07:00 Montreal time. The first apply
run only establishes the grace-period baseline, and every run writes
`work/r2-gc-last-report.json` plus `logs/r2-gc.log`.

## Disk usage

Only selective manifest/term data is downloaded for a missing routing
sidecar. After each region publishes, local artifacts are reclaimed
automatically (disable with `--keep-artifacts`):
the PBF, road source/index, and extractor caches are deleted, the corpus JSONL is compressed
(it is the next diff base and is streamed directly into stats/full builds), and the local
index copy is gutted to manifests + generation id-maps (what future deltas
need). Steady state per region ≈ the gzipped corpus — e.g. Luxembourg
~17 MB on disk vs a 186 MB published index. Transient acquisition is bounded
by weighted stage capacity and per-region disk reservations. Shards still
build one at a time, while `R2_UPLOAD_LANES` shard uploads run concurrently
and up to `R2_UPLOAD_QUEUE_DEPTH` completed shards can await cleanup. Size disk
for the largest active build plus that bounded queue rather than the full corpus.

## Serving

Point a Cloudflare custom domain (or Worker) at the bucket and open it with
rangefind — no server:

```js
import { createSearch } from "rangefind";        // browser
const engine = await createSearch({ baseUrl: "https://osm.example.com/" });
await engine.search({ q: "1234 rue sainte-catherine", size: 5 });
await engine.search({ q: "", geo: { near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
```

For generic same-region or cross-region routing, open the advertised catalog;
regional graphs and portal ranges are fetched lazily:

```js
import { openRouteCatalogUrl } from "rangefind/route";

const roads = await openRouteCatalogUrl(
  "https://osm.rangefind.dev/routes/catalog.json",
  { profile: "car" }
);
const trip = await roads.route({
  from: { lat: 45.5019, lon: -73.5674 },
  to: { lat: 43.6532, lon: -79.3832 }
});
```

For a multi-stop itinerary known to remain in one region, select the catalog
entry whose bbox contains every stop and open that graph directly:

```js
import { openRouteGraphUrl } from "rangefind/route";

const catalog = await fetch("https://osm.rangefind.dev/routes/catalog.json").then(r => r.json());
const quebec = catalog.indexes.find(index => index.region === "quebec" && index.profile === "car");
const roads = await openRouteGraphUrl(`https://osm.rangefind.dev/${quebec.base}`);

const trip = await roads.itinerary({
  stops: [
    { lat: 45.5019, lon: -73.5674 },
    { lat: 45.6066, lon: -73.7124 },
    { lat: 45.5088, lon: -73.5540 }
  ],
  openEnd: true
});
```

`deploy/cloudflare-cache-rules.json` enables a one-year edge and browser TTL
for content-addressed `.bin` and `.bin.gz` objects on `osm.rangefind.dev`,
including `rfrouteportals-v2` per-neighbor range packs. Those names include a
content hash and are immutable. Mutable HTML,
`status.json`, manifests, `.json.gz` metadata, and the root manifest remain
uncached so a newly published index becomes visible atomically. Create the
zone cache ruleset with a token that has Cache Settings Edit access:

```bash
curl --request POST \
  "https://api.cloudflare.com/client/v4/zones/$CLOUDFLARE_ZONE_ID/rulesets" \
  --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
  --json @deploy/cloudflare-cache-rules.json
```
