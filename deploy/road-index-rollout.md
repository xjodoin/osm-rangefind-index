# Rollout: regional route graphs and itinerary planning

This rollout adds `rfroutegraph-v1` objects under `routes/<profile>/<region>/`
and the mutable discovery root `routes/catalog.json`. It does not change the
place-search manifest or any existing search pack.

## First production build

The existing production service normally carries `--force-stats`. Do not use
that command for initial road backfill: it would do valid but unnecessary
search work after the already-completed build. Deploy the code and dependency,
then launch the dedicated resumable lane:

```sh
cd /srv/osm-rangefind-index
git pull --ff-only
npm install
INDEX_LOG_FILE=logs/road-indexing-production.log \
  scripts/nightly.sh --roads-only --prune
```

`--roads-only` does not prepare OpenAddresses, extract place documents,
regenerate scoring stats, build search shards, or flip the search root. It
uses the normal process lock, disk reserve, deadline, direct R2 client, retry
policy, state file, and status page. It resumes at region/profile boundaries.
Use `--deadline HH:MM` or `--max-hours N` exactly as for the regular updater.

Production enables `car`, `bike`, and `foot`. The initial backfill therefore
builds three independent graphs per region; budget roughly three times the
profile-processing work of a car-only rollout (actual output bytes vary by
profile).

## Verification

```sh
curl -fsS https://osm.rangefind.dev/routes/catalog.json | node -e '
  let s=""; process.stdin.on("data", c => s += c).on("end", () => {
    const c = JSON.parse(s);
    console.log(c.format, c.coverage, c.indexes.length);
    if (c.format !== "rangefind-route-catalog-v1") process.exit(1);
  });'

base=$(curl -fsS https://osm.rangefind.dev/routes/catalog.json | node -e '
  let s=""; process.stdin.on("data", c => s += c).on("end", () => {
    const q = JSON.parse(s).indexes.find(x => x.region === "quebec" && x.profile === "car");
    if (!q) process.exit(1); process.stdout.write(q.base);
  });')
curl -fsSI "https://osm.rangefind.dev/${base}manifest.json"
```

Expected results:

- the catalog format is `rangefind-route-catalog-v1`;
- every entry's manifest format is `rfroutegraph-v1`;
- catalog and manifests are uncached mutable JSON;
- `root.*.bin.gz` and pack range requests return `206` and are cached as
  immutable objects;
- `status.json.roadIndexes.catalogEntries` grows throughout the rollout;
- normal search continues serving its existing root throughout.

Run `npm run benchmark:roads` on the deployed checkout before starting the
planet backfill if the Rangefind package or Node runtime changed.

## Ongoing updates and rollback

After the first backfill, ordinary updater runs refresh the regional search
and road artifacts from the same changed PBF. `--no-road-indexes` temporarily
disables road work without deleting the published catalog. To stop a backfill,
stop the service normally; completed uploads and source/build fingerprints
remain resumable. The published route lane is additive and does not alter the
search root, so rollback requires no search-index change. Withdrawing already
published discovery entries should be done as an explicit empty-catalog
publication; the manifest-aware GC releases removed prefixes only after its
normal grace period.
