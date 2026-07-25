# Rollout: range-addressed root suggest routing

Rangefind `rflexicon-v2` replaces the two corpus-sized suggest-routing roots
with the same lookup shape used by text routing:

1. fetch the small lexicon-segment directory root and one directory page;
2. byte-range fetch only the packed lexicon segment intersecting the prefix;
3. follow its direct pointers with byte-range requests to the required
   `authority/packs/` members.

The existing 310 `work/suggest-sets/*.suggest.gz` sidecars remain valid. This
rollout does not require PBF extraction, scoring-stat regeneration, or shard
rebuilds.

## Deploy

1. Publish the Rangefind release containing `rflexicon-v2`.
2. On `ns3222652`, update this repository and install the lockfile:

   ```sh
   git pull --ff-only
   npm ci
   ```

3. Run a protected finalizer:

   ```sh
   INDEX_LOG_FILE=logs/suggest-routing-range-rollout.log \
     scripts/nightly.sh --finalize-only --prune --max-hours 8
   ```

The updater's suggest-routing schema fingerprint forces a rebuild from the
checkpointed sidecars even though the 310 shard fingerprints are unchanged.
It publishes the immutable `authority/` objects before flipping the two root
manifests. With `--prune`, superseded routing objects are removed only after
both manifests are live.

## Verify

- `manifest.min.json` has `suggest_routing.authority.autocomplete.format` set
  to `rflexicon-v2`.
- `suggest_routing.authority.directory` is absent.
- `autocomplete.root.bytes` is only the hot-prefix metadata, and
  `autocomplete.segments.directory` plus
  `autocomplete.segments.pack_dir` are present.
- A cold locality lookup or long-prefix suggestion requests:
  - the lexicon root and segment-directory metadata;
  - a `206` range from `authority/lexicon-packs/`;
  - one or more `206` ranges from `authority/packs/`.
- It does not download a monolithic lexicon root or an authority directory.
- Repeating the same ranges returns `cf-cache-status: HIT`.
- `node scripts/osm_remote_bench.mjs --lanes=suggest,suggest-address` reports
  `stats.suggestRouting: "root-authority"` with no 310-shard fan-out.
- `node scripts/osm_mobile_bench.mjs` confirms the cold and warm phone paths.
- A category plus locality query such as `cinema laval` resolves Laval and
  starts the routed search without a multi-hundred-megabyte transfer.

If finalization is interrupted, rerun the same `--finalize-only --prune`
command. The sidecars and worker checkpoints are reusable.
`--no-suggest-routing` remains the emergency escape hatch; clients then fail
open to shard fan-out.
