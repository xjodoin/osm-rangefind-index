# Rollout: federated text routing (rangefind 0.3.2)

rangefind 0.3.2 adds a root-level text-routing directory: clients look up
their query terms once and open only the shards that contain them, instead
of fanning a text query out to every shard (~1 200 requests / 12–18 MB cold
at 67 shards). The pipeline builds and publishes the artifact automatically
once the dependency is updated — commit `d07f297` wired it in.

## Server update (one time)

```bash
cd /srv/osm-rangefind-index
git stash push --include-untracked -m pre-text-routing-rollout
git pull --ff-only
npm ci                 # installs the locked rangefind 0.3.2 release
node --check scripts/update_index.mjs
```

The stash is a reversible backup of files that were deployed manually before
their commits reached `main`; do not pop it because those changes are already
upstream. `.env` and `work/` are ignored and remain in place. No config changes
are needed. The next scheduled run picks everything up.

## What the first routing-enabled run does

1. **Term-set backfill.** Before starting new shard builds, the run processes
   published shards that were cleaned locally (their term packs were
   reclaimed). It downloads just `manifest*.json` + `terms/**` for each
   published shard from R2 and writes a small sidecar to
   `work/term-sets/<region>.terms.gz`. This is a one-time cost — expect the
   run to be noticeably longer than usual. Later runs regenerate a sidecar
   only for shards rebuilt or delta-updated in that run, right after the
   build and before cleanup.
2. **Routing merge.** All sidecars merge into `work/public/rangefind/
   text-routing/` and the block lands in the root manifest. The merge is
   fingerprinted over the published shard set, so unchanged runs reuse it.
3. **Reserved finalization.** Shard builds stop with 30 minutes reserved for
   the routing merge and root publication. An interrupted shard build retains
   its Rangefind checkpoints and resumes next run.
4. **Publish ordering.** `text-routing/` uploads before the manifests flip
   (files are content-addressed, so clients never see a root that references
   missing objects). Old routing files linger until a `--prune` run.

Everything is deadline-aware and fail-open: if term sets are incomplete or
time runs out, the root manifest publishes **without** the routing block and
clients simply fan out as before. Nothing blocks shard publication.

## Verify after the run

```bash
# Root manifest carries the routing block
curl -s https://osm.rangefind.dev/manifest.json | node -e \
  "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{const m=JSON.parse(d);
   console.log(m.text_routing?.format, m.text_routing?.term_count, m.text_routing?.shard_ids?.length)})"

# A shard-local query opens few shards (stats.shardsQueried, stats.textRouting)
npx rangefind search https://osm.rangefind.dev/ "calgary tower" --json | \
  node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{const r=JSON.parse(d);
   console.log(r.stats.shardsQueried, JSON.stringify(r.stats.textRouting))})"
```

Expected: `rftextroute-v1`, a nine-digit term count, shard count matching the
root, and `shardsQueried` far below the shard total for place-name queries
(`textRouting.fallback` appears only for misspelled/unroutable queries, which
correctly fan out).

## Escape hatches

- `--no-text-routing` on `update_index.mjs` skips sidecars and the merge for
  that run (the root publishes fan-out).
- Deleting `work/term-sets/` + the `textRoutingFingerprint`/
  `termSetFingerprint` entries in `work/state.json` forces a full backfill.
- Routing must cover **exactly** the published shard set; the pipeline
  enforces this, so a stale artifact is never referenced by a fresh root.

## Cloudflare cache rules (independent, big cold-latency win)

The production cache rule is already active: content-addressed
`.bin`/`.bin.gz` objects return `cf-cache-status: HIT` after warm-up, while
manifests and JSON metadata remain uncached so publishes stay atomic. On a new
zone, apply `deploy/cloudflare-cache-rules.json` once with a token that has
Cache Settings Edit:

```bash
curl --request POST \
  "https://api.cloudflare.com/client/v4/zones/$CLOUDFLARE_ZONE_ID/rulesets" \
  --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
  --json @deploy/cloudflare-cache-rules.json
```

Check with `curl -sI https://osm.rangefind.dev/<any .bin.gz> | grep cf-cache-status`
— HIT after the second request.
