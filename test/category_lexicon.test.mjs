import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "../scripts/lib/category_lexicon.mjs";

test("installed rangefind exports the category lexicon builder", async () => {
  const module = await loadCategoryLexiconModule();
  assert.ok(module, "installed rangefind must export the category lexicon builder");
  const artifact = module.buildCategoryLexiconArtifact([{ value: "cinema", n: 5 }]);
  assert.equal(artifact.facet, "type");
  assert.deepEqual(artifact.types, ["cinema"]);
  assert.equal(artifact.aliases["movie theater"], "cinema");
});

test("shard vocabulary merge fails closed on unreadable shards and caches by fingerprint", async () => {
  const work = await mkdtemp(join(tmpdir(), "osm-lexicon-cache-"));
  try {
    const cachePath = join(work, "cache.json");
    const logs = [];
    // A missing shard makes the whole artifact unsafe: callers can then
    // publish no block and let runtimes use the bundled fallback vocabulary.
    await assert.rejects(
      mergeShardTypeVocabulary({
        shards: [{ id: "nowhere", cacheKey: "fp1", remoteBase: "http://127.0.0.1:9/" }],
        cachePath,
        log: line => logs.push(line)
      }),
      /refusing a partial artifact/u
    );
    assert.ok(logs.some(line => line.includes("nowhere")));

    // A cache seeded for the same fingerprint short-circuits any fetch.
    const { writeFileSync } = await import("node:fs");
    writeFileSync(cachePath, JSON.stringify({
      shards: { quebec: { key: "fp2", types: { cinema: 12, bakery: 3 } } }
    }));
    const cached = await mergeShardTypeVocabulary({
      shards: [{ id: "quebec", cacheKey: "fp2", remoteBase: "http://127.0.0.1:9/" }],
      cachePath
    });
    assert.deepEqual(
      cached.sort((a, b) => a.value.localeCompare(b.value)),
      [{ value: "bakery", n: 3 }, { value: "cinema", n: 12 }]
    );
    // The cache file was not clobbered by the read-only run.
    const persisted = JSON.parse(await readFile(cachePath, "utf8"));
    assert.equal(persisted.shards.quebec.key, "fp2");

    // Root-only refreshes do not have the private build fingerprint. They
    // validate the cached entry against the shard manifest identity and can
    // skip the facet read while still noticing future same-total rebuilds.
    let reads = 0;
    const discovered = await mergeShardTypeVocabulary({
      shards: [{ id: "quebec", remoteBase: "https://unused.invalid/" }],
      cachePath,
      readShard: async (_source, cached) => {
        reads++;
        assert.equal(cached.key, "fp2");
        return { cacheKey: "fp2", counts: cached.types, fetched: false, total: 15 };
      }
    });
    assert.equal(reads, 1);
    assert.deepEqual(discovered.sort((a, b) => a.value.localeCompare(b.value)), [
      { value: "bakery", n: 3 },
      { value: "cinema", n: 12 }
    ]);

    await assert.rejects(
      mergeShardTypeVocabulary({
        shards: [{ id: "quebec", expectedTotal: 16, remoteBase: "https://unused.invalid/" }],
        cachePath,
        readShard: async (_source, cached) => ({
          cacheKey: "fp2",
          counts: cached.types,
          fetched: false,
          total: 15
        })
      }),
      /does not match root total/u
    );
  } finally {
    await rm(work, { recursive: true, force: true });
  }
});
