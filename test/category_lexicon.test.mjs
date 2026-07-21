import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  loadCategoryLexiconModule,
  mergeShardTypeVocabulary
} from "../scripts/lib/category_lexicon.mjs";

test("category lexicon module resolves a builder (npm dependency or sibling checkout)", async () => {
  const module = await loadCategoryLexiconModule();
  // Skip-friendly: neither source carrying the builder is a valid state on
  // machines without the sibling checkout and with rangefind <= 0.3.7.
  if (!module) return;
  const artifact = module.buildCategoryLexiconArtifact([{ value: "cinema", n: 5 }]);
  assert.equal(artifact.facet, "type");
  assert.deepEqual(artifact.types, ["cinema"]);
  assert.equal(artifact.aliases["movie theater"], "cinema");
});

test("shard vocabulary merge caches by fingerprint and survives unreadable shards", async () => {
  const work = await mkdtemp(join(tmpdir(), "osm-lexicon-cache-"));
  try {
    const cachePath = join(work, "cache.json");
    const logs = [];
    // No local dir and an unreachable remote: the shard is skipped with a
    // log line instead of failing the publish.
    const empty = await mergeShardTypeVocabulary({
      shards: [{ id: "nowhere", cacheKey: "fp1", remoteBase: "http://127.0.0.1:9/" }],
      cachePath,
      log: line => logs.push(line)
    });
    assert.deepEqual(empty, []);
    assert.ok(logs.some(line => line.includes("nowhere unreadable")));

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
  } finally {
    await rm(work, { recursive: true, force: true });
  }
});
