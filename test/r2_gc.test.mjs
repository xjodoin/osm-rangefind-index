import assert from "node:assert/strict";
import test from "node:test";
import {
  collectManifestProtections,
  createProtections,
  generationManifestPath,
  isProtectedObject,
  updateCandidateState
} from "../scripts/lib/r2_gc.mjs";

test("collects exact paths, content-addressed names, and directory prefixes", () => {
  const protections = createProtections();
  collectManifestProtections({
    directory: {
      root: "terms/directory-root.abc.bin.gz",
      pages: "terms/directory-pages/",
      pack_table: ["0000.current.bin.gz"]
    },
    docs: { pointers: { file: "docs/pointers/current.bin" } }
  }, "shards/quebec/manifest.min.json", protections);

  assert.equal(isProtectedObject("shards/quebec/terms/directory-root.abc.bin.gz", protections), true);
  assert.equal(isProtectedObject("shards/quebec/terms/packs/0000.current.bin.gz", protections), true);
  assert.equal(isProtectedObject("shards/quebec/terms/directory-pages/0000.bin.gz", protections), true);
  assert.equal(isProtectedObject("shards/quebec/docs/pointers/current.bin", protections), true);
  assert.equal(isProtectedObject("shards/quebec/docs/packs/old.bin", protections), false);
});

test("requires a continuously unreferenced grace period before deletion", () => {
  const protections = createProtections();
  protections.basenames.add("live.bin");
  const objects = [
    { path: "shards/q/docs/packs/live.bin", size: 10, modTime: "2020-01-01T00:00:00Z" },
    { path: "shards/q/docs/packs/orphan.bin", size: 20, modTime: "2020-01-01T00:00:00Z" },
    { path: "manifest.min.json", size: 30, modTime: "2020-01-01T00:00:00Z" }
  ];
  const first = updateCandidateState({
    objects,
    protections,
    previous: {},
    now: "2026-07-01T00:00:00Z",
    graceMs: 7 * 86400_000
  });
  assert.deepEqual(first.eligible, []);
  assert.equal(first.summary.pendingObjects, 1);

  const second = updateCandidateState({
    objects,
    protections,
    previous: first.candidates,
    now: "2026-07-08T00:00:01Z",
    graceMs: 7 * 86400_000
  });
  assert.deepEqual(second.eligible, ["shards/q/docs/packs/orphan.bin"]);
  assert.equal(second.summary.eligibleBytes, 20);
});

test("drops a candidate as soon as a manifest references it again", () => {
  const protections = createProtections();
  protections.basenames.add("restored.bin.gz");
  const result = updateCandidateState({
    objects: [{ path: "text-routing/packs/restored.bin.gz", size: 99 }],
    protections,
    previous: { "text-routing/packs/restored.bin.gz": { firstSeenAt: "2026-01-01T00:00:00Z" } },
    now: "2026-07-10T00:00:00Z",
    graceMs: 86400_000
  });
  assert.deepEqual(result.candidates, {});
  assert.deepEqual(result.eligible, []);
});

test("resolves both generation manifest declaration styles", () => {
  assert.equal(
    generationManifestPath("shards/quebec/", { path: "gen-0001/", manifest: "gen-0001/manifest.min.json" }),
    "shards/quebec/gen-0001/manifest.min.json"
  );
  assert.equal(
    generationManifestPath("shards/quebec/", { path: "gen-0001/", manifest: "manifest.min.json" }),
    "shards/quebec/gen-0001/manifest.min.json"
  );
  assert.equal(
    generationManifestPath("shards/quebec/", { path: "../other/", manifest: "manifest.min.json" }),
    null
  );
});
