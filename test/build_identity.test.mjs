import assert from "node:assert/strict";
import test from "node:test";
import {
  buildContentFingerprint,
  buildShardFingerprint,
  previouslyBuiltBuilderVersion,
  previouslyBuiltContentFingerprint,
  selectRootCandidates,
  shouldReuseFrozenStats
} from "../scripts/lib/build_identity.mjs";

test("builder upgrades invalidate shards without invalidating unchanged routing content", () => {
  const content = buildContentFingerprint({
    entry: { extractIdentity: "pbf-v1", extractSchema: 9, docs: 42 },
    statsFingerprint: "100:200",
    overrides: { geoLeafSize: 256 }
  });
  assert.equal(
    buildShardFingerprint({ rangefindVersion: "0.3.15", contentFingerprint: content }),
    `rangefind@0.3.15:${content}`
  );
  assert.equal(
    buildShardFingerprint({
      rangefindVersion: "0.3.18",
      builderVersion: "0.3.16",
      contentFingerprint: content
    }),
    `rangefind@0.3.16:${content}`
  );
  assert.equal(
    previouslyBuiltContentFingerprint({ builtFingerprint: content }),
    content
  );
  assert.equal(
    previouslyBuiltBuilderVersion({ builtRangefindVersion: "0.3.16" }),
    "0.3.16"
  );
  assert.equal(
    previouslyBuiltBuilderVersion({
      builtRangefindVersion: "0.3.18",
      builtRangefindBuilderVersion: "0.3.16"
    }),
    "0.3.16"
  );
});

test("region-scoped production publication retains the complete root", () => {
  const selected = [{ id: "quebec" }];
  const all = [{ id: "quebec" }, { id: "ontario" }];
  assert.equal(selectRootCandidates({
    selected,
    all,
    regionScoped: true,
    partial: false
  }), all);
  assert.equal(selectRootCandidates({
    selected,
    all,
    regionScoped: true,
    partial: true
  }), selected);
});

test("region-scoped production builds reuse planet scoring stats", () => {
  assert.equal(shouldReuseFrozenStats({ regionScoped: true, partial: false }), true);
  assert.equal(shouldReuseFrozenStats({ regionScoped: true, partial: true }), false);
  assert.equal(shouldReuseFrozenStats({ regionScoped: false, partial: false }), false);
});
