import assert from "node:assert/strict";
import test from "node:test";
import {
  buildContentFingerprint,
  buildShardFingerprint,
  previouslyBuiltContentFingerprint,
  selectRootCandidates
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
    previouslyBuiltContentFingerprint({ builtFingerprint: content }),
    content
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
