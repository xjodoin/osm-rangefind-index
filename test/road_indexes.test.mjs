import assert from "node:assert/strict";
import test from "node:test";
import {
  buildRoadCatalog,
  normalizeRoadIndexConfig,
  roadBuildOptions,
  roadIndexesCurrent,
  roadProfileIdentity
} from "../scripts/lib/road_indexes.mjs";

const config = normalizeRoadIndexConfig({ enabled: true, profiles: ["car"], maxShards: 8 });
const region = {
  id: "quebec",
  geofabrik: "north-america/canada/quebec",
  bbox: [44.9, -79.8, 62.6, -57.1],
  groups: ["canada"],
  countryCodes: ["CA"],
  subdivisionCodes: ["CA-QC"]
};

test("road index configuration is generic and shard count scales with source size", () => {
  assert.deepEqual(config.profiles, ["car"]);
  assert.equal(roadBuildOptions(config, 100 * 1024 ** 2).shards, 1);
  assert.equal(roadBuildOptions(config, 2.1 * 1024 ** 3).shards, 5);
  assert.equal(roadBuildOptions(config, 20 * 1024 ** 3).shards, 8);
  assert.throws(() => normalizeRoadIndexConfig({ profiles: ["horse"] }), /car, bike, or foot/u);
});

test("road identities invalidate extraction and builds on their real inputs", () => {
  const state = { regions: { quebec: { pbfLastModified: "v1", pbfBytes: 2 * 1024 ** 3 } } };
  const before = roadProfileIdentity({ region, state, config, profile: "car", rangefindVersion: "0.4.11" });
  state.regions.quebec.pbfLastModified = "v2";
  const after = roadProfileIdentity({ region, state, config, profile: "car", rangefindVersion: "0.4.11" });
  assert.notEqual(before.sourceFingerprint, after.sourceFingerprint);
  assert.notEqual(before.fingerprint, after.fingerprint);
  state.regions.quebec.roadIndexes = { car: { builtFingerprint: after.fingerprint, uploadedFingerprint: after.fingerprint } };
  assert.equal(roadIndexesCurrent({ region, state, config, rangefindVersion: "0.4.11", requireUploaded: true }), true);
});

test("catalog exposes only durable indexes and the single-region contract", () => {
  const state = { regions: { quebec: {
    pbfLastModified: "2026-08-08",
    roadIndexes: { car: {
      builtFingerprint: "current",
      uploadedFingerprint: "current",
      manifest: { format: "rfroutegraph-v1", profile: "car", nodes: 10, edges: 20 }
    } }
  } } };
  const catalog = buildRoadCatalog({ regions: [region], state, config });
  assert.equal(catalog.requiresAllStopsInOneRegion, true);
  assert.equal(catalog.indexes.length, 1);
  assert.equal(catalog.indexes[0].base, "routes/car/quebec/");
  state.regions.quebec.roadIndexes.car.uploadedFingerprint = "old";
  assert.equal(buildRoadCatalog({ regions: [region], state, config }).indexes.length, 0);
});
