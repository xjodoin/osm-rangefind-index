import { createHash } from "node:crypto";

export const ROAD_CATALOG_FORMAT = "rangefind-route-catalog-v1";
export const ROAD_GRAPH_FORMAT = "rfroutegraph-v1";
export const ROAD_SOURCE_FORMAT = "rfroutesrc-v6";
export const ROAD_PIPELINE_SCHEMA = 1;

const SUPPORTED_PROFILES = new Set(["car", "bike", "foot"]);

function positiveInteger(value, fallback, minimum = 1) {
  const number = Number(value);
  return Number.isFinite(number) && number >= minimum ? Math.floor(number) : fallback;
}

export function normalizeRoadIndexConfig(value) {
  if (!value || value.enabled === false) return { enabled: false, profiles: [] };
  const profiles = [...new Set((value.profiles || ["car"]).map(String))];
  for (const profile of profiles) {
    if (!SUPPORTED_PROFILES.has(profile)) {
      throw new Error(`Unsupported road-index profile "${profile}" (expected car, bike, or foot).`);
    }
  }
  if (!profiles.length) throw new Error("roadIndexes.profiles must contain at least one profile.");
  return {
    enabled: true,
    profiles,
    turnCosts: value.turnCosts !== false,
    leafNodes: positiveInteger(value.leafNodes, 1280, 64),
    fanout: positiveInteger(value.fanout, 8, 2),
    topMaxCells: positiveInteger(value.topMaxCells, 8, 2),
    packBytes: positiveInteger(value.packBytes, 2 * 1024 * 1024, 64 * 1024),
    maxShards: positiveInteger(value.maxShards, 8),
    targetPbfBytesPerShard: positiveInteger(value.targetPbfBytesPerShard, 512 * 1024 * 1024),
    timeBuckets: Array.isArray(value.timeBuckets) ? value.timeBuckets : []
  };
}

export function roadBuildOptions(config, pbfBytes = 0) {
  const estimatedShards = Math.max(1, Math.ceil(Number(pbfBytes || 0) / config.targetPbfBytesPerShard));
  return {
    leafNodes: config.leafNodes,
    fanout: config.fanout,
    topMaxCells: config.topMaxCells,
    packBytes: config.packBytes,
    shards: Math.min(config.maxShards, estimatedShards),
    timeBuckets: config.timeBuckets
  };
}

function digest(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

export function roadSourceFingerprint({ pbfIdentity, profile, turnCosts, rangefindVersion }) {
  return digest({
    schema: ROAD_PIPELINE_SCHEMA,
    sourceFormat: ROAD_SOURCE_FORMAT,
    pbfIdentity,
    profile,
    turnCosts,
    rangefindVersion
  });
}

export function roadIndexFingerprint({ sourceFingerprint, buildOptions, rangefindVersion }) {
  return digest({
    schema: ROAD_PIPELINE_SCHEMA,
    graphFormat: ROAD_GRAPH_FORMAT,
    sourceFingerprint,
    buildOptions,
    rangefindVersion
  });
}

export function roadProfileIdentity({ region, state, config, profile, rangefindVersion }) {
  const entry = state.regions?.[region.id] || {};
  const pbfIdentity = region.pinned
    ? region.pbfIdentity
    : entry.pbfLastModified || "";
  if (!pbfIdentity) return null;
  const sourceFingerprint = roadSourceFingerprint({
    pbfIdentity,
    profile,
    turnCosts: config.turnCosts,
    rangefindVersion
  });
  const buildOptions = roadBuildOptions(config, entry.pbfBytes || 0);
  return {
    pbfIdentity,
    sourceFingerprint,
    buildOptions,
    fingerprint: roadIndexFingerprint({ sourceFingerprint, buildOptions, rangefindVersion })
  };
}

export function roadIndexesCurrent({ region, state, config, rangefindVersion, requireUploaded }) {
  if (!config.enabled) return true;
  return config.profiles.every(profile => {
    const identity = roadProfileIdentity({ region, state, config, profile, rangefindVersion });
    if (!identity) return false;
    const profileState = state.regions?.[region.id]?.roadIndexes?.[profile] || {};
    return requireUploaded
      ? profileState.uploadedFingerprint === identity.fingerprint
      : profileState.builtFingerprint === identity.fingerprint;
  });
}

export function buildRoadCatalog({ regions, state, config, requireUploaded = true }) {
  const indexes = [];
  if (config.enabled) {
    for (const region of regions) {
      for (const profile of config.profiles) {
        const profileState = state.regions?.[region.id]?.roadIndexes?.[profile] || {};
        const fingerprint = requireUploaded
          ? profileState.uploadedFingerprint
          : profileState.builtFingerprint;
        if (!fingerprint || fingerprint !== profileState.builtFingerprint || !profileState.manifest) continue;
        indexes.push({
          region: region.id,
          profile,
          base: `routes/${profile}/${region.id}/`,
          bbox: region.bbox || null,
          ...(region.groups?.length ? { groups: region.groups } : {}),
          ...(region.countryCodes?.length ? { countryCodes: region.countryCodes } : {}),
          ...(region.subdivisionCodes?.length ? { subdivisionCodes: region.subdivisionCodes } : {}),
          source: {
            ...(region.geofabrik
              ? { url: `https://download.geofabrik.de/${region.geofabrik}-latest.osm.pbf` }
              : {}),
            ...(state.regions?.[region.id]?.pbfLastModified
              ? { dataVersion: state.regions[region.id].pbfLastModified }
              : {})
          },
          manifest: profileState.manifest
        });
      }
    }
  }
  return {
    format: ROAD_CATALOG_FORMAT,
    routeGraphFormat: ROAD_GRAPH_FORMAT,
    coverage: "single-region",
    requiresAllStopsInOneRegion: true,
    profiles: config.profiles,
    indexes
  };
}
