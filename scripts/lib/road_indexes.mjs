import { createHash } from "node:crypto";

export const ROAD_CATALOG_FORMAT = "rangefind-route-catalog-v1";
export const ROAD_GRAPH_FORMAT = "rfroutegraph-v1";
export const ROAD_PORTAL_FORMAT = "rfrouteportals-v1";
export const ROAD_SOURCE_FORMAT = "rfroutesrc-v8";
export const ROAD_PIPELINE_SCHEMA = 2;

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

function longitudeIntervals(bbox, margin = 0) {
  const min = Math.max(-180, bbox[1] - margin);
  const max = Math.min(180, bbox[3] + margin);
  if (bbox[1] <= bbox[3]) return [[min, max]];
  return [[-180, max], [min, 180]];
}

function expandedCoverageBbox(bbox, margin) {
  const normalizeLon = value => {
    let normalized = value;
    while (normalized < -180) normalized += 360;
    while (normalized > 180) normalized -= 360;
    return normalized;
  };
  return [
    Math.max(-90, bbox[0] - margin),
    normalizeLon(bbox[1] - margin),
    Math.min(90, bbox[2] + margin),
    normalizeLon(bbox[3] + margin)
  ];
}

function bboxIntersectionArea(a, b) {
  const minLat = Math.max(a[0], b[0]);
  const maxLat = Math.min(a[2], b[2]);
  if (minLat >= maxLat) return 0;
  let lonWidth = 0;
  for (const ai of longitudeIntervals(a)) for (const bi of longitudeIntervals(b)) {
    lonWidth += Math.max(0, Math.min(ai[1], bi[1]) - Math.max(ai[0], bi[0]));
  }
  return (maxLat - minLat) * lonWidth;
}

function bboxArea(bbox) {
  const lonWidth = longitudeIntervals(bbox).reduce((sum, interval) => sum + interval[1] - interval[0], 0);
  return Math.max(0, bbox[2] - bbox[0]) * lonWidth;
}

/** Coverage-bbox candidates only; shared OSM ids in the portal sidecars are
 * the authoritative adjacency check. The small margin tolerates independently
 * rounded upstream coverage polygons without inventing a road connection. */
export function roadFederationNeighbors(regions, marginDegrees = 0.02) {
  const result = new Map(regions.map(region => [region.id, []]));
  for (let left = 0; left < regions.length; left++) {
    const a = regions[left];
    if (!a.bbox) continue;
    for (let right = left + 1; right < regions.length; right++) {
      const b = regions[right];
      if (!b.bbox) continue;
      const latOverlaps = a.bbox[0] - marginDegrees <= b.bbox[2]
        && b.bbox[0] - marginDegrees <= a.bbox[2];
      if (!latOverlaps) continue;
      const lonOverlaps = longitudeIntervals(a.bbox, marginDegrees).some(ai => (
        longitudeIntervals(b.bbox, marginDegrees).some(bi => ai[0] <= bi[1] && bi[0] <= ai[1])
      ));
      if (!lonOverlaps) continue;
      // Near-duplicate parent/combined extracts are alternate coverage, not
      // border neighbors. A query whose endpoints share one of them is routed
      // wholly inside that graph; publishing millions of interior "portals"
      // between two copies would waste build time and R2 space.
      const overlapRatio = bboxIntersectionArea(a.bbox, b.bbox) / Math.max(1e-9, Math.min(bboxArea(a.bbox), bboxArea(b.bbox)));
      if (overlapRatio >= 0.8) continue;
      result.get(a.id).push({ id: b.id, bbox: expandedCoverageBbox(b.bbox, marginDegrees) });
      result.get(b.id).push({ id: a.id, bbox: expandedCoverageBbox(a.bbox, marginDegrees) });
    }
  }
  for (const neighbors of result.values()) neighbors.sort((a, b) => a.id.localeCompare(b.id));
  return result;
}

export function roadSourceFingerprint({ pbfIdentity, profile, turnCosts, rangefindVersion, federationNeighbors = [] }) {
  return digest({
    schema: ROAD_PIPELINE_SCHEMA,
    sourceFormat: ROAD_SOURCE_FORMAT,
    pbfIdentity,
    profile,
    turnCosts,
    federationNeighbors,
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
    federationNeighbors: region.federationNeighbors || [],
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
          portals: profileState.manifest.portals || null,
          neighbors: (region.federationNeighbors || []).map(neighbor => neighbor.id),
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
    coverage: "federated-regions",
    requiresAllStopsInOneRegion: false,
    portalFormat: ROAD_PORTAL_FORMAT,
    profiles: config.profiles,
    indexes
  };
}
