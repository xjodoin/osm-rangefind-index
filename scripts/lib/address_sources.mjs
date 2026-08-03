import {
  closeSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { createHash } from "node:crypto";
import { basename, dirname, extname, join, resolve } from "node:path";

function clean(value) {
  return String(value ?? "").trim();
}

function safeId(value) {
  const id = clean(value).toLowerCase();
  if (!/^[a-z0-9][a-z0-9._-]*$/u.test(id)) throw new Error(`Invalid address source id: ${JSON.stringify(value)}`);
  return id;
}

function readJson(path, fallback = null) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return fallback;
  }
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function configFingerprint(source) {
  const relevant = {
    id: source.id,
    name: source.name,
    url: source.url,
    website: source.website,
    format: source.format,
    compression: source.compression,
    archiveEntry: source.archiveEntry,
    delimiter: source.delimiter,
    header: source.header,
    defaults: source.defaults,
    mapping: source.mapping,
    includeAddresses: source.includeAddresses,
    includeUnits: source.includeUnits,
    includeCountry: source.includeCountry,
    partition: source.partition,
    appliesTo: source.appliesTo,
    attribution: source.attribution,
    license: source.license,
    licenseUrl: source.licenseUrl
  };
  return createHash("sha256").update(stableJson(relevant)).digest("hex");
}

function validateSource(raw) {
  const source = {
    ...raw,
    id: safeId(raw.id),
    url: clean(raw.url),
    name: clean(raw.name) || safeId(raw.id),
    format: clean(raw.format || "delimited"),
    enabled: raw.enabled !== false
  };
  if (!source.url) throw new Error(`Address source ${source.id} has no URL.`);
  if (source.format !== "delimited") {
    throw new Error(`Address source ${source.id}: production JSON config currently supports format=delimited; code adapters can use the Rangefind async-iterator API.`);
  }
  if (!source.mapping || typeof source.mapping !== "object") {
    throw new Error(`Address source ${source.id} has no field mapping.`);
  }
  if (source.partition && source.partition.mode !== "spatial") {
    if (!clean(source.partition.field)) throw new Error(`Address source ${source.id} partition has no canonical field.`);
    if (!source.partition.regions || typeof source.partition.regions !== "object") {
      throw new Error(`Address source ${source.id} partition has no region mapping.`);
    }
  }
  return source;
}

export function loadAddressSourcesConfig(projectRoot, explicitPath = process.env.ADDRESS_SOURCES_CONFIG) {
  const path = resolve(explicitPath || join(projectRoot, "address-sources.json"));
  if (!existsSync(path)) return { path, sources: [] };
  const config = readJson(path, null);
  if (!Array.isArray(config?.sources)) throw new Error(`${path} must contain { "sources": [] }.`);
  const sources = config.sources.map(validateSource).filter(source => source.enabled);
  const ids = new Set();
  for (const source of sources) {
    if (ids.has(source.id)) throw new Error(`Duplicate address source id: ${source.id}`);
    ids.add(source.id);
  }
  return { path, sources };
}

export function addressSourcesForRegion(sources, region) {
  return sources.filter(source => {
    const applies = source.appliesTo || {};
    const regionIds = Array.isArray(applies.regions) ? applies.regions.map(String) : [];
    const groups = Array.isArray(applies.groups) ? applies.groups.map(String) : [];
    const excludedRegionIds = Array.isArray(applies.excludeRegions) ? applies.excludeRegions.map(String) : [];
    const excludedGroups = Array.isArray(applies.excludeGroups) ? applies.excludeGroups.map(String) : [];
    if (regionIds.length && !regionIds.includes(region.id)) return false;
    if (groups.length && !groups.some(group => region.groups?.includes(group))) return false;
    if (excludedRegionIds.includes(region.id)) return false;
    if (excludedGroups.some(group => region.groups?.includes(group))) return false;
    if (source.partition?.regions && !Object.hasOwn(source.partition.regions, region.id)) return false;
    return true;
  });
}

function extensionFor(source) {
  const urlName = basename(new URL(source.url).pathname);
  const extension = extname(urlName);
  return extension && extension.length <= 8 ? extension : ".data";
}

function remoteIdentity(response, source) {
  return {
    etag: clean(response.headers.get("etag")),
    lastModified: clean(response.headers.get("last-modified")),
    bytes: Math.max(0, Number(response.headers.get("content-length") || 0)),
    config: configFingerprint(source)
  };
}

function sameRemoteIdentity(left, right) {
  if (!left || !right || left.config !== right.config) return false;
  if (left.etag && right.etag) return left.etag === right.etag;
  if (left.lastModified && right.lastModified) {
    return left.lastModified === right.lastModified && (!left.bytes || !right.bytes || left.bytes === right.bytes);
  }
  return false;
}

async function download(response, path) {
  const partial = `${path}.download`;
  const file = createWriteStream(partial);
  await new Promise((resolveDone, reject) => {
    const reader = response.body.getReader();
    const fail = error => {
      file.destroy();
      reject(error);
    };
    file.once("error", fail);
    const pump = () => reader.read().then(({ done, value }) => {
      if (done) {
        file.end(resolveDone);
        return;
      }
      if (!file.write(Buffer.from(value))) file.once("drain", pump);
      else pump();
    }).catch(fail);
    pump();
  });
  renameSync(partial, path);
}

export async function prepareAddressSource(source, options) {
  const root = resolve(options.root);
  const dir = join(root, source.id);
  const path = join(dir, `source${extensionFor(source)}`);
  const metaPath = join(dir, "source.meta.json");
  mkdirSync(dir, { recursive: true });
  const head = await options.fetchSource(
    source.url,
    { method: "HEAD" },
    { timeoutMs: options.timeoutMs }
  );
  const prior = readJson(metaPath, null);
  let identity = head.ok ? remoteIdentity(head, source) : null;
  if (identity && existsSync(path) && sameRemoteIdentity(prior?.identity, identity)) {
    return { ...source, path, identity };
  }

  // Some public-data hosts reject HEAD. A streamed GET remains a generic
  // fallback; when it exposes validators matching the cache, cancel the body
  // without rewriting the local source.
  const response = await options.fetchSource(
      source.url,
      {},
      { timeoutMs: options.timeoutMs }
  );
  if (!response.ok) {
    throw new Error(`${source.id}: ${head.ok ? "GET" : `HEAD ${head.status}; GET`} ${source.url} → ${response.status}`);
  }
  identity = remoteIdentity(response, source);
  if (existsSync(path) && sameRemoteIdentity(prior?.identity, identity)) {
    await response.body?.cancel();
    return { ...source, path, identity };
  }
  options.log?.(`${source.id}: downloading address source (${identity.lastModified || identity.etag || "unknown version"})`);
  await download(response, path);
  writeFileSync(metaPath, JSON.stringify({
    id: source.id,
    url: source.url,
    identity,
    downloadedAt: new Date().toISOString(),
    localBytes: statSync(path).size
  }, null, 2));
  return { ...source, path, identity };
}

export function regionAddressSourceIdentity(preparedSources) {
  if (!preparedSources.length) return "";
  return createHash("sha256")
    .update(stableJson(preparedSources.map(source => ({ id: source.id, identity: source.identity }))))
    .digest("hex");
}

const SPATIAL_PARTITION_SCHEMA_VERSION = 1;
const GRID_DEGREES = 5;
const PARTITION_BUFFER_BYTES = 1024 * 1024;

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function gridLat(lat) {
  return clamp(Math.floor((lat + 90) / GRID_DEGREES), 0, Math.ceil(180 / GRID_DEGREES) - 1);
}

function gridLon(lon) {
  return clamp(Math.floor((lon + 180) / GRID_DEGREES), 0, Math.ceil(360 / GRID_DEGREES) - 1);
}

function bboxContains(bbox, lat, lon) {
  if (!bbox || lat < bbox[0] || lat > bbox[2]) return false;
  return bbox[1] <= bbox[3]
    ? lon >= bbox[1] && lon <= bbox[3]
    : lon >= bbox[1] || lon <= bbox[3];
}

function longitudeIntervals(bbox) {
  return bbox[1] <= bbox[3]
    ? [[bbox[1], bbox[3]]]
    : [[bbox[1], 180], [-180, bbox[3]]];
}

/** Build a bounded candidate grid for routing coordinates into shard bboxes. */
export function createRegionSpatialRouter(regions) {
  const usable = regions.filter(region => Array.isArray(region.bbox) && region.bbox.length === 4);
  const cells = new Map();
  for (const region of usable) {
    for (let latCell = gridLat(region.bbox[0]); latCell <= gridLat(region.bbox[2]); latCell++) {
      for (const [minLon, maxLon] of longitudeIntervals(region.bbox)) {
        for (let lonCell = gridLon(minLon); lonCell <= gridLon(maxLon); lonCell++) {
          const key = `${latCell}:${lonCell}`;
          if (!cells.has(key)) cells.set(key, []);
          cells.get(key).push(region);
        }
      }
    }
  }
  return {
    route(latValue, lonValue) {
      const lat = Number(latValue);
      const lon = Number(lonValue);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) return [];
      const candidates = cells.get(`${gridLat(lat)}:${gridLon(lon)}`) || [];
      return candidates.filter(region => bboxContains(region.bbox, lat, lon));
    }
  };
}

function createBufferedJsonlWriter(path) {
  const fd = openSync(path, "w");
  let parts = [];
  let bytes = 0;
  let closed = false;
  function flush() {
    if (!parts.length) return;
    const buffer = Buffer.from(parts.join(""));
    let offset = 0;
    while (offset < buffer.length) offset += writeSync(fd, buffer, offset, buffer.length - offset);
    parts = [];
    bytes = 0;
  }
  return {
    write(record) {
      const line = `${JSON.stringify(record)}\n`;
      parts.push(line);
      bytes += Buffer.byteLength(line);
      if (bytes >= PARTITION_BUFFER_BYTES) flush();
    },
    close() {
      if (closed) return;
      flush();
      closeSync(fd);
      closed = true;
    }
  };
}

/**
 * Stream a global provider once and materialize canonical per-shard JSONL
 * partitions. Overlapping shard coverage intentionally receives a copy in
 * every covering shard, matching how the underlying OSM extracts overlap.
 */
export async function partitionAddressSourceSpatially(source, options) {
  const regions = options.regions.filter(region => Array.isArray(region.bbox));
  const root = resolve(options.root || join(dirname(source.path), "partitions"));
  const metaPath = join(root, "partitions.meta.json");
  const routingIdentity = createHash("sha256").update(stableJson({
    schema: SPATIAL_PARTITION_SCHEMA_VERSION,
    source: source.identity,
    regions: regions.map(region => ({ id: region.id, bbox: region.bbox }))
  })).digest("hex");
  const prior = readJson(metaPath, null);
  if (prior?.identity === routingIdentity && prior?.regions
      && Object.entries(prior.regions).every(([id, count]) => !count || existsSync(join(root, `${id}.jsonl`)))) {
    return { ...prior, root };
  }

  const partial = `${root}.partial`;
  rmSync(partial, { recursive: true, force: true });
  mkdirSync(partial, { recursive: true });
  const router = createRegionSpatialRouter(regions);
  const writers = new Map();
  const counts = Object.fromEntries(regions.map(region => [region.id, 0]));
  const stats = { rowsRead: 0, normalized: 0, unmatched: 0, writes: 0 };
  try {
    const iterable = typeof source.records === "function" ? source.records() : source;
    for await (const raw of iterable) {
      stats.rowsRead++;
      const mapped = typeof source.normalize === "function" ? source.normalize(raw) : raw;
      const record = options.normalizeRecord(mapped, source.defaults);
      if (!record || (typeof source.filter === "function" && !source.filter(record, raw))) continue;
      stats.normalized++;
      const matches = router.route(record.lat, record.lon);
      if (!matches.length) {
        stats.unmatched++;
        continue;
      }
      for (const region of matches) {
        if (!writers.has(region.id)) {
          writers.set(region.id, createBufferedJsonlWriter(join(partial, `${region.id}.jsonl`)));
        }
        writers.get(region.id).write(record);
        counts[region.id]++;
        stats.writes++;
      }
      if (stats.rowsRead % 250_000 === 0) {
        options.log?.(`${source.id}: partitioned ${stats.rowsRead.toLocaleString()} rows into ${stats.writes.toLocaleString()} shard records`);
      }
    }
    for (const writer of writers.values()) writer.close();
    writers.clear();
    writeFileSync(join(partial, "partitions.meta.json"), JSON.stringify({
      schemaVersion: SPATIAL_PARTITION_SCHEMA_VERSION,
      identity: routingIdentity,
      source: source.identity,
      regions: counts,
      ...stats,
      builtAt: new Date().toISOString()
    }, null, 2));
    rmSync(root, { recursive: true, force: true });
    renameSync(partial, root);
    return { ...readJson(metaPath), root };
  } catch (error) {
    for (const writer of writers.values()) writer.close();
    rmSync(partial, { recursive: true, force: true });
    throw error;
  }
}

export function spatialPartitionForRegion(source, partition, region) {
  const records = Number(partition.regions?.[region.id] || 0);
  if (!records) return null;
  return {
    ...source,
    format: "jsonl",
    path: join(partition.root, `${region.id}.jsonl`),
    compression: "none",
    archiveEntry: undefined,
    partition: undefined,
    identity: {
      ...source.identity,
      spatialPartition: partition.identity,
      region: region.id,
      records
    }
  };
}

export function addressSourceAdapterOptions(source, region) {
  const allowed = source.partition?.regions?.[region.id];
  const values = allowed == null ? null : new Set((Array.isArray(allowed) ? allowed : [allowed]).map(value => clean(value).toUpperCase()));
  const field = clean(source.partition?.field);
  return {
    id: source.id,
    name: source.name,
    path: source.path,
    url: source.website || source.url,
    version: source.identity?.lastModified || source.identity?.etag || "",
    identity: source.identity,
    license: source.license,
    attribution: source.attribution,
    compression: source.compression,
    archiveEntry: source.archiveEntry,
    delimiter: source.delimiter ?? ",",
    header: source.header,
    defaults: source.defaults,
    mapping: source.mapping,
    includeAddresses: source.includeAddresses,
    includeUnits: source.includeUnits,
    includeCountry: source.includeCountry,
    ...(values ? {
      filter(record) {
        return values.has(clean(record[field]).toUpperCase());
      }
    } : {})
  };
}

export function additionalSourceMetadata(sources) {
  return sources.map(source => ({
    source: source.name,
    ...(source.attribution ? { attribution: source.attribution } : {}),
    ...(source.license ? { license: source.license } : {}),
    ...(source.licenseUrl ? { license_url: source.licenseUrl } : {}),
    ...(source.website || source.url ? { url: source.website || source.url } : {})
  }));
}
