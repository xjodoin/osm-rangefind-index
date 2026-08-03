import {
  closeSync,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  truncateSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { basename, dirname, extname, join, resolve } from "node:path";
import { Readable } from "node:stream";
import { finished, pipeline } from "node:stream/promises";
import { createInterface } from "node:readline";
import { createGunzip, createGzip } from "node:zlib";

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
    provider: source.provider,
    apiUrl: source.apiUrl,
    tokenEnv: source.tokenEnv,
    refreshIntervalHours: source.refreshIntervalHours,
    downloadAttempts: source.downloadAttempts,
    downloadIdleTimeoutMs: source.downloadIdleTimeoutMs,
    partitionConcurrency: source.partitionConcurrency,
    partitionCompressionLevel: source.partitionCompressionLevel,
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
  const provider = clean(raw.provider || "file").toLowerCase();
  const source = {
    ...raw,
    id: safeId(raw.id),
    url: clean(raw.url),
    name: clean(raw.name) || safeId(raw.id),
    format: clean(raw.format || "delimited"),
    provider,
    enabled: raw.enabled !== false
  };
  if (provider === "openaddresses-batch") {
    source.apiUrl = clean(raw.apiUrl || "https://batch.openaddresses.io/api").replace(/\/+$/u, "");
    source.tokenEnv = clean(raw.tokenEnv || "OPENADDRESSES_TOKEN");
    source.format = "openaddresses-geojsonl";
    source.url ||= "https://openaddresses.io/";
    const partitionConcurrency = Number(raw.partitionConcurrency ?? 4);
    if (!Number.isInteger(partitionConcurrency) || partitionConcurrency < 1 || partitionConcurrency > 16) {
      throw new Error(`Address source ${source.id}: partitionConcurrency must be an integer from 1 to 16.`);
    }
    source.partitionConcurrency = partitionConcurrency;
    if (!source.tokenEnv) throw new Error(`Address source ${source.id} has no token environment variable.`);
  } else if (provider !== "file") {
    throw new Error(`Address source ${source.id}: unsupported provider ${JSON.stringify(provider)}.`);
  }
  if (!source.url) throw new Error(`Address source ${source.id} has no URL.`);
  if (provider === "openaddresses-batch") {
    if (source.partition?.mode !== "spatial") {
      throw new Error(`Address source ${source.id}: OpenAddresses requires partition.mode=spatial.`);
    }
    return source;
  }
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

function metadataIdentity(entries, source) {
  const snapshot = entries.map(entry => ({
    source: entry.source,
    layer: entry.layer,
    name: entry.name,
    job: entry.job,
    updated: entry.updated,
    size: entry.size
  }));
  return {
    snapshot: createHash("sha256").update(stableJson(snapshot)).digest("hex"),
    jobs: snapshot.length,
    bytes: snapshot.reduce((total, entry) => total + Math.max(0, Number(entry.size) || 0), 0),
    config: configFingerprint(source)
  };
}

function openAddressesJobKey(entry) {
  return `${entry.job}:${entry.updated || 0}`;
}

function openAddressesSourceUrl(entry) {
  const path = clean(entry.source).split("/").map(encodeURIComponent).join("/");
  return `https://github.com/openaddresses/openaddresses/blob/master/sources/${path}.json`;
}

function redactSecret(value, secret) {
  const message = clean(value);
  if (!secret) return message;
  return message
    .split(secret).join("[REDACTED]")
    .split(encodeURIComponent(secret)).join("[REDACTED]");
}

function openAddressesRecord(feature, entry) {
  const coordinates = feature?.geometry?.type === "Point" ? feature.geometry.coordinates : null;
  const properties = feature?.properties || {};
  if (!Array.isArray(coordinates) || coordinates.length < 2) return null;
  const country = clean(entry.source).split("/")[0].toUpperCase();
  const localId = clean(properties.hash || properties.id);
  return {
    id: [entry.source, entry.layer, entry.name, localId].filter(Boolean).join("/"),
    houseNumber: properties.number,
    street: properties.street,
    unit: properties.unit,
    city: properties.city,
    district: properties.district,
    state: properties.region,
    postcode: properties.postcode,
    country,
    lon: coordinates[0],
    lat: coordinates[1],
    url: openAddressesSourceUrl(entry),
    kind: "address"
  };
}

async function *readOpenAddressesJob(source, entry, options) {
  const token = clean(process.env[source.tokenEnv]);
  const url = new URL(`${source.apiUrl}/job/${entry.job}/output/source.geojson.gz`);
  const controller = new AbortController();
  const idleMs = Math.max(10_000, Number(source.downloadIdleTimeoutMs || 120_000));
  let idleTimer;
  const armIdleTimeout = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => controller.abort(new Error(`OpenAddresses job ${entry.job} stalled for ${idleMs} ms`)), idleMs);
  };
  armIdleTimeout();
  let response;
  try {
    response = await options.fetchSource(url, {
      signal: controller.signal,
      headers: { authorization: `Bearer ${token}` }
    }, { timeoutMs: options.timeoutMs });
  } catch (error) {
    clearTimeout(idleTimer);
    throw new Error(`${source.id}: OpenAddresses job ${entry.job} request failed: ${redactSecret(error.message, token)}`);
  }
  if (!response.ok) {
    await response.body?.cancel();
    clearTimeout(idleTimer);
    throw new Error(`${source.id}: OpenAddresses job ${entry.job} download returned ${response.status}`);
  }
  const compressed = Readable.fromWeb(response.body);
  const gunzip = createGunzip();
  compressed.pipe(gunzip);
  const lines = createInterface({ input: gunzip, crlfDelay: Infinity });
  let row = 0;
  try {
    for await (const line of lines) {
      armIdleTimeout();
      row++;
      if (!line.trim()) continue;
      let feature;
      try {
        feature = JSON.parse(line);
      } catch (error) {
        throw new Error(`${source.id}: invalid GeoJSON in OpenAddresses job ${entry.job} row ${row}: ${error.message}`, { cause: error });
      }
      const record = openAddressesRecord(feature, entry);
      if (record) yield record;
    }
  } finally {
    clearTimeout(idleTimer);
    lines.close();
    if (!compressed.destroyed) compressed.destroy();
    if (!gunzip.destroyed) gunzip.destroy();
  }
}

async function prepareOpenAddressesSource(source, options) {
  const root = resolve(options.root);
  const dir = join(root, source.id);
  const metaPath = join(dir, "source.meta.json");
  mkdirSync(dir, { recursive: true });
  const prior = readJson(metaPath, null);
  const refreshMs = Math.max(0, Number(source.refreshIntervalHours ?? 168)) * 3600_000;
  let entries = Array.isArray(prior?.entries) ? prior.entries : null;
  const cacheFresh = entries && refreshMs > 0
    && Date.now() - Date.parse(prior.checkedAt || prior.downloadedAt || 0) < refreshMs;
  if (!cacheFresh) {
    const response = await options.fetchSource(`${source.apiUrl}/data?layer=addresses`, {}, { timeoutMs: options.timeoutMs });
    if (!response.ok) throw new Error(`${source.id}: OpenAddresses data catalog returned ${response.status}`);
    const catalog = await response.json();
    if (!Array.isArray(catalog)) throw new Error(`${source.id}: OpenAddresses data catalog is not an array.`);
    entries = catalog
      .filter(entry => entry?.output?.output && entry.job && entry.source && entry.layer === "addresses")
      .sort((left, right) => (
        clean(left.source).localeCompare(clean(right.source))
        || clean(left.name).localeCompare(clean(right.name))
        || Number(left.job) - Number(right.job)
      ));
    if (!entries.length) throw new Error(`${source.id}: OpenAddresses returned no address jobs.`);
  }
  const identity = metadataIdentity(entries, source);
  if (!cacheFresh) {
    writeFileSync(metaPath, JSON.stringify({
      id: source.id,
      provider: source.provider,
      apiUrl: source.apiUrl,
      identity,
      checkedAt: new Date().toISOString(),
      entries
    }, null, 2));
  }
  const token = clean(process.env[source.tokenEnv]);
  if (!token) {
    throw new Error(`${source.id}: ${source.tokenEnv} is required. Create a free OpenAddresses API token at https://batch.openaddresses.io/ and store it in the indexer environment.`);
  }
  // Validate authentication without downloading an address payload. Manual
  // redirect mode returns the authenticated CDN hand-off as a small 3xx.
  const probe = new URL(`${source.apiUrl}/job/${entries[0].job}/output/source.geojson.gz`);
  let auth;
  try {
    auth = await options.fetchSource(probe, {
      redirect: "manual",
      headers: { authorization: `Bearer ${token}` }
    }, { timeoutMs: options.timeoutMs });
  } catch (error) {
    throw new Error(`${source.id}: OpenAddresses authentication check failed: ${redactSecret(error.message, token)}`);
  }
  await auth.body?.cancel();
  if (auth.status < 300 || auth.status >= 400) {
    throw new Error(`${source.id}: ${source.tokenEnv} was rejected by OpenAddresses (${auth.status}).`);
  }
  options.log?.(`${source.id}: current OpenAddresses snapshot has ${entries.length.toLocaleString()} address jobs`);
  return {
    ...source,
    path: metaPath,
    identity,
    async *batches(completed = new Set()) {
      for (const entry of entries) {
        const id = openAddressesJobKey(entry);
        if (completed.has(id)) continue;
        yield {
          id,
          label: `${entry.source}/${entry.layer}/${entry.name} (job ${entry.job})`,
          records: () => readOpenAddressesJob(source, entry, options)
        };
      }
    }
  };
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
  if (source.provider === "openaddresses-batch") {
    return prepareOpenAddressesSource(source, options);
  }
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

const SPATIAL_PARTITION_SCHEMA_VERSION = 3;
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
    route(latValue, lonValue, record = null) {
      const lat = Number(latValue);
      const lon = Number(lonValue);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) return [];
      const candidates = cells.get(`${gridLat(lat)}:${gridLon(lon)}`) || [];
      let matches = candidates.filter(region => bboxContains(region.bbox, lat, lon));
      if (matches.length < 2) return matches;

      const country = clean(record?.country).toUpperCase();
      if (/^[A-Z]{2}$/u.test(country)) {
        const countryMatches = matches.filter(region => region.countryCodes?.includes(country));
        if (countryMatches.length) matches = countryMatches;
        else {
          const uncodedMatches = matches.filter(region => !region.countryCodes?.length);
          if (uncodedMatches.length) matches = uncodedMatches;
        }
      }

      if (matches.length < 2) return matches;
      const subdivision = clean(record?.state).toUpperCase();
      if (/^[A-Z]{2}$/u.test(country) && subdivision) {
        const qualified = subdivision.includes("-") ? subdivision : `${country}-${subdivision}`;
        const subdivisionMatches = matches.filter(region => region.subdivisionCodes?.includes(qualified));
        if (subdivisionMatches.length) matches = subdivisionMatches;
      }
      return matches;
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

function createGzipJsonlWriter(path, level = 3, flags = "a") {
  const output = createWriteStream(path, { flags });
  const gzip = createGzip({ level });
  gzip.pipe(output);
  let closed = false;
  return {
    write(record) {
      return gzip.write(`${JSON.stringify(record)}\n`) ? null : once(gzip, "drain");
    },
    async close() {
      if (closed) return;
      closed = true;
      gzip.end();
      await Promise.all([finished(gzip), finished(output)]);
    }
  };
}

function partitionFile(root, regionId, compression = "none") {
  return join(root, `${regionId}.jsonl${compression === "gzip" ? ".gz" : ""}`);
}

function writeJsonAtomic(path, value) {
  const partial = `${path}.tmp`;
  writeFileSync(partial, JSON.stringify(value, null, 2));
  renameSync(partial, path);
}

function batchFragmentDirectory(root, batchId) {
  const digest = createHash("sha256").update(String(batchId)).digest("hex").slice(0, 24);
  return join(root, digest);
}

function restorePendingBatch(partial, progress, progressPath) {
  if (!progress?.pending?.offsets) return progress;
  for (const [regionId, offsetValue] of Object.entries(progress.pending.offsets)) {
    const path = partitionFile(partial, regionId, "gzip");
    const offset = Math.max(0, Number(offsetValue) || 0);
    if (offset) truncateSync(path, offset);
    else rmSync(path, { force: true });
  }
  const restored = { ...progress };
  delete restored.pending;
  writeJsonAtomic(progressPath, restored);
  return restored;
}

async function processAddressBatch(source, batch, options, context) {
  const { fragmentRoot, router } = context;
  const attempts = Math.max(1, Number(source.downloadAttempts || 3));
  const fragmentDir = batchFragmentDirectory(fragmentRoot, batch.id);
  for (let attempt = 1; attempt <= attempts; attempt++) {
    rmSync(fragmentDir, { recursive: true, force: true });
    mkdirSync(fragmentDir, { recursive: true });
    const writers = new Map();
    const counts = {};
    const stats = { rowsRead: 0, normalized: 0, unmatched: 0, writes: 0 };
    try {
      const records = typeof batch.records === "function" ? batch.records() : batch.records;
      for await (const raw of records) {
        stats.rowsRead++;
        const mapped = typeof source.normalize === "function" ? source.normalize(raw) : raw;
        const record = options.normalizeRecord(mapped, source.defaults);
        if (!record || (typeof source.filter === "function" && !source.filter(record, raw))) continue;
        stats.normalized++;
        const matches = router.route(record.lat, record.lon, record);
        if (!matches.length) {
          stats.unmatched++;
          continue;
        }
        for (const region of matches) {
          if (!writers.has(region.id)) {
            const path = partitionFile(fragmentDir, region.id, "gzip");
            const level = Math.max(1, Math.min(9, Number(source.partitionCompressionLevel || 3)));
            writers.set(region.id, createGzipJsonlWriter(path, level, "w"));
          }
          const backpressure = writers.get(region.id).write(record);
          if (backpressure) await backpressure;
          counts[region.id] = Number(counts[region.id] || 0) + 1;
          stats.writes++;
        }
        if (stats.rowsRead % 250_000 === 0) {
          options.log?.(`${source.id}: ${batch.label || batch.id} streamed ${stats.rowsRead.toLocaleString()} rows into ${stats.writes.toLocaleString()} shard records`);
        }
      }
      await Promise.all([...writers.values()].map(writer => writer.close()));
      return { batch, fragmentDir, counts, stats };
    } catch (error) {
      await Promise.allSettled([...writers.values()].map(writer => writer.close()));
      rmSync(fragmentDir, { recursive: true, force: true });
      if (attempt < attempts && typeof batch.records === "function") {
        options.log?.(`${source.id}: retrying ${batch.label || batch.id} after attempt ${attempt}/${attempts}: ${error.message}`);
        continue;
      }
      throw new Error(`${source.id}: failed while partitioning ${batch.label || batch.id}: ${error.message}`, { cause: error });
    }
  }
  throw new Error(`${source.id}: exhausted retries for ${batch.label || batch.id}`);
}

async function appendBatchFragments(result, source, context) {
  const { partial, progressPath, completed, counts, stats, routingIdentity } = context;
  const offsets = {};
  for (const regionId of Object.keys(result.counts)) {
    const path = partitionFile(partial, regionId, "gzip");
    offsets[regionId] = existsSync(path) ? statSync(path).size : 0;
  }
  writeJsonAtomic(progressPath, {
    schemaVersion: SPATIAL_PARTITION_SCHEMA_VERSION,
    identity: routingIdentity,
    source: source.identity,
    compression: "gzip",
    completed: [...completed],
    regions: counts,
    stats,
    pending: { id: result.batch.id, offsets },
    updatedAt: new Date().toISOString()
  });

  for (const regionId of Object.keys(result.counts).sort()) {
    await pipeline(
      createReadStream(partitionFile(result.fragmentDir, regionId, "gzip")),
      createWriteStream(partitionFile(partial, regionId, "gzip"), { flags: "a" })
    );
  }
  await context.afterAppend?.(result.batch);
  for (const [regionId, value] of Object.entries(result.counts)) {
    counts[regionId] = Number(counts[regionId] || 0) + value;
  }
  for (const key of Object.keys(stats)) stats[key] += result.stats[key];
  completed.add(result.batch.id);
  writeJsonAtomic(progressPath, {
    schemaVersion: SPATIAL_PARTITION_SCHEMA_VERSION,
    identity: routingIdentity,
    source: source.identity,
    compression: "gzip",
    completed: [...completed],
    regions: counts,
    stats,
    updatedAt: new Date().toISOString()
  });
  rmSync(result.fragmentDir, { recursive: true, force: true });
  optionsLogCheckpoint(source, result.batch, completed, stats, context.log);
}

function optionsLogCheckpoint(source, batch, completed, stats, log) {
  log?.(`${source.id}: checkpointed ${batch.label || batch.id} (${completed.size.toLocaleString()} jobs, ${stats.rowsRead.toLocaleString()} rows durable)`);
}

async function partitionAddressSourceBatches(source, options, context) {
  const { regions, root, partial, router, routingIdentity } = context;
  const progressPath = join(partial, "partitions.progress.json");
  let progress = readJson(progressPath, null);
  if (progress?.identity !== routingIdentity) {
    rmSync(partial, { recursive: true, force: true });
    progress = null;
  }
  mkdirSync(partial, { recursive: true });
  progress = restorePendingBatch(partial, progress, progressPath);
  const counts = progress?.regions || Object.fromEntries(regions.map(region => [region.id, 0]));
  const stats = progress?.stats || { rowsRead: 0, normalized: 0, unmatched: 0, writes: 0 };
  const completed = new Set(progress?.completed || []);
  if (completed.size) {
    options.log?.(`${source.id}: resuming after ${completed.size.toLocaleString()} completed OpenAddresses jobs`);
  }

  const fragmentRoot = join(partial, ".batch-fragments");
  rmSync(fragmentRoot, { recursive: true, force: true });
  mkdirSync(fragmentRoot, { recursive: true });
  const iterator = source.batches(completed)[Symbol.asyncIterator]();
  const concurrency = Math.max(1, Math.min(16, Number(source.partitionConcurrency || 1)));
  const active = [];
  const launch = async () => {
    const next = await iterator.next();
    if (next.done) return false;
    active.push({
      batch: next.value,
      promise: processAddressBatch(source, next.value, options, { fragmentRoot, router })
        .then(value => ({ value }), error => ({ error }))
    });
    return true;
  };
  for (let slot = 0; slot < concurrency; slot++) {
    if (!await launch()) break;
  }
  options.log?.(`${source.id}: partitioning with ${Math.max(1, active.length).toLocaleString()} concurrent job${active.length === 1 ? "" : "s"}`);
  try {
    while (active.length) {
      const task = active.shift();
      const outcome = await task.promise;
      if (outcome.error) throw outcome.error;
      const result = outcome.value;
      await appendBatchFragments(result, source, {
        partial,
        progressPath,
        completed,
        counts,
        stats,
        routingIdentity,
        log: options.log,
        afterAppend: options.afterBatchAppend
      });
      await launch();
    }
  } catch (error) {
    await iterator.return?.();
    await Promise.allSettled(active.map(task => task.promise));
    rmSync(fragmentRoot, { recursive: true, force: true });
    throw error;
  }
  rmSync(fragmentRoot, { recursive: true, force: true });

  const meta = {
    schemaVersion: SPATIAL_PARTITION_SCHEMA_VERSION,
    identity: routingIdentity,
    source: source.identity,
    compression: "gzip",
    batches: completed.size,
    regions: counts,
    ...stats,
    builtAt: new Date().toISOString()
  };
  writeJsonAtomic(join(partial, "partitions.meta.json"), meta);
  rmSync(progressPath, { force: true });
  rmSync(root, { recursive: true, force: true });
  renameSync(partial, root);
  return { ...meta, root };
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
    regions: regions.map(region => ({
      id: region.id,
      bbox: region.bbox,
      countryCodes: region.countryCodes,
      subdivisionCodes: region.subdivisionCodes
    }))
  })).digest("hex");
  const prior = readJson(metaPath, null);
  if (prior?.identity === routingIdentity && prior?.regions
      && Object.entries(prior.regions).every(([id, count]) => !count || existsSync(partitionFile(root, id, prior.compression)))) {
    return { ...prior, root };
  }

  const partial = `${root}.partial`;
  const router = createRegionSpatialRouter(regions);
  if (typeof source.batches === "function") {
    return partitionAddressSourceBatches(source, options, { regions, root, partial, router, routingIdentity });
  }
  rmSync(partial, { recursive: true, force: true });
  mkdirSync(partial, { recursive: true });
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
      const matches = router.route(record.lat, record.lon, record);
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
    path: partitionFile(partition.root, region.id, partition.compression),
    compression: partition.compression || "none",
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
