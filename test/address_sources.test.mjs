import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync, gzipSync } from "node:zlib";
import {
  additionalSourceMetadata,
  addressSourceAdapterOptions,
  addressSourcesForRegion,
  createRegionSpatialRouter,
  loadAddressSourcesConfig,
  partitionAddressSourceSpatially,
  prepareAddressSource,
  regionAddressSourceIdentity,
  regionRoutingMetadata,
  spatialPartitionForRegion
} from "../scripts/lib/address_sources.mjs";

function sourceConfig(overrides = {}) {
  return {
    id: "postal-test",
    name: "Postal Test",
    url: "https://example.test/postal.zip",
    website: "https://example.test/postal",
    format: "delimited",
    compression: "zip",
    delimiter: "\t",
    header: false,
    defaults: { kind: "postal_code" },
    mapping: { country: 0, postcode: 1, city: 2, state: 3, lat: 4, lon: 5 },
    includeAddresses: false,
    license: "CC-BY-4.0",
    attribution: "Example Authority",
    appliesTo: { groups: ["canada"] },
    partition: { field: "state", regions: { quebec: ["QC"] } },
    ...overrides
  };
}

test("address source configuration is generic, validated, and region-partitioned", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-config-"));
  try {
    const path = join(root, "address-sources.json");
    await writeFile(path, JSON.stringify({ sources: [sourceConfig()] }));
    const loaded = loadAddressSourcesConfig(root, path);
    const quebec = addressSourcesForRegion(loaded.sources, { id: "quebec", groups: ["canada"] });
    const ontario = addressSourcesForRegion(loaded.sources, { id: "ontario", groups: ["canada"] });
    assert.equal(quebec.length, 1);
    assert.equal(ontario.length, 0);

    const openAddressesPath = join(root, "openaddresses.json");
    await writeFile(openAddressesPath, JSON.stringify({ sources: [{
      id: "openaddresses",
      provider: "openaddresses-batch",
      apiUrl: "https://batch.example.test/api",
      tokenEnv: "OPENADDRESSES_TEST_TOKEN",
      partition: { mode: "spatial" }
    }] }));
    assert.equal(loadAddressSourcesConfig(root, openAddressesPath).sources[0].partitionConcurrency, 4);
    await writeFile(openAddressesPath, JSON.stringify({ sources: [{
      id: "openaddresses",
      provider: "openaddresses-batch",
      apiUrl: "https://batch.example.test/api",
      tokenEnv: "OPENADDRESSES_TEST_TOKEN",
      partitionConcurrency: 0,
      partition: { mode: "spatial" }
    }] }));
    assert.throws(() => loadAddressSourcesConfig(root, openAddressesPath), /partitionConcurrency/u);

    const adapter = addressSourceAdapterOptions({
      ...quebec[0],
      path: join(root, "postal.zip"),
      identity: { etag: "v1" }
    }, { id: "quebec" });
    assert.equal(adapter.filter({ state: "qc" }), true);
    assert.equal(adapter.filter({ state: "ON" }), false);
    assert.deepEqual(additionalSourceMetadata(quebec), [{
      source: "Postal Test",
      attribution: "Example Authority",
      license: "CC-BY-4.0",
      url: "https://example.test/postal"
    }]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("countrywide address sources download once and reuse a content/config identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-download-"));
  try {
    const calls = [];
    const fetchSource = async (_url, init = {}) => {
      calls.push(init.method || "GET");
      const headers = new Headers({
        etag: '"postal-v1"',
        "last-modified": "Sat, 01 Aug 2026 00:00:00 GMT",
        "content-length": "12"
      });
      return init.method === "HEAD"
        ? new Response(null, { status: 200, headers })
        : new Response("postal-data", { status: 200, headers });
    };
    const first = await prepareAddressSource(sourceConfig(), { root, fetchSource, timeoutMs: 1000 });
    const second = await prepareAddressSource(sourceConfig(), { root, fetchSource, timeoutMs: 1000 });
    assert.deepEqual(calls, ["HEAD", "GET", "HEAD"]);
    assert.equal(await readFile(first.path, "utf8"), "postal-data");
    assert.equal(first.path, second.path);
    assert.equal(regionAddressSourceIdentity([first]), regionAddressSourceIdentity([second]));

    await prepareAddressSource(sourceConfig({ mapping: { postcode: 1, lat: 4, lon: 5 } }), {
      root,
      fetchSource,
      timeoutMs: 1000
    });
    assert.deepEqual(calls, ["HEAD", "GET", "HEAD", "HEAD", "GET"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("resumed runs freeze a downloaded address source without contacting upstream", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-frozen-"));
  try {
    let calls = 0;
    const source = sourceConfig();
    const first = await prepareAddressSource(source, {
      root,
      timeoutMs: 1000,
      fetchSource: async (_url, init = {}) => {
        calls++;
        const headers = {
          etag: '"postal-v1"',
          "last-modified": "Sat, 01 Aug 2026 00:00:00 GMT",
          "content-length": "12"
        };
        return init.method === "HEAD"
          ? new Response(null, { status: 200, headers })
          : new Response("postal-data", { status: 200, headers });
      }
    });
    const resumed = await prepareAddressSource(source, {
      root,
      timeoutMs: 1000,
      reuseCached: true,
      fetchSource: async () => {
        throw new Error("upstream must not be contacted while resuming");
      }
    });

    assert.equal(calls, 2);
    assert.deepEqual(resumed.identity, first.identity);
    assert.equal(await readFile(resumed.path, "utf8"), "postal-data");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("address source downloads fall back when a provider rejects HEAD", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-headless-"));
  try {
    const calls = [];
    const fetchSource = async (_url, init = {}) => {
      calls.push(init.method || "GET");
      if (init.method === "HEAD") return new Response(null, { status: 405 });
      return new Response("postal-data", {
        status: 200,
        headers: { etag: '"postal-v1"', "content-length": "11" }
      });
    };
    const first = await prepareAddressSource(sourceConfig(), { root, fetchSource, timeoutMs: 1000 });
    const second = await prepareAddressSource(sourceConfig(), { root, fetchSource, timeoutMs: 1000 });
    assert.equal(await readFile(first.path, "utf8"), "postal-data");
    assert.equal(first.path, second.path);
    assert.deepEqual(calls, ["HEAD", "GET", "HEAD", "GET"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("worldwide sources partition once across conventional, overlapping, and wrapped shard coverage", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-spatial-"));
  try {
    const regions = [
      { id: "west", bbox: [40, -80, 50, -70] },
      { id: "overlap", bbox: [44, -76, 46, -72] },
      { id: "dateline", bbox: [-20, 170, 20, -170] }
    ];
    const router = createRegionSpatialRouter(regions);
    assert.deepEqual(router.route(45, -74).map(region => region.id), ["west", "overlap"]);
    assert.deepEqual(router.route(0, 179).map(region => region.id), ["dateline"]);
    assert.deepEqual(router.route(0, -179).map(region => region.id), ["dateline"]);

    const source = {
      id: "global-postal",
      name: "Global Postal",
      identity: { etag: "v1", config: "mapping-v1" },
      defaults: { kind: "postal_code" },
      async *records() {
        yield { kind: "postal_code", postcode: "H2X 1Y4", country: "CA", lat: 45, lon: -74 };
        yield { kind: "postal_code", postcode: "1800", country: "FJ", lat: 0, lon: 179 };
        yield { kind: "postal_code", postcode: "NOWHERE", country: "ZZ", lat: -70, lon: 0 };
      }
    };
    const normalizeRecord = record => ({ ...record, lat: Number(record.lat), lon: Number(record.lon) });
    const partition = await partitionAddressSourceSpatially(source, { root, regions, normalizeRecord });
    assert.equal(partition.rowsRead, 3);
    assert.equal(partition.unmatched, 1);
    assert.equal(partition.writes, 3);
    assert.equal(partition.regions.west, 1);
    assert.equal(partition.regions.overlap, 1);
    assert.equal(partition.regions.dateline, 1);
    const west = spatialPartitionForRegion({ ...source, path: join(root, "source.zip") }, partition, regions[0]);
    assert.equal(JSON.parse(await readFile(west.path, "utf8")).postcode, "H2X 1Y4");
    assert.equal(west.format, "jsonl");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("spatial routing uses ISO country and subdivision metadata to avoid bbox duplication", () => {
  const regions = [
    { id: "quebec", bbox: [44, -80, 63, -57], countryCodes: ["CA"], subdivisionCodes: ["CA-QC"] },
    { id: "ontario", bbox: [41, -96, 57, -74], countryCodes: ["CA"], subdivisionCodes: ["CA-ON"] },
    { id: "vermont", bbox: [42.7, -73.5, 45.1, -71.4], countryCodes: ["US"], subdivisionCodes: ["US-VT"] }
  ];
  const router = createRegionSpatialRouter(regions);
  assert.deepEqual(router.route(45, -74.5).map(region => region.id), ["quebec", "ontario"]);
  assert.deepEqual(router.route(45, -74.5, { country: "CA", state: "QC" }).map(region => region.id), ["quebec"]);
  assert.deepEqual(router.route(45, -74.5, { country: "CA", state: "ON" }).map(region => region.id), ["ontario"]);
  assert.deepEqual(router.route(45, -73.4, { country: "US", state: "VT" }).map(region => region.id), ["vermont"]);
  assert.deepEqual(router.route(45, -73.4, { country: "Canada", state: "Quebec" }).map(region => region.id), ["quebec", "vermont"]);
});

test("region loader metadata preserves normalized ISO routing codes", () => {
  assert.deepEqual(regionRoutingMetadata({
    countryCodes: ["ca", " CA "],
    subdivisionCodes: ["ca-qc", "CA-QC"]
  }), {
    countryCodes: ["CA"],
    subdivisionCodes: ["CA-QC"]
  });
  assert.deepEqual(regionRoutingMetadata({}), { countryCodes: [], subdivisionCodes: [] });
});

test("OpenAddresses streams authenticated jobs into compressed worldwide partitions with provenance", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-openaddresses-"));
  const priorToken = process.env.OPENADDRESSES_TEST_TOKEN;
  process.env.OPENADDRESSES_TEST_TOKEN = "secret-test-token";
  try {
    const path = join(root, "address-sources.json");
    await writeFile(path, JSON.stringify({
      sources: [{
        id: "openaddresses-global",
        name: "OpenAddresses",
        provider: "openaddresses-batch",
        apiUrl: "https://batch.example.test/api",
        tokenEnv: "OPENADDRESSES_TEST_TOKEN",
        refreshIntervalHours: 168,
        includeAddresses: true,
        partition: { mode: "spatial" }
      }]
    }));
    const [source] = loadAddressSourcesConfig(root, path).sources;
    const catalog = [{
      source: "ca/qc/test",
      updated: 1785700000000,
      layer: "addresses",
      name: "province",
      job: 123,
      output: { output: true },
      size: 42
    }];
    const features = [
      { type: "Feature", properties: { hash: "one", number: "214", street: "Rue Libersan", city: "Sainte-Thérèse", region: "QC", postcode: "J7E 3X4" }, geometry: { type: "Point", coordinates: [-73.83, 45.64] } },
      { type: "Feature", properties: { hash: "two", number: "10", street: "Main St", city: "Windsor", region: "ON", postcode: "N9A 1A1" }, geometry: { type: "Point", coordinates: [-83.02, 42.3] } }
    ];
    const calls = [];
    const fetchSource = async (url, init = {}) => {
      const value = String(url);
      const authorization = new Headers(init.headers).get("authorization");
      calls.push({ value, redirect: init.redirect, authorization });
      if (value.endsWith("/data?layer=addresses")) return Response.json(catalog);
      assert.doesNotMatch(value, /secret-test-token/u);
      assert.equal(authorization, "Bearer secret-test-token");
      if (init.redirect === "manual") return new Response(null, { status: 302, headers: { location: "https://cdn.example.test/job.gz" } });
      const jsonl = `${features.map(feature => JSON.stringify(feature)).join("\n")}\n`;
      return new Response(gzipSync(jsonl), { status: 200 });
    };
    const prepared = await prepareAddressSource(source, { root, fetchSource, timeoutMs: 1000 });
    assert.equal(prepared.identity.jobs, 1);
    assert.doesNotMatch(await readFile(prepared.path, "utf8"), /secret-test-token/u);
    const regions = [
      { id: "quebec", bbox: [44, -80, 63, -57] },
      { id: "ontario", bbox: [41, -96, 57, -74] }
    ];
    const partition = await partitionAddressSourceSpatially(prepared, {
      root: join(root, "partitions"),
      regions,
      normalizeRecord: record => ({ ...record, lat: Number(record.lat), lon: Number(record.lon) })
    });
    assert.equal(partition.compression, "gzip");
    assert.equal(partition.batches, 1);
    assert.equal(partition.regions.quebec, 1);
    assert.equal(partition.regions.ontario, 1);
    const quebec = spatialPartitionForRegion(prepared, partition, regions[0]);
    const [record] = gunzipSync(await readFile(quebec.path))
      .toString("utf8").trim().split("\n").map(JSON.parse);
    assert.equal(record.id, "ca/qc/test/addresses/province/one");
    assert.equal(record.country, "CA");
    assert.match(record.url, /sources\/ca\/qc\/test\.json$/u);
    assert.equal(quebec.compression, "gzip");
    assert.equal(calls.length, 3);
  } finally {
    if (priorToken == null) delete process.env.OPENADDRESSES_TEST_TOKEN;
    else process.env.OPENADDRESSES_TEST_TOKEN = priorToken;
    await rm(root, { recursive: true, force: true });
  }
});

test("batched spatial partitioning resumes after the last durable job without duplicate rows", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-resume-"));
  try {
    const region = { id: "quebec", bbox: [44, -80, 63, -57] };
    let failSecond = true;
    const visited = [];
    const source = {
      id: "resumable-global",
      identity: { snapshot: "v1", config: "v1" },
      async *batches(completed) {
        for (const id of ["job-a", "job-b"]) {
          if (completed.has(id)) continue;
          visited.push(id);
          yield {
            id,
            records: (async function *records() {
              yield { id, houseNumber: "1", street: id, country: "CA", lat: 45, lon: -74 };
              if (id === "job-b" && failSecond) throw new Error("interrupted download");
            })()
          };
        }
      }
    };
    const options = {
      root: join(root, "partitions"),
      regions: [region],
      normalizeRecord: record => record
    };
    await assert.rejects(partitionAddressSourceSpatially(source, options), /interrupted download/u);
    failSecond = false;
    const partition = await partitionAddressSourceSpatially(source, options);
    assert.deepEqual(visited, ["job-a", "job-b", "job-b"]);
    assert.equal(partition.rowsRead, 2);
    assert.equal(partition.regions.quebec, 2);
    const shard = spatialPartitionForRegion(source, partition, region);
    const rows = gunzipSync(await readFile(shard.path)).toString("utf8").trim().split("\n");
    assert.equal(rows.length, 2);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("batched spatial partitioning runs a bounded window concurrently and commits in source order", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-concurrent-"));
  try {
    const region = { id: "quebec", bbox: [44, -80, 63, -57], countryCodes: ["CA"] };
    let active = 0;
    let maximumActive = 0;
    const source = {
      id: "concurrent-global",
      identity: { snapshot: "v1", config: "v1" },
      partitionConcurrency: 2,
      async *batches() {
        for (const [id, delay] of [["job-a", 30], ["job-b", 0], ["job-c", 0]]) {
          yield {
            id,
            records: async function *records() {
              active++;
              maximumActive = Math.max(maximumActive, active);
              try {
                await new Promise(resolve => setTimeout(resolve, delay));
                yield { id, houseNumber: "1", street: id, country: "CA", lat: 45, lon: -74 };
              } finally {
                active--;
              }
            }
          };
        }
      }
    };
    const partition = await partitionAddressSourceSpatially(source, {
      root: join(root, "partitions"),
      regions: [region],
      normalizeRecord: record => record
    });
    assert.equal(maximumActive, 2);
    assert.equal(partition.batches, 3);
    assert.equal(partition.rowsRead, 3);
    const shard = spatialPartitionForRegion(source, partition, region);
    const ids = gunzipSync(await readFile(shard.path)).toString("utf8").trim().split("\n").map(line => JSON.parse(line).id);
    assert.deepEqual(ids, ["job-a", "job-b", "job-c"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("batched spatial partitioning rolls back an interrupted fragment commit before resume", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-transaction-"));
  try {
    const region = { id: "quebec", bbox: [44, -80, 63, -57], countryCodes: ["CA"] };
    let visits = 0;
    const source = {
      id: "transactional-global",
      identity: { snapshot: "v1", config: "v1" },
      async *batches(completed) {
        if (completed.has("job-a")) return;
        yield {
          id: "job-a",
          records: async function *records() {
            visits++;
            yield { id: "one", houseNumber: "1", street: "Safe Road", country: "CA", lat: 45, lon: -74 };
          }
        };
      }
    };
    const options = {
      root: join(root, "partitions"),
      regions: [region],
      normalizeRecord: record => record
    };
    await assert.rejects(partitionAddressSourceSpatially(source, {
      ...options,
      afterBatchAppend() {
        throw new Error("simulated process interruption");
      }
    }), /simulated process interruption/u);
    const partition = await partitionAddressSourceSpatially(source, options);
    assert.equal(visits, 2);
    assert.equal(partition.rowsRead, 1);
    assert.equal(partition.regions.quebec, 1);
    const shard = spatialPartitionForRegion(source, partition, region);
    const rows = gunzipSync(await readFile(shard.path)).toString("utf8").trim().split("\n");
    assert.equal(rows.length, 1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
