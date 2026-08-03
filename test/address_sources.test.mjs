import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  additionalSourceMetadata,
  addressSourceAdapterOptions,
  addressSourcesForRegion,
  createRegionSpatialRouter,
  loadAddressSourcesConfig,
  partitionAddressSourceSpatially,
  prepareAddressSource,
  regionAddressSourceIdentity,
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
