import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDiscoveryDocument,
  DISCOVERY_FORMAT,
  DISCOVERY_PATH,
  rootDiscoveryEndpoints
} from "../scripts/lib/discovery.mjs";

const roads = { enabled: true, profiles: ["car", "bike", "foot"] };

test("root endpoints make every service capability discoverable", () => {
  assert.deepEqual(rootDiscoveryEndpoints(roads), {
    discovery: DISCOVERY_PATH,
    status: "status.json",
    routeCatalog: "routes/catalog.json"
  });
});

test("well-known discovery distinguishes search and routing formats", () => {
  const discovery = buildDiscoveryDocument(roads);
  assert.deepEqual(discovery, {
    format: DISCOVERY_FORMAT,
    version: 1,
    endpoints: {
      search: {
        href: "../manifest.min.json",
        format: "rangefind-sharded-v1"
      },
      routing: {
        href: "../routes/catalog.json",
        format: "rangefind-route-catalog-v1",
        graphFormat: "rfroutegraph-v1",
        profiles: ["car", "bike", "foot"]
      },
      status: {
        href: "../status.json",
        schemaVersion: 1
      }
    }
  });
  const base = "https://example.test/index/.well-known/rangefind.json";
  assert.equal(new URL(discovery.endpoints.search.href, base).href, "https://example.test/index/manifest.min.json");
  assert.equal(new URL(discovery.endpoints.routing.href, base).href, "https://example.test/index/routes/catalog.json");
});

test("routing is not advertised when no road catalog is configured", () => {
  const config = { enabled: false, profiles: [] };
  assert.equal(rootDiscoveryEndpoints(config).routeCatalog, undefined);
  assert.equal(buildDiscoveryDocument(config).endpoints.routing, undefined);
});
