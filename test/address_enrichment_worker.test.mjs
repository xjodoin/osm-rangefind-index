import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const projectRoot = dirname(dirname(fileURLToPath(import.meta.url)));

test("enriches an OSM corpus in an isolated worker", async () => {
  const root = mkdtempSync(join(tmpdir(), "address-enrichment-worker-test-"));
  const data = join(root, "addresses.jsonl");
  const osm = join(root, "osm.jsonl");
  const output = join(root, "enriched.jsonl");
  const config = join(root, "config.json");
  writeFileSync(osm, '{"id":"osm/1","name":"Existing place"}\n');
  writeFileSync(data, [
    { id: "a", kind: "address", houseNumber: "214", street: "Rue Libersan", city: "Sainte-Therese", postcode: "J7E 5P8", country: "CA", lat: 45.64, lon: -73.83 },
    { id: "p", kind: "postal_code", city: "Sainte-Therese", postcode: "J7E 5P8", country: "CA", lat: 45.64, lon: -73.83 }
  ].map(value => JSON.stringify(value)).join("\n") + "\n");
  writeFileSync(config, JSON.stringify({
    region: "test",
    regionConfig: { id: "test", groups: ["canada"] },
    root,
    osmPath: osm,
    outputPath: output,
    osmDocs: 1,
    sources: [{ id: "test-source", name: "Test", format: "jsonl", path: data, includeAddresses: true, includeCountry: true }]
  }));

  try {
    const result = await new Promise((resolveDone, rejectDone) => {
      let value;
      const child = fork(join(projectRoot, "scripts/address_enrichment_worker.mjs"), [config], {
        stdio: ["ignore", "ignore", "inherit", "ipc"]
      });
      child.on("message", message => { if (message?.type === "result") value = message.value; });
      child.on("error", rejectDone);
      child.on("exit", code => code === 0 && value
        ? resolveDone(value)
        : rejectDone(new Error(`enrichment worker exited ${code}`)));
    });
    assert.equal(result.meta.addressesWritten, 1);
    assert.equal(result.meta.postalDocs, 1);
    const docs = readFileSync(output, "utf8").trim().split("\n").map(JSON.parse);
    assert.equal(docs.length, 3);
    assert.equal(docs.some(doc => doc.house_number === "214"), true);
    assert.equal(docs.some(doc => doc.type === "postal_code"), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
