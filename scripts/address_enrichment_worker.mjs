#!/usr/bin/env node

// Address enrichment performs millions of synchronous SQLite lookups and JSON
// normalizations. Keep it outside the scheduler process so multiple region
// stages can use separate CPU cores and road work can progress concurrently.

import { readFileSync } from "node:fs";
import * as rangefindOsmNode from "rangefind/osm/node";
import { addressSourceAdapterOptions } from "./lib/address_sources.mjs";

const configPath = process.argv[2];
if (!configPath) throw new Error("usage: address_enrichment_worker.mjs <config.json>");
const config = JSON.parse(readFileSync(configPath, "utf8"));

try {
  const sources = config.sources.map(source => {
    const options = addressSourceAdapterOptions(source, config.regionConfig);
    return (
    source.format === "jsonl"
      ? rangefindOsmNode.createJsonlAddressSource(options)
      : rangefindOsmNode.createDelimitedAddressSource(options)
    );
  });
  const value = await rangefindOsmNode.augmentOsmWithAddressSources({
    root: config.root,
    osmPath: config.osmPath,
    outputPath: config.outputPath,
    sources,
    osmDocs: config.osmDocs,
    log: line => console.log(`${config.region}: ${line}`)
  });
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
