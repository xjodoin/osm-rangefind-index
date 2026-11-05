#!/usr/bin/env node

// Address enrichment performs millions of synchronous SQLite lookups and JSON
// normalizations. Keep it outside the scheduler process so multiple region
// stages can use separate CPU cores and road work can progress concurrently.

import { createHash } from "node:crypto";
import { createReadStream, readFileSync } from "node:fs";
import * as rangefindOsmNode from "rangefind/osm/node";
import { addressSourceAdapterOptions } from "./lib/address_sources.mjs";

const configPath = process.argv[2];
if (!configPath) throw new Error("usage: address_enrichment_worker.mjs <config.json>");
const config = JSON.parse(readFileSync(configPath, "utf8"));

async function fileSha256(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

// The candidate cache must survive a new upstream snapshot that leaves this
// region's addresses untouched. Spatial partitions carry the global snapshot
// hash in their identity (any job anywhere changes it), so key them by the
// region partition's bytes instead; other sources keep their own identity.
async function candidateCacheIdentity(sources, region) {
  return Promise.all(sources.map(async source => ({
    id: source.id,
    region,
    config: source.identity?.config ?? null,
    content: source.identity?.spatialPartition
      ? await fileSha256(source.path)
      : source.identity ?? null
  })));
}

try {
  const sources = config.sources.map(source => {
    const options = addressSourceAdapterOptions(source, config.regionConfig);
    return (
    source.format === "jsonl"
      ? rangefindOsmNode.createJsonlAddressSource(options)
      : rangefindOsmNode.createDelimitedAddressSource(options)
    );
  });
  const candidateCache = config.candidateCachePath
    ? { path: config.candidateCachePath, identity: await candidateCacheIdentity(config.sources, config.region) }
    : undefined;
  const value = await rangefindOsmNode.augmentOsmWithAddressSources({
    root: config.root,
    osmPath: config.osmPath,
    outputPath: config.outputPath,
    sources,
    osmDocs: config.osmDocs,
    candidateCache,
    log: line => console.log(`${config.region}: ${line}`)
  });
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
