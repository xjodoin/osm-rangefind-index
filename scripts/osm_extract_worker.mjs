#!/usr/bin/env node

// Runs the CPU-heavy PBF scan in an isolated process. Multiple acquisition
// lanes can then use separate cores; address enrichment remains in the parent
// so all regions keep sharing one prepared source/partition cache.

import { readFileSync } from "node:fs";
import { extractOsmPlaces } from "rangefind/osm/extract";

const configPath = process.argv[2];
if (!configPath) throw new Error("usage: osm_extract_worker.mjs <config.json>");
const config = JSON.parse(readFileSync(configPath, "utf8"));

try {
  const value = await extractOsmPlaces(config);
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
