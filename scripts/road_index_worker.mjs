#!/usr/bin/env node

import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { buildRouteGraph } from "rangefind/route/build";

const configPath = process.argv[2];
if (!configPath) throw new Error("usage: road_index_worker.mjs <config.json>");
const config = JSON.parse(readFileSync(configPath, "utf8"));
const taskRequire = createRequire(import.meta.url);
const rangefindRoot = dirname(taskRequire.resolve("rangefind/package.json"));
const extractor = await import(pathToFileURL(join(rangefindRoot, "scripts/osm_road_graph.mjs")));
const log = message => console.log(`Rangefind roads: ${message}`);

if (config.mode === "extract") {
  mkdirSync(dirname(config.source), { recursive: true });
  const temporary = `${config.source}.partial`;
  rmSync(temporary, { force: true });
  const graph = extractor.extractRoadGraph(config.pbf, {
    profile: config.profile,
    turnCosts: config.turnCosts,
    log
  });
  extractor.writeRoadGraph(temporary, graph);
  renameSync(temporary, config.source);
  writeFileSync(`${config.source}.identity.json`, `${JSON.stringify({
    fingerprint: config.sourceFingerprint,
    rangefindVersion: config.rangefindVersion,
    profile: config.profile
  }, null, 2)}\n`);
  console.log(`Rangefind roads: extracted ${config.profile} graph with ${graph.nodeLat.length.toLocaleString()} nodes and ${graph.edgeFrom.length.toLocaleString()} edges.`);
} else if (config.mode === "build") {
  if (!existsSync(config.source)) throw new Error(`Missing road graph source: ${config.source}`);
  const temporary = `${config.output}.partial`;
  rmSync(temporary, { recursive: true, force: true });
  mkdirSync(temporary, { recursive: true });
  const graph = await extractor.readRoadGraph(config.source);
  const summary = buildRouteGraph(graph, temporary, { ...config.buildOptions, log });
  rmSync(join(temporary, "node-order.bin"), { force: true });
  mkdirSync(join(temporary, "_build"), { recursive: true });
  writeFileSync(join(temporary, "_build/identity.json"), `${JSON.stringify({
    fingerprint: config.fingerprint,
    rangefindVersion: config.rangefindVersion,
    profile: config.profile
  }, null, 2)}\n`);
  rmSync(config.output, { recursive: true, force: true });
  renameSync(temporary, config.output);
  console.log(`Rangefind roads: built ${config.profile} index with ${summary.shardCount} shard(s), ${summary.leaves.toLocaleString()} leaves.`);
} else {
  throw new Error(`Unknown road worker mode: ${config.mode}`);
}
