#!/usr/bin/env node

// Real-PBF smoke benchmark for the complete road-index lane. By default it
// downloads Liechtenstein, extracts/builds every production travel profile
// through the same isolated worker used in production, opens each graph through
// Rangefind's file-range adapter, and verifies a route between Vaduz and Schaan.

import { spawnSync } from "node:child_process";
import { createWriteStream, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { openRouteGraphDir } from "rangefind/route/node";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sourceUrl = process.env.ROAD_BENCH_PBF_URL
  || "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf";
const profiles = String(process.env.ROAD_BENCH_PROFILES || "car,bike,foot")
  .split(",")
  .map(value => value.trim())
  .filter(Boolean);
for (const profile of profiles) {
  if (!["car", "bike", "foot"].includes(profile)) throw new Error(`Unsupported road benchmark profile: ${profile}`);
}
if (!profiles.length) throw new Error("ROAD_BENCH_PROFILES must contain at least one profile.");
const keep = process.argv.includes("--keep");
const root = mkdtempSync(join(tmpdir(), "rangefind-road-bench-"));
const pbf = join(root, "source.osm.pbf");
const worker = join(projectRoot, "scripts/road_index_worker.mjs");

async function download() {
  const response = await fetch(sourceUrl);
  if (!response.ok || !response.body) throw new Error(`GET ${sourceUrl} -> HTTP ${response.status}`);
  await pipeline(Readable.fromWeb(response.body), createWriteStream(pbf));
}

function run(profile, mode, config) {
  const path = join(root, `${profile}-${mode}.json`);
  writeFileSync(path, `${JSON.stringify({ mode, region: "benchmark", profile, ...config }, null, 2)}\n`);
  const started = performance.now();
  const child = spawnSync(process.execPath, ["--max-old-space-size=4096", worker, path], {
    cwd: projectRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"]
  });
  if (child.status !== 0) throw new Error(`${mode} failed:\n${child.stdout}\n${child.stderr}`);
  return { milliseconds: performance.now() - started, output: child.stdout.trim() };
}

function directoryBytes(path) {
  const manifest = JSON.parse(readFileSync(join(path, "manifest.json"), "utf8"));
  let bytes = 0;
  const visit = directory => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const item = join(directory, entry.name);
      if (entry.isDirectory()) visit(item);
      else bytes += statSync(item).size;
    }
  };
  // Keep this helper synchronous so the timed route starts with a cold engine,
  // not with filesystem work still pending.
  visit(path);
  return { bytes, manifest };
}

try {
  const downloadStarted = performance.now();
  await download();
  const downloadMs = performance.now() - downloadStarted;
  const results = [];
  for (const profile of profiles) {
    const graph = join(root, `${profile}.graph.bin`);
    const output = join(root, `${profile}.index`);
    const extracted = run(profile, "extract", { pbf, source: graph, turnCosts: true });
    const built = run(profile, "build", {
      source: graph,
      output,
      buildOptions: { leafNodes: 1280, fanout: 8, topMaxCells: 8, packBytes: 2 * 1024 * 1024, shards: 1, timeBuckets: [] }
    });
    const { bytes, manifest } = directoryBytes(output);
    const engine = await openRouteGraphDir(output);
    const routeStarted = performance.now();
    const route = await engine.route({
      from: { lat: 47.1410, lon: 9.5209 },
      to: { lat: 47.1662, lon: 9.5109 }
    });
    const routeMs = performance.now() - routeStarted;
    if (!route.geometry?.length || !(route.seconds > 0) || !(route.distanceMeters > 0)) {
      throw new Error(`${profile} route smoke result is incomplete.`);
    }
    results.push({
      profile,
      extractMs: Math.round(extracted.milliseconds),
      buildMs: Math.round(built.milliseconds),
      indexMiB: Number((bytes / 1024 / 1024).toFixed(1)),
      manifest,
      route: {
        milliseconds: Number(routeMs.toFixed(1)),
        seconds: route.seconds,
        distanceMeters: route.distanceMeters,
        geometryPoints: route.geometry.length,
        requests: route.stats?.requests,
        fetchedBytes: route.stats?.bytes
      }
    });
  }
  console.log(JSON.stringify({
    sourceUrl,
    pbfMiB: Number((statSync(pbf).size / 1024 / 1024).toFixed(1)),
    downloadMs: Math.round(downloadMs),
    profiles: results
  }, null, 2));
  if (keep) console.log(`kept ${root}`);
} finally {
  if (!keep) rmSync(root, { recursive: true, force: true });
}
