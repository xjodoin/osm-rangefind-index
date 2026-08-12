#!/usr/bin/env node

import { fork, spawn } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createAdaptiveStageLimiter } from "./lib/adaptive_stage_limiter.mjs";

const projectRoot = join(fileURLToPath(new URL(".", import.meta.url)), "..");
const worker = join(projectRoot, "scripts/osm_extract_worker.mjs");
const enrichmentWorker = join(projectRoot, "scripts/address_enrichment_worker.mjs");
const roadWorker = join(projectRoot, "scripts/road_index_worker.mjs");
const sources = [
  ["liechtenstein", "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"],
  ["luxembourg", "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf"]
];

async function download(url, path) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  writeFileSync(path, Buffer.from(await response.arrayBuffer()));
}

function extract(configPath) {
  return new Promise((resolveDone, rejectDone) => {
    let result;
    const child = fork(worker, [configPath], {
      execArgv: ["--max-old-space-size=4096"],
      stdio: ["ignore", "ignore", "inherit", "ipc"]
    });
    child.on("message", message => {
      if (message?.type === "result") result = message.value;
    });
    child.on("error", rejectDone);
    child.on("exit", code => {
      if (code === 0 && result) resolveDone(result);
      else rejectDone(new Error(`extract worker exited ${code}`));
    });
  });
}

async function runMode(root, mode, concurrency) {
  const stage = join(root, mode);
  mkdirSync(stage, { recursive: true });
  const configs = sources.map(([id]) => {
    const config = join(stage, `${id}.json`);
    writeFileSync(config, JSON.stringify({
      region: id,
      pbf: join(root, `${id}.osm.pbf`),
      root: join(stage, id),
      rqa: false,
      force: true
    }));
    return config;
  });
  const started = performance.now();
  if (concurrency === 1) {
    for (const config of configs) await extract(config);
  } else {
    const limiter = createAdaptiveStageLimiter({ capacity: concurrency });
    await Promise.all(configs.map(config => limiter.run(1, () => extract(config))));
  }
  return Math.round(performance.now() - started);
}

function ipc(workerPath, configPath, heapMb = 4096) {
  return new Promise((resolveDone, rejectDone) => {
    let result;
    const child = fork(workerPath, [configPath], {
      execArgv: [`--max-old-space-size=${heapMb}`],
      stdio: ["ignore", "ignore", "inherit", "ipc"]
    });
    child.on("message", message => { if (message?.type === "result") result = message.value; });
    child.on("error", rejectDone);
    child.on("exit", code => code === 0 && result
      ? resolveDone(result)
      : rejectDone(new Error(`${workerPath} exited ${code}`)));
  });
}

function road(configPath) {
  return new Promise((resolveDone, rejectDone) => {
    const child = spawn(process.execPath, ["--max-old-space-size=4096", roadWorker, configPath], {
      stdio: ["ignore", "ignore", "inherit"]
    });
    child.on("error", rejectDone);
    child.on("exit", code => code === 0 ? resolveDone() : rejectDone(new Error(`road worker exited ${code}`)));
  });
}

async function benchmarkRealOverlap(root) {
  const id = sources[0][0];
  const pbf = join(root, `${id}.osm.pbf`);
  const osmRoot = join(root, "overlap-osm");
  const osmConfig = join(root, "overlap-osm.json");
  writeFileSync(osmConfig, JSON.stringify({ region: id, pbf, root: osmRoot, rqa: false, force: true }));
  const osm = await extract(osmConfig);
  const osmPath = join(osmRoot, "data", "osm-places.jsonl");
  const addressPath = join(root, "addresses.jsonl");
  const addressRows = Math.max(50_000, Number(process.env.ACQUISITION_BENCH_ADDRESS_ROWS || 250_000));
  const lines = [];
  for (let index = 0; index < addressRows; index++) {
    lines.push(JSON.stringify({
      id: `bench-${index}`,
      kind: "address",
      houseNumber: String(index + 1),
      street: "Benchmark Street",
      city: "Vaduz",
      postcode: "9490",
      country: "LI",
      lat: 47.141 + (index % 100) * 0.000001,
      lon: 9.521 + (index % 100) * 0.000001
    }));
  }
  writeFileSync(addressPath, `${lines.join("\n")}\n`);
  const enrichment = label => {
    const config = join(root, `${label}-enrichment.json`);
    writeFileSync(config, JSON.stringify({
      region: id,
      regionConfig: { id, groups: ["europe"] },
      root: join(root, label),
      osmPath,
      outputPath: join(root, label, "enriched.jsonl"),
      osmDocs: osm.docs,
      sources: [{ id: "benchmark", name: "Benchmark", format: "jsonl", path: addressPath, includeAddresses: true }]
    }));
    return () => ipc(enrichmentWorker, config);
  };
  const roadExtract = label => {
    const configs = ["car", "bike", "foot"].map(profile => {
      const config = join(root, `${label}-road-${profile}.json`);
      writeFileSync(config, JSON.stringify({
        mode: "extract",
        region: id,
        profile,
        pbf,
        source: join(root, label, `road-${profile}.bin`),
        turnCosts: true,
        portalRegions: []
      }));
      return config;
    });
    return async () => {
      for (const config of configs) await road(config);
    };
  };
  const sequentialStarted = performance.now();
  await enrichment("sequential")();
  await roadExtract("sequential")();
  const sequentialMs = Math.round(performance.now() - sequentialStarted);
  const parallelStarted = performance.now();
  await Promise.all([enrichment("parallel")(), roadExtract("parallel")()]);
  const parallelMs = Math.round(performance.now() - parallelStarted);
  return { addressRows, sequentialMs, parallelMs, speedup: Number((sequentialMs / parallelMs).toFixed(2)) };
}

const root = join(tmpdir(), `rangefind-acquisition-bench-${process.pid}`);
mkdirSync(root, { recursive: true });
try {
  for (const [id, url] of sources) await download(url, join(root, `${id}.osm.pbf`));
  const sequentialMs = await runMode(root, "sequential", 1);
  const parallelMs = await runMode(root, "parallel", 2);
  const overlap = await benchmarkRealOverlap(root);
  console.log(JSON.stringify({
    sources: sources.map(([id]) => id),
    sequentialMs,
    parallelMs,
    speedup: Number((sequentialMs / parallelMs).toFixed(2)),
    enrichmentRoadOverlap: overlap
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
