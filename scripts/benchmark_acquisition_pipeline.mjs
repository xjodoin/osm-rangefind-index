#!/usr/bin/env node

import { fork } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createAdaptiveStageLimiter } from "./lib/adaptive_stage_limiter.mjs";

const projectRoot = join(fileURLToPath(new URL(".", import.meta.url)), "..");
const worker = join(projectRoot, "scripts/osm_extract_worker.mjs");
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

const root = join(tmpdir(), `rangefind-acquisition-bench-${process.pid}`);
mkdirSync(root, { recursive: true });
try {
  for (const [id, url] of sources) await download(url, join(root, `${id}.osm.pbf`));
  const sequentialMs = await runMode(root, "sequential", 1);
  const parallelMs = await runMode(root, "parallel", 2);
  console.log(JSON.stringify({
    sources: sources.map(([id]) => id),
    sequentialMs,
    parallelMs,
    speedup: Number((sequentialMs / parallelMs).toFixed(2))
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
