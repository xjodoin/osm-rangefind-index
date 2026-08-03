#!/usr/bin/env node

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { partitionAddressSourceSpatially } from "./lib/address_sources.mjs";

function integerArgument(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index < 0) return fallback;
  const value = Number(process.argv[index + 1]);
  if (!Number.isInteger(value) || value < 0) throw new Error(`${name} requires a non-negative integer.`);
  return value;
}

const jobs = integerArgument("--jobs", 24);
const rowsPerJob = integerArgument("--rows-per-job", 20_000);
const parallelism = Math.max(1, integerArgument("--parallelism", 4));
const networkDelayMs = integerArgument("--network-delay-ms", 10);
const regions = [
  { id: "quebec", bbox: [44, -80, 63, -57], countryCodes: ["CA"], subdivisionCodes: ["CA-QC"] },
  { id: "ontario", bbox: [41, -96, 57, -74], countryCodes: ["CA"], subdivisionCodes: ["CA-ON"] },
  { id: "vermont", bbox: [42.7, -73.5, 45.1, -71.4], countryCodes: ["US"], subdivisionCodes: ["US-VT"] },
  { id: "new-york", bbox: [40.4, -80, 45.2, -71.7], countryCodes: ["US"], subdivisionCodes: ["US-NY"] }
];

function benchmarkSource(concurrency) {
  return {
    id: `address-benchmark-${concurrency}`,
    identity: { snapshot: "synthetic-v1", config: `concurrency-${concurrency}` },
    partitionConcurrency: concurrency,
    partitionCompressionLevel: 3,
    async *batches() {
      for (let job = 0; job < jobs; job++) {
        yield {
          id: `job-${job.toString().padStart(4, "0")}`,
          records: async function *records() {
            if (networkDelayMs) await new Promise(resolve => setTimeout(resolve, networkDelayMs));
            for (let row = 0; row < rowsPerJob; row++) {
              const canadian = (job + row) % 2 === 0;
              const firstSubdivision = row % 4 < 2;
              yield canadian
                ? {
                    id: `ca-${job}-${row}`,
                    houseNumber: String(row + 1),
                    street: "Benchmark Road",
                    country: "CA",
                    state: firstSubdivision ? "QC" : "ON",
                    lat: 45,
                    lon: -74.5
                  }
                : {
                    id: `us-${job}-${row}`,
                    houseNumber: String(row + 1),
                    street: "Benchmark Road",
                    country: "US",
                    state: firstSubdivision ? "VT" : "NY",
                    lat: 44.5,
                    lon: -73
                  };
            }
          }
        };
      }
    }
  };
}

async function run(root, concurrency) {
  const started = performance.now();
  const partition = await partitionAddressSourceSpatially(benchmarkSource(concurrency), {
    root: join(root, `partitions-${concurrency}`),
    regions,
    normalizeRecord: record => record
  });
  const seconds = (performance.now() - started) / 1000;
  return {
    concurrency,
    seconds: Number(seconds.toFixed(3)),
    rows: partition.rowsRead,
    writes: partition.writes,
    writeAmplification: Number((partition.writes / Math.max(1, partition.rowsRead)).toFixed(3)),
    rowsPerSecond: Math.round(partition.rowsRead / seconds)
  };
}

const root = await mkdtemp(join(tmpdir(), "rangefind-address-benchmark-"));
try {
  const sequential = await run(root, 1);
  const parallel = parallelism === 1 ? sequential : await run(root, parallelism);
  console.log(JSON.stringify({
    jobs,
    rowsPerJob,
    networkDelayMs,
    sequential,
    parallel,
    speedup: Number((sequential.seconds / parallel.seconds).toFixed(2))
  }, null, 2));
} finally {
  await rm(root, { recursive: true, force: true });
}
