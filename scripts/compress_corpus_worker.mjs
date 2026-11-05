#!/usr/bin/env node

// Compresses one corpus snapshot in an isolated process. The parent sets
// UV_THREADPOOL_SIZE for this child so block compression gets its own
// threads instead of competing with the orchestrator's fs I/O pool.

import { gzipFileParallel } from "./lib/parallel_gzip.mjs";

const [input, output, levelArg, workersArg] = process.argv.slice(2);

try {
  if (!input || !output) {
    throw new Error("usage: compress_corpus_worker.mjs <input.jsonl> <output.gz> [level] [workers]");
  }
  const value = await gzipFileParallel(input, output, {
    level: Number(levelArg) || 6,
    workers: Number(workersArg) || 4
  });
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
