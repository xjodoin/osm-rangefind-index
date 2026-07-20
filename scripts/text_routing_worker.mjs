#!/usr/bin/env node

// Runs memory-heavy term-set and federated routing generation outside the
// long-lived index orchestrator. Exiting after each operation guarantees V8
// releases the heap instead of retaining several shards' term dictionaries.

import { readFileSync } from "node:fs";
import * as rangefindShards from "rangefind/shards";

const [mode, first, second] = process.argv.slice(2);

try {
  let value;
  if (mode === "term-set" && first && second) {
    value = rangefindShards.writeShardTermSet({ dir: first, outFile: second });
  } else if (mode === "routing" && first) {
    const config = JSON.parse(readFileSync(first, "utf-8"));
    value = await rangefindShards.writeTextRoutingIndex(config);
  } else if (mode === "suggest-set" && first && second) {
    value = rangefindShards.writeShardSuggestSet({ dir: first, outFile: second });
  } else if (mode === "suggest-routing" && first) {
    const config = JSON.parse(readFileSync(first, "utf-8"));
    value = await rangefindShards.writeSuggestRoutingIndex(config);
  } else {
    throw new Error("usage: text_routing_worker.mjs term-set <shard-dir> <output> | routing <config.json> | suggest-set <shard-dir> <output> | suggest-routing <config.json>");
  }
  if (process.send) {
    process.send({ type: "result", value }, () => process.disconnect());
  }
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
