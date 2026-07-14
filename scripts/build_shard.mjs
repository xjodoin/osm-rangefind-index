#!/usr/bin/env node

// Child runner for one shard build (full or generational delta). Runs in its
// own process so the orchestrator can SIGTERM it at the idle-window
// deadline; full builds resume from rangefind's stage checkpoints, deltas
// simply re-run (they are small by definition).

import { build } from "rangefind/builder";

const configPath = process.argv[2];
const update = process.argv.includes("--update");
if (!configPath) {
  console.error("usage: build_shard.mjs <config.json> [--update]");
  process.exit(2);
}

let terminating = false;
process.on("SIGTERM", () => {
  // Let the current stage checkpoint flush; the process exits when the
  // event loop drains or the second signal arrives.
  terminating = true;
  console.error("build_shard: SIGTERM — exiting after current write");
  process.exit(143);
});

build({ configPath, update }).then(() => {
  process.exit(0);
}).catch(error => {
  console.error(error);
  process.exit(terminating ? 143 : 1);
});
