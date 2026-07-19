#!/usr/bin/env node

// Computes one corpus delta in an isolated process. The old-document hash map
// can occupy several GiB for country-scale regions; process exit returns that
// heap to the OS before the orchestrator advances to the next shard.

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, renameSync, rmSync } from "node:fs";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { createGunzip } from "node:zlib";

const [snapshotPath, freshPath, deltaPath] = process.argv.slice(2);

function lineDocId(line) {
  const match = line.match(/"id":"([^"]*)"/u);
  return match ? match[1] : "";
}

async function linesOf(stream) {
  return createInterface({ input: stream, crlfDelay: Infinity });
}

try {
  if (!snapshotPath || !freshPath || !deltaPath) {
    throw new Error("usage: compute_delta_worker.mjs <snapshot.jsonl.gz> <fresh.jsonl> <delta.jsonl>");
  }
  const old = new Map();
  for await (const line of await linesOf(createReadStream(snapshotPath).pipe(createGunzip()))) {
    if (line) old.set(lineDocId(line), createHash("sha1").update(line).digest("base64"));
  }

  const tempPath = `${deltaPath}.tmp`;
  const writer = createWriteStream(tempPath);
  let added = 0;
  let changed = 0;
  let fresh = 0;
  for await (const line of await linesOf(createReadStream(freshPath))) {
    if (!line) continue;
    fresh++;
    const id = lineDocId(line);
    const known = old.get(id);
    if (known !== undefined) {
      old.delete(id);
      if (known === createHash("sha1").update(line).digest("base64")) continue;
      changed++;
    } else {
      added++;
    }
    if (!writer.write(`${line}\n`)) await once(writer, "drain");
  }
  writer.end();
  await once(writer, "finish");
  renameSync(tempPath, deltaPath);
  const value = { deltaPath, added, changed, deleted: old.size, fresh };
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  if (deltaPath) rmSync(`${deltaPath}.tmp`, { force: true });
  console.error(error);
  process.exitCode = 1;
}
