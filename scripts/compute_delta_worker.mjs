#!/usr/bin/env node

// Computes one corpus delta in an isolated process. The snapshot index can
// occupy a few GiB for country-scale regions; process exit returns that
// memory to the OS before the orchestrator advances to the next shard.
// The index is a typed-array hash table (corpus_delta_index.mjs), not a Map:
// V8 Maps cap at 2^24 entries and the largest shards hold 24-67M documents.

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, renameSync, rmSync } from "node:fs";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { createGunzip } from "node:zlib";
import { CorpusDeltaIndex, hashId } from "./corpus_delta_index.mjs";

const [snapshotPath, freshPath, deltaPath] = process.argv.slice(2);

function lineDocId(line) {
  const match = line.match(/"id":"([^"]*)"/u);
  return match ? match[1] : "";
}

function lineHash(line) {
  const digest = createHash("sha1").update(line).digest();
  return [digest.readUInt32BE(0), digest.readUInt32BE(4)];
}

async function linesOf(stream) {
  return createInterface({ input: stream, crlfDelay: Infinity });
}

try {
  if (!snapshotPath || !freshPath || !deltaPath) {
    throw new Error("usage: compute_delta_worker.mjs <snapshot.jsonl.gz> <fresh.jsonl[.gz]> <delta.jsonl>");
  }
  const old = new CorpusDeltaIndex();
  for await (const line of await linesOf(createReadStream(snapshotPath).pipe(createGunzip()))) {
    if (!line) continue;
    const [idHi, idLo] = hashId(lineDocId(line));
    const [hashHi, hashLo] = lineHash(line);
    old.set(idHi, idLo, hashHi, hashLo);
  }

  const tempPath = `${deltaPath}.tmp`;
  const writer = createWriteStream(tempPath);
  let added = 0;
  let changed = 0;
  let fresh = 0;
  const freshInput = createReadStream(freshPath);
  const freshStream = freshPath.endsWith(".gz") ? freshInput.pipe(createGunzip()) : freshInput;
  for await (const line of await linesOf(freshStream)) {
    if (!line) continue;
    fresh++;
    const [idHi, idLo] = hashId(lineDocId(line));
    const [hashHi, hashLo] = lineHash(line);
    const status = old.take(idHi, idLo, hashHi, hashLo);
    if (status === "same") continue;
    if (status === "changed") changed++;
    else added++;
    if (!writer.write(`${line}\n`)) await once(writer, "drain");
  }
  writer.end();
  await once(writer, "finish");
  renameSync(tempPath, deltaPath);
  const value = { deltaPath, added, changed, deleted: old.remaining, fresh };
  process.send?.({ type: "result", value }, () => process.disconnect());
} catch (error) {
  if (deltaPath) rmSync(`${deltaPath}.tmp`, { force: true });
  console.error(error);
  process.exitCode = 1;
}
