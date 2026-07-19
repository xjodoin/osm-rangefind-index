import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const projectRoot = dirname(dirname(fileURLToPath(import.meta.url)));

test("computes corpus deltas in an isolated worker", async () => {
  const directory = mkdtempSync(join(tmpdir(), "corpus-delta-worker-test-"));
  const snapshot = join(directory, "snapshot.jsonl.gz");
  const fresh = join(directory, "fresh.jsonl");
  const delta = join(directory, "delta.jsonl");
  writeFileSync(snapshot, gzipSync([
    '{"id":"same","name":"Same"}',
    '{"id":"changed","name":"Before"}',
    '{"id":"deleted","name":"Gone"}'
  ].join("\n") + "\n"));
  writeFileSync(fresh, [
    '{"id":"same","name":"Same"}',
    '{"id":"changed","name":"After"}',
    '{"id":"added","name":"New"}'
  ].join("\n") + "\n");

  try {
    const worker = join(projectRoot, "scripts/compute_delta_worker.mjs");
    const value = await new Promise((resolveDone, rejectDone) => {
      let result;
      const child = fork(worker, [snapshot, fresh, delta], {
        stdio: ["ignore", "ignore", "inherit", "ipc"]
      });
      child.on("message", message => {
        if (message?.type === "result") result = message.value;
      });
      child.on("error", rejectDone);
      child.on("exit", code => {
        if (code === 0 && result) resolveDone(result);
        else rejectDone(new Error(`delta worker exited ${code}`));
      });
    });
    assert.deepEqual(value, { deltaPath: delta, added: 1, changed: 1, deleted: 1, fresh: 3 });
    assert.equal(await import("node:fs/promises").then(fs => fs.readFile(delta, "utf-8")), [
      '{"id":"changed","name":"After"}',
      '{"id":"added","name":"New"}'
    ].join("\n") + "\n");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
