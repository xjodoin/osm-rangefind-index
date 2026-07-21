import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { acquireProcessLock } from "../scripts/lib/process_lock.mjs";

test("index and root refresh commands share one exclusive process lock", () => {
  const directory = mkdtempSync(join(tmpdir(), "osm-process-lock-"));
  const path = join(directory, ".lock");
  try {
    const release = acquireProcessLock(path, { label: "First command" });
    assert.throws(() => acquireProcessLock(path, { label: "Second command" }), /Second command is active/u);
    release();
    const releaseAgain = acquireProcessLock(path);
    releaseAgain();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
