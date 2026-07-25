import assert from "node:assert/strict";
import test from "node:test";
import { appendStaleObjectPaths } from "../scripts/lib/root_artifacts.mjs";

test("collects large stale routing listings without spreading function arguments", () => {
  const objects = Array.from({ length: 200000 }, (_, index) => ({
    path: `authority/packs/${String(index).padStart(6, "0")}.bin`
  }));
  const keep = new Set([
    objects[0].path,
    objects[100000].path,
    objects.at(-1).path
  ]);
  const stale = ["text-routing/old.bin"];

  assert.equal(appendStaleObjectPaths(stale, objects, keep), stale);
  assert.equal(stale.length, 199998);
  assert.equal(stale[0], "text-routing/old.bin");
  assert.equal(stale[1], objects[1].path);
  assert.equal(stale.at(-1), objects.at(-2).path);
});
