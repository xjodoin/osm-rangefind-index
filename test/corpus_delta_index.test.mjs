import assert from "node:assert/strict";
import test from "node:test";
import { CorpusDeltaIndex, hashId } from "../scripts/corpus_delta_index.mjs";

test("holds more entries than a V8 Map allows", () => {
  // Maps throw "Map maximum size exceeded" at 2^24; australia alone is 24M docs.
  const count = 2 ** 24 + 4096;
  const index = new CorpusDeltaIndex();
  for (let i = 0; i < count; i++) index.set(i, Math.imul(i, 0x9e3779b1) >>> 0, i, 7);
  assert.equal(index.size, count);
  assert.equal(index.take(0, 0, 0, 7), "same");
  assert.equal(index.take(count - 1, Math.imul(count - 1, 0x9e3779b1) >>> 0, count - 1, 8), "changed");
  assert.equal(index.take(count, 1, 0, 0), "added");
  assert.equal(index.remaining, count - 2);
});

test("keeps Map semantics for repeated ids", () => {
  const index = new CorpusDeltaIndex();
  const [hi, lo] = hashId("n1");
  index.set(hi, lo, 1, 1);
  index.set(hi, lo, 2, 2); // last snapshot line wins
  assert.equal(index.size, 1);
  assert.equal(index.take(hi, lo, 2, 2), "same");
  assert.equal(index.take(hi, lo, 2, 2), "added"); // consumed, like Map.delete
  assert.equal(index.remaining, 0);
});

test("survives growth with every entry still reachable", () => {
  const index = new CorpusDeltaIndex();
  const ids = Array.from({ length: 50_000 }, (_, i) => `w${i * 7919}`);
  ids.forEach((id, i) => index.set(...hashId(id), i, 0));
  assert.ok(index.capacity >= ids.length * 2);
  ids.forEach((id, i) => assert.equal(index.take(...hashId(id), i, 0), "same"));
  assert.equal(index.remaining, 0);
});

test("hashes distinct ids apart", () => {
  const seen = new Set();
  for (let i = 0; i < 200_000; i++) {
    for (const prefix of ["n", "w", "r"]) seen.add(hashId(`${prefix}${i}`).join(":"));
  }
  assert.equal(seen.size, 600_000);
});
