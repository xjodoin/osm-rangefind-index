import assert from "node:assert/strict";
import test from "node:test";
import {
  acquisitionSessionSignature,
  openAcquisitionSession,
  recordAcquisitionFailure,
  recordAcquisitionSuccess
} from "../scripts/lib/acquisition_resume.mjs";

test("acquisition sessions resume only the same immutable run", () => {
  const signature = acquisitionSessionSignature({ regions: ["a", "b"], schema: 11 });
  const first = openAcquisitionSession(null, signature, ["a", "b"], "2026-08-04T00:00:00.000Z");
  recordAcquisitionSuccess(first.session, "a", "2026-08-04T00:01:00.000Z");
  recordAcquisitionFailure(first.session, "b", new Error("timeout"), 1, "2026-08-04T00:02:00.000Z");

  const resumed = openAcquisitionSession(first.session, signature, ["a", "b"], "2026-08-04T00:03:00.000Z");
  assert.equal(resumed.resumed, true);
  assert.deepEqual(resumed.session.completedRegionIds, ["a"]);
  assert.equal(resumed.session.failures.b.message, "timeout");

  recordAcquisitionSuccess(resumed.session, "b", "2026-08-04T00:04:00.000Z");
  assert.deepEqual(resumed.session.completedRegionIds, ["a", "b"]);
  assert.deepEqual(resumed.session.failures, {});
});

test("acquisition sessions restart when run inputs change", () => {
  const original = openAcquisitionSession(null, "old", ["a", "b"]).session;
  recordAcquisitionSuccess(original, "a");

  const replacement = openAcquisitionSession(original, "new", ["a", "b"]);
  assert.equal(replacement.resumed, false);
  assert.deepEqual(replacement.session.completedRegionIds, []);
});

test("resumed sessions discard regions removed from configuration", () => {
  const current = openAcquisitionSession(null, "same", ["a", "b"]).session;
  recordAcquisitionSuccess(current, "a");
  recordAcquisitionSuccess(current, "b");

  const resumed = openAcquisitionSession(current, "same", ["b"]);
  assert.deepEqual(resumed.session.completedRegionIds, ["b"]);
});
