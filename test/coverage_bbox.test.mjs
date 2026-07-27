import assert from "node:assert/strict";
import test from "node:test";
import { geometryCoverageBbox } from "../scripts/lib/coverage_bbox.mjs";

test("geometryCoverageBbox returns a conventional regional bbox", () => {
  assert.deepEqual(geometryCoverageBbox({
    type: "Polygon",
    coordinates: [[
      [-79.76483, 40.43922],
      [-71.6608, 40.43922],
      [-71.6608, 45.01766],
      [-79.76483, 45.01766],
      [-79.76483, 40.43922]
    ]]
  }), [40.43922, -79.76483, 45.01766, -71.6608]);
});

test("geometryCoverageBbox keeps antimeridian coverage narrow and wrapped", () => {
  assert.deepEqual(geometryCoverageBbox({
    type: "MultiPolygon",
    coordinates: [
      [[[127, 37.3], [179, 82.1], [127, 82.1], [127, 37.3]]],
      [[[-179, 50], [-169, 70], [-179, 70], [-179, 50]]]
    ]
  }), [37.3, 127, 82.1, -169]);
});

test("geometryCoverageBbox rejects missing and invalid positions", () => {
  assert.equal(geometryCoverageBbox(null), null);
  assert.equal(geometryCoverageBbox({ type: "Polygon", coordinates: [] }), null);
  assert.equal(geometryCoverageBbox({
    type: "Point",
    coordinates: [10, 100]
  }), null);
});
