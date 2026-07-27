import assert from "node:assert/strict";
import test from "node:test";
import {
  ROOT_ROUTING_ARTIFACTS,
  rootRoutingArtifactIsPublished
} from "../scripts/lib/root_publish.mjs";

test("root routing artifacts map storage prefixes to manifest blocks", () => {
  assert.deepEqual(ROOT_ROUTING_ARTIFACTS, [
    { prefix: "text-routing", manifestKey: "text_routing" },
    { prefix: "authority", manifestKey: "suggest_routing" }
  ]);
});

test("unchanged root routing blocks can skip immutable re-uploads", () => {
  const block = {
    storage: "range-pack-v1",
    pack_table: [{ file: "packs/0000.bin", bytes: 123 }],
    directory_root: { file: "directory-root.abc.bin.gz", offset: 0, length: 42 }
  };
  const local = { text_routing: block };
  const remote = { text_routing: structuredClone(block) };

  assert.equal(rootRoutingArtifactIsPublished(local, remote, "text_routing"), true);
  remote.text_routing.pack_table[0].bytes++;
  assert.equal(rootRoutingArtifactIsPublished(local, remote, "text_routing"), false);
});

test("missing local or remote routing blocks require an upload", () => {
  assert.equal(rootRoutingArtifactIsPublished({}, {}, "suggest_routing"), false);
  assert.equal(rootRoutingArtifactIsPublished(
    { suggest_routing: { keys: 10 } },
    {},
    "suggest_routing"
  ), false);
});
