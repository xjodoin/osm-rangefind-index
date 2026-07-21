import assert from "node:assert/strict";
import test from "node:test";
import {
  buildRootPayloads,
  parseArgs,
  publishRootManifests
} from "../scripts/refresh_root_lexicon.mjs";

function root(builtAt = "2026-07-20T00:00:00Z") {
  return {
    format: "rfshard-v1",
    built_at: builtAt,
    total: 1,
    shards: [{ id: "quebec", path: "shards/quebec/", total: 1 }]
  };
}

function snapshots(manifest = root()) {
  return Object.fromEntries(["manifest.json", "manifest.min.json"].map((name, index) => {
    const copy = structuredClone(manifest);
    return [name, {
      name,
      text: JSON.stringify(copy),
      etag: `\"base-${index}\"`,
      manifest: copy
    }];
  }));
}

function mockStore(initial, { failName = null } = {}) {
  const state = structuredClone(initial);
  const puts = [];
  return {
    state,
    puts,
    async getTextWithMetadata(name) {
      return { text: state[name].text, etag: state[name].etag, lastModified: null };
    },
    async putBytes(name, text, { ifMatch }) {
      puts.push({ name, text, ifMatch });
      if (state[name].etag !== ifMatch) {
        const error = new Error("precondition failed");
        error.$metadata = { httpStatusCode: 412 };
        throw error;
      }
      if (name === failName && text.includes("category_lexicon")) throw new Error("injected write failure");
      state[name] = { text, etag: `\"write-${puts.length}\"` };
      return { etag: state[name].etag };
    }
  };
}

function responseLostStore(initial) {
  const store = mockStore(initial);
  const put = store.putBytes.bind(store);
  let first = true;
  store.putBytes = async (...args) => {
    const result = await put(...args);
    if (first) {
      first = false;
      const error = new Error("socket closed after upload");
      error.code = "ECONNRESET";
      throw error;
    }
    return result;
  };
  return store;
}

test("root payload refresh preserves build metadata and per-manifest fields", () => {
  const baseline = snapshots();
  baseline["manifest.json"].manifest.full_only = true;
  const payloads = buildRootPayloads(baseline, { version: 1, types: ["cinema"], aliases: {} });
  const full = JSON.parse(payloads["manifest.json"]);
  const min = JSON.parse(payloads["manifest.min.json"]);
  assert.equal(full.built_at, "2026-07-20T00:00:00Z");
  assert.equal(full.full_only, true);
  assert.equal(min.full_only, undefined);
  assert.deepEqual(full.category_lexicon.types, ["cinema"]);
});

test("root publication refuses a root changed during the merge", async () => {
  const baseline = snapshots();
  const store = mockStore(baseline);
  store.state["manifest.min.json"].etag = "\"newer\"";
  const payloads = buildRootPayloads(baseline, { version: 1, types: ["cinema"], aliases: {} });
  await assert.rejects(
    publishRootManifests({ store, baseline, payloads }),
    /refusing to overwrite a newer root/u
  );
  assert.equal(store.puts.length, 0);
});

test("root publication rolls back the first manifest when the second write fails", async () => {
  const baseline = snapshots();
  const store = mockStore(baseline, { failName: "manifest.min.json" });
  const payloads = buildRootPayloads(baseline, { version: 1, types: ["cinema"], aliases: {} });
  await assert.rejects(
    publishRootManifests({ store, baseline, payloads }),
    /Prior writes were rolled back/u
  );
  assert.equal(store.state["manifest.json"].text, baseline["manifest.json"].text);
  assert.equal(store.state["manifest.min.json"].text, baseline["manifest.min.json"].text);
});

test("root publication rolls back an in-flight write whose success response was lost", async () => {
  const baseline = snapshots();
  const store = responseLostStore(baseline);
  const payloads = buildRootPayloads(baseline, { version: 1, types: ["cinema"], aliases: {} });
  await assert.rejects(
    publishRootManifests({ store, baseline, payloads }),
    /Prior writes were rolled back/u
  );
  assert.equal(store.state["manifest.json"].text, baseline["manifest.json"].text);
  assert.equal(store.state["manifest.min.json"].text, baseline["manifest.min.json"].text);
});

test("refresh CLI rejects missing values, duplicate regions, and unsafe base URLs", () => {
  assert.throws(() => parseArgs(["--base-url"]), /requires a value/u);
  assert.throws(() => parseArgs(["--regions", "quebec,quebec"]), /duplicate/u);
  assert.throws(() => parseArgs(["--upload", "--regions", "quebec"]), /partial vocabulary/u);
  assert.throws(() => parseArgs(["--base-url", "file:///tmp/index"]), /http or https/u);
});
