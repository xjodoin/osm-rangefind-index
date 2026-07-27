import assert from "node:assert/strict";
import test from "node:test";
import { fetchSource, SOURCE_USER_AGENT } from "../scripts/lib/source_fetch.mjs";

test("source requests identify the indexer and preserve caller headers", async () => {
  let captured;
  const response = { ok: true };
  const actual = await fetchSource("https://download.example/test.osm.pbf", {
    method: "HEAD",
    headers: { accept: "application/octet-stream" }
  }, {
    fetchImpl: async (url, init) => {
      captured = { url, init };
      return response;
    },
    timeoutMs: 100
  });

  assert.equal(actual, response);
  assert.equal(captured.url, "https://download.example/test.osm.pbf");
  assert.equal(captured.init.method, "HEAD");
  assert.equal(captured.init.headers.get("accept"), "application/octet-stream");
  assert.equal(captured.init.headers.get("user-agent"), SOURCE_USER_AGENT);
});

test("source requests abort instead of hanging indefinitely", async () => {
  await assert.rejects(
    fetchSource("https://download.example/stalled.osm.pbf", {}, {
      fetchImpl: async (_url, init) => new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true });
      }),
      timeoutMs: 5
    }),
    /Source request timed out after 5 ms/u
  );
});
