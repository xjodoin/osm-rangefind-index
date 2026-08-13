import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import test from "node:test";

test("status metrics render large counts compactly without losing exact values", async () => {
  const html = readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  const script = html.match(/<script>([\s\S]*?)<\/script>/u)?.[1];
  assert.ok(script, "status page has an inline renderer");

  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) {
      const attributes = new Map();
      elements.set(id, {
        attributes,
        className: "",
        style: {},
        textContent: "",
        title: "",
        setAttribute(name, value) { attributes.set(name, value); }
      });
    }
    return elements.get(id);
  };
  const status = {
    generatedAt: "2026-08-10T20:00:00.000Z",
    index: {
      phase: "ready",
      acquiredRegions: 310,
      totalRegions: 310,
      acquisitionPercent: 100,
      publishedShards: 310,
      publicationPercent: 100,
      publishedDocuments: 846_996_476,
      latestDataAt: "2026-08-10T00:00:00.000Z"
    },
    run: { state: "idle", lastSuccessfulAt: "2026-08-10T20:00:00.000Z" },
    roadIndexes: { enabled: true, catalogEntries: 927, total: 927, uploaded: 927, unavailable: 0 }
  };
  const context = {
    Date,
    Intl,
    __status: status,
    document: { getElementById: element },
    fetch: async () => ({ ok: true, json: async () => status }),
    setInterval: () => 0
  };

  await runInNewContext(`(async () => {${script}\nrender(__status);})()`, context);

  const exactDocuments = new Intl.NumberFormat().format(status.index.publishedDocuments);
  const expectedCompact = new Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(status.index.publishedDocuments);
  assert.equal(element("documents").textContent, expectedCompact);
  assert.equal(element("documents").title, `${exactDocuments} documents live`);
  assert.equal(element("documents").attributes.get("aria-label"), `${exactDocuments} documents live`);
  assert.equal(element("roads").textContent, "927/927");
  assert.equal(element("roads").title, "927 of 927 road graphs");
});
