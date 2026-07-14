#!/usr/bin/env node

// Generates regions.json (the full shard list) from Geofabrik's official
// region index — never hand-typed paths.
//
// Granularity policy: country-level extracts everywhere, and any region
// whose PBF exceeds EXPAND_PBF_GIB that has subregions is expanded into
// them, recursively — so the US becomes states, Canada provinces, Germany
// Bundesländer, France régions (its overseas territories get their own
// coverage bboxes instead of one France bbox spanning three oceans),
// Russia federal districts, and so on. The policy is data-driven: when a
// region grows past the threshold upstream, regeneration splits it.
//
// Overlap safety: Geofabrik also publishes combined extracts (dach, alps,
// britain-and-ireland, US macro-regions…) that duplicate country data —
// those are excluded, and any ISO-3166 code appearing in two selected
// regions fails the run rather than silently double-indexing a country.
//
// Usage:
//   node scripts/generate_regions.mjs            # verify URLs, write regions.json
//   node scripts/generate_regions.mjs --no-verify
//   node scripts/generate_regions.mjs --dry-run  # print, don't write

import { readFileSync, writeFileSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const INDEX_URL = "https://download.geofabrik.de/index-v1-nogeom.json";
// Regions with a PBF larger than this and available subregions are split.
// ~2.5 GiB ≈ 8-15M places — the comfortable single-shard ceiling.
const EXPAND_PBF_GIB = 2.5;

const CONTINENTS = [
  "africa", "antarctica", "asia", "australia-oceania",
  "central-america", "europe", "north-america", "south-america"
];

// Combined/overlapping extracts that would duplicate country data.
const EXCLUDE = new Set([
  "dach", "alps", "britain-and-ireland",       // combos over European countries
  "sea",                                        // combo over South-East Asia
  "us-midwest", "us-northeast", "us-pacific", "us-south", "us-west" // combos over US states
]);

// Geofabrik's index mislabels several distinct Pacific territories with the
// same ISO code (VU/MH as of 2026-07). They are separate extracts with wrong
// metadata, not overlaps — exempt them from the duplicate-coverage check.
const IGNORE_CODE_COLLISIONS = new Set([
  "american-oceania", "ile-de-clipperton", "polynesie-francaise",
  "tokelau", "wallis-et-futuna", "pitcairn-islands"
]);

const args = new Set(process.argv.slice(2));
const verify = !args.has("--no-verify");
const dryRun = args.has("--dry-run");

const index = await (await fetch(INDEX_URL)).json();
const byId = new Map(index.features.map(f => [f.properties.id, f.properties]));
const byParent = new Map();
for (const f of index.features) {
  const p = f.properties;
  if (!byParent.has(p.parent || "")) byParent.set(p.parent || "", []);
  byParent.get(p.parent || "").push(p);
}

// Subregions are keyed two ways in the index: parent pointers (germany →
// bayern) or id prefixes with a different parent (us/california sits under
// north-america).
function childrenOf(id) {
  const seen = new Set();
  const children = [];
  for (const entry of [...(byParent.get(id) || []), ...index.features.map(f => f.properties).filter(p => p.id.startsWith(`${id}/`))]) {
    if (seen.has(entry.id) || EXCLUDE.has(entry.id)) continue;
    seen.add(entry.id);
    children.push(entry);
  }
  return children;
}

function selectRegions() {
  const picked = [];
  const visit = entry => {
    if (EXCLUDE.has(entry.id)) return;
    if (entry.id.includes("/")) return; // sub-subregions enter via size expansion
    picked.push(entry);
  };
  for (const continent of CONTINENTS) {
    const children = byParent.get(continent) || [];
    if (!children.length) {
      visit(byId.get(continent)); // e.g. antarctica has no subregions
      continue;
    }
    for (const child of children) visit(child);
  }
  visit(byId.get("russia")); // top-level, not under a continent
  return picked;
}

const pbfSizes = new Map();

async function fetchSizes(entries) {
  const queue = entries.filter(entry => entry.urls?.pbf && !pbfSizes.has(entry.id));
  await Promise.all(Array.from({ length: 8 }, async () => {
    for (;;) {
      const entry = queue.shift();
      if (!entry) return;
      const head = await fetch(entry.urls.pbf, { method: "HEAD" });
      if (!head.ok) throw new Error(`HEAD ${entry.urls.pbf} → HTTP ${head.status}`);
      pbfSizes.set(entry.id, Number(head.headers.get("content-length") || 0));
    }
  }));
}

let selected = selectRegions();
for (let depth = 0; depth < 3; depth++) {
  await fetchSizes(selected);
  const threshold = EXPAND_PBF_GIB * 1024 ** 3;
  const oversized = selected.filter(entry => (pbfSizes.get(entry.id) || 0) > threshold && childrenOf(entry.id).length);
  if (!oversized.length) break;
  for (const entry of oversized) {
    console.log(`splitting ${entry.id} (${((pbfSizes.get(entry.id) || 0) / 1024 ** 3).toFixed(1)} GiB) into ${childrenOf(entry.id).length} subregions`);
  }
  const oversizedIds = new Set(oversized.map(entry => entry.id));
  selected = selected.flatMap(entry => (oversizedIds.has(entry.id) ? childrenOf(entry.id) : [entry]));
}

// Safety: no country may be covered by two shards.
const codeOwners = new Map();
for (const entry of selected) {
  if (IGNORE_CODE_COLLISIONS.has(entry.id)) continue;
  for (const code of entry["iso3166-1:alpha2"] || []) {
    if (codeOwners.has(code)) {
      throw new Error(`ISO code ${code} covered twice: ${codeOwners.get(code)} and ${entry.id}`);
    }
    codeOwners.set(code, entry.id);
  }
}

// Hierarchy labels for multi-level querying: `shards: ["canada"]` expands
// to every member shard at query time. The chain is the entry's Geofabrik
// ancestry (id-prefix logical parent first — us/california sits under
// north-america but belongs to "us").
function groupsOf(entry) {
  const groups = [];
  if (entry.id.includes("/")) groups.push(entry.id.split("/")[0]);
  let cursor = entry.parent;
  while (cursor) {
    groups.push(cursor.split("/").pop());
    cursor = byId.get(cursor)?.parent;
  }
  return [...new Set(groups)];
}

const regions = selected
  .map(entry => {
    const pbf = entry.urls?.pbf || "";
    const geofabrik = pbf.replace("https://download.geofabrik.de/", "").replace(/-latest\.osm\.pbf$/u, "");
    return { entryId: entry.id, id: entry.id.split("/").pop(), geofabrik, name: entry.name, groups: groupsOf(entry) };
  })
  .filter(region => region.geofabrik)
  .sort((a, b) => (a.id < b.id ? -1 : 1));

// Short-id collisions (us/georgia vs the country georgia): the subregion
// gets a parent-qualified id, the country keeps the plain one.
const shortIdCounts = new Map();
for (const region of regions) shortIdCounts.set(region.id, (shortIdCounts.get(region.id) || 0) + 1);
for (const region of regions) {
  if (shortIdCounts.get(region.id) > 1 && region.entryId.includes("/")) {
    region.id = region.entryId.replaceAll("/", "-");
  }
}
const ids = new Set();
for (const region of regions) {
  if (ids.has(region.id)) throw new Error(`Duplicate shard id: ${region.id}`);
  ids.add(region.id);
}

console.log(`${regions.length} shards selected, ${codeOwners.size} countries covered.`);

if (verify) {
  console.log("Verifying PBF URLs (HEAD)…");
  let totalBytes = 0;
  const failures = [];
  const queue = regions.slice();
  await Promise.all(Array.from({ length: 8 }, async () => {
    for (;;) {
      const region = queue.shift();
      if (!region) return;
      const url = `https://download.geofabrik.de/${region.geofabrik}-latest.osm.pbf`;
      try {
        if (pbfSizes.has(region.entryId)) {
          totalBytes += pbfSizes.get(region.entryId);
          continue;
        }
        const head = await fetch(url, { method: "HEAD" });
        if (!head.ok) throw new Error(`HTTP ${head.status}`);
        totalBytes += Number(head.headers.get("content-length") || 0);
      } catch (error) {
        failures.push(`${region.id}: ${url} → ${error.message}`);
      }
    }
  }));
  if (failures.length) {
    console.error(failures.join("\n"));
    throw new Error(`${failures.length} region URL(s) failed verification.`);
  }
  console.log(`All URLs OK — total download ≈ ${(totalBytes / 1024 ** 3).toFixed(1)} GiB.`);
}

const existing = JSON.parse(readFileSync(join(projectRoot, "regions.json"), "utf8"));
const output = {
  "//": existing["//"],
  regions: regions.map(({ id, geofabrik, groups }) => ({ id, geofabrik, ...(groups.length ? { groups } : {}) })),
  statsDriftRatio: existing.statsDriftRatio ?? 0.1,
  workerCount: existing.workerCount ?? 0,
  publisher: existing.publisher ?? "",
  maxGenerations: existing.maxGenerations ?? 6,
  maxDeletedRatio: existing.maxDeletedRatio ?? 0.005,
  maxDeltaRatio: existing.maxDeltaRatio ?? 0.3
};

if (dryRun) {
  console.log(regions.map(region => `${region.id.padEnd(28)} ${region.geofabrik}`).join("\n"));
} else {
  writeFileSync(join(projectRoot, "regions.json"), JSON.stringify(output, null, 2) + "\n");
  console.log(`regions.json written (${regions.length} shards).`);
}
