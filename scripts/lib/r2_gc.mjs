import { posix } from "node:path";

const IMMUTABLE_OBJECT = /\.bin(?:\.gz)?$/u;

function safeRelativePath(value) {
  const raw = String(value || "").replace(/^\.\//u, "");
  if (raw.startsWith("/") || raw.split("/").includes("..")) return null;
  const normalized = posix.normalize(raw);
  if (!normalized || normalized === "." || normalized.startsWith("../") || normalized.startsWith("/")) return null;
  return normalized;
}

function relativeTo(base, value) {
  const relative = safeRelativePath(value);
  return relative ? safeRelativePath(posix.join(base, relative)) : null;
}

export function collectManifestProtections(manifest, manifestPath, protections) {
  const base = posix.dirname(manifestPath);
  protections.paths.add(manifestPath);

  const visit = (value, key = "") => {
    if (typeof value === "string") {
      if (IMMUTABLE_OBJECT.test(value)) {
        protections.basenames.add(posix.basename(value));
        if (value.includes("/")) {
          const path = relativeTo(base, value);
          if (path) protections.paths.add(path);
        }
      }
      if ((key === "pages" || key.endsWith("_path")) && value.endsWith("/")) {
        const prefix = relativeTo(base, value);
        if (prefix) protections.prefixes.add(`${prefix.replace(/\/$/u, "")}/`);
      }
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) visit(item, key);
      return;
    }
    if (value && typeof value === "object") {
      for (const [childKey, child] of Object.entries(value)) visit(child, childKey);
    }
  };

  visit(manifest);
}

export function createProtections() {
  return { paths: new Set(), basenames: new Set(), prefixes: new Set() };
}

export function isProtectedObject(path, protections) {
  if (protections.paths.has(path) || protections.basenames.has(posix.basename(path))) return true;
  for (const prefix of protections.prefixes) {
    if (path.startsWith(prefix)) return true;
  }
  return false;
}

export function updateCandidateState({ objects, protections, previous = {}, now, graceMs }) {
  const current = {};
  const eligible = [];
  let immutableObjects = 0;
  let immutableBytes = 0;
  let protectedObjects = 0;
  let protectedBytes = 0;
  let pendingBytes = 0;
  let eligibleBytes = 0;

  for (const object of objects) {
    if (!IMMUTABLE_OBJECT.test(object.path)) continue;
    immutableObjects++;
    immutableBytes += object.size;
    if (isProtectedObject(object.path, protections)) {
      protectedObjects++;
      protectedBytes += object.size;
      continue;
    }

    const known = previous[object.path];
    const candidate = {
      firstSeenAt: known?.firstSeenAt || now,
      lastSeenAt: now,
      size: object.size,
      modTime: object.modTime || null
    };
    current[object.path] = candidate;
    const age = Date.parse(now) - Date.parse(candidate.firstSeenAt);
    if (Number.isFinite(age) && age >= graceMs) {
      eligible.push(object.path);
      eligibleBytes += object.size;
    } else {
      pendingBytes += object.size;
    }
  }

  return {
    candidates: current,
    eligible,
    summary: {
      immutableObjects,
      immutableBytes,
      protectedObjects,
      protectedBytes,
      pendingObjects: Object.keys(current).length - eligible.length,
      pendingBytes,
      eligibleObjects: eligible.length,
      eligibleBytes
    }
  };
}

export function generationManifestPath(shardBase, generation) {
  const path = String(generation.path || "");
  const declared = String(generation.manifest || "manifest.min.json");
  const safePath = path ? safeRelativePath(path) : "";
  const safeDeclared = safeRelativePath(declared);
  if ((path && !safePath) || !safeDeclared) return null;
  if (path && declared.startsWith(path)) return relativeTo(shardBase, safeDeclared);
  return relativeTo(posix.join(shardBase, safePath), safeDeclared);
}
