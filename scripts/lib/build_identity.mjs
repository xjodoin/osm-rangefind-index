export function buildContentFingerprint({ entry = {}, statsFingerprint, overrides = null }) {
  return `${entry.extractIdentity || "?"}:${entry.extractSchema || 0}:${entry.enrichmentIdentity || ""}:${entry.docs || 0}:${statsFingerprint}:${JSON.stringify(overrides)}`;
}

export function buildShardFingerprint({ rangefindVersion, builderVersion = rangefindVersion, contentFingerprint }) {
  return `rangefind@${builderVersion}:${contentFingerprint}`;
}

export function previouslyBuiltContentFingerprint(entry = {}) {
  // Before builder identity was tracked, builtFingerprint was exactly the
  // content fingerprint. Retaining it makes the state migration lossless.
  return entry.builtContentFingerprint || entry.builtFingerprint || "";
}

export function previouslyBuiltBuilderVersion(entry = {}) {
  return entry.builtRangefindBuilderVersion || entry.builtRangefindVersion || "";
}

export function selectRootCandidates({ selected, all, regionScoped, partial }) {
  return regionScoped && !partial ? all : selected;
}

export function shouldReuseFrozenStats({ regionScoped, partial }) {
  return regionScoped && !partial;
}
