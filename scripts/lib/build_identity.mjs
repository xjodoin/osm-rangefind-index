export function buildContentFingerprint({ entry = {}, statsFingerprint, overrides = null }) {
  return `${entry.extractIdentity || "?"}:${entry.extractSchema || 0}:${entry.docs || 0}:${statsFingerprint}:${JSON.stringify(overrides)}`;
}

export function buildShardFingerprint({ rangefindVersion, contentFingerprint }) {
  return `rangefind@${rangefindVersion}:${contentFingerprint}`;
}

export function previouslyBuiltContentFingerprint(entry = {}) {
  // Before builder identity was tracked, builtFingerprint was exactly the
  // content fingerprint. Retaining it makes the state migration lossless.
  return entry.builtContentFingerprint || entry.builtFingerprint || "";
}

export function selectRootCandidates({ selected, all, regionScoped, partial }) {
  return regionScoped && !partial ? all : selected;
}

export function shouldReuseFrozenStats({ regionScoped, partial }) {
  return regionScoped && !partial;
}
