import { createHash } from "node:crypto";

export const ACQUISITION_SESSION_SCHEMA_VERSION = 1;

export function acquisitionSessionSignature(input) {
  return createHash("sha256").update(JSON.stringify(input)).digest("hex");
}

export function openAcquisitionSession(current, signature, regionIds, now = new Date().toISOString()) {
  const allowed = new Set(regionIds);
  const resumable = current?.schemaVersion === ACQUISITION_SESSION_SCHEMA_VERSION
    && current.signature === signature
    && Array.isArray(current.completedRegionIds);
  if (resumable) {
    return {
      resumed: true,
      session: {
        ...current,
        completedRegionIds: [...new Set(current.completedRegionIds.filter(id => allowed.has(id)))],
        failures: current.failures && typeof current.failures === "object" ? current.failures : {},
        resumedAt: now,
        updatedAt: now
      }
    };
  }
  return {
    resumed: false,
    session: {
      schemaVersion: ACQUISITION_SESSION_SCHEMA_VERSION,
      signature,
      startedAt: now,
      updatedAt: now,
      completedRegionIds: [],
      failures: {},
      forcedStatsCompleted: false
    }
  };
}

export function recordAcquisitionSuccess(session, regionId, now = new Date().toISOString()) {
  if (!session.completedRegionIds.includes(regionId)) session.completedRegionIds.push(regionId);
  delete session.failures[regionId];
  session.updatedAt = now;
}

export function recordAcquisitionFailure(session, regionId, error, attempt, now = new Date().toISOString()) {
  session.failures[regionId] = {
    attempt,
    message: String(error?.message || error).slice(0, 500),
    failedAt: now
  };
  session.updatedAt = now;
}

// The upstream PBF a region downloaded within this acquisition cycle. A
// resumed cycle reuses that file instead of chasing a newer Geofabrik
// release: the newer data waits for the next cycle, and an interrupted
// extraction keeps its PBF (and stage checkpoints) instead of restarting.
export function frozenPbfIdentity(session, regionId) {
  return session?.pbfIdentities?.[regionId] || "";
}

export function recordPbfIdentity(session, regionId, identity, now = new Date().toISOString()) {
  if (!session || !identity) return;
  session.pbfIdentities ||= {};
  if (session.pbfIdentities[regionId] === identity) return;
  session.pbfIdentities[regionId] = identity;
  session.updatedAt = now;
}
