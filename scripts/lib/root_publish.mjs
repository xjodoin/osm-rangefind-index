import { isDeepStrictEqual } from "node:util";

export const ROOT_ROUTING_ARTIFACTS = Object.freeze([
  Object.freeze({ prefix: "text-routing", manifestKey: "text_routing" }),
  Object.freeze({ prefix: "authority", manifestKey: "suggest_routing" })
]);

export function rootRoutingArtifactIsPublished(localManifest, remoteManifest, manifestKey) {
  const local = localManifest?.[manifestKey];
  const remote = remoteManifest?.[manifestKey];
  return local != null && remote != null && isDeepStrictEqual(local, remote);
}
