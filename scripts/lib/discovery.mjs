import {
  ROAD_CATALOG_FORMAT,
  ROAD_GRAPH_FORMAT
} from "./road_indexes.mjs";

export const DISCOVERY_FORMAT = "rangefind-discovery-v1";
export const DISCOVERY_PATH = ".well-known/rangefind.json";

function routingEnabled(roadConfig) {
  return Boolean(roadConfig?.enabled && roadConfig.profiles?.length);
}

/**
 * Endpoint links embedded in the search root. Paths are relative to that
 * root, so copied/self-hosted indexes remain portable.
 */
export function rootDiscoveryEndpoints(roadConfig) {
  return {
    discovery: DISCOVERY_PATH,
    status: "status.json",
    ...(routingEnabled(roadConfig) ? { routeCatalog: "routes/catalog.json" } : {})
  };
}

/**
 * Machine-readable service discovery. Hrefs are relative to the descriptor
 * in /.well-known/, not to the origin, so subdirectory deployments work too.
 */
export function buildDiscoveryDocument(roadConfig) {
  return {
    format: DISCOVERY_FORMAT,
    version: 1,
    endpoints: {
      search: {
        href: "../manifest.min.json",
        format: "rangefind-sharded-v1"
      },
      ...(routingEnabled(roadConfig) ? {
        routing: {
          href: "../routes/catalog.json",
          format: ROAD_CATALOG_FORMAT,
          graphFormat: ROAD_GRAPH_FORMAT,
          profiles: [...roadConfig.profiles]
        }
      } : {}),
      status: {
        href: "../status.json",
        schemaVersion: 1
      }
    }
  };
}
