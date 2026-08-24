#!/usr/bin/env bash
# Reclaim unused Docker storage before the weekend index window.
set -euo pipefail

repo_dir="${OSM_RANGEFIND_REPO_DIR:-/srv/osm-rangefind-index}"
cd "$repo_dir"
mkdir -p work

# Use the index launcher's lock. If an index or manifest-aware R2 cleanup is
# active, leave every Docker object untouched and try again next week.
exec 9>"${INDEX_LOCK_FILE:-work/.launcher.lock}"
if ! flock -n 9; then
  printf '[%s] host cleanup: index maintenance holds the lock; skipping\n' "$(date --iso-8601=seconds)"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  printf '[%s] host cleanup: docker is not installed\n' "$(date --iso-8601=seconds)" >&2
  exit 1
fi

printf '[%s] host cleanup: disk usage before cleanup\n' "$(date --iso-8601=seconds)"
df -h "$repo_dir"
docker system df

# Docker never removes objects used by a running container. The second pass
# includes unused named volumes, which `docker system prune --volumes` leaves
# behind on some Docker releases.
docker system prune --all --force --volumes
docker volume prune --all --force

printf '[%s] host cleanup: disk usage after cleanup\n' "$(date --iso-8601=seconds)"
df -h "$repo_dir"
docker system df
