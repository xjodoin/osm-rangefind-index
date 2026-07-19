#!/usr/bin/env bash
# Weekly manifest-aware R2 garbage collection. Uses the indexer's launcher
# lock so a publication and a GC scan can never overlap.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
mkdir -p logs work
log_file="${R2_GC_LOG_FILE:-logs/r2-gc.log}"
exec > >(tee -a "$log_file") 2>&1

exec 9>"${INDEX_LOCK_FILE:-work/.launcher.lock}"
if ! flock -n 9; then
  printf '[%s] R2 GC skipped: indexer or another collector holds the launcher lock\n' "$(date --iso-8601=seconds)"
  exit 0
fi

printf '[%s] R2 GC starting: %s\n' "$(date --iso-8601=seconds)" "$*"
if command -v ionice >/dev/null 2>&1; then
  exec nice -n 19 ionice -c 3 node scripts/r2_gc.mjs "$@"
fi
exec nice -n 19 node scripts/r2_gc.mjs "$@"
