#!/usr/bin/env bash
# Cron entry point: low priority, single instance, env from .env.
# Usage: nightly.sh [update_index.mjs args...]
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
mkdir -p logs
log_file="${INDEX_LOG_FILE:-logs/indexing.log}"
mkdir -p "$(dirname "$log_file")"
exec > >(tee -a "$log_file") 2>&1
printf '[%s] launcher: starting %s\n' "$(date --iso-8601=seconds)" "$*"

if command -v ionice >/dev/null 2>&1; then
  exec nice -n 19 ionice -c 3 node scripts/update_index.mjs "$@"
fi

exec nice -n 19 node scripts/update_index.mjs "$@"
