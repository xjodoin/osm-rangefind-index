#!/usr/bin/env bash
# Cron entry point: low priority, single instance, env from .env.
# Usage: nightly.sh [update_index.mjs args...]
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
mkdir -p logs

exec nice -n 19 ionice -c 3 2>/dev/null \
  node scripts/update_index.mjs "$@" \
  || exec nice -n 19 node scripts/update_index.mjs "$@"
