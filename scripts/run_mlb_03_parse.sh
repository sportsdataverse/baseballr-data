#!/usr/bin/env bash
# Stage 03 -- MLB parse raw bundles -> pbp + pitches (OFFLINE)
#
#   ./scripts/run_mlb_03_parse.sh --season 2026

#
# Pace is env-only -- never edit it into this script:
#   SDV_MLB_WORKERS  concurrent fetches   (default 8; 16 was measured at
#                    26 req/s and is deliberately not the default)
#   SDV_MLB_SLEEP    seconds between requests
#   SDV_MLB_RETRIES  attempts per URL
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage "mlb_03_parse" python/mlb_03_games_parse.py "$@"
