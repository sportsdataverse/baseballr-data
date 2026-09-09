#!/usr/bin/env bash
# Stage 01 -- MLB schedules (ONE statsapi call per season)
#
#   ./scripts/run_mlb_01_schedules.sh --season 2026
#   ./scripts/run_mlb_01_schedules.sh --start 1988 --end 2026
#
# Pace is env-only -- never edit it into this script:
#   SDV_MLB_WORKERS  concurrent fetches   (default 8; 16 was measured at
#                    26 req/s and is deliberately not the default)
#   SDV_MLB_SLEEP    seconds between requests
#   SDV_MLB_RETRIES  attempts per URL
set -uo pipefail
# _env.sh is sourced OFFLINE=1 deliberately: its online block configures the
# NCAA transport (NCAA_VENDOR / Decodo sticky ports), and Decodo does NOT reach
# statsapi.mlb.com -- measured 0/5 ports, "CONNECT tunnel failed". The MLB
# transport builds its own ProxyBonanza pool from PROXY_KEY/PROXY_PKG, which is
# the only path that clears statsapi's 406. Sourcing online here would announce
# a proxy pool this stage never uses.
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage "mlb_01_schedules" python/mlb_01_schedules_scrape.py "$@"
