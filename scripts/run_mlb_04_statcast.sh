#!/usr/bin/env bash
# Stage 04 -- Savant Statcast pitch-level capture (ONLINE, resumable, 2015+).
#
#   ./scripts/run_mlb_04_statcast.sh --season 2024
#   ./scripts/run_mlb_04_statcast.sh --start 2015 --end 2026
#
# Savant needs NO proxy (measured: 200 direct, while statsapi 406s from here),
# so this sources _env.sh OFFLINE -- its online block configures the NCAA
# transport, which is irrelevant and misleading for Savant.
#
# Pace is env-only: SDV_MLB_STATCAST_SLEEP (seconds between month pulls).
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage "mlb_04_statcast" python/mlb_04_statcast_scrape.py "$@"
