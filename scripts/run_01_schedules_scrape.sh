#!/usr/bin/env bash
# User-run launcher (stage 01): team lists + team pages -> persisted html +
# ncaa/schedule_master/parquet/{season}.parquet. ONLINE (~1 page/team).
# Resumable: persisted html is re-read, never re-fetched.
#   ./scripts/run_01_schedules_scrape.sh --season 2026                # all 3 divisions
#   ./scripts/run_01_schedules_scrape.sh --season 2026 --division 1
#   watch:  tail -f logs/schedules_<ts>.log
set -uo pipefail
source "$(dirname "$0")/_env.sh"
run_stage schedules python/ncaa_baseball_01_schedules_scrape.py "$@"
