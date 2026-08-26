#!/usr/bin/env bash
# User-run launcher (stage 04): teams/{id}/roster -> ncaa/rosters_html/{season}/.
# ONLINE (~1 page/team). Resumable: teams with roster html are skipped.
#   ./scripts/run_04_rosters_scrape.sh --season 2026                  # all 3 divisions
#   ./scripts/run_04_rosters_scrape.sh --season 2026 --division 1
#   watch:  tail -f logs/rosters_<ts>.log
set -uo pipefail
source "$(dirname "$0")/_env.sh"
run_stage rosters python/ncaa_baseball_04_rosters_scrape.py "$@"
