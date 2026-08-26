#!/usr/bin/env bash
# User-run launcher (stage 06): NCAA<->ESPN game crosswalk ->
# ncaa/xwalk/espn_game_id/{season}.json. OFFLINE for the NCAA transport (no
# proxy); uncached days hit the friendly ESPN scoreboard API directly and are
# cached to ncaa/xwalk/espn_scoreboard/{season}/ -- re-runs are fully offline.
#   ./scripts/run_06_xwalk_build.sh --season 2026
#   watch:  tail -f logs/xwalk_<ts>.log
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage xwalk python/ncaa_baseball_06_xwalk_build.py "$@"
