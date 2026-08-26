#!/usr/bin/env bash
# User-run launcher (stage 03): raw bundles + legacy R-era trees -> parsed
# payloads under ncaa/json/. OFFLINE (no proxy). Resumable (--force overwrites).
#   ./scripts/run_03_games_parse.sh --season 2026
#   ./scripts/run_03_games_parse.sh --legacy --year 2017
#   watch:  tail -f logs/games_parse_<ts>.log
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage games_parse python/ncaa_baseball_03_games_parse.py "$@"
