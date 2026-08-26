#!/usr/bin/env bash
# User-run launcher (stage 02): capture the 5-tab bundle for a season's
# not-yet-captured contests -> ncaa/raw/{season}/{contest_id}.json.gz.
# ONLINE. CHUNK it (--max) and fan out with disjoint --shard i/N as separate
# PROCESSES; a ban hard-stops the run (rc=1) -- cool down, re-run, it resumes
# (captured contests are skipped).
#   ./scripts/run_02_games_scrape.sh --season 2026 --max 200
#   ./scripts/run_02_games_scrape.sh --season 2026 --shard 0/8 &
#   watch:  tail -f logs/games_<ts>.log
set -uo pipefail
source "$(dirname "$0")/_env.sh"
run_stage games python/ncaa_baseball_02_games_scrape.py "$@"
