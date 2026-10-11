#!/usr/bin/env bash
# Stage 06 -- compile Statcast monthly raws -> one parquet per season (OFFLINE)
#
#   ./scripts/run_mlb_06_statcast_compile.sh --season 2026
#   ./scripts/run_mlb_06_statcast_compile.sh --start 2015 --end 2026
#
# Writes mlb/statcast/mlb_statcast_{season}.parquet, which is release staging
# (gitignored: a season is over GitHub's 100 MiB blob limit). Publish it with
#   ./scripts/run_mlb_05_publish.sh --dataset statcast --season 2026 --publish
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage "mlb_06_statcast_compile" python/mlb_06_statcast_compile.py "$@"
