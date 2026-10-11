#!/usr/bin/env bash
# Stage 05 -- publish MLB season parquet (+ csv.gz/rds) to sportsdataverse-data
#
#   ./scripts/run_mlb_05_publish.sh --dataset pbp --season 2026 --publish
#   ./scripts/run_mlb_05_publish.sh --dataset statcast --start 2015 --end 2026 --dry-run
#
# Datasets: pbp, pitches, runners, statcast, schedule (or all). Offline except gh.
# Upload pace is env-only: SDV_MLB_PUBLISH_SLEEP / _BACKOFF / _RETRIES.
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage "mlb_05_publish" python/mlb_05_datasets_publish.py "$@"
