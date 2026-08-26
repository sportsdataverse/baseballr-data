#!/usr/bin/env bash
# User-run launcher (stage 05): persisted html -> ncaa/{teams,schedule_master,rosters}
# parquet reference frames via the sdv-py scrape.ncaa.reference parsers.
# OFFLINE (no proxy). Pure function of the tree; re-run overwrites.
#   ./scripts/run_05_datasets_build.sh --season 2026
#   watch:  tail -f logs/datasets_<ts>.log
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage datasets python/ncaa_baseball_05_datasets_build.py "$@"
