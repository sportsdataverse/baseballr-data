#!/usr/bin/env bash
# User-run launcher (stage 07): parsed payloads + reference parquet ->
# ncaa/{dataset}/parquet/ season frames [+ release publish via gh].
# OFFLINE except gh (no proxy / NCAA transport). Re-run overwrites; uploads
# are idempotent (--clobber).
#   ./scripts/run_07_datasets_publish.sh build --season 2024
#   ./scripts/run_07_datasets_publish.sh build --season 2024 --publish
#   ./scripts/run_07_datasets_publish.sh check
#   watch:  tail -f logs/datasets_publish_<ts>.log
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage datasets_publish python/ncaa_baseball_07_datasets_publish.py "$@"
