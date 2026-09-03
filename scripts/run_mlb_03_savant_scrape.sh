#!/usr/bin/env bash
# Stage 03 launcher -- MLB raw capture. See RUNBOOK-MLB.md.
# statsapi + Baseball Savant need no proxy/transport block: OFFLINE=1 skips it.
set -uo pipefail
OFFLINE=1 source "$(dirname "$0")/_env.sh"
run_stage mlb_savant python/mlb_raw_03_savant_scrape.py "$@"
