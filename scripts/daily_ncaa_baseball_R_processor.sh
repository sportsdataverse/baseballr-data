#!/bin/bash
# Per-year NCAA baseball processor invoked by .github/workflows/daily_ncaa_baseball.yml.
# For each season in [START_YEAR, END_YEAR] it (re)builds the schedule and
# play-by-play artifacts and pushes rds/csv/parquet copies to the
# sportsdataverse/sportsdataverse-data releases (ncaa_baseball_schedules,
# ncaa_baseball_pbp) via the sportsdataverse_save() calls inside the creation
# scripts, then commits the local artifacts back to this repo.
#
# Flags: -s START_YEAR  -e END_YEAR  -r RESCRAPE(TRUE|FALSE)
# Requires GITHUB_PAT (write access to sportsdataverse-data) in the environment.
set -euo pipefail

RESCRAPE=FALSE
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done

source "$(dirname "$0")/_git_commit.sh"
PROCESS_RC=0

for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    # Non-fatal by design -- a stale tree still builds, and sdv_commit_push
    # rebases onto origin if the push is rejected. But say so: this was the
    # last silent git call in the file. Plain pull (merge), never --rebase:
    # the am backend stalls base64-encoding this repo's parquet/rds.
    git pull > /dev/null || echo "::warning ::pull failed before season $i; building on the local tree"
    git config --local user.email "action@github.com"
    git config --local user.name "GitHub Action"
    Rscript R/ncaa_01_schedules_creation.R -s "$i" -e "$i" -r "$RESCRAPE"
    Rscript R/ncaa_02_pbp_creation.R        -s "$i" -e "$i" -r "$RESCRAPE"
    # Every git call here used to be swallowed: `push || true` turned a rejected
    # push into a green season, `commit || echo "No changes"` reported a FAILED
    # commit as nothing-to-do, and `pull --rebase` used the am backend, which
    # base64-encodes binary and stalls on this parquet/rds tree. The helper
    # syncs with rebase --merge and returns non-zero when the work is not on
    # origin. `ncaa/*` is kept verbatim -- narrowing it here would change which
    # files this driver publishes.
    sdv_commit_push "NCAA Baseball Data Update (Start: $i End: $i)" ncaa/* || PROCESS_RC=1
done

if [ "${PROCESS_RC:-0}" != "0" ]; then
  echo "::error ::At least one season failed to reach origin; the repo mirror is stale."
  exit 1
fi
