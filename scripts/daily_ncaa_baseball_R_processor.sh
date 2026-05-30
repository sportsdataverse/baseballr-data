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

for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    git pull > /dev/null || true
    git config --local user.email "action@github.com"
    git config --local user.name "GitHub Action"
    Rscript R/ncaa_01_schedules_creation.R -s "$i" -e "$i" -r "$RESCRAPE"
    Rscript R/ncaa_02_pbp_creation.R        -s "$i" -e "$i" -r "$RESCRAPE"
    git pull > /dev/null || true
    git add ncaa/* > /dev/null || true
    git commit -m "NCAA Baseball Data Update (Start: $i End: $i)" > /dev/null || echo "No changes to commit for $i"
    git pull --rebase > /dev/null || true
    git push > /dev/null || true
done
