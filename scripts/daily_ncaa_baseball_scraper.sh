#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done

source "$(dirname "$0")/_git_commit.sh"
Rscript R/ncaa_01_schedules_creation.R -s $START_YEAR -e $END_YEAR -r $RESCRAPE
sdv_commit_push "NCAA Schedules update (Start: $START_YEAR End: $END_YEAR)" . || PUSH_RC=1

if [ "${PUSH_RC:-0}" != "0" ]; then
  echo "::error ::At least one commit failed to reach origin; the repo mirror is stale."
  exit 1
fi
