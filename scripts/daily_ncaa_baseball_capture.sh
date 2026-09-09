#!/usr/bin/env bash
# Daily in-season driver for NCAA BASEBALL, run from the DROPLET CRONTAB.
# Composes the existing numbered stages (01 schedules -> 02 games capture ->
# 03 parse -> 04 rosters -> 05 datasets) -- it does NOT reimplement them, and a
# backfill is the same stages via run_backfill_all.sh, not a parallel impl.
#
# WHY THIS EXISTS: the crontab was calling run_backfill_all.sh with the same
# season twice (`run_backfill_all.sh 2026 2026`) as a stand-in for a daily
# driver. That script is a BACKFILL: it shards, it commits per stage, and its
# season loop, disk guard and cooldowns are all sized for replaying whole
# seasons. Using it as a nightly incremental means a daily run inherits
# backfill pacing and a backfill's failure semantics. This is the daily one.
#
# NCAA baseball season is the CALENDAR year (spring sport): season 2026 =
# spring 2026. No academic-year offset, unlike the MFB raw repo.
#
#   run:    NCAA_VENDOR=decodo_patchright ./scripts/daily_ncaa_baseball_capture.sh
#   watch:  tail -f logs/daily_ncaa_baseball_$(date -u +%Y%m%d).log
#
# Tunables (env only -- never edit pace into the script):
#   NCAAB_SEASON   override the resolved season   (default: current calendar yr)
#   NCAAB_MAX      per-run capture cap            (default 200; stage 02's own
#                  "not yet captured" filter makes re-runs free)
#
# SAFE RATE: stage 02's README governs -- ONE worker here, never --shard fan-out
# from cron. run_backfill_all.sh shards only because it replays whole seasons.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

if [ -z "${NCAA_VENDOR:-}" ]; then
  echo "ERROR: NCAA_VENDOR must be set for a cron run (e.g. decodo_patchright)" >&2
  echo "       Without it the stages fall back to ProxyBonanza creds read from" >&2
  echo "       .Renviron, which a cron environment does not source." >&2
  exit 2
fi

export SDV_PY="${SDV_PY:-/mnt/sdv_repos/sdv-py}"
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

# The stages import sportsdataverse from this WORKING TREE (PYTHONPATH), not a
# version pin -- a feature branch there silently runs unreviewed code against a
# hostile, ban-on-sight host. Same guard the MBB/WBB drivers carry.
sdv_py_branch="$(git -C "${SDV_PY}" branch --show-current 2>/dev/null || echo "?")"
if [ "${sdv_py_branch}" != "main" ]; then
  echo "ERROR: SDV_PY (${SDV_PY}) is on branch '${sdv_py_branch}', not main -- refusing to scrape" >&2
  exit 2
fi

SEASON="${NCAAB_SEASON:-$(date -u +%Y)}"
MAX="${NCAAB_MAX:-200}"

LOG="logs/daily_ncaa_baseball_$(date -u +%Y%m%d).log"
mkdir -p logs
{
  echo "[$(date -u '+%F %T')Z] daily ncaa baseball start: season=${SEASON} max=${MAX} vendor=${NCAA_VENDOR}"
  rc_total=0

  # One dead stage does not stop its siblings -- a failed roster scrape must not
  # cost the night's pbp. Each failure is recorded and surfaces in the exit code.
  bash scripts/run_01_schedules_scrape.sh --season "${SEASON}" || { echo "WARN schedules rc=$?"; rc_total=1; }
  bash scripts/run_02_games_scrape.sh    --season "${SEASON}" --max "${MAX}" || { echo "WARN capture rc=$?"; rc_total=1; }
  bash scripts/run_03_games_parse.sh     --season "${SEASON}" || { echo "WARN parse rc=$?"; rc_total=1; }
  bash scripts/run_04_rosters_scrape.sh  --season "${SEASON}" || { echo "WARN rosters rc=$?"; rc_total=1; }
  bash scripts/run_05_datasets_build.sh  --season "${SEASON}" || { echo "WARN datasets rc=$?"; rc_total=1; }

  echo "[$(date -u '+%F %T')Z] daily ncaa baseball stages done (rc_total=${rc_total})"
  exit "$rc_total"
} 2>&1 | tee -a "$LOG"
STAGE_RC="${PIPESTATUS[0]}"

# One commit for the whole night, through the repo's own hardened helper: it
# skips absent pathspecs individually, treats a failed add as fatal rather than
# as "nothing to commit", and returns non-zero unless the work is on origin.
source "$(dirname "$0")/_git_commit.sh"

# Subject follows the PYTHON tree's existing convention
# (`feat(ncaa): season N ... (stage NN)`), NOT the R chain's
# "NCAA Schedules update (Start: Y End: Y)". Those Start/End subjects are
# load-bearing for downstream parsing of the R producer; minting a lookalike
# for a different producer would put a false provenance trail in `git log`.
#
# The subject also carries NO stage numbers on purpose: the repository_dispatch
# path resolves START_YEAR/END_YEAR by grepping a message for its FIRST and LAST
# integers, so "(stages 01-05)" would resolve End=5. Season is the only integer
# in here, twice over, which is the shape that path expects.
#
# Explicit subtrees, not `ncaa/`: the wildcard would also sweep
# ncaa/_release_build/, which is release-asset STAGING (the same reason
# mlb/*/csv and mlb/*/rds are gitignored). sdv_commit_push skips absent paths
# individually, so listing a subtree that does not exist yet is free.
sdv_commit_push "feat(ncaa): season ${SEASON} daily capture" \
  ncaa/teams_html ncaa/schedules_html ncaa/schedule_master ncaa/teams/parquet \
  ncaa/rosters_html ncaa/rosters/parquet "ncaa/raw/${SEASON}" ncaa/json ncaa/xwalk \
  logs/ || STAGE_RC=1

echo "[$(date -u '+%F %T')Z] daily ncaa baseball done EXIT=${STAGE_RC}" | tee -a "$LOG"
exit "$STAGE_RC"
