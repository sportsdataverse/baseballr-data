#!/usr/bin/env bash
# Daily in-season driver for MLB, run from the DROPLET CRONTAB.
# Composes the numbered stages (01 schedules -> 02 capture -> 03 parse) -- it
# does NOT reimplement them, and a backfill is the same stages over a season
# range via run_mlb_backfill.sh, not a parallel implementation.
#
# MLB season is the CALENDAR year. Corpus floor is 1988, the first season with
# complete pitch-by-pitch (measured: 1986-87 carry ~115 pitches/game, 1988-89
# carry 241-300).
#
#   run:    ./scripts/daily_mlb_capture.sh
#   watch:  tail -f logs/daily_mlb_$(date -u +%Y%m%d).log
#
# Tunables (env only -- never edit pace into this script):
#   MLB_SEASON       override the season         (default: current year)
#   MLB_MAX_GAMES    per-run capture cap         (default 0 = all new games;
#                    a normal night is ~15, and stage 02 is file-exists
#                    resumable, so an uncapped daily run is cheap)
#   SDV_MLB_WORKERS  concurrent fetches          (default 8)
#
# statsapi 406s this host directly; the transport routes through ProxyBonanza
# (PROXY_KEY/PROXY_PKG). Decodo does NOT work for statsapi -- 0/5 ports,
# CONNECT tunnel failed -- so do not point this at the NCAA vendor config.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

SEASON="${MLB_SEASON:-$(date -u +%Y)}"
MAX_GAMES="${MLB_MAX_GAMES:-0}"

LOG="logs/daily_mlb_$(date -u +%Y%m%d).log"
mkdir -p logs
{
  echo "[$(date -u '+%F %T')Z] daily mlb start: season=${SEASON} max_games=${MAX_GAMES}"
  rc_total=0

  # 01 is one call: it refreshes the whole season's schedule, which is also how
  # the delta is discovered (new Final games since last night). --force because
  # yesterday's cached copy predates last night's results.
  bash scripts/run_mlb_01_schedules.sh --season "${SEASON}" --root . --force \
    || { echo "WARN schedules rc=$?"; rc_total=1; }

  cap=()
  [ "${MAX_GAMES}" != "0" ] && cap=(--max-games "${MAX_GAMES}")
  bash scripts/run_mlb_02_games.sh --season "${SEASON}" --root . "${cap[@]}" \
    || { echo "WARN capture rc=$?"; rc_total=1; }

  # 03 is OFFLINE and rebuilds the season's parquet from every committed bundle,
  # so it is correct even if tonight's capture was partial.
  bash scripts/run_mlb_03_parse.sh --season "${SEASON}" --root . \
    || { echo "WARN parse rc=$?"; rc_total=1; }

  echo "[$(date -u '+%F %T')Z] daily mlb stages done (rc_total=${rc_total})"
  exit "$rc_total"
} 2>&1 | tee -a "$LOG"
STAGE_RC="${PIPESTATUS[0]}"

source "$(dirname "$0")/_git_commit.sh"
# Explicit subtrees, and no stage numbers in the subject: repository_dispatch
# resolves START/END by grepping a message's first and last integers.
sdv_commit_push "feat(mlb): season ${SEASON} daily capture" \
  "mlb/raw/${SEASON}" mlb/schedule mlb/pbp logs/ || STAGE_RC=1

echo "[$(date -u '+%F %T')Z] daily mlb done EXIT=${STAGE_RC}" | tee -a "$LOG"
exit "$STAGE_RC"
