#!/usr/bin/env bash
# MLB backfill: the SAME numbered stages as the daily driver, over a season
# range, newest-first. Not a parallel implementation -- the only differences are
# the season loop and per-season commits.
#
#   ./scripts/run_mlb_backfill.sh 2026 1988      # newest -> oldest, full corpus
#   ./scripts/run_mlb_backfill.sh 2026 2015      # Statcast era only
#
# Measured cost of the full 1988-2026 corpus (92,340 games, ~6.93 GB gzipped):
#   scrape  ~2.3 h at 8 workers  (17.4 h sequential, ~1 h at 16)
#   build   ~34 min single-core
# The fetch is the cost; the parse is ~2% of it.
#
# Season-sized commits, deliberately: one commit per season keeps a failure
# cheap to resume and the push sizes sane, per the house convention.
#
# Tunables (env only):
#   SDV_MLB_WORKERS  concurrent fetches per season   (default 8; 16 was measured
#                    at 26 req/s against one host and is not a polite default)
#   MLB_MAX_GAMES    cap per season (0 = all)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

START="${1:?start season (newest, e.g. 2026)}"
END="${2:?end season (oldest, e.g. 1988)}"
FLOOR=1988
MAX_GAMES="${MLB_MAX_GAMES:-0}"

if [ "$END" -lt "$FLOOR" ]; then
  echo "NOTE: ${END} is below the ${FLOOR} floor (complete pitch-by-pitch starts" >&2
  echo "      there; 1950-87 pbp coverage is uneven WITHIN a season and needs a" >&2
  echo "      completeness audit first). Clamping to ${FLOOR}." >&2
  END="$FLOOR"
fi

source "$(dirname "$0")/_git_commit.sh"
mkdir -p logs
rc_total=0

for season in $(seq "$START" -1 "$END"); do
  echo "=== SEASON ${season} $(date -u +%FT%TZ) ==="
  LOG="logs/mlb_bf_${season}.log"

  bash scripts/run_mlb_01_schedules.sh --season "${season}" --root . >>"$LOG" 2>&1 \
    || { echo "season ${season}: schedules rc=$? (skipping season)"; rc_total=1; continue; }

  cap=()
  [ "${MAX_GAMES}" != "0" ] && cap=(--max-games "${MAX_GAMES}")
  bash scripts/run_mlb_02_games.sh --season "${season}" --root . "${cap[@]}" >>"$LOG" 2>&1 \
    || { echo "season ${season}: capture rc=$? (continuing to parse what landed)"; rc_total=1; }

  bash scripts/run_mlb_03_parse.sh --season "${season}" --root . >>"$LOG" 2>&1 \
    || { echo "season ${season}: parse rc=$?"; rc_total=1; }

  sdv_commit_push "feat(mlb): season ${season} capture and parse" \
    "mlb/raw/${season}" mlb/schedule mlb/pbp logs/ || rc_total=1
  echo "season ${season} done ($(date -u +%FT%TZ))"
done

echo "backfill done rc=${rc_total}"
exit "$rc_total"
