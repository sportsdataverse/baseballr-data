#!/usr/bin/env bash
# Savant Statcast backfill: stage 04 over a season range, newest-first, with a
# commit+push PER SEASON. Not a parallel implementation -- same stage, plus the
# season loop and the commits.
#
#   ./scripts/run_mlb_statcast_backfill.sh 2026 2015     # newest -> oldest
#
# Floor is 2015: Statcast's first season. Nothing earlier exists to fetch, so
# the stage skips below it rather than pretending to try.
#
# Measured pace: ~5.5 min per month-pull, 9 months per season -> ~55 min/season,
# which matches the ~55 min/season the model publisher's cache already
# documents. 12 seasons is an overnight job, not an afternoon.
#
# Per-season commits are the checkpoint: a crash costs at most the season in
# flight, and stage 04 is file-exists resumable so a re-run refetches nothing.
#
# Tunables (env only): SDV_MLB_STATCAST_SLEEP (seconds between month pulls).
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

START="${1:?start season (newest, e.g. 2026)}"
END="${2:?end season (oldest, e.g. 2015)}"
FLOOR=2015

if [ "$END" -lt "$FLOOR" ]; then
  echo "NOTE: ${END} is before Statcast (${FLOOR}); clamping." >&2
  END="$FLOOR"
fi

source "$(dirname "$0")/_git_commit.sh"
mkdir -p logs
rc_total=0

for season in $(seq "$START" -1 "$END"); do
  echo "=== STATCAST ${season} $(date -u +%FT%TZ) ==="
  bash scripts/run_mlb_04_statcast.sh --season "${season}" --root . \
    >>"logs/mlb_statcast_bf_${season}.log" 2>&1 \
    || { echo "season ${season}: capture rc=$? (committing what landed)"; rc_total=1; }

  sdv_commit_push "feat(mlb): season ${season} statcast capture" \
    "mlb/statcast_raw/${season}" logs/ || rc_total=1
  echo "season ${season} done ($(date -u +%FT%TZ))"
done

echo "statcast backfill done rc=${rc_total}"
exit "$rc_total"
