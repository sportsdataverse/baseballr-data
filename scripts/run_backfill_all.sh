#!/usr/bin/env bash
# Orchestrator: full NCAA-baseball capture campaign, one season at a time.
#   ./scripts/run_backfill_all.sh 2026 2024          # newest -> oldest
# Per season: 01 schedules (D1-3) -> 04 rosters -> 02 games (SHARDS workers)
# -> 03 parse -> 06 xwalk -> 03 re-parse (espn stamps) -> 05 reference
# datasets -> 07 build+publish; git commit+push per stage (season-sized
# batches). Every stage is file-exists resumable; re-running fast-forwards.
# Stops cleanly when a season's D1 team list is empty (coverage floor).
# Watch: tail -f logs/bf_<season>_*.log
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
ROOT="$(pwd)"
START="${1:?start season (e.g. 2026)}"
END="${2:?end season (e.g. 2024)}"
SHARDS="${SHARDS:-8}"
export PYTHONPATH="${ROOT}/python" PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8
# Chromium temp profiles on block storage, not the small root disk
# (2026-08-21: leaked profiles filled / on the MFB campaign).
export TMPDIR=/mnt/sdv_repos/tmp
mkdir -p logs "$TMPDIR"
# sdv-py's venv FIRST: the browser stages need patchright (+ its chromium),
# which is intentionally not a dependency of this repo's own venv.
PY=/mnt/sdv_repos/sdv-py/.venv/bin/python; [ -x "$PY" ] || PY="${ROOT}/.venv/bin/python"
export PYTHONPATH="/mnt/sdv_repos/sdv-py:${PYTHONPATH}"

# NCAA_PROXY_POOL from .Renviron Decodo creds (a pre-set value wins)
if [ -z "${NCAA_PROXY_POOL:-}" ]; then
  getcred() { grep -E "^$1=" "$HOME/.Renviron" | head -1 | cut -d= -f2- | tr -d "\"'" | tr -d '\r'; }
  DU="$(getcred DECODO_USER_NAME)"; DP="$(getcred DECODO_PASSWORD)"
  pool=""; for p in $(seq 10001 10050); do pool="${pool}${pool:+,}http://${DU}:${DP}@us.decodo.com:${p}"; done
  export NCAA_PROXY_POOL="$pool"
fi
GH_TOKEN="$(grep -E '^GITHUB_PAT=' "$HOME/.Renviron" | head -n1 | cut -d= -f2- | tr -d "\"'" | tr -d '\r')"
export GH_TOKEN

commit() { git add "$@" 2>/dev/null; git commit -q -m "$COMMIT_MSG" && git push -q origin main || true; }

for season in $(seq "$START" -1 "$END"); do
  echo "=== SEASON ${season} $(date -u +%FT%TZ) ==="
  rm -rf "$TMPDIR"/.org.chromium.* /tmp/.org.chromium.* 2>/dev/null || true
  free_kb=$(df --output=avail / | tail -1 | tr -d ' ')
  if [ "${free_kb:-0}" -lt 5242880 ]; then
    echo "ROOT DISK LOW (<5G free) -- stopping before ${season}"; exit 1
  fi

  # 1) schedules: team lists + team pages, all divisions
  "$PY" python/ncaa_baseball_01_schedules_scrape.py --season "$season" > "logs/bf_${season}_01.log" 2>&1
  rc=$?
  if [ $rc -ne 0 ] && grep -qi "zero teams\|no teams" "logs/bf_${season}_01.log"; then
    echo "season ${season}: no D1 teams -- COVERAGE FLOOR"; echo "BACKFILL FLOOR REACHED at ${season}"; exit 0
  fi
  [ $rc -ne 0 ] && echo "season ${season} 01 rc=${rc} (continuing)"
  COMMIT_MSG="feat(ncaa): season ${season} schedules discovery (stage 01)" \
    commit ncaa/teams_html ncaa/schedules_html ncaa/schedule_master ncaa/teams/parquet

  # 4) rosters
  "$PY" python/ncaa_baseball_04_rosters_scrape.py --season "$season" > "logs/bf_${season}_04.log" 2>&1 || true
  COMMIT_MSG="feat(ncaa): season ${season} rosters (stage 04)" commit ncaa/rosters_html

  # 2) games: SHARDS workers over the season's contests
  for i in $(seq 0 $((SHARDS - 1))); do
    "$PY" python/ncaa_baseball_02_games_scrape.py --season "$season" --shard "$i/$SHARDS" \
      > "logs/bf_${season}_02_shard${i}.log" 2>&1 &
    sleep 3
  done
  wait
  grep -h 'captured\|capture:' logs/bf_${season}_02_shard*.log | tail -${SHARDS} || true
  COMMIT_MSG="feat(ncaa): season ${season} game bundles (stage 02)" commit "ncaa/raw/${season}"

  # 6) xwalk BEFORE final parse so payloads get espn stamps
  "$PY" python/ncaa_baseball_06_xwalk_build.py --season "$season" > "logs/bf_${season}_06.log" 2>&1 || true
  COMMIT_MSG="feat(ncaa): season ${season} espn xwalk (stage 06)" commit ncaa/xwalk

  # 3) parse (espn index now on disk)
  "$PY" python/ncaa_baseball_03_games_parse.py --season "$season" --workers 8 > "logs/bf_${season}_03.log" 2>&1 || true
  COMMIT_MSG="feat(ncaa): season ${season} parsed payloads (stage 03)" commit ncaa/json

  # 5) reference datasets (offline)
  "$PY" python/ncaa_baseball_05_datasets_build.py --season "$season" > "logs/bf_${season}_05.log" 2>&1 || true
  COMMIT_MSG="feat(ncaa): season ${season} reference datasets (stage 05)" \
    commit ncaa/teams/parquet ncaa/schedule_master ncaa/rosters/parquet

  # 7) season datasets build + publish
  /root/.local/bin/uv run python -m ncaa_baseball_data_build build --dataset all --season "$season" --publish \
    > "logs/bf_${season}_07.log" 2>&1 || echo "season ${season} 07 rc=$?"
  grep -h 'qa ' "logs/bf_${season}_07.log" | tail -1 || true
  COMMIT_MSG="feat(ncaa): season ${season} datasets built + published (stage 07)" \
    commit ncaa/*/parquet ncaa/*/manifest.csv ncaa/qa
  echo "=== season ${season} complete $(date -u +%FT%TZ) ==="
done
echo "BACKFILL COMPLETE ${START}->${END}"
