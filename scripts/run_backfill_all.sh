#!/usr/bin/env bash
# Orchestrator: full NCAA-baseball capture campaign, one season at a time.
#   ./scripts/run_backfill_all.sh 2026 2024          # newest -> oldest
# Per season: 01 schedules (D1-3) -> 04 rosters + 02 games (SHARDS browser shards each)
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
SHARDS="${SHARDS:-8}"            # browser processes for stages 04 + 02 (capped by memory below)
PARSE_WORKERS="${PARSE_WORKERS:-8}"  # stage 03 is CPU-bound: more than the core count buys nothing
MAX_MISSING="${MAX_MISSING:-0}"  # stage-02 games allowed to stay uncaptured before 06/07 publish
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

source "$(dirname "$0")/_git_commit.sh"
# Was: `git add "$@" 2>/dev/null; git commit ... && git push ... || true`, which
# reported success for every rejected push AND staged nothing whenever one of
# the explicit pathspecs did not exist yet. BACKFILL_RC carries the failure to
# the exit code instead of losing it between seasons.
BACKFILL_RC=0

# One browser shard (python + playwright driver + chromium) is ~0.8 GB (measured
# 2026-09-25). Cap the fan-out to what is free right now, so a big SHARDS can't
# OOM the droplet (the 2026-08-23 chromium OOM storm took SSH down with it).
shard_count() {
  local cap=$(( $(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo) / 900 ))
  [ "$cap" -lt 1 ] && cap=1
  if [ "$SHARDS" -gt "$cap" ]; then
    echo "SHARDS=${SHARDS} capped to ${cap} by available memory" >&2; echo "$cap"
  else
    echo "$SHARDS"
  fi
}
# Every fetcher starts at pool index 0, so unrotated shards all open on the same
# proxy (one exit IP taking every shard's Terms acceptances). Rotate the pool so
# shard i starts i/n of the way through it.
shard_pool() {
  local IFS=, off
  local -a p=($NCAA_PROXY_POOL)
  off=$(( $1 * ${#p[@]} / $2 ))
  echo "${p[*]:off}${off:+,}${p[*]:0:off}" | sed 's/,$//'
}
run_shards() {  # $1=stage script  $2=log prefix  $3=season
  local n i
  n=$(shard_count)
  for i in $(seq 0 $((n - 1))); do
    # append: a relaunch must not erase the evidence of why the last run stalled
    echo "=== $(date -u +%FT%TZ) shard ${i}/${n} ===" >> "logs/${2}_shard${i}.log"
    NCAA_PROXY_POOL="$(shard_pool "$i" "$n")" "$PY" "python/$1" --season "$3" --shard "$i/$n" \
      >> "logs/${2}_shard${i}.log" 2>&1 &
    sleep 3
  done
  wait
}
# Sets $missing: the season's contests with no bundle yet. Exits the run if it
# can't tell -- an empty count must never read as "complete" and reach publish.
count_missing() {
  missing=$("$PY" python/ncaa_baseball_02_games_scrape.py --season "$1" --count-missing) \
    && [[ "$missing" =~ ^[0-9]+$ ]] \
    || { echo "season $1: cannot count missing games -- STOPPING"; exit 1; }
}

commit() { sdv_commit_push "$COMMIT_MSG" "$@" || BACKFILL_RC=1; }

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
  COMMIT_MSG="feat(ncaa): season ${season} schedules discovery (stage 01)" \
    commit ncaa/teams_html ncaa/schedules_html ncaa/schedule_master ncaa/teams/parquet
  if [ $rc -ne 0 ]; then
    # No schedule master without a clean stage 01, so every later stage would run
    # on nothing (and 07 could publish it). The pages fetched so far are committed
    # above and stage 01 is file-exists resumable: stop, and a rerun picks up here.
    echo "season ${season} 01 rc=${rc} -- STOPPING (rerun resumes; see logs/bf_${season}_01.log)"
    exit 1
  fi

  # 4) rosters: sharded like stage 02 (one browser each)
  run_shards ncaa_baseball_04_rosters_scrape.py "bf_${season}_04" "$season"
  COMMIT_MSG="feat(ncaa): season ${season} rosters (stage 04)" commit ncaa/rosters_html

  # 2) games: sharded over the season's contests. A game refused through a
  # site-wide Terms lockout is only marked failed and skipped, so re-run the
  # resumable stage while passes still capture games -- and never let a season
  # with gaps reach 06/07 (publish). MAX_MISSING allows known uncapturable games.
  count_missing "$season"
  for pass in 1 2 3; do
    [ "$missing" -le "$MAX_MISSING" ] && break
    before=$missing
    run_shards ncaa_baseball_02_games_scrape.py "bf_${season}_02" "$season"
    COMMIT_MSG="feat(ncaa): season ${season} game bundles (stage 02)" commit "ncaa/raw/${season}"
    count_missing "$season"
    echo "season ${season} 02 pass ${pass}: ${missing} games missing (was ${before})"
    [ "$missing" -lt "$before" ] || break  # a pass that captured nothing: another won't either
  done
  if [ "$missing" -gt "$MAX_MISSING" ]; then
    echo "season ${season} 02: ${missing} games missing (MAX_MISSING=${MAX_MISSING}) -- STOPPING before publish (rerun resumes)"
    exit 1
  fi

  # 6) xwalk BEFORE final parse so payloads get espn stamps
  "$PY" python/ncaa_baseball_06_xwalk_build.py --season "$season" > "logs/bf_${season}_06.log" 2>&1 || true
  COMMIT_MSG="feat(ncaa): season ${season} espn xwalk (stage 06)" commit ncaa/xwalk

  # 3) parse (espn index now on disk)
  "$PY" python/ncaa_baseball_03_games_parse.py --season "$season" --workers "$PARSE_WORKERS" > "logs/bf_${season}_03.log" 2>&1 || true
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
