# RUNBOOK — NCAA baseball raw capture (Python stages)

Numbered, idempotent pipeline stages (SDV pipeline-stage convention), mirroring
`ncaa-mfb-football-raw` / `ncaa-mbb-hoops-raw` so a stage number means the same
thing across the NCAA raw repos. Stage 03 (parse) owns BOTH eras — it is
the offline reconciliation stage: capture bundles and the legacy R-era trees
resolve into one payload shape through the same sdv-py decomposition engine.

Every stage shim in `python/` is a thin argparse wrapper that delegates to the
working `ncaa_pbp.*` modules; `python/ncaa_pbp/run.py` remains the combined
ad-hoc one-session runner.

**`--season` is the calendar year** (2026 = spring 2026); stats.ncaa.org's
`academic_year` param equals it for spring sports — no offset anywhere.
`--division` 1/2/3 = D-I/II/III; online stages default to all three.

## Stages

| NN | stage | entrypoint | launcher | online? | resumability | typical invocation |
| --- | --- | --- | --- | --- | --- | --- |
| 01 | schedules: team lists + team pages → `ncaa/teams_html/{season}_d{div}.html`, `ncaa/schedules_html/{season}/`, `ncaa/schedule_master/parquet/{season}.parquet` | `python/ncaa_baseball_01_schedules_scrape.py` | `scripts/run_01_schedules_scrape.sh` | online (≈1 page/team) | persisted html re-read, not re-fetched | `./scripts/run_01_schedules_scrape.sh --season 2026` |
| 02 | games: 5-tab bundle per contest → `ncaa/raw/{season}/{contest_id}.json.gz` (contest ids from the schedule master, not rediscovery) | `python/ncaa_baseball_02_games_scrape.py` | `scripts/run_02_games_scrape.sh` | online (5 pages/game) | captured contests skipped; ban ⇒ breaker, rc=1 hard-stop, re-run resumes; chunk `--max`, fan out `--shard i/N` (one process each) | `./scripts/run_02_games_scrape.sh --season 2026 --max 200` |
| 03 | parse: raw bundles + legacy R-era trees → `ncaa/json/{game_key}.json.gz` parsed+enriched payloads (both eras through the sdv-py decomposition engine) | `python/ncaa_baseball_03_games_parse.py` | `scripts/run_03_games_parse.sh` | **offline** | resumable (`--force` overwrites); `--season N` and/or `--legacy [--year Y]`; spawn pool `--workers` | `./scripts/run_03_games_parse.sh --legacy --year 2017` |
| 04 | rosters: `teams/{id}/roster` → `ncaa/rosters_html/{season}/` | `python/ncaa_baseball_04_rosters_scrape.py` | `scripts/run_04_rosters_scrape.sh` | online (≈1 page/team) | teams with roster html skipped | `./scripts/run_04_rosters_scrape.sh --season 2026` |
| 05 | datasets: persisted html → `ncaa/teams/parquet/{season}_d{div}.parquet`, `ncaa/schedule_master/parquet/{season}.parquet`, `ncaa/rosters/parquet/{season}.parquet` (sdv-py `scrape.ncaa.reference` parsers) | `python/ncaa_baseball_05_datasets_build.py` | `scripts/run_05_datasets_build.sh` | **offline** (no proxy) | pure function of the tree; re-run overwrites | `./scripts/run_05_datasets_build.sh --season 2026` |
| 06 | xwalk: NCAA↔ESPN game crosswalk → `ncaa/xwalk/espn_game_id/{season}.json`; scoreboard sweep cached to `ncaa/xwalk/espn_scoreboard/{season}/{date}.json` | `python/ncaa_baseball_06_xwalk_build.py` | `scripts/run_06_xwalk_build.sh` | offline for NCAA; uncached days hit the ESPN scoreboard API once, then re-runs are fully offline | re-run overwrites; cache makes it offline | `./scripts/run_06_xwalk_build.sh --season 2026` |
| 07 | season datasets: parsed payloads + reference parquet → `ncaa/{dataset}/parquet/ncaa_baseball_{dataset}_{season}.parquet` (+ `ncaa/qa/qa_pbp_finals_{season}.parquet` on `--dataset all`); `--publish` uploads parquet+csv.gz+rds to the `ncaa_baseball_*` releases; `check` audits built vs published | `python/ncaa_baseball_07_datasets_publish.py` | `scripts/run_07_datasets_publish.sh` | **offline** except `gh` | pure function of the tree; re-run overwrites; uploads idempotent (`--clobber`) | `./scripts/run_07_datasets_publish.sh build --season 2024 --publish` |
| — | combined ad-hoc runner (discover + capture in one browser session) | `python/ncaa_pbp/run.py` | — | online | as above | `NCAA_PROXY_POOL=... python -m ncaa_pbp.run --year 2026 --out ./ncaa` |

`scripts/_env.sh` is sourced by every launcher (not run): repo root, sdv-py
sibling checkout first on `PYTHONPATH` (so its feat branches win over the venv
pin), venv python resolution (this repo's `.venv`, else sdv-py's),
`PYTHONUNBUFFERED`, and — online stages only — `NCAA_PROXY_POOL` (respected if
set; else built from the `.Renviron` Decodo creds at call time). `run_stage`
tees to `logs/<prefix>_<ts>.log` and prints `EXIT=<rc>`.

## Run order (one season)

```sh
./scripts/run_01_schedules_scrape.sh --season 2026            # all 3 divisions
./scripts/run_04_rosters_scrape.sh   --season 2026
./scripts/run_02_games_scrape.sh     --season 2026 --max 200  # repeat until 0 new
./scripts/run_05_datasets_build.sh   --season 2026            # offline
./scripts/run_06_xwalk_build.sh      --season 2026            # ESPN crosswalk
./scripts/run_07_datasets_publish.sh build --season 2026 --publish  # season datasets + release
```

Watch any stage live: `tail -f logs/<schedules|games|rosters|datasets|xwalk|datasets_publish>_<ts>.log`
(the path is printed at start). Completion: grep `EXIT=` in the log.

## Backfill

Same stages, different `--season` — no separate implementation. Re-running any
stage for a completed season fetches nothing (01/04 re-read persisted html, 02
skips captured bundles) and exits 0.

stats.ncaa.org is a hostile host: US-residential transport required (datacenter
IPs get an edge 403), ban = breaker hard stop — cool down before re-running.
The legacy trees (`ncaa/game_pbp`, `ncaa/team_schedules`, `ncaa/contest_pbp`,
`ncaa/schedules`, `ncaa/pbp`) belong to the R daily cron and are not touched by
these stages.

## Tests

```sh
uv run pytest -q         # or:
PYTHONPATH="/mnt/sdv_repos/sdv-py:$PWD/python" \
  /mnt/sdv_repos/sdv-py/.venv/bin/python -m pytest tests -q
```

`tests/test_stage_numbering.py` is the stage gate: the built stage set is
exactly {01..07}, each shim delegates to its working module (`ncaa_pbp.*`, or
`ncaa_baseball_data_build.cli` for 07), each `run_NN_*.sh` invokes its own
shim, and this runbook lists every stage and launcher.

## Division scope

**Game capture is D-I only** (2026-08-27 scope call; D-II/III backfill when the
community wants it): stage 02 filters the schedule master by `division`, and
`ncaa_pbp.schedules.DEFAULT_DIVISIONS = (1,)` carries that policy. Widen with
`--division 2` / `--all-divisions` on stage 02 (and `--division` on stage 04).

**Schedule discovery stays D-I..D-III** — the R-era `ncaa_baseball_schedules`
releases ship all three divisions, so a D-I-only master would regress a
published dataset, and team pages are ~3% of a season's fetch cost. The
published `schedule` dataset therefore keeps its full division coverage while
pbp/box datasets are D-I.

## Campaign orchestrator

`scripts/run_backfill_all.sh START END` runs the full per-season chain
(01 schedules → 04 rosters → 02 games ×`SHARDS` → 06 xwalk → 03 parse →
05 reference datasets → 07 build+publish), committing and pushing after every
stage — season-sized batches. Resumable end to end; stops at the coverage
floor (empty D1 team list). Proxy pool auto-built from `~/.Renviron` Decodo
creds; Chromium tmp on block storage; 5G root-disk guard.

    ./scripts/run_backfill_all.sh 2026 2024      # capture era
    tail -f logs/bf_<season>_*.log
