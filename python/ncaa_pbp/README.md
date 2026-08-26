# python/ — NCAA baseball pbp producer (Python-side)

Python-side capture of NCAA baseball (`sport_code=MBA`) play-by-play from
`stats.ncaa.org`, the replacement for the legacy R scrape path (`R/ncaa_0*.R` via
`baseballr::ncaa_*`) per the standing directive to move -data extraction to Python.

The maintained entry points are the **numbered pipeline stages** — see the
repo-root `RUNBOOK.md` for the stage table (01 schedules, 02 games, 04 rosters,
05 datasets, 06 xwalk; 03 = parse hole, built separately). **Parsing lives in
sdv-py** (`sportsdataverse.baseball.college_baseball.parse_college_baseball_ncaa_pbp`
+ the `sportsdataverse.scrape.ncaa.reference` team-list/schedule/roster parsers);
this producer is capture-only.

```
discover.py       # team list (MBA) -> team pages -> contest_ids; NCAA_PROXY_POOL helper
capture.py        # /contests/{id}/* 5-tab bundle -> {out_dir}/{id}.json.gz
schedules.py      # stage 01: teams_html + schedules_html + schedule master parquet
games.py          # stage 02: master-driven bundle capture -> ncaa/raw/{season}/
rosters.py        # stage 04: teams/{id}/roster -> rosters_html
datasets.py       # stage 05 (offline): persisted html -> reference parquet frames
xwalk.py          # stage 06: NCAA<->ESPN game crosswalk (cached scoreboard sweep)
run.py            # combined ad-hoc runner (holds one browser session)
tests/            # offline: real fixtures + injected fetch_fn (../../tests/)
```

## Run

```sh
./scripts/run_01_schedules_scrape.sh --season 2026     # see RUNBOOK.md
# ad-hoc combined runner:
NCAA_PROXY_POOL="$(cat proxies.txt)" \
  python -m ncaa_pbp.run --sport MBA --year 2026 --out ./ncaa --max 200
```

Transport = `sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher.with_browser`
(patchright + `--headless=new` + a **US-residential** proxy pool — datacenter IPs
get an instant edge 403). Hold ONE session (no per-call relaunch). stats.ncaa.org
IP-bans scrapers — run sparingly, paced, from a residential IP.

Softball (`WSB`) has its own producer repo (`ncaa-softball-raw`).
