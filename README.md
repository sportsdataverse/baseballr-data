# baseballr-data

[![Twitter Follow](https://img.shields.io/twitter/follow/sportsdataverse?color=blue&label=%40sportsdataverse&logo=twitter&style=for-the-badge)](https://twitter.com/sportsdataverse)
[![Twitter Follow](https://img.shields.io/twitter/follow/saiemgilani?color=blue&label=%40saiemgilani&logo=twitter&style=for-the-badge)](https://twitter.com/saiemgilani)
<a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>

[![Update NCAA Baseball Data](https://github.com/sportsdataverse/baseballr-data/actions/workflows/daily_ncaa_baseball.yml/badge.svg)](https://github.com/sportsdataverse/baseballr-data/actions/workflows/daily_ncaa_baseball.yml)

The data-pipeline repository for [**baseballr**](https://github.com/BillPetti/baseballr).
It builds clean, tidy NCAA college baseball datasets and publishes them as
**GitHub release assets on
[`sportsdataverse/sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data/releases)**,
where the `baseballr` `load_ncaa_baseball_*()` functions read them from. Two
small lookup tables are served directly from this repo's `main` branch.

## How the pipeline works

```mermaid
flowchart LR
    A[stats.ncaa.org] -->|ncaa_schedule_info / ncaa_*_pbp| B(baseballr-data<br/>R creation scripts)
    B -->|local rds/csv/parquet| C[(ncaa/ in this repo)]
    B -->|sportsdataverse_save| D{{sportsdataverse-data<br/>releases}}
    D -->|load_ncaa_baseball_pbp / _schedule| E[baseballr users]
    C -->|raw.githubusercontent main| F[load_ncaa_baseball_teams / _season_ids]
    F --> E
```

1. The GitHub Actions workflow
   [`daily_ncaa_baseball.yml`](.github/workflows/daily_ncaa_baseball.yml) runs on
   a seasonal schedule (and on `workflow_dispatch` / `repository_dispatch`).
2. It calls [`scripts/daily_ncaa_baseball_R_processor.sh`](scripts/daily_ncaa_baseball_R_processor.sh),
   which loops over the requested seasons and runs the creation scripts.
3. Each creation script writes per-year `rds`/`csv`/`parquet` artifacts under
   `ncaa/` (committed back to this repo) **and** calls
   `sportsdataversedata::sportsdataverse_save()` to upload those assets to the
   matching release tag on `sportsdataverse-data` (overwriting the season's
   existing assets).
4. `baseballr`'s `load_*()` functions download the release assets on demand.

### NCAA capture campaign (manual)

The daily workflow above maintains the *published* seasons. Capturing a season
from scratch is a separate, human-run campaign against `stats.ncaa.org`: it is
slow, rate-sensitive, and every stage is file-exists resumable, so it is driven
from a terminal rather than from CI.

Run the whole thing with the orchestrator — newest season to oldest, committing
and pushing per stage:

```sh
./scripts/run_backfill_all.sh 2026 2024      # SHARDS=8 by default
tail -f logs/bf_2026_*.log
```

It stops cleanly once a season's D1 team list comes back empty (the coverage
floor). To drive one stage at a time — re-running a failed stage, or filling a
single division — each has its own launcher. The order below is the one
`run_backfill_all.sh` itself uses; [`RUNBOOK.md`](RUNBOOK.md) documents the
manual one-season order and the per-stage detail.

| Stage | Launcher | Network | What it does |
|---|---|---|---|
| 01 | `scripts/run_01_schedules_scrape.sh` | online | team lists + team pages → persisted html + `ncaa/schedule_master/parquet/{season}.parquet` |
| 04 | `scripts/run_04_rosters_scrape.sh` | online | `teams/{id}/roster` → `ncaa/rosters_html/{season}/` |
| 02 | `scripts/run_02_games_scrape.sh` | online | the 5-tab bundle per uncaptured contest → `ncaa/raw/{season}/{contest_id}.json.gz` |
| 03 | `scripts/run_03_games_parse.sh` | offline | raw bundles + legacy R-era trees → parsed payloads under `ncaa/json/` |
| 06 | `scripts/run_06_xwalk_build.sh` | mostly offline | NCAA↔ESPN game crosswalk → `ncaa/xwalk/espn_game_id/{season}.json` |
| 03 | `scripts/run_03_games_parse.sh` | offline | re-run, so parsed payloads pick up the ESPN stamps from 06 |
| 05 | `scripts/run_05_datasets_build.sh` | offline | persisted html → `ncaa/{teams,schedule_master,rosters}` reference parquet |
| 07 | `scripts/run_07_datasets_publish.sh` | offline + `gh` | season frames under `ncaa/{dataset}/parquet/`, then the release upload |

Stage 03 runs twice on purpose: the second pass is what writes the ESPN game
ids that stage 06 resolves. Stage 02 is the one to chunk (`--max`) and fan out
across disjoint `--shard i/N` processes; a ban hard-stops that run with `rc=1`,
so cool down and re-run — it resumes from what is already on disk.

Each launcher prints its own usage; `--season` (the **calendar** year — 2026 is
the spring-2026 season) is required, and `--division` narrows stages 01 and 04.

## Data releases

Published to **`sportsdataverse/sportsdataverse-data`** releases (one tag per
dataset; assets are named `*_{year}.rds` / `.csv` / `.parquet`):

| Release | Loader | Asset pattern |
|---|---|---|
| [`ncaa_baseball_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_pbp) | `baseballr::load_ncaa_baseball_pbp()` | `ncaa_baseball_pbp_{year}.rds` |
| [`ncaa_baseball_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_schedules) | `baseballr::load_ncaa_baseball_schedule()` | `ncaa_baseball_schedule_{year}.rds` |
| [`mlb_game_state`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | `sportsdataverse.mlb.load_mlb_re24_matrix()` / `_we_table` / `_wpa` (Python) | `mlb_{stem}_{season}.{csv,rds,parquet}` |
| [`mlb_hitting_models`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | `load_mlb_expected_stats()` / `_expected_hr` / `_batter_projection` | `mlb_{stem}_{season}.{csv,rds,parquet}` |
| [`mlb_fielding_models`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) | `load_mlb_oaa()` / `_catcher_framing` | `mlb_{stem}_{season}.{csv,rds,parquet}` |
| [`mlb_pitching_models`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | `load_mlb_xera()` / `_stuff_plus` / `_command_plus` | `mlb_{stem}_{season}.{csv,rds,parquet}` |

> Note the schedules release uses the **plural** tag `ncaa_baseball_schedules`
> with **singular** asset names `ncaa_baseball_schedule_{year}`.

Served directly from this repo's `main` branch (not releases):

| File | Loader |
|---|---|
| [`ncaa/teams_info/ncaa_team_lookup.{csv,rds,parquet}`](ncaa/teams_info/) | `baseballr::load_ncaa_baseball_teams()` |
| [`ncaa/seasons_info/ncaa_season_id_lu.{csv,rds,parquet}`](ncaa/seasons_info/) | `baseballr::load_ncaa_baseball_season_ids()` |

## Consuming the data

```r
# install.packages("baseballr")  # or remotes::install_github("BillPetti/baseballr")
library(baseballr)

# play-by-play / schedule come from sportsdataverse-data releases
pbp   <- load_ncaa_baseball_pbp(seasons = 2024)
sched <- load_ncaa_baseball_schedule(seasons = 2023:2024)

# team + season-id lookups come from this repo's main branch
teams   <- load_ncaa_baseball_teams()
seasons <- load_ncaa_baseball_season_ids()
```

You can also read release assets directly without `baseballr`:

```r
# parquet (fastest)
arrow::read_parquet(
  "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_pbp/ncaa_baseball_pbp_2024.parquet"
)
```

## Update schedule

All times UTC. The workflow refreshes the current season (resolved from
`baseballr:::most_recent_ncaa_baseball_season()` when no input is given):

| Window | Cadence | Cron |
|---|---|---|
| In-season (Feb–Jun) | Daily | `0 11 * 2-6 *` |
| Off-season (Jul–Jan) | Monthly (1st) | `0 11 1 1,7-12 *` |

Manual / backfill runs: trigger **Update NCAA Baseball Data** via
*workflow_dispatch* with `start_year` / `end_year` (and `rescrape=TRUE` to
re-pull raw JSON from stats.ncaa.org rather than reuse cached files).

## Repository layout

```
R/
  0000_create_baseballr_releases_init.R   # one-time: create the release tags
  0001_push_existing_release_data.R       # one-time: backfill historical seasons
  ncaa_01_schedules_creation.R            # build + publish schedules (per year)
  ncaa_02_pbp_creation.R                  # build + publish play-by-play (per year)
  ncaa_teams_info.R                       # rebuild ncaa_team_lookup
  ncaa_season_ids.R                       # rebuild ncaa_season_id_lu
  ncaa_teams_season_team_id_backfill.R    # backfill season_team_id (inst_team_list)
  ncaa_teams_roster_gapfill.R             # roster-based season_team_id gap-fill
scripts/
  daily_ncaa_baseball_R_processor.sh      # per-year orchestrator (used by CI)
  daily_ncaa_baseball_scraper.sh          # manual entry point: schedules (git wrapper)
  daily_ncaa_baseball_pbp_scraper.sh      # manual entry point: play-by-play (git wrapper)
  bash_functions.sh                       # shared shell helpers
  run_backfill_all.sh                     # NCAA capture campaign, one season at a time
  run_01_schedules_scrape.sh              # campaign stage 01 (see run order above)
  run_02_games_scrape.sh                  # campaign stage 02
  run_03_games_parse.sh                   # campaign stage 03
  run_04_rosters_scrape.sh                # campaign stage 04
  run_05_datasets_build.sh                # campaign stage 05
  run_06_xwalk_build.sh                   # campaign stage 06
  run_07_datasets_publish.sh              # campaign stage 07
  _env.sh                                 # shared stage env + run_stage helper
.github/workflows/
  daily_ncaa_baseball.yml                 # scheduled NCAA release-update workflow
  mlb_models_cron.yml                     # daily MLB model datasets (Apr-Oct)
pyproject.toml / uv.lock                  # root uv project (Python producers under python/)
python/
  mlb_model_publish/                      # MLB model-dataset publisher (4 release tags)
  ncaa_pbp/                               # NCAA baseball pbp discover+capture producer
  tests/                                  # Python tests (uv run pytest)
ncaa/
  schedules/{rds,csv,parquet}/            # compiled per-year schedule artifacts
  pbp/{rds,csv,parquet}/                  # compiled per-year play-by-play artifacts
  teams_info/                             # ncaa_team_lookup.* (load_ncaa_baseball_teams)
  seasons_info/                           # ncaa_season_id_lu.*  (load_ncaa_baseball_season_ids)
mlb/
  {game_state,hitting_models,...}/parquet/  # committed MLB model-dataset tree (cron-maintained)
  *.rda                                   # static lookup tables (historical archive)
statcast/                                 # static historical Statcast monthly extracts
```

## Maintainer setup

- **Secrets:** the workflow uploads cross-repo to `sportsdataverse-data`, so it
  uses `secrets.SDV_GH_TOKEN` (a PAT with write access to that repo), passed to
  the R scripts as `GITHUB_PAT`. `secrets.GITHUB_TOKEN` is used for this repo's
  git operations and dependency installation.
- **First-time bootstrap (run once, locally or manually):**
  ```r
  Sys.setenv(GITHUB_PAT = "<PAT with write access to sportsdataverse-data>")
  source("R/0000_create_baseballr_releases_init.R")  # create empty release tags
  source("R/0001_push_existing_release_data.R")      # backfill historical seasons
  ```
- **Dependencies:** `arrow`, `data.table`, `dplyr`, `glue`,
  `purrr`, `rvest`, `httr`, `httr2`, `optparse`, plus `piggyback` (release creation) and
  `sportsdataversedata` (`sportsdataverse_save()`); the GitHub CLI (`gh`) must be
  available on the runner for asset uploads.
