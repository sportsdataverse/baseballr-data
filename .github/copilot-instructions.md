# baseballr-data Copilot Instructions

## Project Context

This repo is the R-side scraper stage for the
[`baseballr`](https://github.com/billpetti/baseballr) package. It iterates
`baseballr::ncaa_*()` calls against `stats.ncaa.org` for per-team
schedules and per-game play-by-play, writes the cumulative output to
`ncaa/` as CSV + RDS + Parquet, commits to `main`, and uploads the same
files as GitHub releases on `sportsdataverse-data` via
`sportsdataversedata::sportsdataverse_save()`.

Pipeline: `stats.ncaa.org -> baseballr-data [HERE] -> sportsdataverse-data releases -> baseballr R package`.

`statcast/` is a static archive of historical Statcast monthly extracts;
`mlb/` holds static `.rda` lookup tables PLUS the committed
`mlb/{dataset}/parquet/` model-dataset tree maintained by the MLB models
cron (see the Python section below). The NCAA daily flow only touches
`ncaa/`.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- CI entry points are `scripts/daily_ncaa_baseball_scraper.sh` (schedules) and
  `scripts/daily_ncaa_baseball_pbp_scraper.sh` (play-by-play). Both wrap the R
  scripts with `git pull` / `git add` / `git commit` / `git push`.
- Bug fixes to NCAA HTML parsing belong upstream in `baseballr`, not here.
- Don't reorganize the `ncaa/` output tree without aligning `baseballr`'s
  `load_ncaa_baseball_*()` loaders + the sportsdataverse-data release
  filename layout.

## Build & Development Commands

```sh
# Daily schedule scrape (schedules per team, then unified per season)
bash scripts/daily_ncaa_baseball_scraper.sh -s 2026 -e 2026 -r false

# Daily play-by-play scrape (per-game pbp, then unified per season)
bash scripts/daily_ncaa_baseball_pbp_scraper.sh -s 2026 -e 2026 -r false

# Iterate without the git wrapper
Rscript R/ncaa_01_schedules_creation.R -s 2026 -e 2026 -r FALSE
Rscript R/ncaa_02_pbp_creation.R       -s 2026 -e 2026 -r FALSE

# Maintenance (no flags — manual cadence)
Rscript R/ncaa_season_ids.R
Rscript R/ncaa_teams_info.R
```

`-r true` forces re-scrape per team/game; `-r false` skips files already
on disk. The daily default is `-r false`.

## Outputs

- `ncaa/team_schedules/{csv,json,parquet}/{year}_{team_id}.{ext}` — per-team-season scrape
- `ncaa/schedules/{csv,rds,parquet}/ncaa_baseball_schedule_{year}.{ext}` — unified per-season schedule
- `ncaa/game_pbp/`, `ncaa/contest_pbp/` — intermediate per-game pbp scrape
- `ncaa/pbp/{parquet,rds}/` — unified per-season PBP
- `ncaa/teams_info/ncaa_team_lookup.{csv,rds,parquet}` — team_id lookup
- `ncaa/seasons_info/ncaa_season_id_lu.{csv,rds,parquet}` — year -> season_id map

Each persisted table is wrapped in
`baseballr:::make_baseballr_data("<description>", Sys.time())` before
write, and (for daily artifacts) also uploaded via
`sportsdataversedata::sportsdataverse_save(release_tag = "ncaa_baseball_*", ...)`.

## Code Style

- Follow tidyverse style: `snake_case`, 2-space indentation.
- Load packages at the top with
  `suppressPackageStartupMessages(suppressMessages(library(pkg, lib.loc = lib_path)))`
  where `lib_path <- Sys.getenv("R_LIBS")`. CI provides a pre-warmed library.
- Per-team / per-game iteration uses `purrr::map()` (sequential, single
  process — not `furrr`/`future`). Keep `Sys.sleep()` between calls; NCAA
  aggressively rate-limits. Don't reintroduce parallelism: the loops are
  sleep/I-O bound and sequential keeps proxy rotation + pacing predictable.
- Proxy rotation: `get_proxy_ips()` + `select_proxy()` in `R/utils.R` read
  the `PROXY_KEY`, `PROXY_PKG`, `PROXY_ENDPOINT` env vars. CI supplies
  these via Actions secrets — never commit credentials.
- Don't add bespoke NCAA parsing here — call into `baseballr::ncaa_*()`
  and persist the output.
- File writes are always the trio: `data.table::fwrite` (CSV),
  `saveRDS` (RDS), `arrow::write_parquet` (Parquet). Don't drop one
  without updating `baseballr`'s loaders.

## Cross-Repo References

- Upstream R package: <https://github.com/billpetti/baseballr>
- Release host: <https://github.com/sportsdataverse/sportsdataverse-data>
- Save helper: <https://github.com/sportsdataverse/sportsdataversedata>

## Commit Convention

The two daily shell scripts produce hardcoded subject lines that are
load-bearing for downstream parsing — keep the `(Start: YYYY End: YYYY)`
suffix intact:

```
NCAA Schedules update (Start: 2026 End: 2026)
NCAA PBP update (Start: 2026 End: 2026)
```

For human-authored commits, use Conventional Commits:
`type(scope): description`. Common types: `feat`, `fix`, `chore`, `ci`,
`docs`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer for
breaking changes.

## Python (uv, repo root)

The Python producers live at the repo root (uv: `pyproject.toml` + `uv.lock`,
sportsdataverse pinned to git@main). Tests: `uv run pytest tests/`.

- `mlb_model_publish/` publishes the four MLB model releases on
  sportsdataverse-data (`mlb_game_state`, `mlb_hitting_models`,
  `mlb_fielding_models`, `mlb_pitching_models`) in csv+rds+parquet, and
  commits the parquet-only `mlb/{dataset}/parquet/` tree (csv/rds are
  gitignored staging). Cron: `.github/workflows/mlb_models_cron.yml`, tree
  commits use the load-bearing `MLB Models update (Start: Y End: Y)` subject.
- `ncaa_pbp/` is the NCAA baseball pbp discover+capture producer
  (`uv run python -m ncaa_pbp.run`; see `ncaa_pbp/README.md`).

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
