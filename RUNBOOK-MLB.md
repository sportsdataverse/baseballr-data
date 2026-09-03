# RUNBOOK — MLB raw capture (statsapi + Baseball Savant)

The MLB raw layer: **capture once, commit the payload, reshape
deterministically.** It exists to remove the class of bug the model spine kept
hitting — per-season caches (`SDV_MLB_STATCAST_CACHE`,
`SDV_MLB_STATSAPI_CACHE`) built at different times carry different derived
columns, so a null in an old vintage is indistinguishable from a real zero.
Design rationale, measured sizes and the reshape contract:
[`docs/mlb-raw-layer.md`](docs/mlb-raw-layer.md).

Numbered, idempotent stages (SDV pipeline-stage convention), mirroring the
NCAA stages in [`RUNBOOK.md`](RUNBOOK.md). **`--season` is the calendar year.**

## Stages

| NN | stage | entrypoint | launcher | online? | resumability | typical invocation |
| --- | --- | --- | --- | --- | --- | --- |
| 01 | schedule + manifest: `/api/v1/schedule` for the whole year → `mlb/raw/schedule/{season}.json.gz` and `mlb/raw/manifest/{season}.csv` (+ `manifest/index.csv`) | `python/mlb_raw_01_schedule_scrape.py` | `scripts/run_mlb_01_schedule_scrape.sh` | online (1 request) | re-fetch only with `--force`; the manifest merges, never clobbers, capture columns | `./scripts/run_mlb_01_schedule_scrape.sh --season 2024` |
| 02 | statsapi: one `/api/v1.1/game/{pk}/feed/live` per final game → `mlb/raw/statsapi/{season}/{game_pk}.json.gz` | `python/mlb_raw_02_statsapi_scrape.py` | `scripts/run_mlb_02_statsapi_scrape.sh` | online (1 request/game) | presence-based skip; `--limit N` to chunk; `--commit-every N` commits as it goes | `./scripts/run_mlb_02_statsapi_scrape.sh --season 2024 --commit-every 500` |
| 03 | savant: `/statcast_search/csv` fetched **by day**, sliced **by game** → `mlb/raw/savant/{season}/{game_pk}.csv.gz` | `python/mlb_raw_03_savant_scrape.py` | `scripts/run_mlb_03_savant_scrape.sh` | online (1 request/day, ~186 days/season) | presence-based skip; `--limit N` days; `--commit-every N` | `./scripts/run_mlb_03_savant_scrape.sh --season 2024 --commit-every 60` |

`scripts/_env.sh` is sourced by every launcher (`OFFLINE=1` — neither host needs
the NCAA proxy pool). `run_stage` tees to `logs/mlb_<stage>_<ts>.log` and prints
`EXIT=<rc>`.

## Run order (one season)

```sh
./scripts/run_mlb_01_schedule_scrape.sh --season 2024
./scripts/run_mlb_02_statsapi_scrape.sh --season 2024 --commit-every 500
./scripts/run_mlb_03_savant_scrape.sh   --season 2024 --commit-every 60
```

Watch live: `tail -f logs/mlb_statsapi_<ts>.log` (the path is printed at start).
Completion: grep `EXIT=` in the log. Ctrl-C is always safe — every stage resumes
from what is on disk.

## Knobs (env-only; never hardcode a rate)

| var | default | effect |
| --- | --- | --- |
| `SDV_MLB_RAW_ROOT` | `<repo>/mlb/raw` | capture root; `--root` beats it. Set it to point the same code at a dedicated `mlb-raw` checkout |
| `SDV_MLB_RAW_STATSAPI_SLEEP` | `0.15` | seconds between statsapi games |
| `SDV_MLB_RAW_SAVANT_SLEEP` | `1.0` | seconds between Savant day pulls (Savant is the rate-sensitive one) |

## Guards

* **Nothing empty is ever persisted.** Stage 02 rejects a payload with no
  `gamePk`, no `allPlays`, no boxscore teams, or under 20 KB canonical
  (`MIN_STATSAPI_BYTES`); stage 03 rejects a header-only slice. A rejection is
  counted and logged as `REFUSED`, the file is not written, and the stage exits
  non-zero — an uncaptured game stays visibly uncaptured rather than becoming a
  zero-row file that looks captured.
* **Savant's 25,000-row cap is asserted, not assumed.** A 7-day window returns
  exactly 25,000 rows, silently truncated. A one-day window peaks at 4,777
  (2008-06-15, 16 games). Stage 03 fetches one day at a time and hard-stops if a
  day ever reaches the cap.
* **Every row of a fetched day lands in exactly one game slice** (asserted in
  `capture_day`), so no pitch is silently dropped by the slicing step.
* **Bytes are deterministic.** JSON is canonicalised and gzipped at `mtime=0`,
  so re-capturing an unchanged payload reproduces the file byte-for-byte and git
  sees no diff.

## Manifest contract

`mlb/raw/manifest/{season}.csv` is the file a consumer reads — one row per game,
with `game_pk`, date, `game_type`, status, teams, venue, and for each surface
the relative path, byte count and sha256. `mlb/raw/manifest/index.csv` lists the
seasons. Both are readable over `raw.githubusercontent.com` without cloning and
without listing a directory (GitHub serves no directory listing over the raw
host), which is the whole point: a sibling repo enumerates game ids from one
known URL.
