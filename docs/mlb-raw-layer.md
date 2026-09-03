# MLB raw layer — design

Why the MLB spine gets a `-raw` tier, what it captures, what it costs in
bytes (measured, not estimated), and the contract a `-data` consumer reads.
Operator detail lives in [`RUNBOOK-MLB.md`](../RUNBOOK-MLB.md).

## 1. The gap, stated precisely

Every other sport in the fleet has a `-raw` repo that commits per-game payloads
and a `-data` repo that reshapes and publishes. MLB has neither half of that
split: `baseballr-data` builds its 4 published model tags by **self-collecting
live** from `statsapi.mlb.com` and Baseball Savant into per-season parquet
caches:

| cache | env var | written by | invalidated by |
|---|---|---|---|
| `statcast_{season}.parquet` | `SDV_MLB_STATCAST_CACHE` | `mlb_model_publish/computes.py::load_season_pitches` | **nothing** — `if f.exists(): return pl.read_parquet(f)` |
| `statsapi_pbp_{season}.parquet` | `SDV_MLB_STATSAPI_CACHE` | `computes.py::load_season_pbp` | **nothing**, same shape |

Neither cache records when it was pulled or what schema Savant was serving that
day. Savant has added columns repeatedly inside the window the models train on —
`arm_angle` (2023), `bat_speed` / `swing_length` (2024), `attack_angle` /
`swing_path_tilt` / `intercept_ball_minus_batter_pos_*` (2025). A season cached
before a launch has the column **absent**; a season cached after has it
**present-but-null-where-untracked**. Concatenate the two and every pre-launch
season reads as null — indistinguishable from a real zero, and indistinguishable
from "this pitch had no swing".

That is exactly the class of failure §0 row 8 of the stocktake hit this week:
`xwoba` null on 7,451 of 22,172 shipped rows with a steep time trend, producing
a spurious negative `xwoba`-vs-`xba` correlation. The fix there was a rebuild.
The fix at the root is a raw layer.

---

## 2. Sources — what each is authoritative for

### 2.1 statsapi: one payload per game

`GET https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live` is the whole
game in one request. **Measured containment** (gamePk 746694, 2024-07-06):

| standalone endpoint | relation to `feed/live` | gz/game |
|---|---|---|
| `/game/{pk}/playByPlay` | **byte-identical** to `liveData.plays` once its `copyright` key is dropped | 110 KB |
| `/game/{pk}/linescore` | identical to `liveData.linescore` | 1 KB |
| `/game/{pk}/boxscore` | identical **except under `teams`** — the standalone endpoint re-hydrates season stats as of fetch time, so it is *not reproducible*; `feed/live`'s copy is frozen at the game | 17 KB |
| `/game/{pk}/contextMetrics` | leverage/WE summary, 3 numbers | 0.8 KB |
| `/game/{pk}/winProbability` | re-serves `allPlays` **plus** 4 WP fields per play | 126 KB |
| `/game/{pk}/content` | media metadata (highlights, editorial) | 19 KB |

So **one request per game replaces four**, and the one we keep is the one whose
boxscore is reproducible. `winProbability` and `content` are excluded (§5.3).

Payload is stored **whole — no allowlist.** `liveData.plays.allPlays[].playEvents`
is 94 KB of the 136 KB compressed (69%) and is tempting to prune, but it is the
only source of non-pitch events (substitutions, pickoffs, mound visits, replay
reviews) and of statsapi's own pitch-call vocabulary and `playId` join key.
Savant carries none of those.

Depth: `feed/live` returns `playEvents` with populated `pitchData` back to at
least **2000** (gamePk 5329, 2000-06-15: 78 plays, 262 pitches, 262 with
`pitchData`).

### 2.2 Baseball Savant: per-pitch, fetched by day, committed by game

`GET https://baseballsavant.mlb.com/statcast_search/csv` — the surface
`mlb_statcast_search` already wraps. Two properties decide the design:

**(a) It has no per-game route; its native unit is a date range, and it caps a
response at 25,000 rows with no pagination.** Measured: a 7-day window
(2024-07-01..07) returns **exactly 25,000 rows** — silently truncated. The
busiest single day measured across 2008–2024 is **4,777** (2008-06-15, 16
games). A one-day window is therefore the only provably-uncapped window, with
~5× headroom. Stage 03 fetches one day and **asserts** the cap rather than
assuming it. (`sportsdataverse`'s `_search_core` already halves on the cap; the
raw stage does not rely on that recursion because a raw capture must know
exactly which day produced which byte.)

**(b) The CSV is the only complete surface.** Savant's per-game `/gf` feed
returns the same 321 pitches for gamePk 746694, but the CSV carries **85 fields
`/gf` does not**, including every modelled column the MLB spine reads:
`estimated_woba_using_speedangle`, `estimated_ba_using_speedangle`,
`estimated_slg_using_speedangle`, `delta_run_exp`, `delta_home_win_exp`,
`woba_value`/`woba_denom`, `release_speed`, `release_spin_rate`, `spin_axis`,
`pfx_x`/`pfx_z`, `bat_speed`, `swing_length`, `arm_angle`, `effective_speed`,
`umpire`, the nine `fielder_*` ids and both fielding alignments. `/gf` is a
scoreboard feed; it is the wrong surface for a raw layer and is not captured.

**The finding that makes the whole design work:** the CSV returns **119 columns
in every season from 2008 to 2024** when fetched today. The provider back-fills
the *schema*; only the *values* are null where the tracking did not exist:

| day | rows | cols | `release_spin_rate` null | `launch_speed` null | `estimated_woba…` null | `bat_speed` null | `arm_angle` null |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2008-06-15 | 4,777 | 119 | 4,777 | 4,777 | 4,777 | 4,777 | 4,777 |
| 2010-06-15 | 4,502 | 119 | 4,502 | 4,502 | 4,502 | 4,502 | 4,502 |
| 2015-06-15 | 3,930 | 119 | 195 | 3,061 | 2,932 | 3,930 | 3,930 |
| 2017-06-15 | 3,105 | 119 | 52 | 2,229 | 2,326 | 3,105 | 3,105 |
| 2020-08-15 | 4,562 | 119 | 13 | 3,224 | 3,446 | 4,562 | 1,889 |
| 2024-06-15 | 4,145 | 119 | 12 | 2,767 | 3,100 | 2,248 | 12 |

Stronger still: the column set is identical **and so is the column order** —
verified by fetching one day from each of 2008, 2015, 2020, 2024 and 2025 today
and comparing the header verbatim (`same_set=True same_ORDER=True`, 119 columns,
all five). That is what makes the reshape's vertical concat (§8.2 R2) legal
rather than merely convenient.

A capture taken today is therefore **self-describing across the whole history**:
`bat_speed` null in 2015 is "not tracked in 2015"; `bat_speed` null in 2024 is
"no swing on this pitch", and the two are told apart by `description`/`events` —
not by which day someone happened to run the scraper. That is the property the
per-season cache does not have and cannot be given.

Coverage extends to every game type: 2024-10-29 (postseason) returns 308 rows
with `game_type = W`; 2024-03-05 returns 2,586 rows with `game_type = S`.

---

## 3. Path conventions

Modelled on `nfl-raw` (season subdirectories, globally-unique game ids) rather
than `cfb-raw`'s flat tree: MLB is 27 seasons × ~2,470 games, and 67,000 files
in one directory is hostile to both git and a human.

```
mlb/raw/
  manifest/index.csv                     <- season discovery, ONE known URL
  manifest/{season}.csv                  <- one row per game: ids, paths, bytes, sha256
  schedule/{season}.json.gz              <- /api/v1/schedule, whole year, unpruned
  statsapi/{season}/{game_pk}.json.gz    <- feed/live, whole
  savant/{season}/{game_pk}.csv.gz       <- per-pitch slice, as delivered
```

Every path is reachable over `raw.githubusercontent.com` with no clone. Today
the tree lives in `baseballr-data`; after the move recommended in §7 only the
repository segment changes, which is why consumers should take the prefix from
one constant:

```
# today
https://raw.githubusercontent.com/sportsdataverse/baseballr-data/main/mlb/raw/statsapi/2024/746694.json.gz
# after the §7 move
https://raw.githubusercontent.com/sportsdataverse/mlb-raw/main/mlb/raw/statsapi/2024/746694.json.gz
```

**The manifest is the contract.** GitHub serves no directory listing over the
raw host, so a consumer that cannot enumerate without listing cannot consume.
`manifest/{season}.csv` carries, per game: `game_pk, game_date, season,
game_type, status_code, doubleheader, game_number, home_id, home_abbr, away_id,
away_abbr, venue_id`, then for each surface `path`, `bytes`, `sha256`. One file
serves three jobs — the enumeration a consumer reads, the integrity index it
verifies against, and the resume checkpoint the capture stages work from.

`manifest/index.csv` is one row per season (`games`, `game_types`,
`statsapi_captured`, `statsapi_bytes`, `savant_captured`, `savant_bytes`,
`manifest`) so a consumer discovers seasons from a single fixed URL.

**Gotcha found while building it:** `/api/v1/schedule` over a full calendar year
lists a suspended/resumed game under **both** its original and its completion
date — 39 duplicate `gamePk`s in 2,998 rows for 2024. Stage 01 dedupes on
`game_pk`, which is what makes the manifest idempotent (2,959 rows on the first
write and every rewrite).

---

## 4. Sizing — measured, not guessed

All figures are gzip level 6 of the canonical payload, i.e. the bytes actually
committed.

### 4.1 statsapi `feed/live`, per game

| season | gamePk | raw | **gz** |
|---|---|---:|---:|
| 2000 R | 5329 | 443 KB | **34 KB** |
| 2005 R | 23597 | 545 KB | **45 KB** |
| 2007 R | 69131 | 435 KB | **38 KB** |
| 2008 R | 235070 | 584 KB | **65 KB** |
| 2010 R | 264775 | 687 KB | **78 KB** |
| 2015 R | 414918 | 711 KB | **91 KB** |
| 2024 R | 746694 | 794 KB | **136 KB** |
| 2024 W | 775298 | 832 KB | **143 KB** |
| 2025 R | 777222 | 723 KB | **120 KB** |

### 4.2 Savant per-pitch, per game

Whole-day pulls, and the per-game slices they split into (2024-07-06, 15 games):

| unit | value |
|---|---|
| one day, gz | 659 KB (2015) · 740 KB (2008) · 929 KB (2020) · 1.04 MB (2024-07-06) |
| **per-game slice, gz** | **41 KB (2015) · 41 KB (2008) · 61 KB (2024)** |
| sum of the day's slices | 912 KB — *smaller* than the 1.04 MB whole-day gz |
| per-game parquet-zstd | 98 KB — **larger** than csv.gz at this granularity |

Two conclusions: slicing per game costs nothing in bytes, and **parquet is the
wrong container at per-game granularity** (column-chunk overhead dominates a
300-row frame). csv.gz also preserves the provider's own column names and values
verbatim, which is what a raw layer is for.

### 4.3 Games per season (statsapi, final only) — every season counted, not sampled

| season | R | postseason (F+D+L+W) | spring (S) | duplicate gamePks in the year query |
|---|---:|---:|---:|---:|
| 2000 | 2,464 | 31 | 0 | 5 |
| 2005 | 2,431 | 30 | 0 | 27 |
| 2010 | 2,430 | 32 | 462 | 25 |
| 2015 | 2,429 | 36 | 481 | 41 |
| 2020 | 898 | 53 | 324 | 75 |
| 2024 | 2,429 | 43 | 454 | 39 |
| 2025 | 2,430 | 47 | 458 | 34 |
| 2026 (partial) | 2,093 | 0 | 448 | 28 |

**Total 2000–2026, `R,F,D,L,W`, status `F`: 64,701 games.** Every full season
except 2020 (COVID, 898+53) sits at 2,426–2,464 regular plus 28–47 postseason.
Spring exists in statsapi only from 2006. Every season carries duplicate
`gamePk`s in a full-year schedule query (5 in 2000 to 83 in 2021) — the dedupe
in stage 01 is a whole-history requirement, not a 2024 quirk.

Spring (`S`), exhibition (`E`) and all-star (`A`) are enumerated in the manifest
but not captured; adding spring costs ~+19%.

### 4.4 Season and full-history totals

Applying the §4.1/§4.2 per-game curve to the exact §4.3 counts:

| slice | games | statsapi | savant | total |
|---|---:|---:|---:|---:|
| one 2024 season **(estimated)** | 2,472 | 2,472 × 136 KB = 337 MB | 2,472 × 61 KB = 151 MB | 488 MB |
| one 2024 season **(actually captured)** | 2,472 | **310.4 MiB** (131.6 KB/game) | **141.0 MiB** (59.9 KB/game) | **451.4 MiB** |
| one 2015 season | 2,465 | 224 MB | 101 MB | 325 MB |
| **2000–2026 statsapi** | 64,701 | **≈ 5.2 GB** | — | |
| **2008–2026 savant** | 44,973 | — | **≈ 2.1 GB** | |
| **full history** | | | | **≈ 7.3 GB** |

(Savant's floor is 2008 — the pitch-f/x era. 2000–2007 is statsapi-only.)

### 4.5 Fleet calibration (`git count-objects -vH`, packed)

| repo | games | packed |
|---|---:|---:|
| `nfl-raw` | 7,598 | 229.5 MiB |
| `cfbfastR-cfb-raw` | ~20,700 | **12.78 GiB** |
| `hoopR-nba-raw` | — | **34.03 GiB** |

---

## 5. The git-vs-release decision

**Decision: git, for both surfaces, for the whole history. No release-parquet
fallback for the raw tier.**

Reason: 7.3 GB is *below* what the fleet already operates. `cfb-raw` runs at
12.8 GB and `hoopR-nba-raw` at 34 GB, both with the same commit-as-you-go /
chunked-push pattern this design uses. The per-pitch Savant history — the part
the brief flagged as possibly too large — is only **2.1 GB of that**, because
per-game csv.gz compresses to ~50 KB. There is no size argument for demoting it
to a release asset, and demoting it would forfeit the two things git buys: the
payload is addressable per game over `raw.githubusercontent.com` without
downloading a season, and a schema change on re-capture shows up as a **diff**
instead of as a silently different asset.

Season **parquet on a release** remains the right container — for the `-data`
tier, which is a different layer with a different contract (typed, reshaped,
versioned by tag). The raw tier stays as-delivered.

The one real size caveat is the *host repo*, not the data — see §7.

### 5.3 What is deliberately not captured

| surface | cost if captured | why not |
|---|---:|---|
| `/game/{pk}/winProbability` | +126 KB gz/game = **+310 MB/season**, ~+94% on the statsapi tier | It re-serves `allPlays` verbatim to add 4 floats per play, and those floats are a *model output* (MLB's own WP), not an observation. `mlb_win_expectancy.py` computes ours. If MLB's WP is ever wanted as an oracle, add a compact sidecar (`atBatIndex` + 4 floats ≈ 4 KB/game), not the payload. |
| `/game/{pk}/content` | +19 KB gz/game | Media/editorial metadata; no model reads it. |
| Savant `/gf` | +435 KB gz/game | Missing all 85 modelled columns (§2.2b). |
| `boxscore`, `playByPlay`, `linescore` | +128 KB gz/game | Subsets of `feed/live` (§2.1). |
| Savant leaderboards (37 endpoints) | small | Season/player-level aggregates, not per-game. They belong to the `-data` tier as their own capture, not to a per-game raw tree. |

---

## 6. Capture contract

Three numbered, idempotent stages (SDV pipeline-stage convention, mirroring the
NCAA stages in `RUNBOOK.md`). Full operator detail: `RUNBOOK-MLB.md`.

| NN | stage | entrypoint | requests |
|---|---|---|---|
| 01 | schedule + manifest | `python/mlb_raw_01_schedule_scrape.py` | 1/season |
| 02 | statsapi `feed/live` | `python/mlb_raw_02_statsapi_scrape.py` | 1/game (~2,475) |
| 03 | Savant per-pitch | `python/mlb_raw_03_savant_scrape.py` | 1/**day** (~186) |

```sh
./scripts/run_mlb_01_schedule_scrape.sh --season 2024
./scripts/run_mlb_02_statsapi_scrape.sh --season 2024 --commit-every 500
./scripts/run_mlb_03_savant_scrape.sh   --season 2024 --commit-every 60
```

**Resumability.** Presence-based skip is the only skip condition
(`core.already_captured`). `--limit N` chunks a run; `--commit-every N` commits
as it goes; a resumed run **re-records** path/bytes/sha256 for files already on
disk, so an interrupted run never leaves a captured game with blank manifest
columns. Ctrl-C is always safe.

**Never persist an empty or error payload.** Every write goes through
`persist_json` / `persist_text` with a validator, and a rejected payload leaves
**no file at all** — the stage counts it, logs it as `REFUSED`, and exits
non-zero. Stage 02 rejects: not a dict, no `gamePk`, zero `allPlays`, no
boxscore teams, or under 20 KB canonical (the smallest real completed game
measured is 443 KB raw). Stage 03 rejects a header-only or wrong-shaped slice.
This is the specific failure the fleet keeps re-learning — a zero-row artifact
that looks captured is worse than a visible gap.

**Two more guards.** Stage 03 asserts every row of a fetched day lands in
exactly one game slice (no pitch silently dropped by the split), and hard-stops
if a one-day window ever reaches the 25,000-row cap. Bytes are deterministic:
canonical JSON, gzip `mtime=0` **and `filename=""`** — without the latter,
GzipFile stamps the temp file's own name into the header and two identical
payloads differ on disk (caught by a test, fixed).

**Pacing is env-only, never hardcoded**: `SDV_MLB_RAW_STATSAPI_SLEEP` (0.15s),
`SDV_MLB_RAW_SAVANT_SLEEP` (1.0s). `SDV_MLB_RAW_ROOT` / `--root` point the same
code at any checkout, which is what makes the tree portable to a new repo.

---

## 7. Repo recommendation — a new `sportsdataverse/mlb-raw`

**Warranted. I did not create it** (org-repo creation is the user's call).

Rationale, in order of weight:

1. `baseballr-data` is **already ~11 GB on disk** (`ncaa/` 9.8 GB + `statcast/`
   1.1 GB). Adding 7.3 GB makes it ~18 GB. Creating a fresh worktree there
   already exceeds a 2-minute timeout today (observed while doing this work);
   I had to fall back to a sparse checkout to work in it at all.
2. It is the fleet's own split: `nfl-raw`→`nfl-data`, `cfb-raw`→`cfb-data`,
   `hoopR-nba-raw`→`hoopR-nba-data`. `baseballr-data` is the *data* half; it
   owns reshaping and publishing, and its NCAA raw tree living there is a
   historical accident, not a pattern to extend.
3. A raw repo wants its own cron cadence, its own runbook, and its own
   "scraping-only" charter (the `nfl-raw` SP3 decommission made exactly this
   split).

**What the repo is:** `sportsdataverse/mlb-raw`, public, no releases, MIT.
Contents = the `mlb/raw/` tree plus `python/mlb_raw/`, the three shims, the
three launchers, `scripts/_env.sh`, `tests/test_mlb_raw_stages.py`,
`RUNBOOK-MLB.md` (renamed `RUNBOOK.md`) and `docs/mlb-raw-layer.md`. Because
every path in this design is root-relative and the root is a flag, the move is a
`git mv` plus a one-line `SDV_MLB_RAW_ROOT` default — no code change.

Until it exists, the code and the proof live in `baseballr-data` (this PR),
which is where MLB is owned today.

---

## 8. Reshape contract — what `-data` reads, and why the vintage problem dies

### 8.1 The read path

```python
import io, gzip, csv, urllib.request
import polars as pl

# the ONE constant that changes when the tree moves to mlb-raw (§7)
RAW = "https://raw.githubusercontent.com/sportsdataverse/baseballr-data/main/mlb/raw"

def manifest(season: int) -> pl.DataFrame:            # 1. enumerate, no listing
    return pl.read_csv(f"{RAW}/manifest/{season}.csv")

def season_pitches(season: int) -> pl.DataFrame:      # 2. per-pitch, from raw
    m = manifest(season).filter(pl.col("savant_path").is_not_null())
    frames = [
        pl.read_csv(gzip.decompress(urllib.request.urlopen(f"{RAW}/{p}").read()),
                    infer_schema_length=0)           #    every column Utf8
        for p in m["savant_path"]
    ]
    return pl.concat(frames, how="vertical")          #    NOT diagonal
```

`manifest/index.csv` gives the season list; `savant_sha256` lets the consumer
verify what it fetched.

### 8.2 The three rules that remove the vintage problem

**R1 — every column is read as Utf8 and typed once, by the consumer.**
`infer_schema_length=0` on every slice. Per-file inference is how the same
column ends up `Int64` in April and `Float64` in September; the raw tier stores
what the provider sent and the reshape step owns typing, in one place, for the
whole season.

**R2 — concat vertically against a declared column contract, never
`diagonal`.** The 119-column set is identical in every season 2008–2024 (§2.2),
so a vertical concat is *possible*, and a slice that does not match is a
provider schema change that must fail loudly rather than be papered over with
nulls. `how="diagonal"` is precisely the operation that manufactures the
ambiguity — it turns "this file is from a different vintage" into "this value is
null".

**R3 — a `-data` builder may read a raw column; it may never read a cached
derived column.** `x_woba`, `x_era`, RE24, run values, Stuff+/Command+ inputs
are recomputed from the per-pitch payload on every build. The parquet cache
stops being a source of truth and becomes a *performance* cache **keyed on the
manifest's content** — concretely, `load_season_pitches` becomes:

```python
key = hashlib.sha256(manifest_csv_bytes(season)).hexdigest()[:16]
f = cache / f"statcast_{season}_{key}.parquet"     # raw re-capture -> new key
```

so a re-capture invalidates the cache automatically. Today
`load_season_pitches` returns any file that happens to exist at
`statcast_{season}.parquet`, with no key at all. That one-line change is most of
what the raw layer buys, and it is only possible once the raw tree exists to
hash.

### 8.3 What this fixes, concretely

| symptom today | after |
|---|---|
| `bat_speed` absent in a 2022-vintage cache, present in a 2025-vintage one; a concat nulls the older seasons | column present in every season's capture; null means "not tracked in 2015" or "no swing", disambiguated by `description`/`events` |
| `xwoba` null on 34% of shipped rows with a steep time trend (§0 row 8) | null-ness is a property of the pitch, identical on every re-read, and auditable against `description` |
| a rebuild silently produces different columns than the last one | a re-capture produces a **git diff**; unchanged payloads re-gzip to identical bytes and produce no diff at all |
| no way to reproduce a published artifact | manifest sha256 + commit sha pin the exact inputs |

### 8.4 What it does **not** fix (state it, don't pretend)

Savant recomputes its own modelled columns when it revises a model —
`estimated_woba_using_speedangle` for a 2016 pitch is not guaranteed to be the
same number next year. A raw layer cannot freeze the provider. What it does is
make the change **visible**: a re-capture diffs, and the manifest's sha256 says
which artifact was built from which bytes. That is the difference between a
revision and a mystery.

---

