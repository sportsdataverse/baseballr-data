"""Stage 06 -- compile Statcast monthly raws into one parquet per season.

    mlb/statcast_raw/<Y>/<Y>-MM.parquet  (stage 04, committed)
      -> mlb/statcast/mlb_statcast_<Y>.parquet  (release staging, gitignored)

A pure function of the monthly files: every run rebuilds the season from
scratch and writes atomically, so it is idempotent and is re-run in-season
after stage 04 banks a new month. The monthly raws stay where they are -- they
are stage 04's resume state and the committed parquet tree for every season.

WHY THE OUTPUT IS NOT COMMITTED: a full compiled season is 86-107 MiB (2024 =
103, 2025 = 107 MiB sorted at zstd-3; zstd-19 only gets 2025 to 96.5), at or over
GitHub's 100 MiB per-blob limit, and one rejected blob blocks every later push
from this checkout -- the daily MLB cron included. Stage 05 publishes it to the ``mlb_statcast`` release.

Savant's CSV export infers dtypes per request, so one column arrives as Int64
in one month and Float64 (or all-null Float64) in another. ``DTYPES`` pins each
drifting column to ONE dtype; any drift not listed fails the stage instead of
letting a relaxed concat pick a different supertype per season.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import polars as pl

FLOOR = 2015  # Statcast's first season

# Measured 2026-10-11 over all 99 monthly files (2015-03 .. 2026-09). Each pin is
# the majority dtype, except sv_id: a pitch id ("150407_203357") that is String
# where populated (2015-16) and all-null Float64 after, so the cast moves no value.
DTYPES = {
    "sv_id": pl.String,
    "zone": pl.Float64,
    "release_spin_rate": pl.Float64,
    "spin_axis": pl.Float64,
    "age_pit": pl.Int64,
    "age_bat": pl.Int64,
    "age_pit_legacy": pl.Int64,
    "age_bat_legacy": pl.Int64,
}
# Whole days; sportsdataverse returns Int64 from #1003 on, Float64 before. Pinned
# Int64 here AND at the stage 04 month write, so the committed raw tree stays Int64.
DAYS_COLS = (
    "pitcher_days_since_prev_game",
    "batter_days_since_prev_game",
    "pitcher_days_until_next_game",
    "batter_days_until_next_game",
)
DTYPES.update(dict.fromkeys(DAYS_COLS, pl.Int64))
SORT = ["game_date", "game_pk", "at_bat_number", "pitch_number"]


def raw_months(root: pathlib.Path, season: int) -> "list[pathlib.Path]":
    return sorted((root / "mlb" / "statcast_raw" / str(season)).glob(f"{season}-??.parquet"))


def out_path(root: pathlib.Path, season: int) -> pathlib.Path:
    return root / "mlb" / "statcast" / f"mlb_statcast_{season}.parquet"


def pin_dtypes(df: pl.DataFrame, name: str, cols: "tuple[str, ...] | None" = None) -> pl.DataFrame:
    """Cast the known-drifting columns (or just ``cols``) to their pinned dtype, refusing lossy casts."""
    casts = []
    for c, want in DTYPES.items():
        if cols is not None and c not in cols:
            continue
        have = df.schema.get(c)
        if have is None or have == want:
            continue
        s = df.get_column(c)
        if want == pl.String and s.drop_nulls().len():
            # Never stringify a populated float id ("123.0").
            raise TypeError(f"{name}: {c} is {have} with values -- refusing float->str")
        expr = pl.col(c)
        if want == pl.Int64 and have.is_float():
            nn = s.fill_nan(None).drop_nulls()
            if (nn != nn.round(0)).any():
                raise TypeError(f"{name}: {c} has non-integral values -- refusing Int64 cast")
            expr = expr.fill_nan(None)  # NaN is missing, never a number
        casts.append(expr.cast(want, strict=True))
    return df.with_columns(casts) if casts else df


def compile_frame(frames: "list[tuple[str, pl.DataFrame]]", season: int) -> pl.DataFrame:
    """Pin, verify one dtype per column, concat, and stamp ``season``."""
    pinned = [(n, pin_dtypes(df, n)) for n, df in frames]
    seen: "dict[str, tuple[pl.DataType, str]]" = {}
    for n, df in pinned:
        for c, t in df.schema.items():
            t0, n0 = seen.setdefault(c, (t, n))
            if t0 != t:
                raise TypeError(f"{c}: {t0} in {n0} vs {t} in {n} -- pin it in DTYPES")
    df = pl.concat([d for _, d in pinned], how="diagonal_relaxed")
    if "game_year" in df.columns:
        stray = df.filter(pl.col("game_year") != season).height
        if stray:
            raise ValueError(f"{season}: {stray} rows carry another game_year -- misfiled month")
    return df.with_columns(pl.lit(season, dtype=pl.Int64).alias("season")).sort(SORT)


def compile_season(root: pathlib.Path, season: int) -> "tuple[pathlib.Path, int]":
    months = raw_months(root, season)
    if not months:
        raise FileNotFoundError(f"no mlb/statcast_raw/{season}/*.parquet -- run stage 04 first")
    df = compile_frame([(f.name, pl.read_parquet(f)) for f in months], season)
    out = out_path(root, season)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(f".{os.getpid()}.tmp")
    df.write_parquet(tmp)
    tmp.rename(out)  # atomic: a killed run never leaves a half-written season
    return out, df.height


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int)
    ap.add_argument("--start", type=int)
    ap.add_argument("--end", type=int)
    ap.add_argument("--root", default=".")
    a = ap.parse_args(argv)

    if a.season:
        seasons = [a.season]
    elif a.start and a.end:
        lo, hi = sorted((a.start, a.end))
        seasons = list(range(lo, hi + 1))
    else:
        ap.error("need --season, or --start and --end")

    root = pathlib.Path(a.root)
    rc = 0
    for s in seasons:
        if s < FLOOR:
            print(f"season {s}: before Statcast ({FLOOR}) -- skipping")
            continue
        try:
            out, rows = compile_season(root, s)
            print(f"statcast compile {s}: {rows} rows -> {out}", flush=True)
        except Exception as exc:  # noqa: BLE001 - one season must not kill the range
            print(f"statcast compile {s}: FAILED {type(exc).__name__}: {exc}", flush=True)
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
