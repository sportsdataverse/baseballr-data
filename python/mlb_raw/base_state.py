"""Adapt this repo's tidy pbp columns to sdv-py's RE24 vocabulary.

There is no base-state RECONSTRUCTION here, and that is the point. statsapi
ships post-state occupancy on the play itself -- ``matchup.postOnFirst``,
``postOnSecond``, ``postOnThird`` -- present from 1988, the corpus floor. Stage
03 extracts those as ``post_on_{first,second,third}_id``; this module only
renames them, because sdv-py's ``mlb_run_expectancy_matrix`` reads
``matchup_post_on_first_id`` / ``game_id`` / ``about_*`` / ``result_*`` while
this repo's tidy corpus uses ``post_on_first_id`` / ``game_pk`` / ``inning``.

An earlier version of this file DERIVED occupancy by walking runner movements
across each half-inning. It was both unnecessary and not fixable to the required
tolerance: statsapi's own field resolves plays a movement walk cannot (pinch
runners, appeals, obstruction), and the walk peaked at max |RE24 - published| =
0.378 against a 0.05 gate. Reading the fact instead gives 0.0016.

The scope filter matters too: the published mlb_game_state card is REGULAR
SEASON, so ``regular_season_only`` trims postseason before the matrix is built.
Without it the totals disagree by ~3,000 plate appearances.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import polars as pl

#: sdv-py's mlb_run_expectancy_matrix contract (mlb_run_expectancy.py:61-63).
RENAMES = {
    "game_pk": "game_id",
    "at_bat_index": "about_at_bat_index",
    "inning": "about_inning",
    "half_inning": "about_half_inning",
    "away_score": "result_away_score",
    "home_score": "result_home_score",
    "outs": "count_outs",
    "post_on_first_id": "matchup_post_on_first_id",
    "post_on_second_id": "matchup_post_on_second_id",
    "post_on_third_id": "matchup_post_on_third_id",
}
SDV_PY_COLUMNS = list(RENAMES.values())


def to_sdv_py(plays: pl.DataFrame) -> pl.DataFrame:
    """Rename the tidy columns to what sdv-py's RE24 reads. No derivation."""
    missing = [c for c in RENAMES if c not in plays.columns]
    if missing:
        raise KeyError(
            f"pbp frame is missing {missing} -- reparse with stage 03 "
            "(post_on_*_id were added when RE24 support landed)"
        )
    return plays.rename(RENAMES).select(SDV_PY_COLUMNS)


def regular_season_only(plays: pl.DataFrame, root: pathlib.Path, season: int) -> pl.DataFrame:
    """Trim to gameType R, the scope the published mlb_game_state card states."""
    sched = root / "mlb" / "schedule" / f"{season}.parquet"
    if not sched.exists():
        return plays
    types = pl.read_parquet(sched).select(["game_pk", "game_type"]).unique()
    return plays.join(types, on="game_pk").filter(pl.col("game_type") == "R").drop("game_type")


def build_season(root: pathlib.Path, season: int, *, regular_only: bool = True) -> pl.DataFrame:
    plays = pl.read_parquet(root / "mlb" / "pbp" / f"mlb_pbp_{season}.parquet")
    if regular_only:
        plays = regular_season_only(plays, root, season)
    return to_sdv_py(plays)


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--all-game-types", action="store_true", help="include postseason")
    ap.add_argument("--out", help="optional parquet path to write")
    a = ap.parse_args(argv)

    df = build_season(pathlib.Path(a.root), a.season, regular_only=not a.all_game_types)
    occ = df.select(
        [pl.col(c).is_not_null().sum().alias(c.split("_on_")[1]) for c in SDV_PY_COLUMNS[-3:]]
    ).to_dicts()[0]
    print(f"season {a.season}: {df.height:,} plate appearances")
    print(f"  runners on base after each PA: {occ}")
    if a.out:
        df.write_parquet(a.out)
        print(f"  wrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
