"""Derive base-out state from runner movements, and adapt to sdv-py's contract.

WHY THIS IS NOT IN THE PARSER: reconstructing base occupancy is an
interpretation, not a fact statsapi ships. Baking it into stage 03 would freeze
one reading of ambiguous movements into 91k committed bundles; here it can be
corrected and re-run in ~a minute from raw that never has to be re-fetched.

THE ALGORITHM, and why carry-forward is mandatory rather than a convenience:
statsapi lists a runner only when the runner MOVES. Measured on 2024, just 7 of
249,601 runner rows have ``start_base == end_base``. So the union of
``start_base`` is NOT the pre-state and the union of ``end_base`` is NOT the
post-state -- a runner standing still on second is simply absent, and any
derivation that reads only the rows for one plate appearance loses them.

Occupancy is therefore carried across plate appearances within a half-inning:

    for each PA in order:
        pre  = occupancy
        vacate every start_base named in this PA's rows
        occupy end_base for every runner not put out and not scoring
        post = occupancy

A batter has ``start_base = None`` (nothing to vacate) and ``end_base = "1B"``
on a single. A runner put out has ``is_out`` true and a null ``end_base`` --
verified: 0 of 249,601 rows are out WITH an end base. ``end_base == "score"``
leaves the bases entirely, so it vacates without occupying.

sdv-py's ``mlb_run_expectancy_matrix`` wants a different column vocabulary from
this repo's tidy one (``game_id`` not ``game_pk``, ``about_*``/``result_*``
prefixes), so the adapter renames as well as derives. Both halves live here so
there is one place to look when RE24 disagrees with the corpus.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import polars as pl

BASES = ("1B", "2B", "3B")

#: Exactly what sdv-py's mlb_run_expectancy_matrix documents as required
#: (mlb_run_expectancy.py:61-63). Note it wants POST-state RUNNER IDS, not
#: booleans and not pre-state: it derives occupancy with a null check and the
#: pre-state by shifting post within the half-inning.
SDV_PY_COLUMNS = [
    "game_id",
    "about_inning",
    "about_half_inning",
    "about_at_bat_index",
    "count_outs",
    "result_home_score",
    "result_away_score",
    "matchup_post_on_first_id",
    "matchup_post_on_second_id",
    "matchup_post_on_third_id",
]


def derive_base_state(plays: pl.DataFrame, runners: pl.DataFrame) -> pl.DataFrame:
    """Add matchup_post_on_{first,second,third}_id to a plays frame.

    Post-state RUNNER IDS, because that is what sdv-py's RE24 consumes -- it
    derives occupancy from a null check and the PRE-state by shifting post
    within the half-inning, so emitting pre here would be redundant and a second
    place for the two to disagree.
    """
    # Index runner rows by (game, at-bat) once -- a per-PA filter over 250k rows
    # would be O(n*m) and take minutes on a full season.
    moves: dict[tuple, list] = {}
    for gp, abi, rid, sb, eb, out in runners.select(
        ["game_pk", "at_bat_index", "runner_id", "start_base", "end_base", "is_out"]
    ).iter_rows():
        moves.setdefault((gp, abi), []).append((rid, sb, eb, out))

    ordered = plays.sort(["game_pk", "inning", "half_inning", "at_bat_index"])
    post_cols: "list[list[int | None]]" = [[], [], []]

    occ: "dict[str, int | None]" = {}
    key_prev = None
    for gp, inning, half, abi in ordered.select(
        ["game_pk", "inning", "half_inning", "at_bat_index"]
    ).iter_rows():
        key = (gp, inning, half)
        if key != key_prev:  # a new half-inning always starts with empty bases
            occ = {}
            key_prev = key

        # TWO PASSES, and the order is the whole correctness argument. Doing
        # pop-then-set per row lets a later row's vacate erase an earlier row's
        # arrival: on a single that sends the runner 1B->3B, if statsapi lists
        # the BATTER first (None->1B) and the runner second (1B->3B), the
        # runner's pop of "1B" removes the batter who just arrived there. The
        # state reads __3 instead of 1_3.
        #
        # Measured cost of getting this wrong on 2024: 1_3/0-outs came out 428
        # against a published 874, 123/0-outs 376 against 660, with the missing
        # plate appearances piling up in bases-empty -- and RE24 for __3/0-outs
        # read 1.77 against a canonical ~1.42.
        rows = moves.get((gp, abi), ())
        for _rid, sb, _eb, _out in rows:
            if sb in BASES:
                occ.pop(sb, None)  # everyone leaves first
        for rid, _sb, eb, is_out in rows:
            if not is_out and eb in BASES:  # "score" and None both leave the bases
                occ[eb] = rid

        for i, b in enumerate(BASES):
            post_cols[i].append(occ.get(b))

    names = ("matchup_post_on_first_id", "matchup_post_on_second_id", "matchup_post_on_third_id")
    return ordered.with_columns(
        [pl.Series(n, post_cols[i], dtype=pl.Int64) for i, n in enumerate(names)]
    )


def to_sdv_py(plays_with_state: pl.DataFrame) -> pl.DataFrame:
    """Rename this repo's tidy columns to the vocabulary sdv-py's RE24 reads."""
    return plays_with_state.rename(
        {
            "game_pk": "game_id",
            "at_bat_index": "about_at_bat_index",
            "inning": "about_inning",
            "half_inning": "about_half_inning",
            "away_score": "result_away_score",
            "home_score": "result_home_score",
            "outs": "count_outs",
        }
    ).select(SDV_PY_COLUMNS)


def build_season(root: pathlib.Path, season: int) -> pl.DataFrame:
    d = root / "mlb" / "pbp"
    plays = pl.read_parquet(d / f"mlb_pbp_{season}.parquet")
    runners = pl.read_parquet(d / f"mlb_runners_{season}.parquet")
    return to_sdv_py(derive_base_state(plays, runners))


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--out", help="optional parquet path to write")
    a = ap.parse_args(argv)

    df = build_season(pathlib.Path(a.root), a.season)
    occ = df.select(
        [
            pl.col(c)
            .is_not_null()
            .sum()
            .alias(c.replace("matchup_post_on_", "").replace("_id", ""))
            for c in (
                "matchup_post_on_first_id",
                "matchup_post_on_second_id",
                "matchup_post_on_third_id",
            )
        ]
    ).to_dicts()[0]
    print(f"season {a.season}: {df.height:,} plate appearances")
    print(f"  runners on base AFTER each PA: {occ}")
    if a.out:
        df.write_parquet(a.out)
        print(f"  wrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
