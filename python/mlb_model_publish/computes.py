"""Real per-tag compute wiring over the sdv-py MLB surface.

Each ``compute_*`` takes a season and returns ``{stem: DataFrame}``. The three
Savant-sourced tags (hitting / fielding / pitching) all read ONE shared
per-season pitch pull cached as parquet (:func:`load_season_pitches`) -- a
full-season Savant pull is ~55 minutes / ~700K rows, so it is paid once per
season, never once per model. The cache is also the backfill's resume
checkpoint. ``mlb_game_state`` is statsapi-sourced and independent of the
cache.

The builders inject these; hermetic tests stub them.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def _cache_dir() -> Path:
    return Path(os.environ.get("SDV_MLB_STATCAST_CACHE", ".mlb_statcast_cache"))


def load_season_pitches(season: int, *, cache_dir=None) -> pl.DataFrame:
    """One full-season Savant pitch pull, cached as parquet.

    Uses ``pull_statcast_season`` (the helper the hitting models were fit
    under) so the published tables see the same data conventions as their
    oracle gates.
    """
    cache = Path(cache_dir) if cache_dir else _cache_dir()
    cache.mkdir(parents=True, exist_ok=True)
    f = cache / f"statcast_{season}.parquet"
    if f.exists():
        return pl.read_parquet(f)
    from sportsdataverse.mlb.mlb_hitting_constants import pull_statcast_season

    pitches = pull_statcast_season(season)
    assert isinstance(pitches, pl.DataFrame)
    if pitches.height > 0:
        pitches.write_parquet(f)
    return pitches


def compute_game_state(season: int) -> dict[str, pl.DataFrame]:
    """RE24 matrix + WE state table + per-play WE/WPA from statsapi pbp.

    Pulls the season's regular-season pbp ONCE (pacing via
    ``SDV_MLB_STATSAPI_SLEEP``, default 0.2s/game) and derives everything from
    it -- game results come from each game's terminal ``result_*_score``, so
    no second network surface is touched.
    """
    from sportsdataverse.mlb.mlb_api_extra import mlb_schedule
    from sportsdataverse.mlb.mlb_game_state_constants import collect_statsapi_pbp
    from sportsdataverse.mlb.mlb_run_expectancy import (
        mlb_run_expectancy_matrix,
        pbp_base_out_states,
    )
    from sportsdataverse.mlb.mlb_win_expectancy import (
        build_we_table,
        mlb_win_expectancy,
        mlb_win_probability_added,
    )

    raw = mlb_schedule(season=season, game_type="R")
    pks = [
        int(game["gamePk"])
        for date_entry in (raw.get("dates") or [])
        for game in (date_entry.get("games") or [])
        if (game.get("status") or {}).get("codedGameState") == "F"
    ]
    sleep = float(os.environ.get("SDV_MLB_STATSAPI_SLEEP", "0.2"))
    pbp = collect_statsapi_pbp(pks, sleep=sleep)
    if pbp.height == 0:
        return {"mlb_re24_matrix": pl.DataFrame()}

    results = (
        pbp.sort("game_id", "at_bat_index")
        .group_by("game_id", maintain_order=True)
        .agg(
            pl.col("result_home_score").last().alias("home_score"),
            pl.col("result_away_score").last().alias("away_score"),
        )
    )
    states = pbp_base_out_states(pbp)
    we_table = build_we_table(states, results)
    wpa = mlb_win_probability_added(mlb_win_expectancy(pbp, results))
    matrix = mlb_run_expectancy_matrix(pbp=pbp)
    season_col = pl.lit(season, dtype=pl.Int64).alias("season")
    return {
        "mlb_re24_matrix": matrix.with_columns(season_col),
        "mlb_we_table": we_table.with_columns(season_col),
        "mlb_wpa": wpa.with_columns(season_col),
    }


def compute_hitting(season: int, *, cache_dir=None, history: pl.DataFrame | None = None) -> dict[str, pl.DataFrame]:
    """Expected stats + xHR + (history permitting) the batter projection.

    ``history`` is the concatenated ``mlb_expected_stats`` output of PRIOR
    seasons (the builder accumulates it season-ascending) so the projection
    never re-pulls Savant for its training window.
    """
    from sportsdataverse.mlb.mlb_batter_projection import mlb_batter_projection
    from sportsdataverse.mlb.mlb_expected_home_runs import mlb_expected_home_runs
    from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats

    pitches = load_season_pitches(season, cache_dir=cache_dir)
    if pitches.height == 0:
        return {"mlb_expected_stats": pl.DataFrame()}

    puller = lambda start, end, **kw: pitches  # noqa: E731 -- injection seam
    start, end = f"{season}-01-01", f"{season}-12-01"
    xstats = mlb_expected_stats(start, end, puller=puller).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    xhr = mlb_expected_home_runs(start, end, puller=puller).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    out = {"mlb_expected_stats": xstats, "mlb_expected_hr": xhr}
    if history is not None and history.height > 0:
        out["mlb_batter_projection"] = mlb_batter_projection(season, history=history)
    return out


def compute_fielding(season: int, *, cache_dir=None) -> dict[str, pl.DataFrame]:
    """OAA per (fielder, position) + catcher framing, off the shared cache.

    Mirrors the live full-season oracle's prep exactly
    (``tests/mlb/test_mlb_fielding_oracle_live.py``): balls in play are
    ``type == "X"`` rows; framing takes the raw pitch frame.
    """
    from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
    from sportsdataverse.mlb.mlb_fielding_oaa import mlb_fielding_oaa

    pitches = load_season_pitches(season, cache_dir=cache_dir)
    if pitches.height == 0:
        return {"mlb_oaa": pl.DataFrame()}

    season_col = pl.lit(season, dtype=pl.Int64).alias("season")
    oaa = mlb_fielding_oaa(pitches.filter(pl.col("type") == "X")).with_columns(season_col)
    framing = mlb_catcher_framing(pitches).with_columns(season_col)
    return {"mlb_oaa": oaa, "mlb_catcher_framing": framing}


def compute_pitching(season: int, *, cache_dir=None) -> dict[str, pl.DataFrame]:
    """xERA + arsenal Stuff+ + pitcher Command+ off the shared cache.

    The substrate (``pitch_features`` -> ``add_sequence_features``) passes the
    raw Savant columns through, so xERA's inputs survive the feature step.
    SIERA-like (unfitted coefficients) and tunneling (no public oracle) are
    deliberately not built -- see the model card.
    """
    from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
    from sportsdataverse.mlb.mlb_pitch_era import x_era
    from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features
    from sportsdataverse.mlb.mlb_stuff_plus import mlb_stuff_plus

    pitches = load_season_pitches(season, cache_dir=cache_dir)
    if pitches.height == 0:
        return {"mlb_xera": pl.DataFrame()}

    feats = add_sequence_features(pitch_features(pitches))
    season_col = pl.lit(season, dtype=pl.Int64).alias("season")
    return {
        "mlb_xera": x_era(feats, season).with_columns(season_col),
        "mlb_stuff_plus": mlb_stuff_plus(feats, level="arsenal").with_columns(season_col),
        "mlb_command_plus": mlb_command_plus(feats, level="pitcher").with_columns(season_col),
    }
