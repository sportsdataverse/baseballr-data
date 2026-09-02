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


def _pbp_cache_dir() -> Path:
    return Path(os.environ.get("SDV_MLB_STATSAPI_CACHE", ".mlb_statsapi_cache"))


def load_season_pbp(season: int, *, cache_dir=None) -> pl.DataFrame:
    """One regular-season statsapi pbp pull (final games only), cached as parquet.

    The game_state analogue of :func:`load_season_pitches`: a season is
    ~2,430 games at ``SDV_MLB_STATSAPI_SLEEP`` pacing (default 0.2s/game), so
    the pull is paid once and the parquet doubles as the backfill's resume
    checkpoint. Same staleness contract as the Savant cache: delete an
    in-progress season's file to pick up new games.
    """
    cache = Path(cache_dir) if cache_dir else _pbp_cache_dir()
    cache.mkdir(parents=True, exist_ok=True)
    f = cache / f"statsapi_pbp_{season}.parquet"
    if f.exists():
        return pl.read_parquet(f)
    from sportsdataverse.mlb.mlb_api_extra import mlb_schedule
    from sportsdataverse.mlb.mlb_game_state_constants import collect_statsapi_pbp

    raw = mlb_schedule(season=season, game_type="R")
    pks = [
        int(game["gamePk"])
        for date_entry in (raw.get("dates") or [])
        for game in (date_entry.get("games") or [])
        if (game.get("status") or {}).get("codedGameState") == "F"
    ]
    sleep = float(os.environ.get("SDV_MLB_STATSAPI_SLEEP", "0.2"))
    pbp = collect_statsapi_pbp(pks, sleep=sleep)
    if pbp.height > 0:
        pbp.write_parquet(f)
    return pbp


#: state-bucket key shared by the WE table and the leverage-index table.
_WE_KEY = ["inning_capped", "half", "base_state", "outs_start", "score_diff_bucket"]

#: A state bucket with fewer than this many observations is flagged ``thin``.
#: DERIVED FROM OBSERVATION, not chosen: comparing each bucket's WE estimate
#: against the same bucket in the next season (every adjacent pair of full
#: seasons 2015-2026, 2020 excluded as the anomaly under test), mean |dWE| by
#: bucket size is .1260 (n<10), .0823 (10-25), .0586 (25-50), .0429 (50-100),
#: .0312 (100-200), .0296 (200+). Disagreement roughly doubles from the
#: large-bucket floor as size falls through 50, so that is where an estimate
#: stops being reusable across seasons. 2020's short season is the motivating
#: case: median bucket n=5 against 11 in every full season, and 93.1% of its
#: buckets thin against ~81.5% normally. The docs/models/game_state.qmd
#: writeup recomputes both tables on every render.
THIN_BUCKET_N = 50


def compute_game_state(season: int, *, cache_dir=None) -> dict[str, pl.DataFrame]:
    """RE24 matrix + WE state table + leverage index + per-play WE/WPA.

    Pulls the season's regular-season pbp ONCE (:func:`load_season_pbp`) and
    derives everything from it -- game results come from each game's terminal
    ``result_*_score``, so no second network surface is touched.

    The WE and leverage tables both carry the bucket count ``n`` and a
    ``thin`` flag (``n < THIN_BUCKET_N``) so a consumer can tell a
    well-estimated state from one backed by a handful of plate appearances --
    2020's short season was previously indistinguishable from a full one.
    """
    from sportsdataverse.mlb.mlb_run_expectancy import (
        mlb_run_expectancy_matrix,
        pbp_base_out_states,
    )
    from sportsdataverse.mlb.mlb_win_expectancy import (
        build_we_table,
        leverage_index,
        mlb_win_expectancy,
        mlb_win_probability_added,
    )

    pbp = load_season_pbp(season, cache_dir=cache_dir)
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

    thin = (pl.col("n") < THIN_BUCKET_N).alias("thin")
    # Leverage index is per state bucket on the same key as the WE table, so
    # the bucket count + thin flag carry straight over -- a high-LI state
    # backed by 6 plate appearances must be readable as such.
    li = leverage_index(states, we_table).join(
        we_table.select(*_WE_KEY, "n"), on=_WE_KEY, how="left"
    )

    season_col = pl.lit(season, dtype=pl.Int64).alias("season")
    return {
        "mlb_re24_matrix": matrix.with_columns(season_col),
        "mlb_we_table": we_table.with_columns(thin, season_col),
        "mlb_leverage_index": li.with_columns(thin, season_col),
        "mlb_wpa": wpa.with_columns(season_col),
    }


_RELEASE_DL = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download"


def bootstrap_history(season: int) -> pl.DataFrame | None:
    """Age-joined expected-stats history for ``season`` from the PUBLISHED assets.

    A single-season invocation (the daily current-season cron) has no
    in-process history accumulator, so without this the projection stem for
    the current season would never publish. Downloads the prior three
    seasons' ``mlb_expected_stats_{s}.parquet`` best-effort (missing seasons
    skip) and age-joins them -- no Savant pulls.
    """
    import io
    import urllib.request

    frames: list[pl.DataFrame] = []
    for s in (season - 3, season - 2, season - 1):
        url = f"{_RELEASE_DL}/mlb_hitting_models/mlb_expected_stats_{s}.parquet"
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                frames.append(pl.read_parquet(io.BytesIO(r.read())))
        except Exception:  # noqa: BLE001 -- missing season = skip, not fail
            continue
    if not frames:
        return None
    return age_join(pl.concat(frames, how="diagonal_relaxed"))


def compute_hitting(
    season: int, *, cache_dir=None, history: pl.DataFrame | None = None
) -> dict[str, pl.DataFrame]:
    """Expected stats + xHR + (history permitting) the batter projection.

    ``history`` is the AGE-JOINED concatenated ``mlb_expected_stats`` output of
    PRIOR seasons (the builder accumulates it season-ascending through
    :func:`age_join`) so the projection never re-pulls Savant for its training
    window. ``mlb_batter_projection``'s documented history contract requires
    the ``age`` column ("ages must already be present on a supplied history
    frame").
    """
    from sportsdataverse.mlb.mlb_batter_projection import mlb_batter_projection
    from sportsdataverse.mlb.mlb_expected_home_runs import mlb_expected_home_runs
    from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats

    pitches = load_season_pitches(season, cache_dir=cache_dir)
    if pitches.height == 0:
        return {"mlb_expected_stats": pl.DataFrame()}

    puller = lambda start, end, **kw: pitches  # noqa: E731 -- injection seam
    start, end = f"{season}-01-01", f"{season}-12-01"
    xstats = mlb_expected_stats(start, end, puller=puller).with_columns(
        pl.lit(season, dtype=pl.Int64).alias("season")
    )
    # Publish-blocking SCALE gate (2026-09-01 incident): the rank-based oracle
    # gates are scale-blind, and mis-scaled seasons shipped with league-mean
    # "xwOBA" of .44-.73. An absolute band on the qualified league mean makes
    # that class un-shippable. Never widen a band to pass -- debug the build.
    #
    # The observed columns are REQUIRED, not optional: a missing-column skip
    # would silently disarm the half of this gate that catches an expected
    # stat drifting away from the outcome it is supposed to estimate.
    missing = {"woba", "ba"} - set(xstats.columns)
    if missing:
        raise ValueError(
            f"mlb_expected_stats is missing the observed columns {sorted(missing)} -- "
            "this builder requires an sdv-py carrying the observed wOBA/BA columns; "
            "re-lock with `uv lock --upgrade-package sportsdataverse`"
        )
    qualified = xstats.filter(pl.col("pa") >= 100)
    if qualified.height >= 50:
        mean_xwoba = float(qualified["xwoba"].mean())
        mean_xba = float(qualified["xba"].mean())
        mean_woba = float(qualified["woba"].mean())
        mean_ba = float(qualified["ba"].mean())
        assert 0.26 <= mean_xwoba <= 0.38, (
            f"xwoba scale out of band for {season}: qualified league mean {mean_xwoba:.4f} "
            "(plausible band .26-.38) -- refusing to publish a mis-scaled season"
        )
        # Floor re-derived 2026-09-02 after the untracked-batted-ball fix
        # (sdv-py): xBA previously ran .2026-.2229 because balls in play with
        # no launch data counted in `ab` with a zero numerator. Rebuilt on the
        # real 2015-2021 seasons it is .2400-.2532 against an observed BA of
        # .2410-.2575, so the band tightens from .18-.30 to .21-.29 -- a
        # re-derivation against a newly-measured quantity, not a loosening.
        assert 0.21 <= mean_xba <= 0.29, (
            f"xba scale out of band for {season}: qualified league mean {mean_xba:.4f} "
            "(plausible band .21-.29) -- refusing to publish a mis-scaled season"
        )
        # The observed columns must land on real MLB levels too -- they come
        # from the same denominators, so a denominator bug moves both.
        assert 0.26 <= mean_woba <= 0.38, (
            f"observed woba out of band for {season}: qualified league mean {mean_woba:.4f}"
        )
        assert 0.21 <= mean_ba <= 0.29, (
            f"observed ba out of band for {season}: qualified league mean {mean_ba:.4f}"
        )
        # An expected stat that diverges wildly from its own observed
        # counterpart is the scale-drift signature the rank gates cannot see.
        assert abs(mean_xwoba - mean_woba) <= 0.02, (
            f"xwoba/woba divergence for {season}: {mean_xwoba:.4f} vs {mean_woba:.4f} "
            "(observed max gap .0012 across 2015-2021)"
        )
        assert abs(mean_xba - mean_ba) <= 0.02, (
            f"xba/ba divergence for {season}: {mean_xba:.4f} vs {mean_ba:.4f} "
            "(observed max gap .0024, in 2020, across 2015-2021)"
        )
    xhr = mlb_expected_home_runs(start, end, puller=puller).with_columns(
        pl.lit(season, dtype=pl.Int64).alias("season")
    )
    out = {"mlb_expected_stats": xstats, "mlb_expected_hr": xhr}
    if history is not None and history.height > 0:
        out["mlb_batter_projection"] = mlb_batter_projection(season, history=history)
    return out


#: batter id -> ISO birthDate string, cached for the process (a backfill sees
#: mostly the same ~1-3K batters across seasons; the bulk people endpoint
#: resolves ~100 ids per call, so the whole backfill costs a few dozen calls).
_BIRTHDATES: dict[int, str] = {}


def _resolve_birthdates(batter_ids: list[int]) -> None:
    from sportsdataverse.mlb.mlb_api import mlb_people

    missing = sorted({int(b) for b in batter_ids} - set(_BIRTHDATES))
    for i in range(0, len(missing), 100):
        chunk = missing[i : i + 100]
        people = mlb_people(person_ids=chunk)
        if people is None or people.height == 0:
            continue
        bd_col = next(
            (c for c in people.columns if c.lower().replace("_", "") == "birthdate"),
            None,
        )
        if bd_col is None:
            continue
        for pid, bd in zip(people["id"].to_list(), people[bd_col].to_list()):
            if bd:
                _BIRTHDATES[int(pid)] = str(bd)


def age_join(xstats: pl.DataFrame) -> pl.DataFrame:
    """Join MLB seasonal age (age on June 30 of ``season``) onto expected stats.

    Batters whose birthdate cannot be resolved get a null ``age`` and are
    dropped -- the aging curve cannot place them, and silently guessing an age
    would corrupt the year-over-year deltas.
    """
    if xstats.height == 0:
        return xstats
    _resolve_birthdates(xstats["batter"].to_list())
    bd = pl.DataFrame(
        {
            "batter": list(_BIRTHDATES.keys()),
            "_birth_date": list(_BIRTHDATES.values()),
        }
    ).with_columns(pl.col("batter").cast(pl.Int64), pl.col("_birth_date").str.to_date())
    return (
        xstats.with_columns(pl.col("batter").cast(pl.Int64))
        .join(bd, on="batter", how="left")
        .with_columns(
            (
                pl.col("season")
                - pl.col("_birth_date").dt.year()
                # seasonal age = age on June 30: born after the cutoff -> one year younger
                - (
                    (pl.col("_birth_date").dt.month() > 6)
                    | (
                        (pl.col("_birth_date").dt.month() == 6)
                        & (pl.col("_birth_date").dt.day() > 30)
                    )
                ).cast(pl.Int64)
            ).alias("age")
        )
        .drop("_birth_date")
        .drop_nulls(subset=["age"])
    )


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
    bip = pitches.filter(pl.col("type") == "X")
    oaa = mlb_fielding_oaa(bip).with_columns(season_col)
    # Additive per-direction split of the SAME scored balls in play (sdv-py
    # re-groups rather than re-fitting), so the in/back/lateral rows sum
    # exactly to mlb_oaa -- asserted in tests/test_mlb_model_publish.py.
    oaa_dir = mlb_fielding_oaa(bip, by_direction=True).with_columns(season_col)
    framing = mlb_catcher_framing(pitches).with_columns(season_col)
    return {
        "mlb_oaa": oaa,
        "mlb_oaa_direction": oaa_dir,
        "mlb_catcher_framing": framing,
    }


def compute_pitching(season: int, *, cache_dir=None) -> dict[str, pl.DataFrame]:
    """xERA + arsenal Stuff+ + pitcher Command+ off the shared cache.

    The substrate (``pitch_features`` -> ``add_sequence_features``) passes the
    raw Savant columns through, so xERA's inputs survive the feature step.
    SIERA-like (unfitted coefficients) and tunneling (no public oracle) are
    deliberately not built -- see the model card.
    """
    from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
    from sportsdataverse.mlb.mlb_pitch_era import x_era
    from sportsdataverse.mlb.mlb_pitch_features import (
        add_sequence_features,
        pitch_features,
    )
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
