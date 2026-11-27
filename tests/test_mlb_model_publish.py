"""Hermetic tests for the mlb model-publish builders.

Compute seams are stubbed -- these assert orchestration (three-format
tree writes, season ordering, empty-stem refusal, floor, merged cards,
per-file upload, the hitting history accumulation + bootstrap) -- not the
model math (gated in sdv-py's MLB oracle suites).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl
import pytest
from mlb_model_publish.artifacts import upload_artifacts
from mlb_model_publish.builders import MIN_SEASON, build_tag, write_card
from mlb_model_publish.cli import _seasons, main


def _fake_game_state(season: int) -> dict:
    return {
        "mlb_re24_matrix": pl.DataFrame({"base_state": ["___"], "season": [season]}),
        "mlb_we_table": pl.DataFrame({"base_state": ["___"], "season": [season]}),
        "mlb_wpa": pl.DataFrame({"game_id": [1, 2], "season": [season, season]}),
    }


def test_build_tag_writes_three_formats_per_stem_per_season(tmp_path):
    results = build_tag("mlb_game_state", [2016, 2015], tmp_path, compute=_fake_game_state)

    # processed ascending regardless of input order
    assert [r["season"] for r in results] == [2015, 2016]
    for season in (2015, 2016):
        for stem in ("mlb_re24_matrix", "mlb_we_table", "mlb_wpa"):
            # the family tree convention: parquet + csv + rds per stem-season
            for fmt in ("parquet", "csv", "rds"):
                assert (tmp_path / fmt / f"{stem}_{season}.{fmt}").exists(), (stem, fmt)
            frame = pl.read_parquet(tmp_path / "parquet" / f"{stem}_{season}.parquet")
            assert frame["season"].unique().to_list() == [season]
    assert results[0]["rows"] == 1  # primary stem = re24 matrix


def test_build_tag_refuses_an_empty_stem(tmp_path):
    def compute(season):
        out = _fake_game_state(season)
        out["mlb_wpa"] = out["mlb_wpa"].clear()
        return out

    with pytest.raises(ValueError, match="mlb_wpa"):
        build_tag("mlb_game_state", [2020], tmp_path, compute=compute)

    assert not (tmp_path / "parquet" / "mlb_wpa_2020.parquet").exists()


def test_build_tag_rejects_seasons_below_the_statcast_floor(tmp_path):
    with pytest.raises(ValueError, match=str(MIN_SEASON)):
        build_tag("mlb_game_state", [MIN_SEASON - 1], tmp_path, compute=_fake_game_state)


def test_card_carries_stem_rows_and_gate_anchors(tmp_path):
    results = build_tag("mlb_game_state", [2020], tmp_path, compute=_fake_game_state)
    path = write_card("mlb_game_state", results, tmp_path)

    card = json.loads(path.read_text(encoding="utf-8"))
    assert card["tag"] == "mlb_game_state"
    assert card["rows_by_season"]["2020"]["mlb_wpa"] == 2
    assert card["gate_anchors"]["re24_vs_tango_max_abs_diff"] == pytest.approx(0.05)


def test_card_merges_with_the_published_card(tmp_path):
    """A partial-range invocation (the daily current-season cron) must carry
    the already-published seasons forward -- this run's seasons win on
    collision. Three clobber incidents made this a rule."""
    results = build_tag("mlb_game_state", [2026], tmp_path, compute=_fake_game_state)
    existing = {
        "seasons": [2015, 2026],
        "rows_by_season": {
            "2015": {"mlb_re24_matrix": 24, "mlb_we_table": 5023, "mlb_wpa": 186878},
            "2026": {"mlb_re24_matrix": 24, "mlb_we_table": 1, "mlb_wpa": 1},  # stale
        },
    }
    path = write_card("mlb_game_state", results, tmp_path, existing=existing)

    card = json.loads(path.read_text(encoding="utf-8"))
    assert card["seasons"] == [2015, 2026]
    assert card["rows_by_season"]["2015"]["mlb_wpa"] == 186878  # carried forward
    assert card["rows_by_season"]["2026"]["mlb_wpa"] == 2  # this run wins


def test_hitting_history_accumulates_across_seasons(tmp_path, monkeypatch):
    """The projection stem appears from the SECOND season on, trained on the
    accumulated expected-stats history -- never a fresh Savant pull."""
    import mlb_model_publish.cli as cli
    import mlb_model_publish.computes as computes

    seen_history: dict[int, int | None] = {}

    def fake_hitting(season, *, cache_dir=None, history=None):
        seen_history[season] = None if history is None else history.height
        out = {
            "mlb_expected_stats": pl.DataFrame({"batter": [1, 2], "season": [season, season]}),
            "mlb_expected_hr": pl.DataFrame({"batter": [1], "season": [season]}),
        }
        if history is not None:
            out["mlb_batter_projection"] = pl.DataFrame({"batter": [1], "season": [season]})
        return out

    monkeypatch.setattr(computes, "compute_hitting", fake_hitting)
    # the accumulator age-joins each season's frame; stub it as identity so
    # the hermetic test stays offline (the join hits the statsapi people API)
    monkeypatch.setattr(computes, "age_join", lambda df: df)
    # no published assets to bootstrap from in this scenario (fresh backfill)
    monkeypatch.setattr(computes, "bootstrap_history", lambda season: None)
    monkeypatch.setattr(
        cli,
        "upload_artifacts",
        lambda *a, **k: pytest.fail("--build-only must not upload"),
    )

    rc = main(["hitting", "--seasons", "2015:2017", "--out", str(tmp_path), "--build-only"])

    assert rc == 0
    assert seen_history == {2015: None, 2016: 2, 2017: 4}
    assert not (tmp_path / "parquet" / "mlb_batter_projection_2015.parquet").exists()
    assert (tmp_path / "parquet" / "mlb_batter_projection_2016.parquet").exists()
    assert (tmp_path / "mlb_hitting_models_card.json").exists()


def test_single_season_run_bootstraps_history_from_published_assets(tmp_path, monkeypatch):
    """The daily current-season cron runs one season with an empty accumulator;
    without the bootstrap its projection stem would never publish."""
    import mlb_model_publish.cli as cli
    import mlb_model_publish.computes as computes

    seen_history: dict[int, int | None] = {}

    def fake_hitting(season, *, cache_dir=None, history=None):
        seen_history[season] = None if history is None else history.height
        out = {
            "mlb_expected_stats": pl.DataFrame({"batter": [1], "season": [season]}),
            "mlb_expected_hr": pl.DataFrame({"batter": [1], "season": [season]}),
        }
        if history is not None:
            out["mlb_batter_projection"] = pl.DataFrame({"batter": [1], "season": [season]})
        return out

    monkeypatch.setattr(computes, "compute_hitting", fake_hitting)
    monkeypatch.setattr(computes, "age_join", lambda df: df)
    monkeypatch.setattr(
        computes,
        "bootstrap_history",
        lambda season: pl.DataFrame({"batter": [1, 2, 3], "season": [season - 1] * 3}),
    )
    monkeypatch.setattr(
        cli,
        "upload_artifacts",
        lambda *a, **k: pytest.fail("--build-only must not upload"),
    )

    rc = main(["hitting", "--seasons", "2026", "--out", str(tmp_path), "--build-only"])

    assert rc == 0
    assert seen_history == {2026: 3}  # the bootstrapped frame reached the compute
    assert (tmp_path / "parquet" / "mlb_batter_projection_2026.parquet").exists()


def test_upload_pattern_reaches_format_subdirs_and_card(tmp_path):
    for fmt in ("parquet", "csv", "rds"):
        (tmp_path / fmt).mkdir()
        (tmp_path / fmt / f"mlb_re24_matrix_2020.{fmt}").write_bytes(b"x")
    (tmp_path / "mlb_game_state_card.json").write_text("{}")
    (tmp_path / "unrelated.txt").write_text("no")

    calls: list = []
    res = upload_artifacts(
        tmp_path,
        "mlb_game_state",
        "sportsdataverse/sportsdataverse-data",
        pattern="**/mlb_*.*",
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )

    names = sorted(p.rsplit("\\", 1)[-1].rsplit("/", 1)[-1] for p in res["files"])
    assert names == [
        "mlb_game_state_card.json",
        "mlb_re24_matrix_2020.csv",
        "mlb_re24_matrix_2020.parquet",
        "mlb_re24_matrix_2020.rds",
    ]
    assert res["uploaded"] == 4
    assert all("--clobber" in c for c in calls)


def test_seasons_parses_range_and_single():
    assert _seasons("2024") == [2024]
    assert _seasons("2015:2018") == [2015, 2016, 2017, 2018]


def test_game_state_publishes_leverage_index_with_bucket_counts(monkeypatch):
    """[79]/[80]: LI ships as its own stem, and both state-bucket tables carry
    `n` + a `thin` flag so a state backed by a handful of plate appearances is
    distinguishable from a well-estimated one (2020's short season was not)."""
    from mlb_model_publish import computes

    we = pl.DataFrame(
        {
            "inning_capped": [1, 9],
            "half": ["top", "bottom"],
            "base_state": ["___", "1_3"],
            "outs_start": [0, 2],
            "score_diff_bucket": [0, 1],
            "home_win_exp": [0.5, 0.8],
            "n": [500, 7],  # one well-estimated bucket, one thin
        }
    )
    li = we.drop("home_win_exp", "n").with_columns(pl.Series("leverage_index", [0.9, 3.4]))

    fake_pbp = pl.DataFrame(
        {
            "game_id": ["g1", "g1"],
            "at_bat_index": [0, 1],
            "result_home_score": [0, 3],
            "result_away_score": [0, 1],
            "matchup_batter_id": ["1", "2"],
            "runners": ["[]", "[]"],
        }
    )
    monkeypatch.setattr(computes, "load_season_pbp", lambda season, cache_dir=None: fake_pbp)
    # the stub LI has two buckets; the leverage gate and the WE pool have their own tests below
    monkeypatch.setattr(computes, "check_leverage_index", lambda li, season: None)
    monkeypatch.setattr(computes, "pooled_we_table", lambda season, we: we)
    # `import sportsdataverse.mlb.mlb_x as m` binds the FUNCTION, not the
    # module -- the package __init__ re-exports each function under its
    # module's own name and shadows it. sys.modules is the unambiguous handle.
    import sportsdataverse.mlb.mlb_run_expectancy  # noqa: F401 -- populate sys.modules
    import sportsdataverse.mlb.mlb_win_expectancy  # noqa: F401

    re_mod = sys.modules["sportsdataverse.mlb.mlb_run_expectancy"]
    we_mod = sys.modules["sportsdataverse.mlb.mlb_win_expectancy"]

    # one regulation PA and one extra half opening on 2nd, so the automatic-runner gate passes
    states = pl.DataFrame(
        {
            "game_id": ["g1", "g1"],
            "inning": [1, 10],
            "half": ["top", "top"],
            "at_bat_index": [0, 1],
            "base_state": ["___", "_2_"],
            "outs_start": [0, 0],
        }
    )
    monkeypatch.setattr(re_mod, "pbp_base_out_states", lambda pbp: states)
    monkeypatch.setattr(
        re_mod, "mlb_run_expectancy_matrix", lambda pbp=None: pl.DataFrame({"re": [0.5]})
    )
    monkeypatch.setattr(we_mod, "build_we_table", lambda s, r: we)
    monkeypatch.setattr(we_mod, "leverage_index", lambda s, t: li)
    monkeypatch.setattr(we_mod, "mlb_win_expectancy", lambda pbp, res: pl.DataFrame({"w": [1]}))
    monkeypatch.setattr(we_mod, "mlb_win_probability_added", lambda w: pl.DataFrame({"wpa": [0.1]}))

    out = computes.compute_game_state(2020)

    assert "mlb_leverage_index" in out
    for stem in ("mlb_we_table", "mlb_leverage_index"):
        assert "thin" in out[stem].columns and "n" in out[stem].columns, stem
        # the n=7 bucket is thin, the n=500 one is not -- a flag that marked
        # everything (or nothing) would pass a mere presence check
        assert out[stem]["thin"].to_list() == [False, True], stem


def test_fielding_direction_splits_sum_to_the_published_oaa(monkeypatch):
    """[76]: the in/back/lateral stem must PARTITION mlb_oaa. Re-fitting per
    direction (the tempting implementation) would not sum back."""
    from mlb_model_publish import computes

    plain = pl.DataFrame(
        {
            "fielder_id": ["10", "20"],
            "position": [8, 9],
            "opportunities": [30, 20],
            "oaa": [2.5, -1.5],
        }
    )
    split = pl.DataFrame(
        {
            "fielder_id": ["10", "10", "10", "20", "20"],
            "position": [8, 8, 8, 9, 9],
            "direction": ["in", "back", "lateral", "in", "back"],
            "opportunities": [10, 12, 8, 11, 9],
            "oaa": [1.0, 1.0, 0.5, -1.0, -0.5],
        }
    )
    monkeypatch.setattr(
        computes,
        "load_season_pitches",
        lambda season, cache_dir=None: pl.DataFrame({"type": ["X"]}),
    )
    import sportsdataverse.mlb.mlb_catcher_framing  # noqa: F401
    import sportsdataverse.mlb.mlb_fielding_oaa  # noqa: F401

    fr_mod = sys.modules["sportsdataverse.mlb.mlb_catcher_framing"]
    oaa_mod = sys.modules["sportsdataverse.mlb.mlb_fielding_oaa"]

    monkeypatch.setattr(
        oaa_mod,
        "mlb_fielding_oaa",
        lambda bip, by_direction=False: split if by_direction else plain,
    )
    monkeypatch.setattr(
        fr_mod, "mlb_catcher_framing", lambda p: pl.DataFrame({"catcher_id": ["1"]})
    )

    out = computes.compute_fielding(2024)

    assert "mlb_oaa_direction" in out
    rolled = (
        out["mlb_oaa_direction"]
        .group_by(["fielder_id", "position"])
        .agg(pl.col("oaa").sum().alias("s"), pl.col("opportunities").sum().alias("o"))
        .join(out["mlb_oaa"], on=["fielder_id", "position"], how="inner")
    )
    assert rolled.height == out["mlb_oaa"].height
    assert float((rolled["s"] - rolled["oaa"]).abs().max()) < 1e-9
    assert int((rolled["o"] - rolled["opportunities"]).abs().max()) == 0


def test_hitting_requires_the_observed_columns_rather_than_skipping_the_gate(monkeypatch):
    """The observed-column half of the scale gate must FAIL LOUDLY when the
    pinned sdv-py predates the columns -- a presence check that silently
    skipped would disarm exactly the gate the 2026-09-01 incident added."""
    from mlb_model_publish import computes

    stale = pl.DataFrame(
        {
            "batter": [1],
            "season": [2024],
            "pa": [500],
            "ab": [450],
            "xwoba": [0.32],
            "xba": [0.25],
            "xslg": [0.42],
        }
    )
    monkeypatch.setattr(
        computes,
        "load_season_pitches",
        lambda season, cache_dir=None: pl.DataFrame({"type": ["X"]}),
    )
    import sportsdataverse.mlb.mlb_expected_stats  # noqa: F401

    xs_mod = sys.modules["sportsdataverse.mlb.mlb_expected_stats"]

    monkeypatch.setattr(xs_mod, "mlb_expected_stats", lambda s, e, puller=None: stale)

    with pytest.raises(ValueError, match="observed columns"):
        computes.compute_hitting(2024)


# Real statsapi pbp (games 718735 and 718511, 2023): 6 extra half-innings, each opening
# with the automatic runner on 2nd. See tests/fixtures/mlb_game_state/README.md.
EXTRA_INNINGS_FIXTURE = (
    Path(__file__).parent / "fixtures" / "mlb_game_state" / "pbp_extra_innings_2023.parquet"
)


def _extra_innings_pbp() -> pl.DataFrame:
    # the live parse carries at_bat_index beside about_at_bat_index; the fixture kept one
    return pl.read_parquet(EXTRA_INNINGS_FIXTURE).with_columns(
        pl.col("about_at_bat_index").alias("at_bat_index")
    )


def test_automatic_runner_gate_passes_on_real_extra_innings():
    """The locked sdv-py seats the runner on the real fixture, so the gate passes."""
    from mlb_model_publish import computes
    from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

    pbp = _extra_innings_pbp()
    states = pbp_base_out_states(pbp)
    firsts = states.filter(
        (pl.col("inning") >= 10)
        & (pl.col("at_bat_index") == pl.col("at_bat_index").min().over("game_id", "inning", "half"))
    )
    assert firsts.height == 6
    assert firsts["base_state"].unique().to_list() == ["_2_"]
    computes.check_automatic_runner(pbp, states, 2023)


def test_automatic_runner_gate_refuses_pbp_without_the_runner_columns():
    from mlb_model_publish import computes
    from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

    pbp = _extra_innings_pbp().drop("matchup_batter_id", "runners")
    with pytest.raises(ValueError, match="lacks"):
        computes.check_automatic_runner(pbp, pbp_base_out_states(pbp), 2023)


def test_automatic_runner_gate_refuses_extra_halves_opened_empty():
    """The pre-seat behaviour (every half opens bases-empty) is what the 2026-10 build shipped."""
    from mlb_model_publish import computes
    from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

    pbp = _extra_innings_pbp()
    states = pbp_base_out_states(pbp)
    first = pl.col("at_bat_index") == pl.col("at_bat_index").min().over("game_id", "inning", "half")
    unseated = states.with_columns(
        pl.when(first).then(pl.lit("___")).otherwise("base_state").alias("base_state")
    )
    with pytest.raises(ValueError, match="6 of 6 extra half-innings"):
        computes.check_automatic_runner(pbp, unseated, computes.AUTOMATIC_RUNNER_FIRST_SEASON)
    # before the rule there is no runner to seat, so the same frame is accepted
    computes.check_automatic_runner(pbp, unseated, computes.AUTOMATIC_RUNNER_FIRST_SEASON - 1)


def test_game_state_build_seats_the_automatic_runner_end_to_end(monkeypatch):
    """compute_game_state on real pbp with the real sdv-py models. The 3 RE24-eligible extra
    half-openings (the tops of the 10th and 11th; bottoms of the 9th on are excluded) move from
    ___/0 to _2_/0, and the WE table changes only in its capped-9 (9th-and-later) buckets.
    With a lock older than the seat the gate raises instead."""
    from mlb_model_publish import computes
    from sportsdataverse.mlb.mlb_run_expectancy import (
        mlb_run_expectancy_matrix,
        pbp_base_out_states,
    )
    from sportsdataverse.mlb.mlb_win_expectancy import build_we_table

    pbp = _extra_innings_pbp()
    monkeypatch.setattr(computes, "load_season_pbp", lambda season, cache_dir=None: pbp)
    # two games are too few for the season-level leverage bands, and the WE pool needs the
    # published tables; both have their own tests below
    seen = []
    monkeypatch.setattr(computes, "check_leverage_index", lambda li, season: seen.append(season))
    monkeypatch.setattr(computes, "pooled_we_table", lambda season, we: we)
    out = computes.compute_game_state(2023)

    def n_at(m, base):
        hit = m.filter((pl.col("base_state") == base) & (pl.col("outs") == 0))["n"]
        return hit.item() if hit.len() else 0

    unseated = pbp.drop("matchup_batter_id", "runners")
    old_m = mlb_run_expectancy_matrix(pbp=unseated)
    new_m = out["mlb_re24_matrix"]
    assert n_at(new_m, "_2_") - n_at(old_m, "_2_") == 3
    assert n_at(old_m, "___") - n_at(new_m, "___") == 3

    res = (
        pbp.sort("game_id", "at_bat_index")
        .group_by("game_id", maintain_order=True)
        .agg(
            pl.col("result_home_score").last().alias("home_score"),
            pl.col("result_away_score").last().alias("away_score"),
        )
    )
    key = ["inning_capped", "half", "base_state", "outs_start", "score_diff_bucket"]
    old_we = build_we_table(pbp_base_out_states(unseated), res).select(*key, "home_win_exp", "n")
    new_we = out["mlb_we_table"].select(*key, "home_win_exp", "n")
    both = new_we.join(old_we, on=key, how="full", coalesce=True, suffix="_old")
    changed = both.filter(
        (pl.col("n") != pl.col("n_old")).fill_null(True)
        | (pl.col("home_win_exp") != pl.col("home_win_exp_old")).fill_null(True)
    )
    assert changed.height > 0
    assert changed["inning_capped"].unique().to_list() == [9]
    assert seen == [2023]  # the build ran the leverage gate on its own table

    # the build itself refuses the unseated input
    monkeypatch.setattr(computes, "load_season_pbp", lambda season, cache_dir=None: unseated)
    with pytest.raises(ValueError, match="lacks"):
        computes.compute_game_state(2023)


def test_automatic_runner_gate_refuses_a_completed_season_with_no_extra_innings():
    from mlb_model_publish import computes
    from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

    pbp = _extra_innings_pbp()
    regulation = pbp_base_out_states(pbp).filter(pl.col("inning") <= 9)
    with pytest.raises(ValueError, match="no extra half-innings"):
        computes.check_automatic_runner(pbp, regulation, 2023)


FIX = Path(__file__).parent / "fixtures" / "mlb_game_state"


def _li_2024_published() -> pl.DataFrame:
    # mlb_leverage_index_2024 as published 2026-11-26: every PA credited with the swing before it
    return pl.read_parquet(FIX / "leverage_index_2024_published_2026_11_26.parquet")


def test_leverage_gate_refuses_the_backward_swing_table():
    from mlb_model_publish import computes

    with pytest.raises(ValueError, match=r"first-PA leverage 0\.414 outside"):
        computes.check_leverage_index(_li_2024_published(), 2024)


def test_leverage_gate_passes_the_rebuilt_table():
    # forward swing, WE pooled over 2015-2026: first PA 0.931, bottom 9th tied 2.238 x first
    from mlb_model_publish import computes

    li = pl.read_parquet(FIX / "leverage_index_2024_rebuilt_2026_11_27.parquet")
    computes.check_leverage_index(li, 2024)


def test_leverage_gate_refuses_a_mean_off_one_and_a_missing_bucket():
    from mlb_model_publish import computes

    li = _li_2024_published()
    with pytest.raises(ValueError, match="PA-weighted mean"):
        computes.check_leverage_index(li.with_columns(pl.col("leverage_index") * 1.01), 2024)
    first = (
        (pl.col("inning_capped") == 1)
        & (pl.col("half") == "top")
        & (pl.col("base_state") == "___")
        & (pl.col("outs_start") == 0)
        & (pl.col("score_diff_bucket") == 0)
    )
    rest = li.filter(~first)
    # renormalized, so the missing bucket is the only thing wrong
    rest = rest.with_columns(
        pl.col("leverage_index") / ((rest["leverage_index"] * rest["n"]).sum() / rest["n"].sum())
    )
    with pytest.raises(ValueError, match="0 top 1 tied"):
        computes.check_leverage_index(rest, 2024)


def test_leverage_gate_skips_a_season_in_progress():
    from mlb_model_publish import computes

    # the same failing table is let through while its season is still being played
    computes.check_leverage_index(_li_2024_published(), computes.date.today().year + 1)


def _we(season: int) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"we_table_{season}.parquet")


def test_pooled_we_table_sums_the_seasons_win_counts(monkeypatch):
    from mlb_model_publish import computes

    monkeypatch.setattr(computes, "LI_POOL_FIRST_SEASON", 2023)
    monkeypatch.setattr(computes, "LI_POOL_LAST_SEASON", 2024)
    calls = []

    def fetch(s):
        calls.append(s)
        return _we(s)

    pooled = computes.pooled_we_table(2024, _we(2024), fetch=fetch)
    assert calls == [2023]  # the building season uses its own table, never its published copy
    key = ["inning_capped", "half", "base_state", "outs_start", "score_diff_bucket"]
    a, b = _we(2023), _we(2024)
    both = a.join(b, on=key, how="inner", suffix="_b").join(pooled, on=key, suffix="_p")
    assert both.height > 4000
    assert (both["n_p"] == both["n"] + both["n_b"]).all()
    wins = (both["home_win_exp"] * (both["n"] + 1) - 0.5) + (both["home_win_exp_b"] * (both["n_b"] + 1) - 0.5)
    assert ((wins + 0.5) / (both["n_p"] + 1) - both["home_win_exp_p"]).abs().max() < 1e-9
    # a bucket only one season reached keeps that season's count
    assert pooled.height == a.join(b, on=key, how="full", coalesce=True).height


def test_pooled_we_table_refuses_a_missing_season(monkeypatch):
    from mlb_model_publish import computes

    monkeypatch.setattr(computes, "LI_POOL_FIRST_SEASON", 2023)
    monkeypatch.setattr(computes, "LI_POOL_LAST_SEASON", 2024)

    def fetch(s):
        raise OSError(f"HTTP 404 for {s}")

    with pytest.raises(OSError, match="2023"):
        computes.pooled_we_table(2024, _we(2024), fetch=fetch)
