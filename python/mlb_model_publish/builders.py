"""Build the four MLB model-dataset tags for the sportsdataverse-data release.

Thin orchestration over :mod:`mlb_model_publish.computes`, mirroring the
cfb/pwhl/mbb `*_model_publish` builders. Each tag publishes MULTIPLE stems per
season (e.g. ``mlb_hitting_models`` carries expected-stats + xHR + projection
parquet) under ONE release tag -- the nfl_model_artifacts precedent.

The compute seams are injectable for hermetic tests; the real wiring lives in
``computes.py``.
"""

from __future__ import annotations

import json
from pathlib import Path

# Statcast era. game_state could go deeper on statsapi, but a uniform floor
# keeps the four tags' spans aligned (plan Phase 4).
MIN_SEASON = 2015

#: primary stem per tag -- the one whose emptiness means "season not built".
_PRIMARY = {
    "mlb_game_state": "mlb_re24_matrix",
    "mlb_hitting_models": "mlb_expected_stats",
    "mlb_fielding_models": "mlb_oaa",
    "mlb_pitching_models": "mlb_xera",
}


#: the committed data tree at the repo root -- absolute, so a CLI run from
#: python/ can never silently write a python/mlb_models/ shadow tree (the
#: wbb_data_build --base lesson). Layout: mlb_models/{tag}/{format}/{stem}_{season}.{ext}
REPO_TREE = Path(__file__).resolve().parents[2] / "mlb"


def _write_stem(df, out_dir: Path, stem: str, season: int) -> list[str]:
    """Write one stem-season in the family's three formats (parquet+csv+rds).

    The rds is written natively (sdv-py ``write_rds``) with baseballr's S3
    class chain + attribute pair so R consumers get the family print method;
    unsigned columns are cast to Int64 for the rds only (R has no unsigned).
    """
    from datetime import datetime, timezone

    import polars as pl
    from sportsdataverse._rds import write_rds

    paths: list[str] = []
    for fmt in ("parquet", "csv", "rds"):
        d = out_dir / fmt
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{stem}_{season}.{fmt}"
        if fmt == "parquet":
            df.write_parquet(path)
        elif fmt == "csv":
            df.write_csv(path)
        else:
            rds_df = df.with_columns(
                pl.col(c).cast(pl.Int64)
                for c, t in df.schema.items()
                if t in (pl.UInt32, pl.UInt64, pl.UInt16, pl.UInt8)
            )
            stamped = datetime.now(timezone.utc)
            write_rds(
                rds_df,
                path,
                cls=["baseballr_data", "tbl_df", "tbl", "data.table", "data.frame"],
                attributes={
                    "baseballr_timestamp": stamped,
                    "baseballr_type": f"MLB {stem} data",
                },
            )
        paths.append(str(path))
    return paths


def build_tag(
    tag: str,
    seasons: list[int],
    out_dir,
    *,
    compute,
) -> list[dict]:
    """Shared season loop: compute -> refuse-empty -> write one parquet per stem.

    Args:
        tag: Release tag (keys :data:`_PRIMARY`).
        seasons: Seasons to build, processed ascending (the hitting builder's
            projection-history accumulation depends on this).
        out_dir: Output directory (created if absent).
        compute: ``compute(season) -> {stem: DataFrame}``.

    Returns:
        List of ``{"season", "rows" (primary stem), "stems": {stem: rows},
        "paths"}`` dicts, in season order.

    Raises:
        ValueError: If a season is below :data:`MIN_SEASON`, or any returned
            stem is empty (publishing a silently-empty asset is refused).
    """
    too_old = [s for s in seasons if s < MIN_SEASON]
    if too_old:
        raise ValueError(f"{tag}: seasons {too_old} predate the {MIN_SEASON} Statcast floor")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    for season in sorted(seasons):
        frames = compute(season)
        empty = [stem for stem, df in frames.items() if df.height == 0]
        if empty:
            raise ValueError(
                f"{tag}: season {season} produced 0 rows for {empty} -- refusing to publish an empty asset"
            )
        stems: dict[str, int] = {}
        paths: list[str] = []
        for stem, df in frames.items():
            paths.extend(_write_stem(df, out_dir, stem, season))
            stems[stem] = df.height
        primary = _PRIMARY[tag]
        results.append(
            {
                "season": season,
                "rows": stems.get(primary, 0),
                "stems": stems,
                "paths": paths,
            }
        )
        print(f"{tag}: season={season} " + " ".join(f"{k}={v}" for k, v in stems.items()))
    return results


_CARD_META = {
    "mlb_game_state": {
        "grain": "re24_matrix: one row per base-out state; we_table + leverage_index: one row per state bucket; wpa: one row per plate appearance",
        "source": "sdv-py mlb_run_expectancy / mlb_win_expectancy over statsapi.mlb.com regular-season pbp",
        "gates": {
            "re24_vs_tango_max_abs_diff": 0.05,
            "wpa_spearman_vs_statsapi_wp": 0.95,
            "per_game_wpa_sum_identity_tol": 0.02,
        },
        "notes": [
            "Game results derive from each game's terminal result_*_score --"
            " one statsapi pull per season, no second surface.",
            "Leverage index IS published (mlb_leverage_index): E|delta WE| over"
            " the empirical next-state distribution from each state, normalized"
            " so the PA-weighted mean is 1.0 (abs_wpa is averaged over plate"
            " appearances, not over states).",
            "we_table and leverage_index carry the bucket count `n` and a `thin`"
            " flag (n < 50). The threshold is measured, not chosen: pooled"
            " adjacent-season disagreement in the WE estimate is .0586 mean"
            " |dWE| for buckets of 25-50 against .0312 at 100-200 and .0296 at"
            " 200+ -- roughly a doubling as bucket size falls through 50."
            " 2020's short season is 93.1% thin buckets (median n=5) against"
            " ~81.5% (median n=11) in a full season.",
        ],
    },
    "mlb_hitting_models": {
        "grain": "one row per batter-season (expected_stats, expected_hr); one row per batter per target season (batter_projection)",
        "source": "sdv-py mlb_expected_stats / mlb_expected_home_runs / mlb_batter_projection over Baseball Savant",
        "gates": {
            "xwoba_spearman_same_input": 0.95,
            "xba_spearman_same_input": 0.95,
            "xhr_full_season_spearman_live": 0.90,
            "qualified_league_mean_xwoba_band": [0.26, 0.38],
            "qualified_league_mean_xba_band": [0.21, 0.29],
            "qualified_league_mean_expected_vs_observed_max_gap": 0.02,
        },
        "notes": [
            "The projection for season S trains only on seasons < S (as-of"
            " enforced) and uses the accumulated expected-stats history, so"
            " the backfill pays no extra Savant pulls.",
            "The earliest built season carries no projection stem (no prior history inside the run).",
            "Observed `woba`/`ba` ship beside the expected columns on the SAME"
            " denominators, so a luck-vs-skill delta is `xwoba - woba` with no"
            " second source. The scale gates are ABSOLUTE bands (the Spearman"
            " gates above are rank-based and therefore scale-blind -- that is"
            " how the 2026-09-01 mis-scaled seasons shipped).",
        ],
    },
    "mlb_fielding_models": {
        "grain": "oaa: one row per (fielder_id, position, season); oaa_direction: one row per (fielder_id, position, direction, season); catcher_framing: one row per catcher-season",
        "source": "sdv-py mlb_fielding_oaa / mlb_catcher_framing over Baseball Savant (balls in play = type=='X')",
        "gates": {
            "oaa_full_season_pearson_live": 0.55,
            "framing_full_season_pearson_live": 0.40,
            # REGISTRY.md lists the partition as a publish gate, so the card must
            # declare its tolerance; test_fielding_direction_splits_sum_to_the_
            # published_oaa enforces it (observed max drift 3.6e-15 on 2021).
            "oaa_direction_partition_max_abs_diff": 1e-9,
        },
        "notes": [
            "Observed full-season 2024: OAA 0.605, framing 0.468. Ceilings are"
            " feature-capped -- the public per-pitch feed lacks fielder start"
            " coordinates and receiving data.",
            "mlb_oaa_direction splits each fielder-position into in/back/lateral."
            " Savant splits directional OAA against tracked fielder START"
            " coordinates, which the public feed lacks, so the position's own"
            " median landing spot stands in (documented approximation). The"
            " split re-groups the same scored balls in play rather than"
            " re-fitting, so its rows sum exactly to mlb_oaa.",
            "Catcher throwing/blocking, baserunning and SB value are EXCLUDED:"
            " data-ceiling-limited (live floors 0.03-0.073 vs 0.80+ design"
            " targets; only ~23% of SB/CS attempts are narrated in this feed).",
        ],
    },
    "mlb_pitching_models": {
        "grain": "xera: one row per qualifying pitcher-season; stuff_plus: one row per (pitcher, pitch_type, season); command_plus: one row per pitcher-season",
        "source": "sdv-py x_era / mlb_stuff_plus / mlb_command_plus over the pitch_features substrate (Baseball Savant)",
        "gates": {
            "xera_mae_vs_savant_xera": 0.30,
            "stuff_plus_spearman_vs_run_value": 0.20,
            "command_plus_spearman_vs_run_value": 0.04,
        },
        "notes": [
            "Command+ carries a DIRECTIONAL gate only (0.04) -- treat it as a"
            " weak ordinal signal, not a calibrated scale.",
            "SIERA-like is not published (coefficients are unfitted literature"
            " placeholders); pitch tunneling / sequence run value are not"
            " published (no public oracle to gate against).",
        ],
    },
}


def write_card(tag: str, results: list[dict], out_dir, *, existing: dict | None = None) -> Path:
    """Write the tag's model card next to the season parquet.

    Args:
        existing: The currently-published card (fetched best-effort by the
            CLI) -- its per-season rows are carried forward so the card always
            reflects the full published span, not just this invocation's.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = _CARD_META[tag]
    # Merge with the previously-published card so a partial-range invocation
    # (the daily current-season cron, a range extension) never clobbers the
    # other seasons' record. This run's seasons win on collision.
    by_season: dict[int, dict] = {}
    if existing:
        for s, stems in (existing.get("rows_by_season") or {}).items():
            by_season[int(s)] = stems
    for r in results:
        by_season[int(r["season"])] = r["stems"]
    card = {
        "tag": tag,
        "grain": meta["grain"],
        "source": meta["source"],
        "seasons": sorted(by_season),
        "rows_by_season": {str(s): by_season[s] for s in sorted(by_season)},
        "gate_anchors": meta["gates"],
        "notes": meta["notes"],
    }
    path = out_dir / f"{tag}_card.json"
    path.write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")
    print(f"card: {path} (seasons {card['seasons'][0]}..{card['seasons'][-1]})")
    return path
