"""Stage 06 -- NCAA <-> ESPN game crosswalk.

Writes ``ncaa/xwalk/espn_game_id/{season}.json`` -- one row per NCAA contest:
``{contest_id, espn_game_id, match_method}``. Match engine modeled on
ncaa-mfb-football-raw's stage 06 (itself a port of sdv-py
``scrape.ncaa.espn_game_xwalk``): NCAA baseball has no NCAA<->ESPN team
crosswalk, so games match on **(date, score)** with a team-name disambiguator:

1. ``score_exact``      -- ``(game_date, home_score, away_score)``.
2. ``score_window``     -- same key, ESPN date shifted +/-1 day (UTC vs ET).
3. ``score_pair``       -- exact date, unordered score pair (neutral-site /
   orientation flips).
4. ``score_pair_names`` -- the score pair PLUS the normalized unordered
   team-name pair, splitting keys tiers 1-3 dropped as ambiguous (e.g. two
   games that date with the same score). Doubleheader twins with identical
   scores stay ambiguous -> NULL, never a guess.

Every tier drops keys resolving to more than one game on EITHER side; an ESPN
game id claimed by two contests is voided on both; unmatched keeps NULL.

NCAA side: the stage-01/05 schedule master (date ``MM/DD/YYYY(N)``, ``@``
opponent prefix = own team away, team/opponent scores from the W/L result).
ESPN side: ``espn_college_baseball_scoreboard(dates=YYYYMMDD)`` swept over the
master's game dates (+/-1 day, bounded Feb 1 - Jul 1); each day's raw payload is
cached to ``ncaa/xwalk/espn_scoreboard/{season}/{YYYYMMDD}.json`` so re-runs
are fully offline.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Callable, List, Optional

import polars as pl

from ncaa_pbp.datasets import master_parquet_path
from ncaa_pbp.schedules import REPO_ROOT

#: Tier labels, in the order they are attempted.
MATCH_METHODS = ("score_exact", "score_window", "score_pair", "score_pair_names")

SWEEP_START = (2, 1)  # Feb 1
SWEEP_END = (7, 1)  # Jul 1

FetchDayFn = Callable[[str], dict]  # (YYYYMMDD) -> raw scoreboard payload


def xwalk_path(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "xwalk" / "espn_game_id" / f"{season}.json"


def scoreboard_cache_dir(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "xwalk" / "espn_scoreboard" / str(season)


def _norm_name(column: str) -> pl.Expr:
    """Team-name key: lowercase letters only (drops ranks '#3', '@', punctuation)."""
    return pl.col(column).str.to_lowercase().str.replace_all(r"[^a-z]", "")


def ncaa_side(root: "str | Path", season: int) -> pl.DataFrame:
    """One row per contest: date, oriented scores + names from the schedule master.

    ``@``-prefixed opponent = the row's own team was AWAY; both of a contest's
    two schedule rows describe the same game -- prefer the copy with both scores.
    """
    schema = {
        "contest_id": pl.Utf8,
        "game_date": pl.Date,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
        "home_name": pl.Utf8,
        "away_name": pl.Utf8,
    }
    path = master_parquet_path(root, season)
    if not path.is_file():
        return pl.DataFrame(schema=schema)
    raw = pl.read_parquet(path)
    is_away = pl.col("opponent").str.strip_chars().str.starts_with("@")
    opp_name = pl.col("opponent").str.strip_chars().str.strip_prefix("@").str.strip_chars()
    frame = raw.select(
        pl.col("contest_id").cast(pl.Utf8),
        pl.col("date").str.replace(r"\s*\(\d+\)\s*$", "").str.to_date("%m/%d/%Y", strict=False).alias("game_date"),
        pl.when(is_away)
        .then(pl.col("opponent_score"))
        .otherwise(pl.col("team_score"))
        .cast(pl.Int64)
        .alias("home_score"),
        pl.when(is_away)
        .then(pl.col("team_score"))
        .otherwise(pl.col("opponent_score"))
        .cast(pl.Int64)
        .alias("away_score"),
        pl.when(is_away).then(opp_name).otherwise(pl.col("team_name")).alias("home_name"),
        pl.when(is_away).then(pl.col("team_name")).otherwise(opp_name).alias("away_name"),
    )
    return (
        frame.filter(pl.col("contest_id").is_not_null())
        .sort(pl.col("home_score").is_null() | pl.col("away_score").is_null())
        .unique(subset=["contest_id"], keep="first", maintain_order=True)
        .sort("contest_id")
    )


def _live_fetch_day(yyyymmdd: str) -> dict:
    from sportsdataverse.baseball import espn_college_baseball_scoreboard

    return espn_college_baseball_scoreboard(dates=yyyymmdd, return_parsed=False)


def fetch_scoreboard_day(
    root: "str | Path", season: int, yyyymmdd: str, fetch_day: "Optional[FetchDayFn]" = None
) -> dict:
    """Cache-through read of one day's raw scoreboard payload."""
    cache = scoreboard_cache_dir(root, season) / f"{yyyymmdd}.json"
    if cache.is_file():
        return json.loads(cache.read_text(encoding="utf-8"))
    payload = (fetch_day or _live_fetch_day)(yyyymmdd)
    cache.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(cache)
    return payload


def sweep_dates(ncaa: pl.DataFrame, season: int) -> "List[str]":
    """The master's game dates +/-1 day, bounded to the Feb 1 - Jul 1 window."""
    lo, hi = dt.date(season, *SWEEP_START), dt.date(season, *SWEEP_END)
    days: "set[dt.date]" = set()
    for d in ncaa.get_column("game_date").drop_nulls().unique().to_list():
        for off in (-1, 0, 1):
            day = d + dt.timedelta(days=off)
            if lo <= day <= hi:
                days.add(day)
    return [d.strftime("%Y%m%d") for d in sorted(days)]


def _event_rows(payload: dict) -> "list[dict]":
    rows = []
    for ev in payload.get("events") or []:
        comps = ev.get("competitions") or [{}]
        sides: "dict[str, dict]" = {}
        for c in comps[0].get("competitors") or []:
            team = c.get("team") or {}
            score = c.get("score")
            sides[c.get("homeAway", "")] = {
                "id": str(team.get("id")) if team.get("id") is not None else None,
                "name": team.get("location") or team.get("displayName"),
                "score": int(score) if isinstance(score, str) and score.lstrip("-").isdigit() else None,
            }
        home, away = sides.get("home", {}), sides.get("away", {})
        rows.append(
            {
                "espn_game_id": str(ev.get("id")) if ev.get("id") is not None else None,
                "game_date": (ev.get("date") or "")[:10] or None,
                "espn_home_id": home.get("id"),
                "espn_away_id": away.get("id"),
                "home_name": home.get("name"),
                "away_name": away.get("name"),
                "home_score": home.get("score"),
                "away_score": away.get("score"),
            }
        )
    return rows


def espn_side(
    root: "str | Path",
    season: int,
    dates: "Optional[List[str]]" = None,
    *,
    ncaa: "Optional[pl.DataFrame]" = None,
    fetch_day: "Optional[FetchDayFn]" = None,
) -> pl.DataFrame:
    """Date-swept ESPN scoreboard games (cache-through; one API call per uncached day)."""
    schema = {
        "espn_game_id": pl.Utf8,
        "game_date": pl.Date,
        "espn_home_id": pl.Utf8,
        "espn_away_id": pl.Utf8,
        "home_name": pl.Utf8,
        "away_name": pl.Utf8,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
    }
    if dates is None:
        dates = sweep_dates(ncaa if ncaa is not None else ncaa_side(root, season), season)
    rows: "list[dict]" = []
    for yyyymmdd in dates:
        rows.extend(_event_rows(fetch_scoreboard_day(root, season, yyyymmdd, fetch_day)))
    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(rows, schema_overrides={k: pl.Utf8 for k in schema if k not in ("home_score", "away_score")})
    return (
        frame.select(
            pl.col("espn_game_id").cast(pl.Utf8),
            pl.col("game_date").cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False),
            pl.col("espn_home_id").cast(pl.Utf8),
            pl.col("espn_away_id").cast(pl.Utf8),
            pl.col("home_name").cast(pl.Utf8),
            pl.col("away_name").cast(pl.Utf8),
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
        )
        .drop_nulls(["espn_game_id", "game_date", "home_score", "away_score"])
        .unique(subset=["espn_game_id"], keep="first")
    )


def _unambiguous(frame: pl.DataFrame, keys: "List[str]") -> pl.DataFrame:
    """``keys -> espn_game_id``, keeping only keys resolving to exactly one game."""
    return (
        frame.group_by(keys)
        .agg(
            pl.col("espn_game_id").n_unique().alias("_candidates"),
            pl.col("espn_game_id").first().alias("espn_game_id"),
        )
        .filter(pl.col("_candidates") == 1)
        .drop("_candidates")
    )


def _apply_tier(pending: pl.DataFrame, lookup: pl.DataFrame, keys: "List[str]", method: str):
    # NCAA-side keys must also be unambiguous: two contests sharing a key would
    # both grab the same ESPN game -- the collision voider would then kill a
    # pair a later tier could have split correctly.
    all_keyed = pl.all_horizontal(pl.col(k).is_not_null() for k in keys)
    counted = pending.with_columns(pl.len().over(keys).alias("_n"))
    joinable = counted.filter((pl.col("_n") == 1) & all_keyed).drop("_n")
    rest = counted.filter((pl.col("_n") > 1) | ~all_keyed).drop("_n")
    joined = joinable.join(lookup, on=keys, how="left")
    matched = joined.filter(pl.col("espn_game_id").is_not_null()).with_columns(
        pl.lit(method, dtype=pl.Utf8).alias("match_method")
    )
    still = pl.concat([joined.filter(pl.col("espn_game_id").is_null()).drop("espn_game_id"), rest])
    return matched, still


def build_season_xwalk(root: "str | Path", season: int, espn: "Optional[pl.DataFrame]" = None) -> pl.DataFrame:
    """``contest_id / espn_game_id / match_method`` -- one row per contest, none dropped."""
    out_schema = {"contest_id": pl.Utf8, "espn_game_id": pl.Utf8, "match_method": pl.Utf8}
    ncaa = ncaa_side(root, season)
    if ncaa.height == 0:
        return pl.DataFrame(schema=out_schema)
    espn = espn if espn is not None else espn_side(root, season, ncaa=ncaa)
    if espn.height == 0:
        return ncaa.select(
            "contest_id",
            pl.lit(None, dtype=pl.Utf8).alias("espn_game_id"),
            pl.lit(None, dtype=pl.Utf8).alias("match_method"),
        )

    windowed = pl.concat([espn.with_columns(pl.col("game_date") + pl.duration(days=d)) for d in (-1, 1)])
    score_lo = pl.min_horizontal("home_score", "away_score").alias("score_lo")
    score_hi = pl.max_horizontal("home_score", "away_score").alias("score_hi")
    nh, na = _norm_name("home_name"), _norm_name("away_name")
    name_lo = pl.when(nh <= na).then(nh).otherwise(na).alias("name_lo")
    name_hi = pl.when(nh <= na).then(na).otherwise(nh).alias("name_hi")
    score_keys = ["game_date", "home_score", "away_score"]
    pair_keys = ["game_date", "score_lo", "score_hi"]
    name_keys = ["game_date", "score_lo", "score_hi", "name_lo", "name_hi"]

    pending = ncaa
    matched: "List[pl.DataFrame]" = []

    def run_tier(method: str, keys: "List[str]", lookup: pl.DataFrame, extra=()):
        nonlocal pending
        if pending.height == 0:
            return
        prepared = pending.with_columns(*extra) if extra else pending
        hit, pending_new = _apply_tier(prepared, _unambiguous(lookup, keys), keys, method)
        if hit.height:
            matched.append(hit.select("contest_id", "espn_game_id", "match_method"))
        pending = pending_new.select(ncaa.columns)

    run_tier("score_exact", score_keys, espn)
    run_tier("score_window", score_keys, windowed)
    run_tier("score_pair", pair_keys, espn.with_columns(score_lo, score_hi), (score_lo, score_hi))
    run_tier(
        "score_pair_names",
        name_keys,
        espn.with_columns(score_lo, score_hi, name_lo, name_hi),
        (score_lo, score_hi, name_lo, name_hi),
    )

    unmatched = pending.select(
        "contest_id",
        pl.lit(None, dtype=pl.Utf8).alias("espn_game_id"),
        pl.lit(None, dtype=pl.Utf8).alias("match_method"),
    )
    result = pl.concat([*matched, unmatched]) if matched else unmatched

    # One ESPN game belongs to one contest; a collision voids both claimants.
    contested = (
        result.drop_nulls("espn_game_id")
        .group_by("espn_game_id")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
        .get_column("espn_game_id")
        .to_list()
    )
    if contested:
        clash = pl.col("espn_game_id").is_in(contested)
        result = result.with_columns(
            pl.when(clash).then(None).otherwise(pl.col("espn_game_id")).alias("espn_game_id"),
            pl.when(clash).then(None).otherwise(pl.col("match_method")).alias("match_method"),
        )
    return result.sort("contest_id")


def write_season_xwalk(root: "str | Path", season: int, frame: pl.DataFrame) -> Path:
    path = xwalk_path(root, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(frame.to_dicts()), encoding="utf-8")
    tmp.replace(path)
    return path


def load_espn_game_index(root: "str | Path", season: int) -> "dict[str, str]":
    """``{contest_id: espn_game_id}`` for one season -- pure offline read."""
    path = xwalk_path(root, season)
    if not path.is_file():
        return {}
    rows = json.loads(path.read_text(encoding="utf-8"))
    return {r["contest_id"]: r["espn_game_id"] for r in rows if r.get("espn_game_id")}


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True, help="calendar year (2026 = spring 2026)")
    ap.add_argument("--root", default=str(REPO_ROOT))
    args = ap.parse_args(argv)
    root = Path(args.root)

    frame = build_season_xwalk(root, args.season)
    path = write_season_xwalk(root, args.season, frame)
    n = frame.height
    hit = frame.get_column("espn_game_id").is_not_null().sum() if n else 0
    by = frame.drop_nulls("match_method").group_by("match_method").agg(pl.len()).to_dicts() if n else []
    print(f"[xwalk] {args.season}: {hit}/{n} matched  {by} -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
