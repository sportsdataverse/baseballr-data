"""Stage 06 -- NCAA <-> ESPN game crosswalk.

Writes ``ncaa/xwalk/espn_game_id/{season}.json`` -- one row per NCAA contest:
``{contest_id, espn_game_id, match_method}``. Same contract, keys and tiers as the
basketball crosswalk (sdv-py ``scrape.ncaa.espn_game_xwalk``, bound per league in
ncaa-{mbb,wbb}-hoops-raw stage 06): games match on **ESPN team ids**, never on
score alone. Both sides carry them:

* NCAA side -- the stage-01/05 schedule master (date ``MM/DD/YYYY(N)``, ``@``
  opponent prefix = own team away, scores from the W/L result). Its ``team_id`` /
  ``opponent_id`` are stats.ncaa.org SEASON team ids, mapped to ESPN college-baseball
  team ids through the published ``ncaa_baseball_crosswalk`` release asset
  ``ncaa_baseball_espn_team_crosswalk.parquet`` (``ncaa_season_team_id`` ->
  ``espn_team_id``; D-I only, so a non-D-I opponent has no ESPN id).
* ESPN side -- ``espn_college_baseball_scoreboard(dates=YYYYMMDD)`` swept over the
  master's game dates (+/-1 day, bounded Feb 1 - Jul 1); each day's raw payload is
  cached to ``ncaa/xwalk/espn_scoreboard/{season}/{YYYYMMDD}.json`` so re-runs
  never re-hit ESPN.

Tiers, tried in order and recorded in ``match_method``:

1. ``exact``                -- ``(game_date, home_espn_id, away_espn_id)``.
2. ``date_window``          -- same key + the score, ESPN date shifted +/-1 day (a
   game played after midnight, or a listing a day off). Score-confirmed because a
   Fri-Sun series puts the same pair on adjacent dates.
3. ``unordered_pair``       -- exact date, unordered team pair (neutral sites).
4. ``unordered_pair_score`` -- the pair PLUS the unordered score pair. Baseball only:
   a doubleheader puts the same pair twice on one date, so tiers 1-3 drop it as
   ambiguous; the scores split the two games. Identical-score twins stay NULL.
5. ``single_team``          -- date + the ONE ESPN id a contest has (non-D-I
   opponent) + the score pair, only when that team has one such ESPN game that day.

Score-only keys (the previous engine) are gone: the master covers D-I to D-III
(~22.7k contests in 2026) against ~5.5k ESPN games, so a common score on a busy
date matched a D-III game to a D-I broadcast (2026: 84% of its +/-1-day tier).

Every tier drops keys resolving to more than one game on EITHER side; an ESPN
game id claimed by two contests is voided on both; unmatched keeps NULL. Ids are
``Utf8`` (cast through ``Int64`` where a source is numeric, so never ``"123.0"``).
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import urllib.request
from pathlib import Path
from typing import Callable, List, Optional

import polars as pl

from ncaa_pbp.datasets import master_parquet_path
from ncaa_pbp.schedules import REPO_ROOT

#: Tier labels, in the order they are attempted.
MATCH_METHODS = ("exact", "date_window", "unordered_pair", "unordered_pair_score", "single_team")

#: NCAA season team id -> ESPN college-baseball team id (sdv-reference-data, J8).
TEAM_XWALK_URL = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    "ncaa_baseball_crosswalk/ncaa_baseball_espn_team_crosswalk.parquet"
)

SWEEP_START = (2, 1)  # Feb 1
SWEEP_END = (7, 1)  # Jul 1

FetchDayFn = Callable[[str], dict]  # (YYYYMMDD) -> raw scoreboard payload


def xwalk_path(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "xwalk" / "espn_game_id" / f"{season}.json"


def scoreboard_cache_dir(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "xwalk" / "espn_scoreboard" / str(season)


def fetch_team_xwalk(url: str = TEAM_XWALK_URL) -> pl.DataFrame:
    """The published NCAA <-> ESPN baseball team crosswalk (all seasons). Raises on failure."""
    with urllib.request.urlopen(url, timeout=120) as resp:
        return pl.read_parquet(io.BytesIO(resp.read()))


def team_map(team_xwalk: pl.DataFrame, season: int) -> pl.DataFrame:
    """``ncaa_season_team_id -> espn_team_id`` (both ``Utf8``) for one season.

    Raises when the season has no rows: matching against an empty map would publish
    an all-NULL crosswalk that looks built (the 2027 KenPom failure mode, hoopR-mbb-data #48).
    """
    out = (
        team_xwalk.filter(pl.col("season") == season)
        .select(
            pl.col("ncaa_season_team_id").cast(pl.Int64).cast(pl.Utf8),
            pl.col("espn_team_id").cast(pl.Utf8),
        )
        .drop_nulls()
        .unique(subset=["ncaa_season_team_id"], keep="first")
    )
    if out.height == 0:
        raise ValueError(f"ncaa_baseball_espn_team_crosswalk has no rows for season {season}")
    return out


def ncaa_side(root: "str | Path", season: int, teams: pl.DataFrame) -> pl.DataFrame:
    """One row per contest: date, oriented ESPN team ids + scores from the schedule master.

    ``@``-prefixed opponent = the row's own team was AWAY; both of a contest's two
    schedule rows describe the same game -- prefer the copy with both scores.
    """
    schema = {
        "contest_id": pl.Utf8,
        "game_date": pl.Date,
        "home_espn_id": pl.Utf8,
        "away_espn_id": pl.Utf8,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
    }
    path = master_parquet_path(root, season)
    if not path.is_file():
        return pl.DataFrame(schema=schema)
    raw = pl.read_parquet(path)
    is_away = pl.col("opponent").str.strip_chars().str.starts_with("@")
    # Through Int64 so a numeric-typed master can never yield "614563.0".
    team_id, opp_id = (
        pl.col(c).cast(pl.Int64, strict=False).cast(pl.Utf8) for c in ("team_id", "opponent_id")
    )
    frame = raw.select(
        pl.col("contest_id").cast(pl.Utf8),
        pl.col("date")
        .str.replace(r"\s*\(\d+\)\s*$", "")
        .str.to_date("%m/%d/%Y", strict=False)
        .alias("game_date"),
        pl.when(is_away).then(opp_id).otherwise(team_id).alias("home_ncaa_id"),
        pl.when(is_away).then(team_id).otherwise(opp_id).alias("away_ncaa_id"),
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
    )
    lookup = teams.select("ncaa_season_team_id", "espn_team_id")
    for side in ("home", "away"):
        key = f"{side}_ncaa_id"
        assert frame.schema[key] == lookup.schema["ncaa_season_team_id"], "id dtype drift"
        frame = frame.join(
            lookup.rename({"ncaa_season_team_id": key, "espn_team_id": f"{side}_espn_id"}),
            on=key,
            how="left",
        )
    return (
        frame.filter(pl.col("contest_id").is_not_null())
        .sort(pl.col("home_score").is_null() | pl.col("away_score").is_null())
        .unique(subset=["contest_id"], keep="first", maintain_order=True)
        .select(list(schema))
        .sort("contest_id")
    )


def _live_fetch_day(yyyymmdd: str) -> dict:
    # The wrapper lives in the league subpackage; `sportsdataverse.baseball` itself
    # does not re-export it. Resolved at call time so tests can patch it there.
    from sportsdataverse.baseball import college_baseball

    return college_baseball.espn_college_baseball_scoreboard(dates=yyyymmdd, return_parsed=False)


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
                "score": int(score)
                if isinstance(score, str) and score.lstrip("-").isdigit()
                else None,
            }
        home, away = sides.get("home", {}), sides.get("away", {})
        rows.append(
            {
                "espn_game_id": str(ev.get("id")) if ev.get("id") is not None else None,
                "game_date": ev.get("date") or None,  # UTC, e.g. 2026-03-07T00:05Z
                "home_espn_id": home.get("id"),
                "away_espn_id": away.get("id"),
                "home_score": home.get("score"),
                "away_score": away.get("score"),
            }
        )
    return rows


def espn_side(
    root: "str | Path",
    season: int,
    dates: "List[str]",
    *,
    fetch_day: "Optional[FetchDayFn]" = None,
) -> pl.DataFrame:
    """Date-swept ESPN scoreboard games (cache-through; one API call per uncached day)."""
    schema = {
        "espn_game_id": pl.Utf8,
        "game_date": pl.Date,
        "home_espn_id": pl.Utf8,
        "away_espn_id": pl.Utf8,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
    }
    rows: "list[dict]" = []
    for yyyymmdd in dates:
        rows.extend(_event_rows(fetch_scoreboard_day(root, season, yyyymmdd, fetch_day)))
    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(
        rows, schema_overrides={k: pl.Utf8 for k in schema if k not in ("home_score", "away_score")}
    )
    return (
        frame.select(
            pl.col("espn_game_id").cast(pl.Utf8),
            # ESPN stamps UTC; NCAA dates are local. A 7pm ET first pitch is the next
            # UTC day, and in a Fri-Sun series the +/-1 window is then ambiguous, so
            # take the US/Eastern calendar date (a 7pm PT start is still same-day ET).
            # Any other stamp shape falls back to its date part rather than dropping the game.
            pl.coalesce(
                pl.col("game_date")
                .str.to_datetime("%Y-%m-%dT%H:%MZ", time_zone="UTC", strict=False)
                .dt.convert_time_zone("America/New_York")
                .dt.date(),
                pl.col("game_date").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False),
            ).alias("game_date"),
            pl.col("home_espn_id").cast(pl.Utf8),
            pl.col("away_espn_id").cast(pl.Utf8),
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
        )
        .drop_nulls(["espn_game_id", "game_date", "home_espn_id", "away_espn_id"])
        .unique(subset=["espn_game_id"], keep="first")
    )


def _unambiguous(frame: pl.DataFrame, keys: "List[str]") -> pl.DataFrame:
    """``keys -> espn_game_id``, keeping only keys resolving to exactly one game."""
    return (
        frame.drop_nulls(keys)
        .group_by(keys)
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
    for k in keys:
        assert joinable.schema[k] == lookup.schema[k], f"join key {k} dtype drift"
    joined = joinable.join(lookup, on=keys, how="left")
    matched = joined.filter(pl.col("espn_game_id").is_not_null()).with_columns(
        pl.lit(method, dtype=pl.Utf8).alias("match_method")
    )
    still = pl.concat([joined.filter(pl.col("espn_game_id").is_null()).drop("espn_game_id"), rest])
    return matched, still


def _with_pairs(frame: pl.DataFrame) -> pl.DataFrame:
    """Unordered team pair + score pair (+ the single known team id) for tiers 3-5."""
    h, a = pl.col("home_espn_id"), pl.col("away_espn_id")
    return frame.with_columns(
        pl.min_horizontal(h, a).alias("id_lo"),
        pl.max_horizontal(h, a).alias("id_hi"),
        pl.min_horizontal("home_score", "away_score").alias("score_lo"),
        pl.max_horizontal("home_score", "away_score").alias("score_hi"),
    )


def build_season_xwalk(
    root: "str | Path",
    season: int,
    *,
    team_xwalk: "Optional[pl.DataFrame]" = None,
    fetch_day: "Optional[FetchDayFn]" = None,
) -> pl.DataFrame:
    """``contest_id / espn_game_id / match_method`` -- one row per contest, none dropped."""
    out_schema = {"contest_id": pl.Utf8, "espn_game_id": pl.Utf8, "match_method": pl.Utf8}
    if not master_parquet_path(root, season).is_file():
        return pl.DataFrame(schema=out_schema)
    teams = team_map(team_xwalk if team_xwalk is not None else fetch_team_xwalk(), season)
    ncaa = ncaa_side(root, season, teams)
    if ncaa.height == 0:
        return pl.DataFrame(schema=out_schema)
    espn = espn_side(root, season, sweep_dates(ncaa, season), fetch_day=fetch_day)

    pending = _with_pairs(ncaa)
    cols = pending.columns
    matched: "List[pl.DataFrame]" = []
    if espn.height:
        espn = _with_pairs(espn)
        windowed = pl.concat(
            [espn.with_columns(pl.col("game_date") + pl.duration(days=d)) for d in (-1, 1)]
        )
        # single_team: each ESPN game listed once per participating team.
        by_team = pl.concat(
            [espn.with_columns(pl.col(f"{s}_espn_id").alias("one_id")) for s in ("home", "away")]
        )
        h, a = pl.col("home_espn_id"), pl.col("away_espn_id")
        one_id = (
            pl.when(h.is_not_null() & a.is_null())
            .then(h)
            .when(a.is_not_null() & h.is_null())
            .then(a)
            .alias("one_id")
        )
        triple = ["game_date", "home_espn_id", "away_espn_id"]
        pair = ["game_date", "id_lo", "id_hi"]
        tiers = [
            ("exact", triple, espn),
            ("date_window", [*triple, "home_score", "away_score"], windowed),
            ("unordered_pair", pair, espn),
            ("unordered_pair_score", [*pair, "score_lo", "score_hi"], espn),
            ("single_team", ["game_date", "one_id", "score_lo", "score_hi"], by_team),
        ]
        for method, keys, source in tiers:
            if pending.height == 0:
                break
            prepared = pending.with_columns(one_id) if "one_id" in keys else pending
            hit, rest = _apply_tier(prepared, _unambiguous(source, keys), keys, method)
            if hit.height:
                matched.append(hit.select("contest_id", "espn_game_id", "match_method"))
            pending = rest.select(cols)

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
    by = (
        frame.drop_nulls("match_method").group_by("match_method").agg(pl.len()).to_dicts()
        if n
        else []
    )
    print(f"[xwalk] {args.season}: {hit}/{n} matched  {by} -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
