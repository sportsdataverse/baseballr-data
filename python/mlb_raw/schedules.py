"""Stage 01 -- schedules: one statsapi call per season -> schedule parquet.

The whole season comes back in a single request. Measured 2026-09-09: a full
``startDate``/``endDate`` range returned 181 dates and 2,456 games in 3.22 MB in
1.01 s. So discovery for the entire 1988-2026 corpus is 39 calls, not ~7,000 --
and a daily run needs exactly one call to learn which games are new.

Output: ``mlb/schedule/{season}.parquet`` plus the raw json alongside it, so a
reparse never needs the network.
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib

import polars as pl

from . import transport

FLOOR = 1988  # first season with complete pitch-by-pitch; see the scoping note

# statsapi gameType codes. The default corpus is REGULAR SEASON + POSTSEASON.
# Spring training (S), exhibition (E) and the All-Star game (A) are excluded by
# default: they add ~570 games to a season and are not the same product --
# spring games carry NO Statcast tracking, so including them silently dilutes
# every tracking-derived dataset with nulls. This was caught the honest way: a
# 5-game capture of 2024 returned 873 pitches with 0 non-null start_speed,
# because the first games of a calendar year are February spring training.
GAME_TYPES = {
    "R": "regular season",
    "F": "wild card",
    "D": "division series",
    "L": "league championship",
    "W": "world series",
    "S": "spring training",
    "E": "exhibition",
    "A": "all-star",
}
DEFAULT_TYPES = ("R", "F", "D", "L", "W")

# Explicit dtypes, for the same reason parse.py carries them -- and this frame
# is the one that most needs it: it holds game_pk, the join key against
# mlb_pbp_*. An EARLY-SEASON build (every game still "Preview", scores absent)
# infers home_score/away_score as Null dtype, and a later
# pl.concat([null_season, int_season]) or scan_parquet("schedule/*.parquet")
# then raises SchemaError -- order-dependently, which makes it harder to
# diagnose, not easier.
_I, _S = pl.Int64, pl.Utf8
SCHEDULE_SCHEMA = {
    "game_pk": _I, "game_date": _S, "game_datetime": _S, "season": _I,
    "game_type": _S, "status_code": _S, "detailed_state": _S, "abstract_state": _S,
    "home_team_id": _I, "home_team_name": _S, "home_score": _I,
    "away_team_id": _I, "away_team_name": _S, "away_score": _I,
    "venue_id": _I, "venue_name": _S,
    "double_header": _S, "game_number": _I, "series_description": _S,
}


def _iid(v):
    """Int64 or None -- never a float, never a stringified float."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def schedule_url(season: int) -> str:
    # Wide bounds rather than the published season window: spring training,
    # postseason and any rescheduled game all carry the same sportId, and a
    # narrow window silently drops them.
    return (
        f"{transport.STATSAPI}/v1/schedule?sportId=1"
        f"&startDate={season}-01-01&endDate={season}-12-31"
    )


def raw_path(root: pathlib.Path, season: int) -> pathlib.Path:
    return root / "mlb" / "schedule" / "raw" / f"{season}.json.gz"


def parquet_path(root: pathlib.Path, season: int) -> pathlib.Path:
    return root / "mlb" / "schedule" / f"{season}.parquet"


def rows_from(payload: dict) -> "list[dict]":
    rows = []
    for d in payload.get("dates") or []:
        for g in d.get("games") or []:
            teams = g.get("teams") or {}
            home, away = teams.get("home") or {}, teams.get("away") or {}
            status = g.get("status") or {}
            rows.append(
                {
                    "game_pk": _iid(g.get("gamePk")),
                    "game_date": d.get("date"),
                    "game_datetime": g.get("gameDate"),
                    "season": _iid(g.get("season")),
                    "game_type": g.get("gameType"),
                    "status_code": status.get("statusCode"),
                    "detailed_state": status.get("detailedState"),
                    "abstract_state": status.get("abstractGameState"),
                    "home_team_id": _iid((home.get("team") or {}).get("id")),
                    "home_team_name": (home.get("team") or {}).get("name"),
                    "home_score": _iid(home.get("score")),
                    "away_team_id": _iid((away.get("team") or {}).get("id")),
                    "away_team_name": (away.get("team") or {}).get("name"),
                    "away_score": _iid(away.get("score")),
                    "venue_id": _iid((g.get("venue") or {}).get("id")),
                    "venue_name": (g.get("venue") or {}).get("name"),
                    "double_header": g.get("doubleHeader"),
                    "game_number": _iid(g.get("gameNumber")),
                    "series_description": g.get("seriesDescription"),
                }
            )
    return rows


def completed_game_pks(
    root: pathlib.Path, season: int, game_types: "tuple[str, ...]" = DEFAULT_TYPES
) -> "list[int]":
    """gamePks worth capturing: Final, and of a wanted gameType.

    Final only: a scheduled or in-progress game's feed/live is not the finished
    record, and capturing it would freeze a half-played game into the raw corpus.

    Type-filtered: without it the first games of a season are February spring
    training, so a capture capped at N games quietly fills the corpus with the
    least useful games of the year -- and with no tracking data at all.
    """
    p = raw_path(root, season)
    if not p.exists():
        return []
    payload = json.loads(gzip.open(p).read())
    # DEDUPE, order-preserving. statsapi lists a game under more than one date
    # when it is suspended and resumed -- 27 of 2,208 gamePks in 2026. Handing
    # duplicates to a thread pool puts two workers on the same output path,
    # where the loser can truncate the winner's file between its write and its
    # rename: a corrupt bundle, not merely a noisy error.
    seen = set()
    out = []
    for r in rows_from(payload):
        pk = r["game_pk"]
        if (
            r["abstract_state"] == "Final"
            and pk
            and r["game_type"] in game_types
            and pk not in seen
        ):
            seen.add(pk)
            out.append(pk)
    return out


def build_season(root: pathlib.Path, season: int, *, force: bool = False) -> int:
    out_raw, out_pq = raw_path(root, season), parquet_path(root, season)
    if out_raw.exists() and not force:
        payload = json.loads(gzip.open(out_raw).read())
    else:
        payload = transport.get_json(schedule_url(season))
        out_raw.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out_raw, "wt", encoding="utf-8") as fh:
            json.dump(payload, fh)
    rows = rows_from(payload)
    if not rows:
        # Refuse to write an empty season rather than publishing a green zero.
        raise transport.TransportError(f"season {season} schedule returned 0 games")
    df = pl.DataFrame(rows, schema=SCHEDULE_SCHEMA)
    out_pq.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_pq)
    return len(rows)


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, help="single season")
    ap.add_argument("--start", type=int, help="range start (inclusive)")
    ap.add_argument("--end", type=int, help="range end (inclusive)")
    ap.add_argument("--root", default=".", help="repo root")
    ap.add_argument("--force", action="store_true", help="refetch even if cached")
    a = ap.parse_args(argv)

    root = pathlib.Path(a.root)
    if a.season:
        seasons = [a.season]
    elif a.start and a.end:
        seasons = list(range(min(a.start, a.end), max(a.start, a.end) + 1))
    else:
        ap.error("need --season, or --start and --end")

    rc = 0
    for s in seasons:
        if s < FLOOR:
            print(f"season {s}: below the {FLOOR} floor (complete pbp starts there) -- skipping")
            continue
        try:
            n = build_season(root, s, force=a.force)
            print(f"season {s}: {n} games -> {parquet_path(root, s)}")
        except Exception as exc:  # noqa: BLE001 - one season must not kill the range
            print(f"season {s}: FAILED {type(exc).__name__}: {exc}")
            rc = 1
    return rc
