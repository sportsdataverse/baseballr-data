"""Stage 05 -- datasets: persisted html -> reference parquet frames. OFFLINE.

Pure function of the committed tree (re-run overwrites, fetches nothing):

* ``ncaa/teams_html/{season}_d{division}.html`` -> ``ncaa/teams/parquet/{season}_d{division}.parquet``
* ``ncaa/schedules_html/{season}/*.html``       -> ``ncaa/schedule_master/parquet/{season}.parquet``
* ``ncaa/rosters_html/{season}/*.html``         -> ``ncaa/rosters/parquet/{season}.parquet``

Parsing lives in sdv-py (``sportsdataverse.scrape.ncaa.reference`` -- the
sport-neutral stats.ncaa.org platform parsers, validated on real MBA fixtures).
The schedule master carries one row per team-game with ``division``,
``game_number`` (baseball doubleheaders print ``MM/DD/YYYY(N)``), ``contest_id``
and a stamped ``season``. Empty inputs write zero-row frames with the documented
schema.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Tuple

import polars as pl

from ncaa_pbp.schedules import DIVISIONS, REPO_ROOT, teams_html_path


def teams_parquet_path(root: "str | Path", season: int, division: int) -> Path:
    return Path(root) / "ncaa" / "teams" / "parquet" / f"{season}_d{division}.parquet"


def master_parquet_path(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "schedule_master" / "parquet" / f"{season}.parquet"


def rosters_parquet_path(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "rosters" / "parquet" / f"{season}.parquet"


def team_divisions(root: "str | Path", season: int) -> pl.DataFrame:
    """``team_id`` (Utf8) -> ``division`` (Int64) from the persisted team lists."""
    from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_list

    frames = []
    for division in DIVISIONS:
        path = teams_html_path(root, season, division)
        if path.is_file():
            frames.append(
                parse_ncaa_team_list(path.read_text(encoding="utf-8")).with_columns(
                    pl.lit(division, dtype=pl.Int64).alias("division")
                )
            )
    if not frames:
        return pl.DataFrame(schema={"team_id": pl.Utf8, "team_name": pl.Utf8, "division": pl.Int64})
    return pl.concat(frames).unique(subset=["team_id"], keep="first", maintain_order=True)


def build_teams(root: "str | Path", season: int) -> "List[Path]":
    """One parquet per division with persisted team-list html; stamps season."""
    from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_list

    written: "List[Path]" = []
    for division in DIVISIONS:
        html_path = teams_html_path(root, season, division)
        if not html_path.is_file():
            continue
        frame = parse_ncaa_team_list(html_path.read_text(encoding="utf-8")).with_columns(
            pl.lit(division, dtype=pl.Int64).alias("division"),
            pl.lit(season, dtype=pl.Int64).alias("season"),
        )
        out = teams_parquet_path(root, season, division)
        out.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(out)
        written.append(out)
    return written


def build_schedule_master(root: "str | Path", season: int) -> "Tuple[Path, pl.DataFrame]":
    """Re-parse every persisted team page -> one row per team-game (+division/season)."""
    from sportsdataverse.scrape.ncaa.reference import (
        TEAM_SCHEDULE_SCHEMA,
        parse_ncaa_team_schedule,
    )

    html_dir = Path(root) / "ncaa" / "schedules_html" / str(season)
    frames = [
        parse_ncaa_team_schedule(p.read_text(encoding="utf-8"), team_id=p.stem) for p in sorted(html_dir.glob("*.html"))
    ]
    base = (
        pl.concat([f for f in frames if f.height])
        if any(f.height for f in frames)
        else pl.DataFrame(schema=dict(TEAM_SCHEDULE_SCHEMA))
    )
    div_map = team_divisions(root, season)
    assert base.schema["team_id"] == div_map.schema["team_id"]  # join-key dtype discipline
    frame = base.join(div_map.select("team_id", "division"), on="team_id", how="left").with_columns(
        pl.lit(season, dtype=pl.Int64).alias("season")
    )
    out = master_parquet_path(root, season)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)
    return out, frame


def build_rosters(root: "str | Path", season: int) -> "Tuple[Path, pl.DataFrame]":
    """Parse every persisted roster page -> one row per player (+season)."""
    from sportsdataverse.scrape.ncaa.reference import (
        TEAM_ROSTER_SCHEMA,
        parse_ncaa_team_roster,
    )

    html_dir = Path(root) / "ncaa" / "rosters_html" / str(season)
    frames = [
        parse_ncaa_team_roster(p.read_text(encoding="utf-8"), team_id=p.stem) for p in sorted(html_dir.glob("*.html"))
    ]
    base = (
        pl.concat([f for f in frames if f.height])
        if any(f.height for f in frames)
        else pl.DataFrame(schema=dict(TEAM_ROSTER_SCHEMA))
    )
    frame = base.with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    out = rosters_parquet_path(root, season)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)
    return out, frame


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True, help="calendar year (2026 = spring 2026)")
    ap.add_argument("--root", default=str(REPO_ROOT))
    args = ap.parse_args(argv)
    root = Path(args.root)

    teams = build_teams(root, args.season)
    print(f"[datasets] teams: {len(teams)} division parquet(s)", flush=True)
    path, master = build_schedule_master(root, args.season)
    print(f"[datasets] schedule_master: {master.height} rows -> {path}", flush=True)
    path, rosters = build_rosters(root, args.season)
    print(f"[datasets] rosters: {rosters.height} rows -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
