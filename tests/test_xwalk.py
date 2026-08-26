"""Stage 06 crosswalk tests -- synthetic master + scoreboard cache, fully offline.

The scoreboard cache under ``ncaa/xwalk/espn_scoreboard/{season}/`` is the
offline contract: every test passes a ``fetch_day`` that raises, proving the
sweep never hits the network when the cache is populated.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from ncaa_pbp import xwalk
from ncaa_pbp.datasets import master_parquet_path

SEASON = 2026


def _boom(yyyymmdd: str) -> dict:
    raise AssertionError(f"network fetch attempted for {yyyymmdd}")


def _event(eid: str, date: str, home: str, hs: int, away: str, as_: int) -> dict:
    return {
        "id": eid,
        "date": f"{date}T18:00Z",
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "score": str(hs), "team": {"id": "1", "location": home}},
                    {"homeAway": "away", "score": str(as_), "team": {"id": "2", "location": away}},
                ]
            }
        ],
    }


def _master_row(cid: str, date: str, team: str, opp: str, ts: int, os_: int) -> dict:
    return {
        "contest_id": cid,
        "date": date,
        "team_name": team,
        "opponent": opp,
        "team_score": ts,
        "opponent_score": os_,
    }


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    rows = [
        # C1: 5-3, both schedule perspectives (dedupe); exact espn match.
        _master_row("C1", "03/01/2026", "State U", "Bulldogs", 5, 3),
        _master_row("C1", "03/01/2026", "Bulldogs", "@ State U", 3, 5),
        # C2: espn lists it a day later (UTC) -> score_window.
        _master_row("C2", "03/02/2026", "Miners", "Owls", 7, 2),
        # C3: orientation flipped on the espn side -> score_pair.
        _master_row("C3", "03/05/2026", "Aggies", "Bears", 4, 6),
        # C4/C5: doubleheader twins, identical 2-1 scores vs two different
        # opponents pairs that date -> only the name tier can split them.
        _master_row("C4", "03/07/2026(1)", "Wildcats", "Eagles", 2, 1),
        _master_row("C5", "03/07/2026(2)", "Lions", "Tigers", 2, 1),
        # C6: no espn game at all -> stays NULL.
        _master_row("C6", "03/02/2026", "Pilots", "Waves", 9, 9),
    ]
    path = master_parquet_path(tmp_path, SEASON)
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path)

    ncaa = xwalk.ncaa_side(tmp_path, SEASON)
    cache = xwalk.scoreboard_cache_dir(tmp_path, SEASON)
    cache.mkdir(parents=True)
    for day in xwalk.sweep_dates(ncaa, SEASON):
        (cache / f"{day}.json").write_text(json.dumps({"events": []}), encoding="utf-8")

    def put(day: str, *events: dict) -> None:
        (cache / f"{day}.json").write_text(json.dumps({"events": list(events)}), encoding="utf-8")

    put("20260301", _event("E1", "2026-03-01", "State U", 5, "Bulldogs", 3))
    put("20260303", _event("E2", "2026-03-03", "Miners", 7, "Owls", 2))
    put("20260305", _event("E3", "2026-03-05", "Bears", 6, "Aggies", 4))
    put(
        "20260307",
        _event("E4", "2026-03-07", "Wildcats", 2, "Eagles", 1),
        _event("E5", "2026-03-07", "Lions", 2, "Tigers", 1),
    )
    return tmp_path


def test_ncaa_side_orientation_and_doubleheaders(root: Path) -> None:
    ncaa = xwalk.ncaa_side(root, SEASON)
    assert ncaa.height == 6  # C1's two perspective rows collapsed
    c1 = ncaa.filter(pl.col("contest_id") == "C1").to_dicts()[0]
    assert (c1["home_score"], c1["away_score"]) == (5, 3)
    assert c1["home_name"] == "State U"
    c4 = ncaa.filter(pl.col("contest_id") == "C4").to_dicts()[0]
    assert str(c4["game_date"]) == "2026-03-07"  # "(1)" suffix stripped


def test_espn_side_reads_cache_offline(root: Path) -> None:
    espn = xwalk.espn_side(root, SEASON, fetch_day=_boom)
    assert espn.height == 5
    assert espn.schema["espn_game_id"] == pl.Utf8
    assert espn.schema["game_date"] == pl.Date
    e1 = espn.filter(pl.col("espn_game_id") == "E1").to_dicts()[0]
    assert (e1["home_score"], e1["away_score"]) == (5, 3)


def test_fetch_scoreboard_day_caches(tmp_path: Path) -> None:
    calls: "list[str]" = []

    def fake(day: str) -> dict:
        calls.append(day)
        return {"events": []}

    p1 = xwalk.fetch_scoreboard_day(tmp_path, SEASON, "20260214", fetch_day=fake)
    p2 = xwalk.fetch_scoreboard_day(tmp_path, SEASON, "20260214", fetch_day=_boom)
    assert p1 == p2 == {"events": []}
    assert calls == ["20260214"]  # second read came from the cache


def test_build_season_xwalk_tiers(root: Path) -> None:
    frame = xwalk.build_season_xwalk(root, SEASON)
    got = {r["contest_id"]: (r["espn_game_id"], r["match_method"]) for r in frame.to_dicts()}
    assert got["C1"] == ("E1", "score_exact")
    assert got["C2"] == ("E2", "score_window")
    assert got["C3"] == ("E3", "score_pair")
    assert got["C4"] == ("E4", "score_pair_names")
    assert got["C5"] == ("E5", "score_pair_names")
    assert got["C6"] == (None, None)  # unmatched keeps NULL, row never dropped
    assert set(got) == {"C1", "C2", "C3", "C4", "C5", "C6"}


def test_identical_doubleheader_stays_null(root: Path) -> None:
    # Make E4/E5 name-identical too (same pairing twice, same score):
    # even the name tier must refuse to guess.
    cache = xwalk.scoreboard_cache_dir(root, SEASON)
    (cache / "20260307.json").write_text(
        json.dumps(
            {
                "events": [
                    _event("E4", "2026-03-07", "Wildcats", 2, "Eagles", 1),
                    _event("E5", "2026-03-07", "Wildcats", 2, "Eagles", 1),
                ]
            }
        ),
        encoding="utf-8",
    )
    master = master_parquet_path(root, SEASON)
    rows = [
        _master_row("C4", "03/07/2026(1)", "Wildcats", "Eagles", 2, 1),
        _master_row("C5", "03/07/2026(2)", "Wildcats", "Eagles", 2, 1),
    ]
    pl.DataFrame(rows).write_parquet(master)
    frame = xwalk.build_season_xwalk(root, SEASON)
    assert frame.get_column("espn_game_id").to_list() == [None, None]


def test_write_and_load_roundtrip(root: Path) -> None:
    frame = xwalk.build_season_xwalk(root, SEASON)
    path = xwalk.write_season_xwalk(root, SEASON, frame)
    assert path == xwalk.xwalk_path(root, SEASON)
    index = xwalk.load_espn_game_index(root, SEASON)
    assert index["C1"] == "E1"
    assert "C6" not in index  # unmatched not in the id index
