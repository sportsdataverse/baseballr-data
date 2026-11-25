"""Stage 06 crosswalk tests -- synthetic master + scoreboard cache, fully offline.

The scoreboard cache under ``ncaa/xwalk/espn_scoreboard/{season}/`` is the
offline contract: every test passes a ``fetch_day`` that raises, proving the
sweep never hits the network when the cache is populated, and an injected
``team_xwalk`` frame stands in for the published team crosswalk.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from ncaa_pbp import xwalk
from ncaa_pbp.datasets import master_parquet_path

SEASON = 2026

#: NCAA season team id -> ESPN id (the release's dtypes: Int64 -> String).
TEAMS = {"101": "1", "102": "2", "103": "3", "104": "4", "105": "5", "106": "6", "107": "7"}
TEAM_XWALK = pl.DataFrame(
    {
        "season": [SEASON] * len(TEAMS),
        "ncaa_season_team_id": [int(k) for k in TEAMS],
        "espn_team_id": list(TEAMS.values()),
    }
)


def _boom(yyyymmdd: str) -> dict:
    raise AssertionError(f"network fetch attempted for {yyyymmdd}")


def _event(eid: str, date: str, home: str, hs: int, away: str, as_: int) -> dict:
    return {
        "id": eid,
        "date": f"{date}Z" if "T" in date else f"{date}T18:00Z",  # UTC, as ESPN stamps it
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "score": str(hs), "team": {"id": home}},
                    {"homeAway": "away", "score": str(as_), "team": {"id": away}},
                ]
            }
        ],
    }


def _row(cid: str, date: str, tid: "str | None", oid: "str | None", opp: str, ts: int, os_: int):
    return {
        "contest_id": cid,
        "date": date,
        "team_id": tid,
        "team_name": f"T{tid}",
        "opponent_id": oid,
        "opponent": opp,
        "team_score": ts,
        "opponent_score": os_,
    }


def _write_master(root: Path, rows: "list[dict]") -> None:
    path = master_parquet_path(root, SEASON)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema_overrides={"team_id": pl.Utf8, "opponent_id": pl.Utf8}).write_parquet(
        path
    )


def _put(root: Path, day: str, *events: dict) -> None:
    cache = xwalk.scoreboard_cache_dir(root, SEASON)
    (cache / f"{day}.json").write_text(json.dumps({"events": list(events)}), encoding="utf-8")


def _empty_cache(root: Path) -> None:
    teams = xwalk.team_map(TEAM_XWALK, SEASON)
    cache = xwalk.scoreboard_cache_dir(root, SEASON)
    cache.mkdir(parents=True, exist_ok=True)
    for day in xwalk.sweep_dates(xwalk.ncaa_side(root, SEASON, teams), SEASON):
        (cache / f"{day}.json").write_text(json.dumps({"events": []}), encoding="utf-8")


def _build(root: Path) -> "dict[str, tuple]":
    frame = xwalk.build_season_xwalk(root, SEASON, team_xwalk=TEAM_XWALK, fetch_day=_boom)
    return {r["contest_id"]: (r["espn_game_id"], r["match_method"]) for r in frame.to_dicts()}


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    _write_master(
        tmp_path,
        [
            # C1: 101 hosts 102, both schedule perspectives (dedupe) -> exact.
            _row("C1", "03/01/2026", "101", "102", "T102", 5, 3),
            _row("C1", "03/01/2026", "102", "101", "@ T101", 3, 5),
            # C2: ESPN lists it a day later (UTC) -> date_window.
            _row("C2", "03/02/2026", "103", "104", "T104", 7, 2),
            # C3: neutral site, ESPN has the other team as home -> unordered_pair.
            _row("C3", "03/05/2026", "105", "106", "T106 @Clearwater, FL", 4, 6),
            # C4/C5: doubleheader, same pair, different scores -> unordered_pair_score.
            _row("C4", "03/07/2026(1)", "101", "103", "T103", 2, 1),
            _row("C5", "03/07/2026(2)", "101", "103", "T103", 8, 4),
            # C6: vs a non-D-I opponent (no NCAA id, so no ESPN id) -> single_team.
            _row("C6", "03/08/2026", "107", None, "Some D-II", 9, 0),
            # C7: no ESPN game at all -> stays NULL.
            _row("C7", "03/02/2026", "105", "107", "T107", 9, 9),
            # C8: D-III game (no ESPN ids) on C1's date with C1's score: the old
            # score-only engine matched exactly this to E1. It must stay NULL.
            _row("C8", "03/01/2026", "9001", "9002", "D3 Opp", 5, 3),
        ],
    )
    _empty_cache(tmp_path)
    _put(tmp_path, "20260301", _event("E1", "2026-03-01", "1", 5, "2", 3))
    _put(tmp_path, "20260303", _event("E2", "2026-03-03", "3", 7, "4", 2))
    _put(tmp_path, "20260305", _event("E3", "2026-03-05", "6", 6, "5", 4))
    _put(
        tmp_path,
        "20260307",
        _event("E4", "2026-03-07", "1", 2, "3", 1),
        _event("E5", "2026-03-07", "1", 8, "3", 4),
    )
    _put(tmp_path, "20260308", _event("E6", "2026-03-08", "7", 9, "99", 0))
    return tmp_path


def test_team_map_casts_ids_to_utf8_and_refuses_an_empty_season() -> None:
    teams = xwalk.team_map(TEAM_XWALK, SEASON)
    assert teams.schema["ncaa_season_team_id"] == pl.Utf8
    assert dict(teams.iter_rows()) == TEAMS  # "101", never "101.0"
    with pytest.raises(ValueError, match="no rows for season 2027"):
        xwalk.team_map(TEAM_XWALK, 2027)


def test_ncaa_side_orients_and_maps_espn_ids(root: Path) -> None:
    ncaa = xwalk.ncaa_side(root, SEASON, xwalk.team_map(TEAM_XWALK, SEASON))
    assert ncaa.height == 8  # C1's two perspective rows collapsed
    c1 = ncaa.filter(pl.col("contest_id") == "C1").to_dicts()[0]
    assert (c1["home_espn_id"], c1["away_espn_id"]) == ("1", "2")
    assert (c1["home_score"], c1["away_score"]) == (5, 3)
    c4 = ncaa.filter(pl.col("contest_id") == "C4").to_dicts()[0]
    assert str(c4["game_date"]) == "2026-03-07"  # "(1)" suffix stripped
    c6 = ncaa.filter(pl.col("contest_id") == "C6").to_dicts()[0]
    assert (c6["home_espn_id"], c6["away_espn_id"]) == ("7", None)


def test_espn_side_reads_cache_offline(root: Path) -> None:
    espn = xwalk.espn_side(root, SEASON, ["20260301", "20260307"], fetch_day=_boom)
    assert espn.height == 3
    assert espn.schema["espn_game_id"] == pl.Utf8
    assert espn.schema["game_date"] == pl.Date
    e1 = espn.filter(pl.col("espn_game_id") == "E1").to_dicts()[0]
    assert (e1["home_espn_id"], e1["home_score"]) == ("1", 5)


def test_fetch_scoreboard_day_caches(tmp_path: Path) -> None:
    calls: "list[str]" = []

    def fake(day: str) -> dict:
        calls.append(day)
        return {"events": []}

    p1 = xwalk.fetch_scoreboard_day(tmp_path, SEASON, "20260214", fetch_day=fake)
    p2 = xwalk.fetch_scoreboard_day(tmp_path, SEASON, "20260214", fetch_day=_boom)
    assert p1 == p2 == {"events": []}
    assert calls == ["20260214"]  # second read came from the cache


def test_live_fetch_day_resolves_the_sdv_py_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every other test injects fetch_day, so the live import was never run: it named
    # sportsdataverse.baseball.espn_college_baseball_scoreboard, which does not exist,
    # and every season's stage 06 died on ImportError behind `|| true` (2014-2026).
    from sportsdataverse.baseball import college_baseball

    calls: "list[tuple]" = []

    def fake(**kwargs: object) -> dict:
        calls.append(tuple(sorted(kwargs.items())))
        return {"events": []}

    monkeypatch.setattr(college_baseball, "espn_college_baseball_scoreboard", fake)
    assert xwalk._live_fetch_day("20250301") == {"events": []}
    assert calls == [(("dates", "20250301"), ("return_parsed", False))]


def test_build_season_xwalk_tiers(root: Path) -> None:
    got = _build(root)
    assert got["C1"] == ("E1", "exact")
    assert got["C2"] == ("E2", "date_window")
    assert got["C3"] == ("E3", "unordered_pair")
    assert got["C4"] == ("E4", "unordered_pair_score")
    assert got["C5"] == ("E5", "unordered_pair_score")
    assert got["C6"] == ("E6", "single_team")
    assert got["C7"] == (None, None)  # unmatched keeps NULL, row never dropped
    assert set(got) == {f"C{i}" for i in range(1, 9)}


def test_score_alone_never_matches(root: Path) -> None:
    # Regression: a D-III contest sharing a D-I broadcast's date and score.
    assert _build(root)["C8"] == (None, None)


def test_night_games_in_a_series_match_on_the_eastern_date(tmp_path: Path) -> None:
    # Fri 7:05pm ET is Sat 00:05Z. Read as a UTC date, both games of the series sit on
    # Saturday, so every key is ambiguous and the whole series went NULL.
    _write_master(
        tmp_path,
        [
            _row("S1", "03/06/2026", "101", "102", "T102", 3, 2),
            _row("S2", "03/07/2026", "101", "102", "T102", 6, 1),
        ],
    )
    _empty_cache(tmp_path)
    _put(tmp_path, "20260306", _event("F1", "2026-03-07T00:05", "1", 3, "2", 2))
    _put(tmp_path, "20260307", _event("F2", "2026-03-07T23:30", "1", 6, "2", 1))
    assert _build(tmp_path) == {"S1": ("F1", "exact"), "S2": ("F2", "exact")}


def test_identical_doubleheader_stays_null(tmp_path: Path) -> None:
    _write_master(
        tmp_path,
        [
            _row("C4", "03/07/2026(1)", "101", "103", "T103", 2, 1),
            _row("C5", "03/07/2026(2)", "101", "103", "T103", 2, 1),
        ],
    )
    _empty_cache(tmp_path)
    _put(
        tmp_path,
        "20260307",
        _event("E4", "2026-03-07", "1", 2, "3", 1),
        _event("E5", "2026-03-07", "1", 2, "3", 1),
    )
    assert _build(tmp_path) == {"C4": (None, None), "C5": (None, None)}


def test_write_and_load_roundtrip(root: Path) -> None:
    frame = xwalk.build_season_xwalk(root, SEASON, team_xwalk=TEAM_XWALK, fetch_day=_boom)
    path = xwalk.write_season_xwalk(root, SEASON, frame)
    assert path == xwalk.xwalk_path(root, SEASON)
    index = xwalk.load_espn_game_index(root, SEASON)
    assert index["C1"] == "E1"
    assert "C7" not in index  # unmatched not in the id index
