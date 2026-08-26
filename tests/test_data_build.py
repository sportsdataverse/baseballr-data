"""Hermetic build test: a synthetic payload tree in tmp_path -> release layout.

Two capture-era payloads (2024) + one legacy R-era payload (2015, empty
linescore/team/player/situational families) + reference parquet + a committed
legacy-schedule parquet. Asserts per-dataset outputs, the per-payload stamps,
the schedules plural-tag/singular-stem quirk, and the finals-QA frame.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from ncaa_baseball_data_build.cli import main
from ncaa_baseball_data_build.config import REGISTRY

SEASON = 2024
LEGACY_SEASON = 2015


def _pbp_row(cid: str, n: int, away: int, home: int) -> dict:
    return {
        "contest_id": cid,
        "inning": 9,
        "inning_top_bot": "bot",
        "batting": "Home U.",
        "fielding": "Away U.",
        "play_number": n,
        "score_away": away,
        "score_home": home,
        "batter": "B. Batter",
        "play_type": "single",
        "is_hit": True,
        "rbi": 0,
        "runs_scored": 0,
        "scoring_runners": ["A. Runner"],
        "runners_advanced": [],
        "is_scoring_play": False,
        "description": "B. Batter singled.",
    }


def _capture_payload(cid: str, away_final: int, home_final: int, pbp_home: int) -> dict:
    return {
        "game_key": cid,
        "contest_id": cid,
        "game_pbp_id": None,
        "season": SEASON,
        "source": "capture",
        "espn_game_id": "401" + cid,
        "teams": [
            {"team": "Away U.", "home_away": "away", "final": away_final},
            {"team": "Home U.", "home_away": "home", "final": home_final},
        ],
        "pbp": [_pbp_row(cid, 1, 0, 0), _pbp_row(cid, 2, away_final, pbp_home)],
        "linescore": [
            {"contest_id": cid, "team": "Away U.", "home_away": "away", "inning": "1", "runs": 1},
            {"contest_id": cid, "team": "Home U.", "home_away": "home", "inning": "1", "runs": 2},
        ],
        "team_stats": [{"contest_id": cid, "category": "hitting", "stat": "H", "away_value": "9"}],
        "situational_stats": {
            "with_runners_on": [{"contest_id": cid, "team": "Away U.", "ab": "12"}]
        },
        "player_stats": {
            "hitting": [{"contest_id": cid, "name": "B. Batter", "ab": "4"}],
            "pitching": [{"contest_id": cid, "name": "P. Pitcher", "ip": "9.0"}],
        },
    }


def _legacy_payload(gid: int) -> dict:
    key = str(gid)
    return {
        "game_key": key,
        "contest_id": None,
        "game_pbp_id": gid,
        "season": LEGACY_SEASON,
        "source": "legacy_r",
        "espn_game_id": None,
        "teams": [
            {"team": "Old Away", "home_away": "away", "final": 3},
            {"team": "Old Home", "home_away": "home", "final": 5},
        ],
        "pbp": [_pbp_row(key, 1, 3, 5)],
        "linescore": [],
        "team_stats": [],
        "situational_stats": [],  # legacy shape: LIST, not dict
        "player_stats": {},
    }


def _write_payload(root: Path, payload: dict) -> None:
    d = root / "ncaa" / "json"
    d.mkdir(parents=True, exist_ok=True)
    with gzip.open(d / f"{payload['game_key']}.json.gz", "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)


@pytest.fixture()
def tree(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    # payloads: one finals-matching capture game, one mismatching, one legacy
    _write_payload(root, _capture_payload("7000001", 4, 6, 6))
    _write_payload(root, _capture_payload("7000002", 2, 3, 9))  # pbp_home 9 != final 3
    _write_payload(root, _legacy_payload(4400001))
    # capture-era reference parquet
    (root / "ncaa/teams/parquet").mkdir(parents=True)
    for div in (1, 2):
        pl.DataFrame({"team_id": [str(div)], "division": [div]}).write_parquet(
            root / f"ncaa/teams/parquet/{SEASON}_d{div}.parquet"
        )
    (root / "ncaa/rosters/parquet").mkdir(parents=True)
    pl.DataFrame({"player_id": ["9"]}).write_parquet(
        root / f"ncaa/rosters/parquet/{SEASON}.parquet"
    )
    (root / "ncaa/schedule_master/parquet").mkdir(parents=True)
    pl.DataFrame({"contest_id": ["7000001", "7000002"]}).write_parquet(
        root / f"ncaa/schedule_master/parquet/{SEASON}.parquet"
    )
    # committed legacy R-era schedule (no season column; columns are load-bearing)
    (root / "ncaa/schedules/parquet").mkdir(parents=True)
    pl.DataFrame(
        {"year": [LEGACY_SEASON], "home_team": ["Old Home"], "away_team": ["Old Away"]}
    ).write_parquet(root / f"ncaa/schedules/parquet/ncaa_baseball_schedule_{LEGACY_SEASON}.parquet")
    return root


def _build(base: Path, root: Path, season: int, dataset: str = "all") -> int:
    return main(
        [
            "build",
            "--dataset",
            dataset,
            "--season",
            str(season),
            "--base",
            str(base),
            "--raw-root",
            str(root),
        ]
    )


PAYLOAD_TESTED = ["pbp", "linescore", "team_stats", "player_stats", "situational_stats", "games"]


def test_build_all_capture_season(tree: Path, tmp_path: Path) -> None:
    base = tmp_path / "out"
    assert _build(base, tree, SEASON) == 0

    for name in ["teams", "schedule", "rosters", *PAYLOAD_TESTED]:
        stem = REGISTRY[name].stem
        out = base / "ncaa" / name / "parquet" / f"{stem}_{SEASON}.parquet"
        assert out.is_file(), name
        df = pl.read_parquet(out)
        assert df.get_column("season").to_list() == [SEASON] * df.height, name
        if name in PAYLOAD_TESTED:
            assert set(df.get_column("source").to_list()) == {"capture"}, name
            assert set(df.get_column("espn_game_id").to_list()) == {"4017000001", "4017000002"}

    teams = pl.read_parquet(base / f"ncaa/teams/parquet/ncaa_baseball_teams_{SEASON}.parquet")
    assert sorted(teams.get_column("division").to_list()) == [1, 2]

    pbp = pl.read_parquet(base / f"ncaa/pbp/parquet/ncaa_baseball_pbp_{SEASON}.parquet")
    assert pbp.height == 4  # 2 games x 2 plays
    assert pbp.schema["scoring_runners"] == pl.List(pl.Utf8)
    assert pbp.schema["inning"] == pl.Int64
    assert set(pbp.get_column("game_key").to_list()) == {"7000001", "7000002"}

    ps = pl.read_parquet(
        base / f"ncaa/player_stats/parquet/ncaa_baseball_player_stats_{SEASON}.parquet"
    )
    assert sorted(set(ps.get_column("category").to_list())) == ["hitting", "pitching"]
    ss = pl.read_parquet(
        base / f"ncaa/situational_stats/parquet/ncaa_baseball_situational_stats_{SEASON}.parquet"
    )
    assert set(ss.get_column("category").to_list()) == {"with_runners_on"}
    assert ss.height == 2  # one row per payload

    games = pl.read_parquet(base / f"ncaa/games/parquet/ncaa_baseball_games_{SEASON}.parquet")
    assert games.height == 2
    row = games.filter(pl.col("game_key") == "7000001").to_dicts()[0]
    assert row["away_team"] == "Away U." and row["away_final"] == 4
    assert row["home_team"] == "Home U." and row["home_final"] == 6
    assert row["source"] == "capture" and row["espn_game_id"] == "4017000001"


def test_qa_frame_compares_finals_to_last_pbp_score(tree: Path, tmp_path: Path) -> None:
    base = tmp_path / "out"
    assert _build(base, tree, SEASON) == 0
    qa = pl.read_parquet(base / "ncaa" / "qa" / f"qa_pbp_finals_{SEASON}.parquet")
    assert qa.height == 2
    by_key = {r["game_key"]: r for r in qa.to_dicts()}
    assert by_key["7000001"]["finals_match"] is True
    assert by_key["7000002"]["finals_match"] is False
    assert by_key["7000002"]["pbp_home"] == 9
    assert by_key["7000002"]["final_home"] == 3


def test_build_all_legacy_season(tree: Path, tmp_path: Path) -> None:
    """Legacy season: teams/rosters skipped (no source), schedule from the
    committed legacy parquet AS-IS + season stamp, empty capture-only families
    still written with their documented schemas."""
    base = tmp_path / "out"
    assert _build(base, tree, LEGACY_SEASON) == 0

    # teams / rosters have no 2015 source -> skipped, not fatal
    assert not (
        base / "ncaa/teams/parquet" / f"ncaa_baseball_teams_{LEGACY_SEASON}.parquet"
    ).exists()
    assert not (
        base / "ncaa/rosters/parquet" / f"ncaa_baseball_rosters_{LEGACY_SEASON}.parquet"
    ).exists()

    sched = pl.read_parquet(
        base / "ncaa/schedule/parquet" / f"ncaa_baseball_schedule_{LEGACY_SEASON}.parquet"
    )
    # AS-IS columns preserved, season added
    assert {"year", "home_team", "away_team", "season"} == set(sched.columns)
    assert sched.schema["season"] == pl.Int64
    assert sched.get_column("season").to_list() == [LEGACY_SEASON]

    pbp = pl.read_parquet(base / f"ncaa/pbp/parquet/ncaa_baseball_pbp_{LEGACY_SEASON}.parquet")
    assert pbp.height == 1
    assert pbp.get_column("source").to_list() == ["legacy_r"]
    assert pbp.get_column("game_key").to_list() == ["4400001"]

    # capture-only families: empty but present, with the documented schema
    ls = pl.read_parquet(
        base / f"ncaa/linescore/parquet/ncaa_baseball_linescore_{LEGACY_SEASON}.parquet"
    )
    assert ls.height == 0
    assert {"contest_id", "team", "runs", "source", "game_key", "season"} <= set(ls.columns)

    games = pl.read_parquet(
        base / f"ncaa/games/parquet/ncaa_baseball_games_{LEGACY_SEASON}.parquet"
    )
    row = games.to_dicts()[0]
    assert row["game_pbp_id"] == 4400001
    assert row["contest_id"] is None
    assert row["source"] == "legacy_r"

    qa = pl.read_parquet(base / "ncaa" / "qa" / f"qa_pbp_finals_{LEGACY_SEASON}.parquet")
    assert qa.to_dicts()[0]["finals_match"] is True


def test_single_dataset_build_rescans(tree: Path, tmp_path: Path) -> None:
    base = tmp_path / "out"
    assert _build(base, tree, SEASON, dataset="pbp") == 0
    out = base / f"ncaa/pbp/parquet/ncaa_baseball_pbp_{SEASON}.parquet"
    assert out.is_file()
    # no sibling datasets written by a single-dataset build
    assert not (base / "ncaa/linescore").exists()
    assert not (base / "ncaa/qa").exists()


def test_schedule_prefers_capture_master(tree: Path, tmp_path: Path) -> None:
    base = tmp_path / "out"
    assert _build(base, tree, SEASON, dataset="schedule") == 0
    sched = pl.read_parquet(base / f"ncaa/schedule/parquet/ncaa_baseball_schedule_{SEASON}.parquet")
    assert "contest_id" in sched.columns  # came from schedule_master, not legacy


def test_missing_season_fails_loudly(tree: Path, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _build(tmp_path / "out", tree, 2013, dataset="pbp")
