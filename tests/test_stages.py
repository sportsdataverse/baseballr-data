"""Offline stage-module tests -- real fixtures + injected fetch_fn, tmp roots.

Covers stage 01 (scrape flow + resumability), 02 (master-driven contest ids,
sharding, raw-tree layout), 04 (roster scrape + skip), and 05 (offline parquet
builds from persisted html). Stage 06 lives in test_xwalk.py. No network.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from ncaa_pbp import datasets, games, rosters, schedules
from ncaa_pbp.discover import team_list_path

FIXTURES = Path(__file__).resolve().parent / "fixtures"
TEAM_LIST = (FIXTURES / "mba_team_list_2026_d1.html").read_text(encoding="utf-8")
TEAM_PAGE = (FIXTURES / "mba_team_page_614839.html").read_text(encoding="utf-8")
ROSTER = (FIXTURES / "mba_roster_614839.html").read_text(encoding="utf-8")

TINY_LIST = '<a href="/teams/614839">Team A</a><a href="/teams/900001">Team B</a>'


def _fetch(pages: "dict[str, str]", calls: "list[str] | None" = None):
    def fetch(path: str) -> str:
        if calls is not None:
            calls.append(path)
        return pages[path]

    return fetch


def _scraped_root(tmp_path: Path) -> Path:
    """A tmp repo root with a scraped season: tiny d1 list + one real team page."""
    pages = {
        team_list_path(2026, 1): TINY_LIST,
        "teams/614839": TEAM_PAGE,
        "teams/900001": "<html>no games</html>",
    }
    schedules.scrape_season(2026, (1,), root=tmp_path, fetch_fn=_fetch(pages))
    return tmp_path


# --- stage 01 -------------------------------------------------------------


def test_schedules_scrape_persists_html_and_resumes(tmp_path: Path) -> None:
    pages = {
        team_list_path(2026, 1): TINY_LIST,
        "teams/614839": TEAM_PAGE,
        "teams/900001": "<html>no games</html>",
    }
    calls: "list[str]" = []
    got = schedules.scrape_season(2026, (1,), root=tmp_path, fetch_fn=_fetch(pages, calls))
    assert got == {1: ["614839", "900001"]}
    assert schedules.teams_html_path(tmp_path, 2026, 1).is_file()
    assert schedules.schedule_html_path(tmp_path, 2026, "614839").is_file()
    assert len(calls) == 3
    # resumable: a completed season fetches NOTHING
    calls.clear()
    schedules.scrape_season(2026, (1,), root=tmp_path, fetch_fn=_fetch(pages, calls))
    assert calls == []


def test_schedules_scrape_zero_teams_raises(tmp_path: Path) -> None:
    pages = {team_list_path(2026, 2): "<html>empty</html>"}
    with pytest.raises(ValueError, match="no teams"):
        schedules.scrape_season(2026, (2,), root=tmp_path, fetch_fn=_fetch(pages))


# --- stage 05 -------------------------------------------------------------


def test_build_teams_real_fixture(tmp_path: Path) -> None:
    path = schedules.teams_html_path(tmp_path, 2026, 1)
    path.parent.mkdir(parents=True)
    path.write_text(TEAM_LIST, encoding="utf-8")
    written = datasets.build_teams(tmp_path, 2026)
    assert written == [datasets.teams_parquet_path(tmp_path, 2026, 1)]
    frame = pl.read_parquet(written[0])
    assert frame.height > 250  # 2026 D1 is ~300 teams
    assert frame.get_column("division").unique().to_list() == [1]
    assert frame.get_column("season").unique().to_list() == [2026]
    assert frame.schema["team_id"] == pl.Utf8


def test_build_schedule_master_real_fixture(tmp_path: Path) -> None:
    _scraped_root(tmp_path)
    path, frame = datasets.build_schedule_master(tmp_path, 2026)
    assert path == datasets.master_parquet_path(tmp_path, 2026)
    assert path.is_file()
    for col in ("contest_id", "game_number", "division", "season", "date", "opponent"):
        assert col in frame.columns, col
    assert frame.height > 20  # one real team page = a full season of games
    assert frame.get_column("team_id").unique().to_list() == ["614839"]
    assert frame.get_column("division").unique().to_list() == [1]
    assert frame.get_column("contest_id").drop_nulls().len() > 0


def test_build_schedule_master_empty_tree_keeps_schema(tmp_path: Path) -> None:
    _, frame = datasets.build_schedule_master(tmp_path, 2027)
    assert frame.height == 0
    assert {"contest_id", "division", "season"} <= set(frame.columns)


def test_build_rosters_real_fixture(tmp_path: Path) -> None:
    path = rosters.roster_html_path(tmp_path, 2026, "614839")
    path.parent.mkdir(parents=True)
    path.write_text(ROSTER, encoding="utf-8")
    out, frame = datasets.build_rosters(tmp_path, 2026)
    assert out.is_file()
    assert frame.height > 10
    assert frame.get_column("team_id").unique().to_list() == ["614839"]
    assert frame.get_column("season").unique().to_list() == [2026]


# --- stage 02 -------------------------------------------------------------


def test_contest_ids_from_master(tmp_path: Path) -> None:
    _scraped_root(tmp_path)
    datasets.build_schedule_master(tmp_path, 2026)
    ids = games.contest_ids_from_master(tmp_path, 2026)
    assert ids == sorted(set(ids))
    assert len(ids) > 20
    assert all(i.isdigit() for i in ids)


def test_contest_ids_require_stage_01(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="run stage 01"):
        games.contest_ids_from_master(tmp_path, 2026)


def test_shard_parse_and_disjoint() -> None:
    assert games.parse_shard("0/2") == (0, 2)
    with pytest.raises(Exception):
        games.parse_shard("2/2")
    with pytest.raises(Exception):
        games.parse_shard("nope")
    ids = [str(i) for i in range(11)]
    shards = [ids[i::4] for i in range(4)]
    assert sorted(sum(shards, [])) == sorted(ids)  # disjoint + complete


def test_games_capture_writes_raw_season_tree(tmp_path: Path) -> None:
    from ncaa_pbp.capture import bundle_path, capture_season

    pbp = (FIXTURES / "mba_pbp_6357953.html").read_text(encoding="utf-8")
    out_dir = games.raw_dir(tmp_path, 2026)
    stats = capture_season(["6357953"], lambda p: pbp, out_dir)
    assert stats == {"captured": 1, "skipped": 0, "failed": 0}
    expected = tmp_path / "ncaa" / "raw" / "2026" / "6357953.json.gz"
    assert bundle_path("6357953", out_dir) == expected
    assert expected.is_file()


# --- stage 04 -------------------------------------------------------------


def test_rosters_scrape_resumable(tmp_path: Path) -> None:
    _scraped_root(tmp_path)
    pages = {
        "teams/614839/roster": ROSTER,
        "teams/900001/roster": "<html>bare</html>",
    }
    stats = rosters.scrape_rosters(2026, (1,), root=tmp_path, fetch_fn=_fetch(pages))
    assert stats == {"fetched": 2, "skipped": 0}
    assert rosters.roster_html_path(tmp_path, 2026, "614839").is_file()
    stats = rosters.scrape_rosters(2026, (1,), root=tmp_path, fetch_fn=_fetch(pages))
    assert stats == {"fetched": 0, "skipped": 2}


def test_rosters_scrape_requires_stage_01(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="run stage 01"):
        rosters.scrape_rosters(2026, (1,), root=tmp_path, fetch_fn=lambda p: "")
