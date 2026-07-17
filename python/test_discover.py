"""Offline discovery tests -- pure parsers + an injected fetch_fn (no network)."""

from __future__ import annotations

import pytest
from discover import discover_season, parse_contest_ids, parse_team_ids, team_list_path


def test_team_list_path_baseball_and_softball() -> None:
    assert "sport_code=MBA" in team_list_path(2025)
    assert "division=1" in team_list_path(2025)
    assert "sport_code=WSB" in team_list_path(2025, sport_code="WSB")
    assert "division=2" in team_list_path(2024, division=2)


def test_parse_team_ids_dedups_and_orders() -> None:
    html = '<a href="/teams/596471">A</a><a href="/teams/596477">B</a><a href="/teams/596471">dup</a>'
    assert parse_team_ids(html) == ["596471", "596477"]


def test_parse_contest_ids() -> None:
    html = '<a href="/contests/6357953/box_score">x</a><a href="/contests/6356679/play_by_play">y</a>'
    assert parse_contest_ids(html) == ["6357953", "6356679"]


def test_parse_empty() -> None:
    assert parse_team_ids("") == []
    assert parse_contest_ids(None) == []  # type: ignore[arg-type]


def test_discover_season_with_injected_fetch() -> None:
    pages = {
        team_list_path(2025): '<a href="/teams/1">A</a><a href="/teams/2">B</a>',
        "teams/1": '<a href="/contests/100/box_score">x</a>',
        "teams/2": '<a href="/contests/200/play_by_play">y</a><a href="/contests/100/box_score">dup</a>',
    }
    got = discover_season(2025, fetch_fn=lambda p: pages[p])
    assert got == ["100", "200"]  # sorted + deduped across teams


def test_discover_season_zero_teams_raises() -> None:
    with pytest.raises(ValueError, match="no teams"):
        discover_season(2025, fetch_fn=lambda p: "<html>nothing</html>")


def test_discover_season_wsb_error_notes_todo() -> None:
    with pytest.raises(ValueError, match="WSB team-list flow is a known TODO"):
        discover_season(2025, sport_code="WSB", fetch_fn=lambda p: "<html>shell</html>")
