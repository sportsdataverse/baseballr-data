"""Stage-03 payload builders: capture era (real pbp fixture) + legacy R-era
adapter (a real committed ncaa/game_pbp file when the tree is present)."""

from __future__ import annotations

import glob
from pathlib import Path

import pytest
from ncaa_pbp.legacy import build_legacy_payload, read_legacy_game
from ncaa_pbp.parse import build_capture_payload

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures"

PAYLOAD_KEYS = {
    "game_key", "contest_id", "game_pbp_id", "season", "source", "espn_game_id",
    "teams", "pbp", "linescore", "team_stats", "situational_stats", "player_stats",
}


def test_capture_payload_shape_and_pbp() -> None:
    bundle = {
        "contest_id": "6357953",
        "play_by_play": (FIX / "mba_pbp_6357953.html").read_text(encoding="utf-8"),
    }
    p = build_capture_payload(bundle, 2025)
    assert PAYLOAD_KEYS <= set(p)
    assert p["source"] == "capture" and p["game_key"] == "6357953"
    assert len(p["pbp"]) > 100
    assert p["pbp"][0]["contest_id"] == "6357953"
    # pbp-only bundle still derives a teams block from the play rows
    assert [t["home_away"] for t in p["teams"]] == ["away", "home"]


def test_legacy_payload_reconciles() -> None:
    files = sorted(glob.glob(str(ROOT / "ncaa" / "game_pbp" / "json" / "*.json")))
    if not files:
        pytest.skip("legacy tree not present (sparse checkout)")
    p = build_legacy_payload(read_legacy_game(files[100]))
    assert PAYLOAD_KEYS <= set(p)
    assert p["source"] == "legacy_r" and p["game_pbp_id"] is not None
    assert p["season"] and p["pbp"]
    # identical pbp column set as the capture era (the reconciliation contract)
    cap_cols = set(
        build_capture_payload(
            {"contest_id": "x", "play_by_play": (FIX / "mba_pbp_6357953.html").read_text(encoding="utf-8")},
            2025,
        )["pbp"][0]
    )
    assert set(p["pbp"][0]) == cap_cols
    # summary rows filtered; decomposition engaged (mostly classified)
    types = [r["play_type"] for r in p["pbp"]]
    assert types.count("unknown") / len(types) < 0.3


def test_capture_payload_finals_come_from_runs_total() -> None:
    """Baseball linescores name the final ``runs_total``; reading the football
    ``final`` nulled every capture-era final (2026-08-27). Locked here."""
    from ncaa_pbp.parse import _teams_block

    rows = [
        {"team": "UC Irvine", "home_away": "away", "inning": "1", "runs": 0, "runs_total": 5},
        {"team": "UC Irvine", "home_away": "away", "inning": "2", "runs": 0, "runs_total": 5},
        {"team": "Sacramento St.", "home_away": "home", "inning": "1", "runs": 1, "runs_total": 7},
    ]
    teams = _teams_block(rows, None)
    assert [(t["team"], t["home_away"], t["final"]) for t in teams] == [
        ("UC Irvine", "away", 5),
        ("Sacramento St.", "home", 7),
    ]


def test_legacy_game_key_is_namespaced() -> None:
    """A game_pbp_id key is prefixed `g`; a contest_id key stays bare.

    The two id spaces overlap numerically (legacy 4.28-5.42M vs 2024 contest
    ids 4.49-5.34M), so a bare legacy key silently collides with a real
    contest and stage 03 "skips" the capture-era game -- that cost season 2024
    1,775 games before it was caught (2026-08-27).
    """
    rows = [
        {
            "game_pbp_id": 4491801,
            "year": 2018,
            "inning": 1,
            "inning_top_bot": "top",
            "batting": "A",
            "fielding": "B",
            "score": "0-0",
            "description": "Smith, J. singled to left field.",
        }
    ]
    assert build_legacy_payload(rows)["game_key"] == "g4491801"
    bridge = [dict(rows[0], contest_id="6357953")]
    p = build_legacy_payload(bridge)
    assert p["game_key"] == "6357953" and p["game_pbp_id"] == 4491801
