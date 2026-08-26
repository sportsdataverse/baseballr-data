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
