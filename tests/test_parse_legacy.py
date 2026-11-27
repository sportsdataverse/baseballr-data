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


def test_bundle_with_no_pbp_or_box_writes_no_payload(tmp_path: Path) -> None:
    # 2016 Longwood-VMI: stats.ncaa.org renders the page shell with no pbp or box
    # table, so the payload would be a blank games row -- stage 03 skips it.
    import gzip
    import json

    from ncaa_pbp.parse import _capture_job, parsed_path

    page = (FIX / "mba_pbp_no_plays_294867.html").read_text(encoding="utf-8")
    raw = tmp_path / "ncaa" / "raw" / "2016" / "294867.json.gz"
    raw.parent.mkdir(parents=True)
    tabs = ("play_by_play", "box_score", "team_stats", "individual_stats", "situational_stats")
    with gzip.open(raw, "wt", encoding="utf-8") as fh:
        json.dump({"contest_id": "294867", **{t: page for t in tabs}}, fh)
    assert _capture_job((str(tmp_path), "294867", 2016, None, False, frozenset())) == ("294867", "empty")
    assert not parsed_path(tmp_path, "294867").exists()


def test_capture_replaces_a_legacy_payload_at_its_path_but_not_a_capture(tmp_path: Path) -> None:
    # 2023's legacy payloads are keyed by contest id -- the capture's own path. Skipping
    # on "file exists" left 84 re-captured games on legacy data; capture supersedes.
    import gzip
    import json

    from ncaa_pbp.parse import _capture_job, parsed_path, write_payload

    raw = tmp_path / "ncaa" / "raw" / "2023" / "6357953.json.gz"
    raw.parent.mkdir(parents=True)
    pbp = (FIX / "mba_pbp_6357953.html").read_text(encoding="utf-8")
    with gzip.open(raw, "wt", encoding="utf-8") as fh:
        json.dump({"contest_id": "6357953", "play_by_play": pbp}, fh)
    legacy = {"game_key": "6357953", "source": "legacy_r", "season": 2023, "game_pbp_id": 4491801}
    write_payload(tmp_path, legacy)
    assert _capture_job((str(tmp_path), "6357953", 2023, None, False, frozenset())) == ("6357953", "parsed")
    with gzip.open(parsed_path(tmp_path, "6357953"), "rt", encoding="utf-8") as fh:
        got = json.load(fh)
    assert got["source"] == "capture"
    assert got["game_pbp_id"] == 4491801  # the R-era join key survives the swap
    assert _capture_job((str(tmp_path), "6357953", 2023, None, False, frozenset())) == ("6357953", "skipped")


def test_capture_without_plays_keeps_the_legacy_plays(tmp_path: Path) -> None:
    import gzip
    import json

    from ncaa_pbp.parse import _capture_job, parsed_path, write_payload

    raw = tmp_path / "ncaa" / "raw" / "2023" / "294867.json.gz"
    raw.parent.mkdir(parents=True)
    page = (FIX / "mba_pbp_no_plays_294867.html").read_text(encoding="utf-8")
    with gzip.open(raw, "wt", encoding="utf-8") as fh:
        json.dump({"contest_id": "294867", "play_by_play": page}, fh)
    write_payload(tmp_path, {"game_key": "294867", "source": "legacy_r", "pbp": [{"inning": 1}]})
    assert _capture_job((str(tmp_path), "294867", 2023, None, False, frozenset())) == ("294867", "kept-legacy")
    with gzip.open(parsed_path(tmp_path, "294867"), "rt", encoding="utf-8") as fh:
        assert json.load(fh)["source"] == "legacy_r"


def test_truncated_payload_does_not_stop_the_sweep(tmp_path: Path) -> None:
    import gzip

    from ncaa_pbp.parse import _capture_job, parsed_path

    out = parsed_path(tmp_path, "9")
    out.parent.mkdir(parents=True)
    out.write_bytes(gzip.compress(b'{"source": "legacy_r"}')[:-6])  # cut off -> EOFError
    assert _capture_job((str(tmp_path), "9", 2023, None, False, frozenset())) == ("9", "skipped")


def _bundle(cid: str, *, pbp: bool = True) -> dict:
    """Real 2012 stats.ncaa.org pages (captured 2026-11-25/26, csrf tokens redacted)."""
    b = {"contest_id": cid, "box_score": (FIX / f"mba_box_{cid}.html").read_text(encoding="utf-8")}
    if pbp:
        b["play_by_play"] = (FIX / f"mba_pbp_{cid}.html").read_text(encoding="utf-8")
    return b


def _finals(payload: dict) -> dict:
    return {t["home_away"]: (t["team"], t["final"], t["final_source"]) for t in payload["teams"]}


def test_doubleheader_r_total_loses_to_the_pbp_backed_inning_sum() -> None:
    """755683 Morehead St. at Mercer, game 1 of a 02/17/2012 doubleheader: Morehead's innings
    0 0 0 1 5 0 0 0 0 = 6 and the pbp ends 6-22 (schedule: L 6-22), but the page's R column says 9,
    game 2's score."""
    p = build_capture_payload(_bundle("755683"), 2012)
    assert _finals(p) == {
        "away": ("Morehead St.", 6, "inning_sum"),
        "home": ("Mercer", 22, "linescore"),
    }


def test_inning_sum_needs_corroboration() -> None:
    """Without pbp the sum wins only on the schedule's word: a linescore missing an extra-inning
    column would sum short, so with no evidence the R total stays (flagged unconfirmed)."""
    bare = build_capture_payload(_bundle("755683", pbp=False), 2012)
    assert _finals(bare)["away"] == ("Morehead St.", 9, "linescore_unconfirmed")
    backed = build_capture_payload(
        _bundle("755683", pbp=False), 2012, schedule_pairs=frozenset({(6, 22)})
    )
    assert _finals(backed)["away"] == ("Morehead St.", 6, "inning_sum")


def test_home_team_batting_first_keeps_score_away_the_away_teams() -> None:
    """759907 Lehigh at San Jose St. (03/08/2012, final 4-8): the host batted first, so the pbp
    table reads "San Jose St. | Score | Lehigh" and its left score is the HOME team's."""
    p = build_capture_payload(_bundle("759907"), 2012)
    assert p["home_bats_first"] is True
    assert _finals(p) == {
        "away": ("Lehigh", 4, "linescore"),
        "home": ("San Jose St.", 8, "linescore"),
    }
    last = p["pbp"][-1]
    assert (last["score_away"], last["score_home"]) == (4, 8)
    assert p["pbp"][0]["inning_top_bot"] == "top" and p["pbp"][0]["batting"] == "San Jose St."


def test_away_team_batting_first_is_left_alone() -> None:
    p = build_capture_payload(_bundle("755683"), 2012)
    assert p["home_bats_first"] is False
    assert (p["pbp"][-1]["score_away"], p["pbp"][-1]["score_home"]) == (6, 22)


def test_legacy_final_comes_from_the_posted_schedule_when_names_line_up() -> None:
    """2022 g5197212 (Miles at Shorter): the page score column ends 3-14 because it misses two runs printed as
    doubled runner names; the posted score is 3-16."""
    f = ROOT / "ncaa" / "game_pbp" / "json" / "5197212.json"
    if not f.exists():
        pytest.skip("legacy tree not present (sparse checkout)")
    from ncaa_pbp.legacy import legacy_schedule

    rows = read_legacy_game(f)
    plain = build_legacy_payload(rows)
    assert [(t["final"], t["final_source"]) for t in plain["teams"]] == [(3, "score_column"), (14, "score_column")]
    posted = build_legacy_payload(rows, schedule=legacy_schedule(str(ROOT), 2022))
    assert [(t["team"], t["final"], t["final_source"]) for t in posted["teams"]] == [
        ("Miles", 3, "schedule"),
        ("Shorter", 16, "schedule"),
    ]


def test_schedule_final_orientation() -> None:
    """Swapped names (the listed home team batted first) take the swapped scores; other names take none."""
    from ncaa_pbp.legacy import _schedule_final

    row = ("Miles", 3, "Shorter", 16)
    assert _schedule_final(row, "Miles", "Shorter") == (3, 16)
    assert _schedule_final(row, "Shorter", "Miles") == (16, 3)
    assert _schedule_final(row, "Hitting", None) is None
    assert _schedule_final(("Miles", None, "Shorter", 16), "Miles", "Shorter") is None
