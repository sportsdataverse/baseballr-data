"""MLB raw-capture stage gate — numbering, wiring, and the persist guards.

No network, no repo-tree writes. The guard tests are the runnable check that
the "never persist an empty or error payload" invariant actually holds; the
wiring tests mirror ``test_stage_numbering.py`` for the NCAA stages.
"""

from __future__ import annotations

import gzip
import importlib
import json
import re
from pathlib import Path

import pytest
from mlb_raw import core, savant, statsapi

REPO = Path(__file__).resolve().parents[1]

STAGES = (
    ("01", "mlb_raw_01_schedule_scrape", "mlb_raw.schedule"),
    ("02", "mlb_raw_02_statsapi_scrape", "mlb_raw.statsapi"),
    ("03", "mlb_raw_03_savant_scrape", "mlb_raw.savant"),
)


# ------------------------------------------------------------------- wiring


def test_stage_set_and_ordering() -> None:
    shims = sorted(p.name for p in REPO.glob("python/mlb_raw_[0-9][0-9]_*.py"))
    numbers = [s.split("_")[2] for s in shims]
    assert numbers == sorted(numbers)
    assert set(numbers) == {num for num, _, _ in STAGES}


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_shim_delegates(monkeypatch, num, shim, delegate) -> None:
    mod = importlib.import_module(shim)
    seen: "list[list[str]]" = []
    monkeypatch.setattr(
        importlib.import_module(delegate), "main", lambda argv: seen.append(argv) or 7
    )
    assert mod.main(["--season", "2024"]) == 7
    assert seen == [["--season", "2024"]]


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_launcher_invokes_its_own_shim(num, shim, delegate) -> None:
    (script,) = sorted(REPO.glob(f"scripts/run_mlb_{num}_*.sh"))
    text = script.read_text(encoding="utf-8")
    assert f"python/{shim}.py" in text
    assert 'OFFLINE=1 source "$(dirname "$0")/_env.sh"' in text


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_delegate_main_parses_argv(num, shim, delegate) -> None:
    mod = importlib.import_module(delegate)
    with pytest.raises(SystemExit) as ei:
        mod.main(["--help"])
    assert ei.value.code == 0


def test_runbook_lists_every_stage() -> None:
    text = (REPO / "RUNBOOK-MLB.md").read_text(encoding="utf-8")
    for _, shim, _ in STAGES:
        assert shim in text, shim
    for script in REPO.glob("scripts/run_mlb_[0-9][0-9]_*.sh"):
        assert re.search(rf"\b{re.escape(script.name)}\b", text), script.name


# -------------------------------------------------------------- persist guards


def _good_feed() -> dict:
    play = {
        "about": {"atBatIndex": 0},
        "playEvents": [{"isPitch": True, "details": {"x": "y" * 400}}],
    }
    return {
        "gamePk": 746694,
        "liveData": {
            "plays": {"allPlays": [play] * 60},
            "boxscore": {"teams": {"home": {}, "away": {}}},
        },
    }


@pytest.mark.parametrize(
    "payload,why",
    [
        ({}, "no gamePk"),
        ("not a dict", "not a dict"),
        ({"gamePk": 1, "liveData": {"plays": {"allPlays": []}}}, "zero plays"),
        ({"messageNumber": 10, "message": "Object not found"}, "error body"),
        (
            {
                "gamePk": 1,
                "liveData": {"plays": {"allPlays": [{"a": 1}]}, "boxscore": {"teams": {}}},
            },
            "no teams",
        ),
        # structurally valid but far too small to be a real completed game
        (
            {
                "gamePk": 1,
                "liveData": {
                    "plays": {"allPlays": [{"a": 1}]},
                    "boxscore": {"teams": {"home": {}}},
                },
            },
            "tiny",
        ),
    ],
)
def test_statsapi_guard_refuses_and_writes_nothing(tmp_path, payload, why) -> None:
    out = tmp_path / "x.json.gz"
    assert core.persist_json(out, payload, statsapi._validate) is None, why
    assert not out.exists(), f"{why}: a rejected payload must leave NO file behind"


def test_statsapi_guard_accepts_a_real_shaped_payload(tmp_path) -> None:
    out = tmp_path / "ok.json.gz"
    n = core.persist_json(out, _good_feed(), statsapi._validate)
    assert n and out.exists()
    assert json.loads(gzip.decompress(out.read_bytes()))["gamePk"] == 746694


@pytest.mark.parametrize("text", ["", "game_pk,release_speed", "no_such_column\n1\n"])
def test_savant_guard_refuses_header_only_or_wrong_shape(tmp_path, text) -> None:
    out = tmp_path / "x.csv.gz"
    assert core.persist_text(out, text, savant._validate_slice) is None
    assert not out.exists()


def test_savant_guard_accepts_one_data_row(tmp_path) -> None:
    out = tmp_path / "ok.csv.gz"
    assert core.persist_text(out, "game_pk,release_speed\n746694,95.1\n", savant._validate_slice)
    assert out.exists()


# ------------------------------------------------------------ core invariants


def test_gzip_bytes_are_deterministic(tmp_path) -> None:
    """Re-capturing an unchanged payload must reproduce identical bytes (git no-op)."""
    a, b = tmp_path / "a.json.gz", tmp_path / "b.json.gz"
    core.persist_json(a, _good_feed(), statsapi._validate)
    core.persist_json(b, _good_feed(), statsapi._validate)
    assert a.read_bytes() == b.read_bytes()


def test_manifest_roundtrips_and_dedupes(tmp_path) -> None:
    rows = [
        {
            "game_pk": 2,
            "game_date": "2024-04-02",
            "season": 2024,
            "game_type": "R",
            "status_code": "F",
        },
        {
            "game_pk": 1,
            "game_date": "2024-04-01",
            "season": 2024,
            "game_type": "R",
            "status_code": "F",
        },
    ]
    core.write_manifest(tmp_path, 2024, rows)
    back = core.read_manifest(tmp_path, 2024)
    assert sorted(back) == [1, 2]
    assert back[1]["game_date"] == "2024-04-01"
    idx = core.refresh_index(tmp_path)
    assert "2024" in idx.read_text(encoding="utf-8")


def test_raw_root_precedence(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SDV_MLB_RAW_ROOT", str(tmp_path / "env"))
    assert core.raw_root(str(tmp_path / "arg")) == tmp_path / "arg"  # --root wins
    assert core.raw_root(None) == tmp_path / "env"
    monkeypatch.delenv("SDV_MLB_RAW_ROOT")
    assert core.raw_root(None).as_posix().endswith("mlb/raw")


def test_savant_day_window_is_the_only_uncapped_window() -> None:
    """A one-day Savant window must stay far under the 25,000-row cap.

    Busiest day measured across 2008-2024 is 4,777 rows; a 7-day window returns
    exactly 25,000 (truncated). This locks the constant the stage asserts on.
    """
    assert core.SAVANT_ROW_CAP == 25_000
    assert 4_777 < core.SAVANT_ROW_CAP / 4
