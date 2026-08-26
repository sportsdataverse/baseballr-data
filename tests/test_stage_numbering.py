"""Stage-numbering gate (mirrors the ncaa-mfb-football-raw / hoops twins).

Each numbered ``ncaa_baseball_NN_*`` shim delegates to its working
``ncaa_pbp.*`` module, each ``scripts/run_NN_*.sh`` invokes its OWN shim, and
RUNBOOK.md lists every stage + launcher. **03 (parse) is a deliberate,
parent-owned HOLE** -- it is built separately on ``feat/ncaa-baseball-parse``;
this suite asserts the hole stays open rather than renumbering around it.
No network, no repo-tree writes.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# (number, shim module, delegate module)
STAGES = (
    ("01", "ncaa_baseball_01_schedules_scrape", "ncaa_pbp.schedules"),
    ("02", "ncaa_baseball_02_games_scrape", "ncaa_pbp.games"),
    ("04", "ncaa_baseball_04_rosters_scrape", "ncaa_pbp.rosters"),
    ("05", "ncaa_baseball_05_datasets_build", "ncaa_pbp.datasets"),
    ("06", "ncaa_baseball_06_xwalk_build", "ncaa_pbp.xwalk"),
)
PARENT_OWNED_HOLES = {"03"}  # parse -- built on feat/ncaa-baseball-parse
OFFLINE_STAGES = {"05", "06"}  # no NCAA transport needed


def test_stage_set_and_ordering() -> None:
    shims = sorted(p.name for p in REPO.glob("python/ncaa_baseball_[0-9][0-9]_*.py"))
    numbers = [s.split("_")[2] for s in shims]
    assert numbers == sorted(numbers)  # ordering
    assert set(numbers) == {num for num, _, _ in STAGES}
    assert not (set(numbers) & PARENT_OWNED_HOLES)  # the 03 hole stays open here


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_shim_delegates(monkeypatch, num, shim, delegate) -> None:
    mod = importlib.import_module(shim)
    seen: "list[list[str]]" = []
    monkeypatch.setattr(importlib.import_module(delegate), "main", lambda argv: seen.append(argv) or 7)
    assert mod.main(["--season", "2026"]) == 7
    assert seen == [["--season", "2026"]]


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_launcher_invokes_its_own_shim(num, shim, delegate) -> None:
    (script,) = sorted(REPO.glob(f"scripts/run_{num}_*.sh"))
    text = script.read_text(encoding="utf-8")
    assert f"python/{shim}.py" in text
    assert 'source "$(dirname "$0")/_env.sh"' in text
    # 05/06 need no NCAA transport: they must say so (OFFLINE=1).
    assert ("OFFLINE=1 source" in text) == (num in OFFLINE_STAGES)


@pytest.mark.parametrize("num,shim,delegate", STAGES)
def test_delegate_main_parses_argv(num, shim, delegate) -> None:
    """Every stage delegate is argparse-driven -- --help smoke, no work done."""
    mod = importlib.import_module(delegate)
    with pytest.raises(SystemExit) as ei:
        mod.main(["--help"])
    assert ei.value.code == 0


def test_runbook_lists_every_stage() -> None:
    text = (REPO / "RUNBOOK.md").read_text(encoding="utf-8")
    for _, shim, _ in STAGES:
        assert shim in text, shim
    for script in REPO.glob("scripts/run_[0-9][0-9]_*.sh"):
        assert re.search(rf"\b{re.escape(script.name)}\b", text), script.name
    assert "03" in text  # the parse hole is documented
