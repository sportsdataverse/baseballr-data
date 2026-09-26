"""Offline capture tests -- injected fetch_fn, tmp out_dir, real fixture (no network)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from ncaa_pbp.capture import bundle_path, capture_contest, capture_season, is_captured

FIX = Path(__file__).resolve().parent / "fixtures" / "mba_pbp_6357953.html"


def _real_pbp() -> str:
    return FIX.read_text(encoding="utf-8")


def _fetch(pbp: str, box: str = "<html>box</html>"):
    def f(path: str) -> str:
        return pbp if "play_by_play" in path else box

    return f


def test_capture_contest_writes_bundle(tmp_path: Path) -> None:
    res = capture_contest(_fetch(_real_pbp()), "6357953", tmp_path)
    assert res == "captured"
    p = bundle_path("6357953", tmp_path)
    assert p.exists()
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        bundle = json.load(fh)
    assert bundle["contest_id"] == "6357953"
    assert 'class="table"' in bundle["play_by_play"].lower()
    assert bundle["box_score"] is not None
    assert "captured_at" in bundle


def test_all_game_tabs_are_bundled(tmp_path: Path) -> None:
    import re

    def fetch(path: str) -> str:
        if "play_by_play" in path:
            return _real_pbp()
        m = re.search(r"contests/\d+/(\w+)", path)
        return f"<html>TAB:{m.group(1)}" if m else "x"

    assert capture_contest(fetch, "6357953", tmp_path) == "captured"
    with gzip.open(bundle_path("6357953", tmp_path), "rt", encoding="utf-8") as fh:
        b = json.load(fh)
    for tab in ("box_score", "team_stats", "individual_stats", "situational_stats"):
        assert b[tab] == f"<html>TAB:{tab}", tab


def test_capture_is_idempotent(tmp_path: Path) -> None:
    fetch = _fetch(_real_pbp())
    assert capture_contest(fetch, "6357953", tmp_path) == "captured"
    assert is_captured("6357953", tmp_path)
    assert capture_contest(fetch, "6357953", tmp_path) == "skipped"


def test_null_tab_is_not_captured_and_is_repaired_alone(tmp_path: Path) -> None:
    """A tab refused through a Terms lockout is stored null. That bundle used to
    count as captured forever -- the retry passes and publish gate never saw it."""

    def refuse_team_stats(path: str) -> str:
        if path.endswith("team_stats"):
            raise RuntimeError("stats.ncaa.org terms gate not accepted")
        return _real_pbp() if "play_by_play" in path else f"<html>{path}"

    assert capture_contest(refuse_team_stats, "6357953", tmp_path) == "captured"
    assert not is_captured("6357953", tmp_path)  # partial: stage 02 must come back for it

    calls: "list[str]" = []

    def ok(path: str) -> str:
        calls.append(path)
        return f"<html>{path}"

    assert capture_contest(ok, "6357953", tmp_path) == "captured"
    assert calls == ["contests/6357953/team_stats"]  # only the hole; pbp + good tabs kept
    assert is_captured("6357953", tmp_path)
    with gzip.open(bundle_path("6357953", tmp_path), "rt", encoding="utf-8") as fh:
        b = json.load(fh)
    assert 'class="table"' in b["play_by_play"].lower()  # the stored pbp survived the repair
    assert b["box_score"] == "<html>contests/6357953/box_score"
    assert capture_contest(ok, "6357953", tmp_path) == "skipped"


def test_unreadable_bundle_is_refetched(tmp_path: Path) -> None:
    """A write cut short by a kill must not pass as captured."""
    p = bundle_path("6357953", tmp_path)
    p.write_bytes(gzip.compress(b'{"contest_id": "6357')[:-8])  # truncated gzip
    assert not is_captured("6357953", tmp_path)
    assert capture_contest(_fetch(_real_pbp()), "6357953", tmp_path) == "captured"
    assert is_captured("6357953", tmp_path)
    assert not list(tmp_path.glob("*.tmp"))  # the atomic write left no temp file


def test_capture_rejects_stub(tmp_path: Path) -> None:
    # a tiny/table-less body is not real content
    assert capture_contest(_fetch("NCAA Statistics"), "9", tmp_path) == "failed"
    assert not is_captured("9", tmp_path)


def test_capture_counts_fetch_failure_as_failed(tmp_path: Path) -> None:
    def boom(path: str) -> str:
        raise RuntimeError("transport down")

    assert capture_contest(boom, "9", tmp_path) == "failed"


def test_capture_season_stats_and_breaker(tmp_path: Path) -> None:
    real = _real_pbp()

    import re

    def flaky(path: str) -> str:
        # contest ids 1..3 real, the rest stub -> exercises captured + failed.
        # capture_contest fetches "contests/{id}/play_by_play" (no leading slash).
        m = re.search(r"contests/(\d+)/", path)
        return real if m and m.group(1) in {"1", "2", "3"} else "x"

    stats = capture_season(["1", "2", "3", "8", "9"], flaky, tmp_path, max_consecutive_failures=25)
    assert stats["captured"] == 3
    assert stats["failed"] == 2

    # breaker trips on a storm
    with pytest.raises(RuntimeError, match="breaker tripped"):
        capture_season(
            [str(i) for i in range(100, 130)],
            lambda p: "x",
            tmp_path,
            max_consecutive_failures=5,
        )
