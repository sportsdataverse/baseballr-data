"""Capture raw NCAA baseball/softball pbp bundles from stats.ncaa.org --
idempotent + resumable.

Fetches ``/contests/{id}/play_by_play`` (+ ``box_score``) via an injectable
``fetch_fn`` (live: a held ``NcaaFetcher.with_browser`` session) and writes one
gzipped JSON bundle per contest to ``{out_dir}/json/{id}.json.gz`` -- the tree the
``-data`` ingest reads. Resume is file-exists based (Ctrl-C safe). A consecutive-
failure breaker hard-stops a ban/challenge storm instead of grinding.

Sport-agnostic: the same capture serves baseball (MBA) and softball (WSB) -- both
render the per-inning ``<table class="table">`` pbp page. The
:func:`sportsdataverse.baseball.college_baseball.parse_college_baseball_ncaa_pbp`
parser (shipped in sdv-py) consumes these bundles.
"""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

FetchFn = Callable[[str], str]

_MIN_PBP_BYTES = 15_000  # a real baseball/softball pbp page is ~45-55 KB; a stub/ban is < 2 KB


def bundle_path(contest_id: "str | int", out_dir: "str | Path") -> Path:
    return Path(out_dir) / "json" / f"{contest_id}.json.gz"


def is_captured(contest_id: "str | int", out_dir: "str | Path") -> bool:
    """Resume predicate -- a bundle already on disk is skipped."""
    return bundle_path(contest_id, out_dir).exists()


def _looks_real(html: "Optional[str]") -> bool:
    # a real pbp page renders innings as <table class="table">; a challenge stub /
    # ban page is tiny and table-less.
    return bool(html) and len(html) >= _MIN_PBP_BYTES and 'class="table"' in html.lower()


def capture_contest(fetch_fn: FetchFn, contest_id: "str | int", out_dir: "str | Path") -> str:
    """Fetch + persist one contest bundle.

    Returns ``"skipped"`` (already captured), ``"captured"``, or ``"failed"``
    (fetch raised, or the pbp page was not real content).
    """
    if is_captured(contest_id, out_dir):
        return "skipped"
    try:
        pbp = fetch_fn(f"contests/{contest_id}/play_by_play")
    except Exception:  # noqa: BLE001 - any transport failure = a failed capture, breaker counts it
        return "failed"
    if not _looks_real(pbp):
        return "failed"
    try:
        box = fetch_fn(f"contests/{contest_id}/box_score")
    except Exception:  # noqa: BLE001 - box is best-effort; pbp already landed
        box = None
    path = bundle_path(contest_id, out_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(
            {
                "contest_id": str(contest_id),
                "play_by_play": pbp,
                "box_score": box,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            },
            fh,
        )
    return "captured"


def capture_season(
    contest_ids: Iterable["str | int"],
    fetch_fn: FetchFn,
    out_dir: "str | Path",
    *,
    max_contests: "Optional[int]" = None,
    max_consecutive_failures: int = 25,
) -> "dict[str, int]":
    """Capture every not-yet-captured contest. Idempotent; hard-stops on a storm.

    Args:
        contest_ids: from ``discover.discover_season``.
        fetch_fn: ``(path) -> html`` (hold one browser session -- no per-call relaunch).
        out_dir: repo root for the raw ``json/`` tree.
        max_contests: stop after this many NEW captures (chunking; None = all).
        max_consecutive_failures: trip the breaker after this many failures in a row.

    Returns:
        ``{"captured": n, "skipped": n, "failed": n}``.

    Raises:
        RuntimeError: the failure breaker tripped (likely a ban / unsolved
            challenge storm) -- raised loudly so a launcher exits non-zero.
    """
    stats = {"captured": 0, "skipped": 0, "failed": 0}
    consecutive = 0
    for contest_id in contest_ids:
        if max_contests is not None and stats["captured"] >= max_contests:
            break
        result = capture_contest(fetch_fn, contest_id, out_dir)
        stats[result] += 1
        consecutive = consecutive + 1 if result == "failed" else 0
        if consecutive >= max_consecutive_failures:
            raise RuntimeError(
                f"{consecutive} consecutive failures -- breaker tripped "
                f"(likely a ban / unsolved-challenge storm); captured "
                f"{stats['captured']} before stopping"
            )
    return stats
