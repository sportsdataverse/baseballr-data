"""Legacy R-era adapter: ``ncaa/game_pbp/json`` + ``ncaa/contest_pbp/json`` ->
the SAME parsed payload shape stage 03 writes for captured bundles.

The R era (2012-2023, 102,963 games) persisted one JSON per game holding
R-parsed play rows that preserve the raw ``description`` text plus
inning/top-bot/batting/fielding and the running away-home ``score`` -- exactly
the base fields sdv-py's ``decompose_college_baseball_plays`` consumes. This
adapter re-parses those rows through the graduated decomposition seam, so
legacy games resolve into the IDENTICAL 30-column pbp the new capture path
produces, with **zero re-scraping**. Families the R era never captured
(linescore / team / player / situational stats) are empty; ``source``
distinguishes the eras downstream.

Game keys: the legacy trees are keyed by the old ``/game/play_by_play/{id}``
``game_pbp_id``; the newer ``contest_pbp`` files (the bridge era) also carry a
modern ``contest_id``. These are two DIFFERENT id namespaces that OVERLAP
numerically -- legacy game ids run 4,279,874..5,424,024 and the 2024 season's
contest ids run 4,491,801..5,336,815, entirely inside that band. A bare
``game_pbp_id`` key therefore collides with real contest ids, and the collision
is silent: stage 03 sees the key taken and "skips" the capture-era game, which
cost season 2024 1,775 games before this was caught on 2026-08-27.

So a ``game_pbp_id`` key is prefixed ``g``; a ``contest_id`` key stays bare
(it IS a contest id, and the capture era writes the same key for the same game).
Both ids remain available as payload fields regardless of which one keys the file.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

__all__ = ["build_legacy_payload", "iter_legacy_games", "read_legacy_game"]


def iter_legacy_games(root: "str | Path", year: "Optional[int]" = None):
    """Yield (path, rows) for every legacy per-game JSON, newest tree first.

    ``contest_pbp`` (bridge era, has contest_id) is yielded before
    ``game_pbp``; a ``game_pbp_id`` seen in contest_pbp is skipped in
    game_pbp so a bridge-era game is emitted once, with its contest_id.
    """
    root = Path(root)
    seen: "set[int]" = set()
    for tree in ("contest_pbp", "game_pbp"):
        d = root / "ncaa" / tree / "json"
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.json")):
            try:
                rows = json.loads(p.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001 -- one bad file must not kill a sweep
                continue
            if not isinstance(rows, list) or not rows:
                continue
            gp = rows[0].get("game_pbp_id")
            if gp in seen:
                continue
            if gp is not None:
                seen.add(gp)
            if year is not None and rows[0].get("year") != year:
                continue
            yield p, rows


def read_legacy_game(path: "str | Path") -> "list[dict]":
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _final_score(rows: "list[dict]") -> "tuple[Optional[int], Optional[int]]":
    for r in reversed(rows):
        s = str(r.get("score") or "")
        m = s.strip()
        if m and "-" in m:
            a, _, h = m.partition("-")
            if a.strip().isdigit() and h.strip().isdigit():
                return int(a), int(h)
    return None, None


def build_legacy_payload(rows: "list[dict]") -> "dict[str, Any]":
    """One legacy per-game row list -> the stage-03 payload dict (pure, offline)."""
    from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays

    meta = rows[0]
    gp_id = meta.get("game_pbp_id")
    contest_id = meta.get("contest_id")  # bridge-era files only
    # "g" namespaces the legacy game-id space away from contest ids (see the
    # module docstring -- the ranges overlap and a bare key silently collides)
    game_key = str(contest_id) if contest_id else f"g{gp_id}"
    year = meta.get("year")

    # away team bats the top of an inning; derive the away/home identities from
    # the first top/bot rows rather than trusting column order
    away = next((r["batting"] for r in rows if r.get("inning_top_bot") == "top"), None)
    home = next((r["batting"] for r in rows if r.get("inning_top_bot") == "bot"), None)
    fa, fh = _final_score(rows)

    # the R-era scrape kept the inning run-summary rows ("R: 1 H: 2 LOB: 1")
    # the modern HTML parser filters out -- they are not plays
    rows = [r for r in rows if not re.match(r"^\s*R:\s*\d", str(r.get("description") or ""))]
    base = [
        {
            "contest_id": game_key,
            "inning": int(r["inning"]) if str(r.get("inning") or "").isdigit() else None,
            "inning_top_bot": r.get("inning_top_bot"),
            "batting": r.get("batting"),
            "fielding": r.get("fielding"),
            "score": r.get("score"),
            "description": r.get("description") or "",
        }
        for r in rows
    ]
    pbp = decompose_college_baseball_plays(base).to_dicts()

    return {
        "game_key": game_key,
        "contest_id": str(contest_id) if contest_id else None,
        "game_pbp_id": gp_id,
        "season": year,
        "source": "legacy_r",
        "espn_game_id": None,
        "game_date": meta.get("game_date"),
        "location": meta.get("location"),
        "attendance": meta.get("attendance"),
        "teams": [
            {"team": away, "home_away": "away", "final": fa, "espn_game_id": None},
            {"team": home, "home_away": "home", "final": fh, "espn_game_id": None},
        ],
        "pbp": pbp,
        "linescore": [],
        "team_stats": [],
        "situational_stats": [],
        "player_stats": {},
    }
