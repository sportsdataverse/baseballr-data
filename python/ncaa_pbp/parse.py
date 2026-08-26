"""Stage 03: captured bundle -> parsed + enriched game payload (``ncaa/json/``).

One gzip JSON per game, the MBB/WBB/MFB parsed-payload shape, produced from a
5-tab capture bundle (``ncaa/raw/{season}/{contest_id}.json.gz``) via the
sdv-py college-baseball parsers::

    {
      "game_key": "6526442",            # contest_id (capture era) / game_pbp_id (legacy)
      "contest_id": "6526442" | null,
      "game_pbp_id": null | 4334695,    # legacy R-era stats.ncaa.org game id
      "season": 2026,                   # calendar year (spring 2026)
      "source": "capture" | "legacy_r",
      "espn_game_id": "401..." | null,  # stage-06 xwalk (null when unmatched/not built)
      "teams": [...],                   # away/home, final scores
      "pbp": [...],                     # 30-col PBP_SCHEMA rows
      "linescore": [...], "team_stats": [...], "situational_stats": [...],
      "player_stats": {category: [...]},
    }

The legacy R-era trees (2012-2023) produce the SAME shape through
:mod:`ncaa_pbp.legacy` -- pbp via the graduated decomposition seam, the other
families null -- so both eras resolve into identical downstream datasets.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Optional

__all__ = ["build_capture_payload", "parsed_path", "write_payload"]


def parsed_path(root: "str | Path", game_key: "str | int") -> Path:
    return Path(root) / "ncaa" / "json" / f"{game_key}.json.gz"


def write_payload(root: "str | Path", payload: "dict[str, Any]") -> Path:
    out = parsed_path(root, payload["game_key"])
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
        json.dump(payload, fh, default=str)
    tmp.replace(out)
    return out


def _teams_block(linescore_rows: "list[dict]", espn_game_id: "Optional[str]") -> "list[dict]":
    out = []
    for r in linescore_rows:
        if not r.get("team"):
            continue
        out.append(
            {
                "team": r["team"],
                "home_away": r.get("home_away"),
                "final": r.get("final"),
                "espn_game_id": espn_game_id,
            }
        )
    # linescore repeats one row per period; keep one entry per team
    seen: "set[str]" = set()
    uniq = []
    for t in out:
        if t["team"] not in seen:
            seen.add(t["team"])
            uniq.append(t)
    return uniq


def build_capture_payload(
    bundle: "dict[str, Any]",
    season: int,
    *,
    espn_game_id: "Optional[str]" = None,
) -> "dict[str, Any]":
    """Parse one captured 5-tab bundle into the payload dict (pure, offline)."""
    from sportsdataverse.baseball.college_baseball import (
        parse_college_baseball_ncaa_linescore,
        parse_college_baseball_ncaa_pbp,
        parse_college_baseball_ncaa_player_stats,
        parse_college_baseball_ncaa_situational_stats,
        parse_college_baseball_ncaa_team_stats,
    )

    cid = str(bundle["contest_id"])
    box_html = bundle.get("box_score") or ""
    linescore = parse_college_baseball_ncaa_linescore(box_html, contest_id=cid).to_dicts()
    pbp = parse_college_baseball_ncaa_pbp(
        bundle.get("play_by_play") or "", contest_id=cid
    ).to_dicts()
    teams = _teams_block(linescore, espn_game_id)
    if not teams and pbp:
        # pbp-only bundle: away bats the top of an inning; finals from the last score
        away = next((r["batting"] for r in pbp if r.get("inning_top_bot") == "top"), None)
        home = next((r["batting"] for r in pbp if r.get("inning_top_bot") == "bot"), None)
        fa = next((r["score_away"] for r in reversed(pbp) if r.get("score_away") is not None), None)
        fh = next((r["score_home"] for r in reversed(pbp) if r.get("score_home") is not None), None)
        teams = [
            {"team": away, "home_away": "away", "final": fa, "espn_game_id": espn_game_id},
            {"team": home, "home_away": "home", "final": fh, "espn_game_id": espn_game_id},
        ]
    return {
        "game_key": cid,
        "contest_id": cid,
        "game_pbp_id": None,
        "season": season,
        "source": "capture",
        "espn_game_id": espn_game_id,
        "teams": teams,
        "pbp": pbp,
        "linescore": linescore,
        "team_stats": parse_college_baseball_ncaa_team_stats(
            bundle.get("team_stats") or "", contest_id=cid
        ).to_dicts(),
        "situational_stats": {
            cat: frame.to_dicts()
            for cat, frame in parse_college_baseball_ncaa_situational_stats(
                bundle.get("situational_stats") or "", contest_id=cid
            ).items()
        },
        "player_stats": {
            cat: frame.to_dicts()
            for cat, frame in parse_college_baseball_ncaa_player_stats(
                bundle.get("individual_stats") or "", contest_id=cid
            ).items()
        },
    }
