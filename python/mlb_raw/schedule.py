"""Stage 01 -- season schedule + the game manifest.

Writes two things per season:

* ``schedule/{season}.json.gz`` -- the statsapi ``/api/v1/schedule`` payload,
  whole and unpruned. The provenance record.
* ``manifest/{season}.csv`` -- one row per game, flat, committed. This is the
  file a downstream consumer reads over ``raw.githubusercontent.com`` to
  enumerate game ids **without listing a directory** (GitHub has no directory
  listing over the raw host). It also carries per-game byte counts and
  sha256s, so a consumer can verify what it fetched, and stage 02/03 use it
  as their work list.

Re-running merges: capture columns already recorded for a game are preserved,
schedule columns are refreshed from the live schedule.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from mlb_raw import core


def _games(payload: dict) -> "list[dict]":
    return [g for d in (payload.get("dates") or []) for g in (d.get("games") or [])]


def _validate_schedule(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return "not a dict"
    if not _games(payload):
        return "zero games"
    return None


def _row(game: dict, season: int) -> dict:
    teams = game.get("teams") or {}
    home = (teams.get("home") or {}).get("team") or {}
    away = (teams.get("away") or {}).get("team") or {}
    return {
        "game_pk": int(game["gamePk"]),
        "game_date": (game.get("officialDate") or (game.get("gameDate") or "")[:10]),
        "season": season,
        "game_type": game.get("gameType") or "",
        "status_code": (game.get("status") or {}).get("codedGameState") or "",
        "doubleheader": game.get("doubleHeader") or "",
        "game_number": game.get("gameNumber") or "",
        "home_id": home.get("id") or "",
        "home_abbr": home.get("abbreviation") or "",
        "away_id": away.get("id") or "",
        "away_abbr": away.get("abbreviation") or "",
        "venue_id": (game.get("venue") or {}).get("id") or "",
    }


def build(season: int, root: Path, *, force: bool = False) -> "tuple[Path, Path, int]":
    """Fetch the season schedule, persist it, and (re)build the manifest."""
    from sportsdataverse.mlb.mlb_api_extra import mlb_schedule

    sched_p = core.schedule_path(root, season)
    if force or not core.already_captured(sched_p, min_bytes=10_000):
        payload = mlb_schedule(
            start_date=f"{season}-01-01",
            end_date=f"{season}-12-31",
            sport_id=1,
            hydrate="team,venue",
        )
        if core.persist_json(sched_p, payload, _validate_schedule) is None:
            raise SystemExit(f"schedule {season}: refused to persist an empty/invalid payload")
    else:
        import json

        payload = json.loads(core.read_gz_text(sched_p))

    prior = core.read_manifest(root, season)
    # Dedupe on game_pk: /api/v1/schedule lists a suspended/resumed game under
    # BOTH its original and its completion date, so a full-year pull carries
    # duplicate gamePks (39 of 2,998 in 2024). One row per game, last wins --
    # without this the first manifest write and every subsequent one disagree.
    rows: "dict[int, dict]" = {}
    for g in _games(payload):
        r = _row(g, season)
        old = prior.get(r["game_pk"], {})
        for k in (
            "statsapi_path",
            "statsapi_bytes",
            "statsapi_sha256",
            "savant_path",
            "savant_rows",
            "savant_bytes",
            "savant_sha256",
        ):
            r[k] = old.get(k, "")
        rows[r["game_pk"]] = r
    man_p = core.write_manifest(root, season, rows.values())
    core.refresh_index(root)
    return sched_p, man_p, len(rows)


def main(argv: "Optional[list[str]]" = None) -> int:
    ap = argparse.ArgumentParser(description="Stage 01 -- MLB season schedule + game manifest")
    ap.add_argument("--season", type=int, required=True, help="calendar year")
    ap.add_argument(
        "--root", default=None, help="capture root (default $SDV_MLB_RAW_ROOT or <repo>/mlb/raw)"
    )
    ap.add_argument(
        "--force", action="store_true", help="re-fetch the schedule even if already on disk"
    )
    ap.add_argument(
        "--commit", action="store_true", help="git-commit the schedule + manifest when done"
    )
    a = ap.parse_args(argv)

    root = core.raw_root(a.root)
    sched_p, man_p, n = build(a.season, root, force=a.force)
    print(f"season {a.season}: schedule={sched_p} manifest={man_p} games={n}")
    if a.commit:
        core.git_commit(
            root,
            [sched_p, man_p, core.index_path(root)],
            f"data(mlb-raw): schedule + manifest for {a.season} ({n} games)",
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
