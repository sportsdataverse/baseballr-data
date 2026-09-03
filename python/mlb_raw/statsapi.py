"""Stage 02 -- one statsapi ``feed/live`` payload per game.

``GET /api/v1.1/game/{gamePk}/feed/live`` is the whole game in one request.
Measured containment (2024-07-06, gamePk 746694): ``/game/{pk}/playByPlay``
minus its ``copyright`` key is **byte-identical** to ``liveData.plays``, and
``/game/{pk}/linescore`` to ``liveData.linescore``; ``/game/{pk}/boxscore``
matches except under ``teams`` (the standalone endpoint re-hydrates season
stats as-of fetch time, so it is not reproducible -- another reason
``feed/live`` is the canonical capture). So one request per game replaces
four, and nothing is lost.

The payload is stored whole. No allowlist: ``liveData.plays.allPlays[].playEvents``
is 69% of the compressed bytes, and it is the only source of non-pitch events
(substitutions, pickoffs, mound visits, replay reviews) and of statsapi's own
pitch-call vocabulary -- Savant carries neither.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from mlb_raw import core


def _validate(payload: object) -> Optional[str]:
    """Reject anything that is not a completed game's real feed."""
    if not isinstance(payload, dict):
        return "not a dict"
    if not payload.get("gamePk"):
        return "no gamePk"
    live = payload.get("liveData") or {}
    plays = (live.get("plays") or {}).get("allPlays") or []
    if not plays:
        return "zero plays"
    if not (live.get("boxscore") or {}).get("teams"):
        return "no boxscore teams"
    if len(core.canonical_json(payload)) < core.MIN_STATSAPI_BYTES:
        return "payload below the minimum-real-game size"
    return None


def capture_game(root: Path, season: int, game_pk: int, *, force: bool = False) -> "Optional[dict]":
    """Fetch + persist one game. Returns the manifest fragment, or ``None`` on a refusal."""
    from sportsdataverse.mlb.mlb_api_extra import mlb_pbp_live

    out = core.statsapi_path(root, season, game_pk)
    if not force and core.already_captured(out, min_bytes=core.MIN_STATSAPI_BYTES // 4):
        # Re-hash on the resume path too: the manifest doubles as the integrity
        # index, so a resumed run must not leave a captured game with a blank sha.
        return {
            "statsapi_path": core.rel(root, out),
            "statsapi_bytes": out.stat().st_size,
            "statsapi_sha256": core.sha256_of(out),
        }
    payload = mlb_pbp_live(game_pk)
    core.pace("SDV_MLB_RAW_STATSAPI_SLEEP", 0.15)
    n = core.persist_json(out, payload, _validate)
    if n is None:
        return None
    return {
        "statsapi_path": core.rel(root, out),
        "statsapi_bytes": n,
        "statsapi_sha256": core.sha256_of(out),
    }


def run(
    season: int,
    root: Path,
    *,
    game_types: "tuple[str, ...]" = core.DEFAULT_GAME_TYPES,
    limit: Optional[int] = None,
    force: bool = False,
    commit_every: int = 0,
) -> int:
    manifest = core.read_manifest(root, season)
    if not manifest:
        raise SystemExit(f"no manifest for {season} -- run stage 01 first")
    todo = [
        r
        for r in manifest.values()
        if r["game_type"] in game_types
        and r["status_code"] == "F"
        and (
            force
            or core.outstanding(
                root, r, "statsapi", core.statsapi_path(root, season, int(r["game_pk"]))
            )
        )
    ]
    todo.sort(key=lambda r: (r["game_date"], int(r["game_pk"])))
    todo = core.head(todo, limit)
    print(
        f"statsapi {season}: {len(todo)} games to capture (of {len(manifest)} in manifest)",
        flush=True,
    )

    done = refused = 0
    for i, r in enumerate(todo, 1):
        pk = int(r["game_pk"])
        frag = capture_game(root, season, pk, force=force)
        if frag is None:
            refused += 1
            print(
                f"  [{i}/{len(todo)}] {pk} {r['game_date']}: REFUSED (empty/invalid payload, nothing written)",
                flush=True,
            )
        else:
            manifest[pk].update(frag)
            done += 1
            if i % 25 == 0 or i == len(todo):
                print(
                    f"  [{i}/{len(todo)}] {pk} {r['game_date']} ok={done} refused={refused}",
                    flush=True,
                )
        if commit_every and i % commit_every == 0:
            core.write_manifest(root, season, manifest.values())
            core.refresh_index(root)
            core.git_commit(
                root,
                [
                    root / "statsapi" / str(season),
                    core.manifest_path(root, season),
                    core.index_path(root),
                ],
                f"data(mlb-raw): statsapi feed/live {season} ({i}/{len(todo)})",
            )
    core.write_manifest(root, season, manifest.values())
    core.refresh_index(root)
    print(f"statsapi {season}: captured={done} refused={refused}", flush=True)
    return refused


def main(argv: "Optional[list[str]]" = None) -> int:
    ap = argparse.ArgumentParser(description="Stage 02 -- MLB statsapi feed/live per-game capture")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=None)
    ap.add_argument(
        "--game-types",
        default=",".join(core.DEFAULT_GAME_TYPES),
        help="comma-separated statsapi gameType codes (default R,F,D,L,W -- regular + postseason)",
    )
    ap.add_argument(
        "--limit", type=core.nonneg_int, default=None, help="stop after N games (chunked runs)"
    )
    ap.add_argument("--force", action="store_true", help="re-fetch games already on disk")
    ap.add_argument(
        "--commit-every", type=int, default=0, help="git-commit every N games (0 = only at the end)"
    )
    a = ap.parse_args(argv)
    root = core.raw_root(a.root)
    refused = run(
        a.season,
        root,
        game_types=tuple(t.strip() for t in a.game_types.split(",") if t.strip()),
        limit=a.limit,
        force=a.force,
        commit_every=a.commit_every,
    )
    return 1 if refused else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
