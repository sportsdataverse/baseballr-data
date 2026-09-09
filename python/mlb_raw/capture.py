"""Stage 02 -- capture: one feed/live payload per completed game.

``mlb/raw/{season}/{game_pk}.json.gz``. File-exists resumable, so a re-run after
a partial night costs nothing and re-fetches nothing -- the same contract the
NCAA stages hold.

One fetch yields pbp, pitches, all three box tables, officials and linescore, so
those datasets are free at build time rather than extra requests.

Measured 2026-09-09 through the ProxyBonanza pool: 0.68 s mean sequential,
11.4 games/s at 8 workers, 26.4 at 16, with zero rejections across ~150
requests. Default is 8; 16 is 26 req/s at one host and is not the polite default.
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import schedules, transport


def feed_url(game_pk: int) -> str:
    return f"{transport.STATSAPI}/v1.1/game/{game_pk}/feed/live"


def bundle_path(root: pathlib.Path, season: int, game_pk: int) -> pathlib.Path:
    return root / "mlb" / "raw" / str(season) / f"{game_pk}.json.gz"


def capture_one(root: pathlib.Path, season: int, game_pk: int, worker: int = 0) -> str:
    """Return 'skipped' | 'captured'. Raises on a real transport failure."""
    out = bundle_path(root, season, game_pk)
    if out.exists():
        return "skipped"
    body = transport.get(feed_url(game_pk), worker=worker)
    payload = json.loads(body)  # parse before writing: never persist a non-JSON body
    plays = ((payload.get("liveData") or {}).get("plays") or {}).get("allPlays")
    if plays is None:
        raise transport.TransportError(
            f"game {game_pk}: feed/live has no liveData.plays -- not a usable bundle"
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)
    tmp.rename(out)  # atomic: a killed run never leaves a half-written bundle
    return "captured"


def capture_season(
    root: pathlib.Path, season: int, *, limit: int = 0, workers: int = 0, game_types=None
) -> dict:
    pks = schedules.completed_game_pks(root, season, game_types or schedules.DEFAULT_TYPES)
    if not pks:
        raise transport.TransportError(
            f"season {season}: no completed games known. Run stage 01 first."
        )
    todo = [pk for pk in pks if not bundle_path(root, season, pk).exists()]
    if limit:
        todo = todo[:limit]
    workers = workers or transport.WORKERS
    stats = {"captured": 0, "skipped": len(pks) - len(todo), "failed": 0}
    if not todo:
        return stats

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(capture_one, root, season, pk, i): pk for i, pk in enumerate(todo)}
        done = 0
        for f in as_completed(futs):
            done += 1
            try:
                stats[f.result()] += 1
            except Exception as exc:  # noqa: BLE001 - one game must not kill the season
                stats["failed"] += 1
                print(f"  game {futs[f]}: {type(exc).__name__}: {str(exc)[:100]}")
            if done % 250 == 0:
                el = time.perf_counter() - t0
                print(f"  {done}/{len(todo)} in {el:.0f}s ({done / el:.1f} games/s)")
    stats["elapsed_s"] = round(time.perf_counter() - t0, 1)
    return stats


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--max-games", type=int, default=0, help="cap per run (0 = all)")
    ap.add_argument(
        "--workers", type=int, default=0, help=f"default {transport.WORKERS} (SDV_MLB_WORKERS)"
    )
    ap.add_argument(
        "--game-types",
        default=",".join(schedules.DEFAULT_TYPES),
        help="comma-separated statsapi gameType codes (default: regular season + "
        "postseason; spring training/exhibition/all-star excluded)",
    )
    a = ap.parse_args(argv)
    try:
        st = capture_season(
            pathlib.Path(a.root),
            a.season,
            limit=a.max_games,
            workers=a.workers,
            game_types=tuple(t for t in a.game_types.split(",") if t),
        )
    except transport.TransportError as exc:
        print(f"ERROR: {exc}")
        return 2
    print(f"capture season {a.season}: {st}")
    # A failure count is not a warning here: the bundle is missing and the
    # dataset built from it would be silently short.
    return 1 if st.get("failed") else 0
