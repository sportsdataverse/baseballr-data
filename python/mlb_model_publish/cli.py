from __future__ import annotations

import argparse

from .artifacts import upload_artifacts
from .builders import build_tag, write_card


def _seasons(spec: str) -> list[int]:
    """Parse ``2015:2025`` or ``2024`` into a season list."""
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(spec)]


_TAGS = {
    "game-state": "mlb_game_state",
    "hitting": "mlb_hitting_models",
    "fielding": "mlb_fielding_models",
    "pitching": "mlb_pitching_models",
}


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="mlb_model_publish")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for cmd, tag in _TAGS.items():
        p = sub.add_parser(cmd, help=f"build + publish the {tag} release")
        p.add_argument(
            "--seasons",
            required=True,
            help="a season (2024) or an inclusive range (2015:2025)",
        )
        p.add_argument("--out", default=f"out/{tag}")
        p.add_argument("--tag", default=tag)
        p.add_argument("--repo", default="sportsdataverse/sportsdataverse-data")
        if cmd != "game-state":
            p.add_argument(
                "--statcast-cache",
                default=None,
                help="shared per-season Savant parquet cache (default: $SDV_MLB_STATCAST_CACHE or .mlb_statcast_cache)",
            )
        p.add_argument("--dry-run", action="store_true")
        p.add_argument(
            "--build-only",
            action="store_true",
            help="write parquet + card, skip the upload",
        )
    return ap


def _make_compute(cmd: str, args):
    """Bind the real compute for a subcommand (hermetic tests bypass main)."""
    from . import computes

    cache = getattr(args, "statcast_cache", None)
    if cmd == "game-state":
        return computes.compute_game_state
    if cmd == "fielding":
        return lambda season: computes.compute_fielding(season, cache_dir=cache)
    if cmd == "pitching":
        return lambda season: computes.compute_pitching(season, cache_dir=cache)

    # hitting: accumulate expected-stats history season-ascending so the
    # projection's as-of training window never re-pulls Savant.
    import polars as pl

    history_frames: list[pl.DataFrame] = []

    def compute(season: int):
        history = pl.concat(history_frames[-3:]) if history_frames else None
        out = computes.compute_hitting(season, cache_dir=cache, history=history)
        if "mlb_expected_stats" in out and out["mlb_expected_stats"].height > 0:
            history_frames.append(out["mlb_expected_stats"])
        return out

    return compute


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    tag = args.tag
    results = build_tag(_TAGS[args.cmd], _seasons(args.seasons), args.out, compute=_make_compute(args.cmd, args))
    write_card(_TAGS[args.cmd], results, args.out)
    total = sum(r["rows"] for r in results)
    if args.build_only:
        print(f"{tag}: built seasons={len(results)} rows={total} -> {args.out} (build-only)")
        return 0
    res = upload_artifacts(
        args.out,
        tag,
        args.repo,
        pattern="mlb_*.*",
        dry_run=args.dry_run,
    )
    created = " (created release)" if res.get("created_release") else ""
    print(
        f"publish: seasons={len(results)} rows={total} uploaded={res['uploaded']} "
        f"-> {args.repo}:{res['tag']}" + created + (" (dry-run)" if args.dry_run else "")
    )
    return 0
