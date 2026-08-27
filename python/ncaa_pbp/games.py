"""Stage 02 -- games: capture the 5-tab bundle for every not-yet-captured contest.

Contest ids come from the schedule master parquet (stage 01) -- NOT rediscovery:
``ncaa/schedule_master/parquet/{season}.parquet`` -> unique non-null
``contest_id`` -> ``ncaa_pbp.capture.capture_season`` ->
``ncaa/raw/{season}/{contest_id}.json.gz``.

Resumable (captured contests skipped, Ctrl-C safe); chunk with ``--max N`` and
fan out with disjoint ``--shard i/N`` as separate PROCESSES (each holds its own
browser session). A ban/challenge storm trips the consecutive-failure breaker
and the run exits rc=1 -- cool down, re-run, it resumes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from ncaa_pbp.capture import capture_season
from ncaa_pbp.datasets import master_parquet_path
from ncaa_pbp.discover import browser_fetch_fn, proxy_pool_from_env
from ncaa_pbp.schedules import DEFAULT_DIVISIONS, DIVISIONS, REPO_ROOT


def raw_dir(root: "str | Path", season: int) -> Path:
    return Path(root) / "ncaa" / "raw" / str(season)


def contest_ids_from_master(
    root: "str | Path",
    season: int,
    divisions: "Optional[Tuple[int, ...]]" = DEFAULT_DIVISIONS,
) -> "List[str]":
    """Sorted unique non-null contest ids from the season's schedule master.

    ``divisions`` filters on the master's ``division`` column (D-I only by
    default -- see :data:`ncaa_pbp.schedules.DEFAULT_DIVISIONS`); pass ``None``
    for every division. A cross-division contest is kept when EITHER side is in
    ``divisions`` (the master holds one row per team-game, so a D-I team's game
    against a D-II opponent appears under the D-I team's row).
    """
    import polars as pl

    path = master_parquet_path(root, season)
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} missing -- run stage 01 (schedules_scrape) for season {season} first"
        )
    frame = pl.read_parquet(path).drop_nulls("contest_id")
    if divisions and "division" in frame.columns:
        frame = frame.filter(pl.col("division").is_in(list(divisions)))
    elif divisions:
        # a master without the column predates the division sweep -- capture
        # everything rather than silently returning nothing
        print(f"[games] {path.name} has no division column; capturing all contests", flush=True)
    return sorted(frame.get_column("contest_id").unique().to_list())


def parse_shard(spec: str) -> "Tuple[int, int]":
    """``"i/N"`` -> ``(i, N)`` with ``0 <= i < N`` (disjoint modulo shards)."""
    try:
        i_s, n_s = spec.split("/", 1)
        i, n = int(i_s), int(n_s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"--shard wants i/N, got {spec!r}") from None
    if not 0 <= i < n:
        raise argparse.ArgumentTypeError(f"--shard wants 0 <= i < N, got {spec!r}")
    return i, n


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True, help="calendar year (2026 = spring 2026)")
    ap.add_argument("--root", default=str(REPO_ROOT))
    ap.add_argument("--max", type=int, default=None, help="cap NEW captures this run (chunking)")
    ap.add_argument(
        "--shard",
        type=parse_shard,
        default=None,
        help="i/N -- capture only contests where index %% N == i (fan-out, one process each)",
    )
    ap.add_argument(
        "--division",
        type=int,
        choices=DIVISIONS,
        default=None,
        help="one division (default: D-I only; --all-divisions for every one)",
    )
    ap.add_argument(
        "--all-divisions",
        action="store_true",
        help="capture every division in the master (D-II/III backfill)",
    )
    args = ap.parse_args(argv)
    root = Path(args.root)

    divisions = None if args.all_divisions else ((args.division,) if args.division else DEFAULT_DIVISIONS)
    contests = contest_ids_from_master(root, args.season, divisions)
    if args.shard:
        i, n = args.shard
        contests = contests[i::n]
        print(f"[games] shard {i}/{n}: {len(contests)} contests", flush=True)
    else:
        print(f"[games] {len(contests)} contests", flush=True)

    pool = proxy_pool_from_env()
    if not pool:
        print("NCAA_PROXY_POOL is empty -- set a US-residential proxy pool", file=sys.stderr)
        return 2
    fetch = browser_fetch_fn(proxy_pool=pool)  # one held session

    try:
        stats = capture_season(contests, fetch, raw_dir(root, args.season), max_contests=args.max)
    except RuntimeError as exc:  # breaker tripped -- ban hard-stop
        print(f"[games] HARD STOP: {exc}", file=sys.stderr)
        return 1
    print(f"[games] {stats}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
