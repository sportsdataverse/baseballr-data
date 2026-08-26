"""Live runner: discover a season's contests + capture their pbp bundles.

Holds ONE ``NcaaFetcher.with_browser`` session (no per-call relaunch -- avoids the
patchright relaunch-storm crash) backed by a US-residential proxy pool read from
the ``NCAA_PROXY_POOL`` env var (newline/comma-separated ``http://user:pass@host:port``).

Usage::

    NCAA_PROXY_POOL="$(cat proxies.txt)" python python/run.py --sport MBA --year 2025 --out ./ncaa

stats.ncaa.org IP-bans scrapers -- run sparingly, paced, from a residential IP.

The numbered stage pipeline (``python/ncaa_baseball_NN_*.py``; see RUNBOOK.md) is
the maintained path -- this combined runner remains for one-shot ad-hoc captures.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ncaa_pbp.capture import capture_season
from ncaa_pbp.discover import browser_fetch_fn, discover_season, proxy_pool_from_env


def main() -> int:
    ap = argparse.ArgumentParser(description="NCAA baseball/softball raw pbp capture")
    ap.add_argument("--sport", default="MBA", choices=["MBA", "WSB"], help="MBA=baseball, WSB=softball")
    ap.add_argument("--year", type=int, required=True, help="season = calendar year, e.g. 2026")
    ap.add_argument("--division", type=int, default=1)
    ap.add_argument("--out", required=True, help="output root; bundles land in {out}/raw/{year}/")
    ap.add_argument("--max", type=int, default=None, help="cap NEW captures this run (chunking)")
    args = ap.parse_args()

    pool = proxy_pool_from_env()
    if not pool:
        print("NCAA_PROXY_POOL is empty -- set a US-residential proxy pool", file=sys.stderr)
        return 2

    fetch = browser_fetch_fn(proxy_pool=pool)  # one held session
    print(f"[discover] {args.sport} {args.year} D{args.division} ...", flush=True)
    contests = discover_season(args.year, args.division, args.sport, fetch_fn=fetch)
    print(f"[discover] {len(contests)} contests", flush=True)
    out_dir = Path(args.out) / "raw" / str(args.year)
    stats = capture_season(contests, fetch, out_dir, max_contests=args.max)
    print(f"[capture] {stats}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
