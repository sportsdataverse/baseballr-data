"""Stage 04 -- rosters: ``/teams/{id}/roster`` -> ``ncaa/rosters_html/{season}/``.

Team ids come from the persisted stage-01 team lists (``ncaa/teams_html/``), NOT
rediscovery. File-exists resumable: a team whose roster html is on disk is
skipped, so a completed season fetches nothing. Stage 05 parses the persisted
pages into ``ncaa/rosters/parquet/{season}.parquet``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Tuple

from ncaa_pbp.discover import FetchFn, browser_fetch_fn, proxy_pool_from_env
from ncaa_pbp.schedules import (
    DEFAULT_DIVISIONS,
    DIVISIONS,
    REPO_ROOT,
    fetch_persisted,
    teams_html_path,
)


def roster_html_path(root: "str | Path", season: int, team_id: str) -> Path:
    return Path(root) / "ncaa" / "rosters_html" / str(season) / f"{team_id}.html"


def scrape_rosters(
    season: int,
    divisions: "Tuple[int, ...]" = DEFAULT_DIVISIONS,
    *,
    root: "str | Path" = REPO_ROOT,
    fetch_fn: FetchFn,
) -> "Dict[str, int]":
    """Fetch every missing roster page. Returns ``{"fetched": n, "skipped": n}``."""
    from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_list

    team_ids: "list[str]" = []
    for division in divisions:
        path = teams_html_path(root, season, division)
        if not path.is_file():
            raise FileNotFoundError(f"{path} missing -- run stage 01 (schedules_scrape) for season {season} first")
        team_ids.extend(parse_ncaa_team_list(path.read_text(encoding="utf-8")).get_column("team_id").to_list())
    stats = {"fetched": 0, "skipped": 0}
    for team_id in dict.fromkeys(team_ids):
        path = roster_html_path(root, season, team_id)
        if path.is_file():
            stats["skipped"] += 1
            continue
        fetch_persisted(path, fetch_fn, f"teams/{team_id}/roster")
        stats["fetched"] += 1
    return stats


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True, help="calendar year (2026 = spring 2026)")
    ap.add_argument(
        "--division",
        type=int,
        choices=DIVISIONS,
        default=None,
        help="one division (default: D-I only)",
    )
    ap.add_argument("--root", default=str(REPO_ROOT))
    args = ap.parse_args(argv)

    pool = proxy_pool_from_env()
    if not pool:
        print("NCAA_PROXY_POOL is empty -- set a US-residential proxy pool", file=sys.stderr)
        return 2
    fetch = browser_fetch_fn(proxy_pool=pool)  # one held session

    divisions = (args.division,) if args.division else DEFAULT_DIVISIONS
    stats = scrape_rosters(args.season, divisions, root=Path(args.root), fetch_fn=fetch)
    print(f"[rosters] {args.season}: {stats}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
