"""Stage 01 -- schedules: team lists + team pages -> persisted html + schedule master.

Per division (1/2/3): fetch the ``team/inst_team_list`` page ->
``ncaa/teams_html/{season}_d{division}.html``, then every team's ``/teams/{id}``
page -> ``ncaa/schedules_html/{season}/{team_id}.html``. File-exists resumable:
persisted html is re-read, never re-fetched (a completed season fetches nothing).
Finishes by building ``ncaa/schedule_master/parquet/{season}.parquet`` from the
persisted tree (the same builder stage 05 runs offline).

Season key = calendar year (2026 = spring 2026); stats.ncaa.org's
``academic_year`` param equals it for spring sports -- no offset anywhere.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from ncaa_pbp.discover import (
    FetchFn,
    browser_fetch_fn,
    proxy_pool_from_env,
    team_list_path,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DIVISIONS = (1, 2, 3)
#: Divisions the EXPENSIVE stages touch by default: D-I only (2026-08-27 scope
#: call -- D-II/III game capture is a `--division` flag away when wanted).
#: Discovery (this module) deliberately still sweeps all of DIVISIONS: the
#: R-era `ncaa_baseball_schedules` releases ship D-I..D-III, so a D-I-only
#: schedule master would REGRESS a published dataset, and team pages are ~3%
#: of a season's fetch cost.
DEFAULT_DIVISIONS = (1,)


def teams_html_path(root: "str | Path", season: int, division: int) -> Path:
    return Path(root) / "ncaa" / "teams_html" / f"{season}_d{division}.html"


def schedule_html_path(root: "str | Path", season: int, team_id: str) -> Path:
    return Path(root) / "ncaa" / "schedules_html" / str(season) / f"{team_id}.html"


def fetch_persisted(path: Path, fetch_fn: FetchFn, url_path: str) -> str:
    """Resumable fetch: re-read persisted html; fetch + persist only on a miss."""
    if path.is_file():
        return path.read_text(encoding="utf-8")
    html = fetch_fn(url_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return html


def scrape_season(
    season: int,
    divisions: "Tuple[int, ...]" = DIVISIONS,
    *,
    root: "str | Path" = REPO_ROOT,
    fetch_fn: FetchFn,
) -> "Dict[int, List[str]]":
    """Persist team-list + team-page html for each division.

    Returns ``{division: [team_id, ...]}``. Raises ``ValueError`` on a division
    resolving zero teams (a hollow list would silently skip the whole division).
    """
    from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_list

    out: "Dict[int, List[str]]" = {}
    for division in divisions:
        html = fetch_persisted(
            teams_html_path(root, season, division),
            fetch_fn,
            team_list_path(season, division),
        )
        teams = parse_ncaa_team_list(html).get_column("team_id").to_list()
        if not teams:
            raise ValueError(f"no teams for season={season} division={division}")
        for team_id in teams:
            fetch_persisted(schedule_html_path(root, season, team_id), fetch_fn, f"teams/{team_id}")
        out[division] = teams
    return out


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True, help="calendar year (2026 = spring 2026)")
    ap.add_argument(
        "--division",
        type=int,
        choices=DIVISIONS,
        default=None,
        help="one division (default: all three -- schedules stay D-I..D-III)",
    )
    ap.add_argument("--root", default=str(REPO_ROOT))
    args = ap.parse_args(argv)

    pool = proxy_pool_from_env()
    if not pool:
        print("NCAA_PROXY_POOL is empty -- set a US-residential proxy pool", file=sys.stderr)
        return 2
    fetch = browser_fetch_fn(proxy_pool=pool)  # one held session

    divisions = (args.division,) if args.division else DIVISIONS
    got = scrape_season(args.season, divisions, root=Path(args.root), fetch_fn=fetch)
    for division, teams in got.items():
        print(f"[schedules] {args.season} d{division}: {len(teams)} teams", flush=True)

    from ncaa_pbp.datasets import build_schedule_master

    path, frame = build_schedule_master(Path(args.root), args.season)
    print(f"[schedules] master: {frame.height} team-game rows -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
