"""Stage 03 -- Baseball Savant per-pitch, fetched by DAY, committed by GAME.

Savant's ``/statcast_search/csv`` has no per-game route; its native unit is a
date range, and it hard-caps a response at **25,000 rows with no pagination**
(a 7-day window returns exactly 25,000 -- silently truncated). The busiest
single day measured is 4,777 rows (2008-06-15, 16 games), so a **one-day**
window is the only window that is provably never truncated; the cap is
asserted per fetch rather than assumed.

Each day's CSV is then split by ``game_pk`` and written as one
``savant/{season}/{game_pk}.csv.gz`` per game, so the per-game addressing
matches the statsapi tree and a sibling repo can read one game without
downloading a season.

**Why the CSV and not the per-game ``/gf`` feed.** Measured on gamePk 746694:
``/gf`` returns the same 321 pitches, but the CSV carries 85 fields ``/gf``
does not -- including every modelled column the MLB spine reads
(``estimated_woba_using_speedangle``, ``estimated_ba_using_speedangle``,
``estimated_slg_using_speedangle``, ``delta_run_exp``, ``delta_home_win_exp``,
``woba_value``/``woba_denom``, ``release_speed``, ``release_spin_rate``,
``spin_axis``, ``pfx_x``/``pfx_z``, ``bat_speed``, ``swing_length``,
``arm_angle``, ``umpire``, the ``fielder_*`` ids and the fielding alignments).
``/gf`` is the wrong surface for a raw layer; it is a scoreboard feed.
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path
from typing import Optional

import polars as pl

from mlb_raw import core

SEARCH_URL = "https://baseballsavant.mlb.com/statcast_search/csv"


def _day_params(season: int, day: str) -> dict:
    return {
        "all": "true",
        "hfSea": f"{season}|",
        "player_type": "pitcher",
        "game_date_gt": day,
        "game_date_lt": day,
        "type": "details",
        "min_pitches": "0",
        "min_results": "0",
        "min_pas": "0",
        "group_by": "name",
        "sort_col": "pitches",
        "player_event_sort": "api_p_release_speed",
        "sort_order": "desc",
    }


def fetch_day(season: int, day: str) -> pl.DataFrame:
    """One day of Savant per-pitch rows, every column read as Utf8.

    ``infer_schema_length=0`` is deliberate: a raw layer stores what the
    provider sent. Typing is the reshape step's job, and inferring per-day
    would give the same column two dtypes in two days of the same season.
    """
    from sportsdataverse.mlb.mlb_statcast_runtime import _get

    text = _get(SEARCH_URL, _day_params(season, day))
    if not isinstance(text, str) or "game_pk" not in text.split("\n", 1)[0]:
        return pl.DataFrame()
    df = pl.read_csv(io.StringIO(text), infer_schema_length=0)
    if df.height >= core.SAVANT_ROW_CAP:
        raise SystemExit(
            f"savant {day}: hit the {core.SAVANT_ROW_CAP}-row cap on a ONE-DAY window "
            f"({df.height} rows) -- the day is truncated and must not be persisted"
        )
    return df


def _validate_slice(text: str) -> Optional[str]:
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return "header only / empty"
    if "game_pk" not in lines[0]:
        return "no game_pk column"
    return None


def capture_day(
    root: Path,
    season: int,
    day: str,
    manifest: "dict[int, dict]",
    wanted: "set[int]",
    *,
    force: bool = False,
) -> "tuple[int, int]":
    """Fetch one day and write a slice per wanted game. Returns (written, refused)."""
    df = fetch_day(season, day)
    if df.height == 0:
        return 0, 0
    core.pace("SDV_MLB_RAW_SAVANT_SLEEP", 1.0)

    written = refused = 0
    covered = 0
    for (gpk_s,), sub in df.group_by(["game_pk"]):
        covered += sub.height
        try:
            gpk = int(gpk_s)
        except (TypeError, ValueError):
            continue
        if gpk not in wanted:
            continue
        out = core.savant_path(root, season, gpk)
        if not force and core.already_captured(out):
            # Already on disk: re-record it so a resumed run never leaves a
            # captured game with blank manifest/integrity columns.
            manifest[gpk].update(
                {
                    "savant_path": core.rel(root, out),
                    "savant_rows": sub.height,
                    "savant_bytes": out.stat().st_size,
                    "savant_sha256": core.sha256_of(out),
                }
            )
            continue
        buf = io.StringIO()
        sub.write_csv(buf)
        n = core.persist_text(out, buf.getvalue(), _validate_slice)
        if n is None:
            refused += 1
            continue
        manifest[gpk].update(
            {
                "savant_path": core.rel(root, out),
                "savant_rows": sub.height,
                "savant_bytes": n,
                "savant_sha256": core.sha256_of(out),
            }
        )
        written += 1
    # every row of the day landed in exactly one slice -- no pitch silently dropped
    assert covered == df.height, f"{day}: sliced {covered} of {df.height} rows"
    return written, refused


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
    wanted = {
        int(r["game_pk"])
        for r in manifest.values()
        if r["game_type"] in game_types and r["status_code"] == "F"
    }
    days: "dict[str, list[int]]" = {}
    for r in manifest.values():
        if int(r["game_pk"]) in wanted and r["game_date"]:
            days.setdefault(r["game_date"], []).append(int(r["game_pk"]))
    todo = sorted(
        d
        for d, pks in days.items()
        if force
        or any(
            core.outstanding(root, manifest[p], "savant", core.savant_path(root, season, p))
            for p in pks
        )
    )
    todo = core.head(todo, limit)
    print(f"savant {season}: {len(todo)} days to fetch ({len(wanted)} games in scope)", flush=True)

    written = refused = 0
    for i, day in enumerate(todo, 1):
        w, x = capture_day(root, season, day, manifest, wanted, force=force)
        written += w
        refused += x
        print(f"  [{i}/{len(todo)}] {day}: +{w} games (refused {x}) total={written}", flush=True)
        if commit_every and i % commit_every == 0:
            core.write_manifest(root, season, manifest.values())
            core.refresh_index(root)
            core.git_commit(
                root,
                [
                    root / "savant" / str(season),
                    core.manifest_path(root, season),
                    core.index_path(root),
                ],
                f"data(mlb-raw): savant per-pitch {season} (through {day})",
            )
    core.write_manifest(root, season, manifest.values())
    core.refresh_index(root)
    print(f"savant {season}: games written={written} refused={refused}", flush=True)
    return refused


def main(argv: "Optional[list[str]]" = None) -> int:
    ap = argparse.ArgumentParser(
        description="Stage 03 -- Baseball Savant per-pitch capture (day fetch, game slice)"
    )
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=None)
    ap.add_argument("--game-types", default=",".join(core.DEFAULT_GAME_TYPES))
    ap.add_argument(
        "--limit", type=core.nonneg_int, default=None, help="stop after N days (chunked runs)"
    )
    ap.add_argument("--force", action="store_true")
    ap.add_argument(
        "--commit-every", type=int, default=0, help="git-commit every N days (0 = only at the end)"
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
