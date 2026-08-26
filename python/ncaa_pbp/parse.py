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


# --- stage-03 drivers ------------------------------------------------------


def _capture_job(args: "tuple[str, str, int, Optional[str], bool]") -> "tuple[str, str]":
    """Worker: one raw bundle -> one payload. (spawn pool; imports stay local)."""
    root_s, cid, season, espn, force = args
    root = Path(root_s)
    out = parsed_path(root, cid)
    if out.exists() and not force:
        return cid, "skipped"
    raw = root / "ncaa" / "raw" / str(season) / f"{cid}.json.gz"
    try:
        with gzip.open(raw, "rt", encoding="utf-8") as fh:
            bundle = json.load(fh)
        write_payload(root, build_capture_payload(bundle, season, espn_game_id=espn))
        return cid, "parsed"
    except Exception as exc:  # noqa: BLE001 -- one bad bundle must not kill the sweep
        return cid, f"error: {type(exc).__name__}: {exc}"


def _legacy_job(args: "tuple[str, str, bool]") -> "tuple[str, str]":
    root_s, path_s, force = args
    from ncaa_pbp.legacy import build_legacy_payload, read_legacy_game

    root = Path(root_s)
    try:
        rows = read_legacy_game(path_s)
        payload = build_legacy_payload(rows)
        out = parsed_path(root, payload["game_key"])
        if out.exists() and not force:
            return payload["game_key"], "skipped"
        write_payload(root, payload)
        return payload["game_key"], "parsed"
    except Exception as exc:  # noqa: BLE001
        return Path(path_s).stem, f"error: {type(exc).__name__}: {exc}"


def _run_pool(jobs: "list", worker, workers: int) -> "dict[str, int]":
    # spawn, not fork: polars/Rayon locks deadlock forked children at 0% CPU
    from multiprocessing import get_context

    stats: "dict[str, int]" = {}
    if workers <= 1:
        results = map(worker, jobs)
        for key, status in results:
            s = status.split(":")[0]
            stats[s] = stats.get(s, 0) + 1
            if status.startswith("error"):
                print(f"{key}: {status}", flush=True)
        return stats
    with get_context("spawn").Pool(workers) as pool:
        for key, status in pool.imap_unordered(worker, jobs, chunksize=16):
            s = status.split(":")[0]
            stats[s] = stats.get(s, 0) + 1
            if status.startswith("error"):
                print(f"{key}: {status}", flush=True)
    return stats


def main(argv: "list[str] | None" = None) -> int:
    """CLI -- capture-era sweep (``--season``) and/or legacy sweep (``--legacy``)."""
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    ap.add_argument("--season", type=int, default=None, help="parse ncaa/raw/{season} bundles")
    ap.add_argument("--legacy", action="store_true", help="parse the legacy R-era trees")
    ap.add_argument("--year", type=int, default=None, help="restrict --legacy to one year")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    if args.season is None and not args.legacy:
        ap.error("pass --season N and/or --legacy")
    root = Path(args.root)

    if args.season is not None:
        from ncaa_pbp.xwalk import load_espn_game_index

        idx = load_espn_game_index(root, args.season)
        raw_dir = root / "ncaa" / "raw" / str(args.season)
        cids = sorted(p.name.removesuffix(".json.gz") for p in raw_dir.glob("*.json.gz"))
        jobs = [(str(root), c, args.season, idx.get(c), args.force) for c in cids]
        print(f"capture {args.season}: {_run_pool(jobs, _capture_job, args.workers)}", flush=True)

    if args.legacy:
        from ncaa_pbp.legacy import iter_legacy_games

        paths = [str(p) for p, _rows in iter_legacy_games(root, year=args.year)]
        jobs = [(str(root), p, args.force) for p in paths]
        label = f"legacy{f' {args.year}' if args.year else ''}"
        print(f"{label}: {_run_pool(jobs, _legacy_job, args.workers)}", flush=True)
    return 0
