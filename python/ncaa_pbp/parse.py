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
import zlib
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


def _teams_block(
    linescore_rows: "list[dict]",
    espn_game_id: "Optional[str]",
    *,
    pbp_final: "Optional[dict[str, int]]" = None,
    schedule_pairs: "frozenset[tuple[int, int]]" = frozenset(),
) -> "list[dict]":
    """One entry per team from the linescore rows (which repeat per inning).

    The baseball linescore names the final ``runs_total`` -- ``final`` is the
    FOOTBALL column name, and reading it here silently nulled every
    capture-era final (caught 2026-08-27 by an all-unverifiable QA frame).
    Accept either so the block is right whichever schema built the payload.

    stats.ncaa.org's R total can disagree with its own inning cells: in a same-day doubleheader it
    often carries the other game's score (2012: 305 games; the schedule backed the inning sum in 290
    and R in 6). The inning sum wins only when corroborated -- the side's last pbp score
    (*pbp_final*, by ``home_away``) or the schedule master's result (*schedule_pairs*, sorted score
    pairs) -- since a linescore missing an extra-inning column would sum short. ``final_source``
    says which: ``linescore`` (R), ``inning_sum``, or ``linescore_unconfirmed`` (R kept, they disagree).
    """
    teams: "dict[str, dict]" = {}  # linescore repeats one row per period; keep one entry per team
    sums: "dict[str, int]" = {}
    for r in linescore_rows:
        if not r.get("team"):
            continue
        teams.setdefault(
            r["team"],
            {
                "team": r["team"],
                "home_away": r.get("home_away"),
                "final": r.get("runs_total", r.get("final")),
            },
        )
        if str(r.get("inning", "")).isdigit():
            sums[r["team"]] = sums.get(r["team"], 0) + (r.get("runs") or 0)
    sum_pair = tuple(sorted(sums.values())) if len(sums) == 2 else None
    for name, t in teams.items():
        total, inning_sum = t["final"], sums.get(name)
        if inning_sum is None or total == inning_sum:
            t["final_source"] = "linescore"
        elif (pbp_final or {}).get(t["home_away"]) == inning_sum or sum_pair in schedule_pairs:
            t["final"], t["final_source"] = inning_sum, "inning_sum"
        else:
            t["final_source"] = "linescore_unconfirmed"
        t["espn_game_id"] = espn_game_id
    return list(teams.values())


def _orient_pbp(pbp: "list[dict]", linescore_rows: "list[dict]") -> bool:
    """Swap the pbp score columns in place when the listed HOME team bats first; return whether it did.

    The pbp table reads its score left-right in batting order, so ``score_away`` is the team batting
    the top half. When the host bats first (2012 contest 759907: "San Jose St. | Score | Lehigh" while
    the linescore lists Lehigh away), that is the home team's score. Swapping keeps ``score_away`` the
    listed away team's, matching ``teams[].home_away``; ``inning_top_bot``/``batting`` stay as played.
    """
    home = next((r.get("team") for r in linescore_rows if r.get("home_away") == "home"), None)
    first = next((r.get("batting") for r in pbp if r.get("inning_top_bot") == "top"), None)
    if not home or first != home:
        return False
    for r in pbp:
        r["score_away"], r["score_home"] = r.get("score_home"), r.get("score_away")
    return True


def build_capture_payload(
    bundle: "dict[str, Any]",
    season: int,
    *,
    espn_game_id: "Optional[str]" = None,
    schedule_pairs: "frozenset[tuple[int, int]]" = frozenset(),
) -> "dict[str, Any]":
    """Parse one captured 5-tab bundle into the payload dict (pure, offline).

    *schedule_pairs*: the schedule master's sorted score pairs for this contest, which corroborate a
    linescore inning sum over a disagreeing R total (see :func:`_teams_block`).
    """
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
    home_bats_first = _orient_pbp(pbp, linescore)
    pbp_final = {
        side: next(
            (r[f"score_{side}"] for r in reversed(pbp) if r.get(f"score_{side}") is not None), None
        )
        for side in ("away", "home")
    }
    teams = _teams_block(
        linescore, espn_game_id, pbp_final=pbp_final, schedule_pairs=schedule_pairs
    )
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
    ls0 = linescore[0] if linescore else {}
    return {
        "game_key": cid,
        "contest_id": cid,
        "game_pbp_id": None,
        "season": season,
        "source": "capture",
        "espn_game_id": espn_game_id,
        # game meta, top-level like the legacy payloads (R-era pbp compat cols)
        "game_date": ls0.get("game_date"),
        "location": ls0.get("venue"),
        "attendance": ls0.get("attendance"),
        "home_bats_first": home_bats_first,
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


# game meta a capture without a box score lacks; carried over from a superseded legacy payload
_LEGACY_META = ("game_pbp_id", "game_date", "location", "attendance")


def _legacy_at(path: Path) -> "Optional[dict[str, Any]]":
    """The legacy payload at a capture's path, else None (absent, a capture, or unreadable)."""
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, EOFError, ValueError, zlib.error):
        return None  # unreadable: keep today's skip, --force rebuilds it
    return payload if str(payload.get("source", "")).startswith("legacy") else None


def _schedule_pairs(root: Path, season: int) -> "dict[str, frozenset[tuple[int, int]]]":
    """contest_id -> the schedule master's sorted (score, score) pairs ({} without a master)."""
    from ncaa_pbp.datasets import master_parquet_path

    path = master_parquet_path(root, season)
    if not path.is_file():
        return {}
    import polars as pl

    pairs: "dict[str, set[tuple[int, int]]]" = {}
    frame = pl.read_parquet(
        path, columns=["contest_id", "team_score", "opponent_score"]
    ).drop_nulls("contest_id")
    for cid, a, b in frame.iter_rows():
        if str(a).isdigit() and str(b).isdigit():
            pairs.setdefault(str(cid), set()).add(tuple(sorted((int(a), int(b)))))
    return {cid: frozenset(v) for cid, v in pairs.items()}


def _capture_job(
    args: "tuple[str, str, int, Optional[str], bool, frozenset[tuple[int, int]]]",
) -> "tuple[str, str]":
    """Worker: one raw bundle -> one payload. (spawn pool; imports stay local)."""
    root_s, cid, season, espn, force, sched = args
    root = Path(root_s)
    out = parsed_path(root, cid)
    # a legacy payload can sit at the capture's own path (2023 legacy is keyed by
    # contest id); the capture supersedes it, so only a capture payload is kept
    legacy = _legacy_at(out)
    if out.exists() and not force and legacy is None:
        return cid, "skipped"
    raw = root / "ncaa" / "raw" / str(season) / f"{cid}.json.gz"
    try:
        with gzip.open(raw, "rt", encoding="utf-8") as fh:
            bundle = json.load(fh)
        payload = build_capture_payload(bundle, season, espn_game_id=espn, schedule_pairs=sched)
        if legacy is not None and not payload["pbp"] and legacy.get("pbp"):
            return cid, "kept-legacy"  # the site has no plays for it now; the legacy ones stay
        if not payload["pbp"] and not payload["teams"]:
            # a game stats.ncaa.org has no pbp or box table for: a payload would be a blank
            # games row, and without one the build keeps any legacy copy of the game
            return cid, "empty"
        for key in _LEGACY_META if legacy is not None else ():
            if payload.get(key) is None:
                payload[key] = legacy.get(key)
        write_payload(root, payload)
        return cid, "parsed"
    except Exception as exc:  # noqa: BLE001 -- one bad bundle must not kill the sweep
        return cid, f"error: {type(exc).__name__}: {exc}"


def _legacy_job(args: "tuple[str, str, bool]") -> "tuple[str, str]":
    root_s, path_s, force = args
    from ncaa_pbp.legacy import build_legacy_payload, legacy_schedule, read_legacy_game

    root = Path(root_s)
    try:
        rows = read_legacy_game(path_s)
        year = rows[0].get("year") if rows else None
        sched = legacy_schedule(root_s, int(year)) if year else None
        payload = build_legacy_payload(rows, fallback_id=Path(path_s).stem, schedule=sched)
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
    ap.add_argument(
        "--contests",
        default=None,
        help="with --season: only these contest ids (comma-separated, or @file with one per line)",
    )
    args = ap.parse_args(argv)
    if args.season is None and not args.legacy:
        ap.error("pass --season N and/or --legacy")
    root = Path(args.root)

    if args.season is not None:
        from ncaa_pbp.xwalk import load_espn_game_index

        idx = load_espn_game_index(root, args.season)
        raw_dir = root / "ncaa" / "raw" / str(args.season)
        cids = sorted(p.name.removesuffix(".json.gz") for p in raw_dir.glob("*.json.gz"))
        if args.contests:
            text = (
                Path(args.contests[1:]).read_text()
                if args.contests.startswith("@")
                else args.contests
            )
            want = set(text.replace(",", " ").split())
            cids = [c for c in cids if c in want]
        sched = _schedule_pairs(root, args.season)
        jobs = [
            (str(root), c, args.season, idx.get(c), args.force, sched.get(c, frozenset()))
            for c in cids
        ]
        print(f"capture {args.season}: {_run_pool(jobs, _capture_job, args.workers)}", flush=True)

    if args.legacy:
        from ncaa_pbp.legacy import iter_legacy_games

        paths = [str(p) for p, _rows in iter_legacy_games(root, year=args.year)]
        jobs = [(str(root), p, args.force) for p in paths]
        label = f"legacy{f' {args.year}' if args.year else ''}"
        print(f"{label}: {_run_pool(jobs, _legacy_job, args.workers)}", flush=True)
    return 0
