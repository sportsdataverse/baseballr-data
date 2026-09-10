"""Stage 04 -- Statcast raw: Savant pitch-level tracking -> committed parquet.

WHY THIS EXISTS: the four MLB model tags re-pull Savant live at build time
(``SDV_MLB_STATCAST_CACHE``, ~55 min/season), so there is no reproducible raw
Statcast corpus -- a parser or model fix cannot be replayed without re-scraping
the provider. The repo's ``statcast/`` tree is the R-era archive: 2015-2022 only,
monthly grain, and it predates ``bat_speed`` (the 2023+ bat-tracking field).

Uses ``mlb_statcast_search`` -- the shipped date-chunked search that
``pull_statcast_season`` delegates to, and the helper the hitting models were
FIT under. Hand-rolling the CSV URL here would silently diverge from the data
conventions the models' oracle gates were set against.

Savant needs NO proxy (measured: HTTP 200 direct, while statsapi 406s), and
Statcast begins in 2015 -- there is nothing earlier to fetch.

Grain is per MONTH: it bounds a resumable unit (~8 per season), keeps each
request well under Savant's row cap, and matches the existing archive's shape.

Pace is env-only:
  SDV_MLB_STATCAST_SLEEP   seconds between month pulls (default 2)
"""

from __future__ import annotations

import argparse
import calendar
import os
import pathlib
import sys
import time

FLOOR = 2015  # Statcast's first season; nothing earlier exists to fetch
MONTHS = range(3, 12)  # Mar-Nov covers spring through the World Series
SLEEP = float(os.environ.get("SDV_MLB_STATCAST_SLEEP", "2"))


def scheduled_game_dates(root: pathlib.Path, season: int, month: int) -> "set[str]":
    """Game dates the stage-01 schedule says exist in this month.

    This is the independent signal that makes "empty" earnable. Without it a
    Savant 403 is indistinguishable from a month with no baseball: sdv-py's
    ``download`` RETURNS the error response once its retry budget is spent
    rather than raising, ``_csv_to_frame`` turns the 403 body into an empty
    frame, and a zero-row month reads as legitimately empty. Schedule data is
    already on disk from stage 01, so this costs one parquet scan.
    """
    import polars as pl

    sched = root / "mlb" / "schedule" / f"{season}.parquet"
    if not sched.exists():
        return set()
    pre = f"{season}-{month:02d}-"
    return set(
        pl.scan_parquet(sched)
        .filter(
            pl.col("abstract_state").eq("Final")
            & pl.col("game_date").cast(pl.Utf8).str.starts_with(pre)
        )
        .select("game_date")
        .collect()["game_date"]
        .cast(pl.Utf8)
        .to_list()
    )


def month_path(root: pathlib.Path, season: int, month: int) -> pathlib.Path:
    return root / "mlb" / "statcast_raw" / str(season) / f"{season}-{month:02d}.parquet"


def capture_month(
    root: pathlib.Path, season: int, month: int, *, force: bool = False
) -> "tuple[str, int]":
    """Return (status, rows). Raises on a transport failure or a partial month."""
    out = month_path(root, season, month)
    if out.exists() and not force:
        return "skipped", 0

    from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

    last = calendar.monthrange(season, month)[1]
    df = mlb_statcast_search(f"{season}-{month:02d}-01", f"{season}-{month:02d}-{last}")

    expected = scheduled_game_dates(root, season, month)

    if df is None or df.height == 0:
        # "Empty" must be EARNED, never inferred from a zero-row frame. A
        # persistent Savant 403/5xx arrives here as zero rows, not an exception.
        if expected:
            raise RuntimeError(
                f"{season}-{month:02d}: Savant returned 0 rows but the schedule "
                f"has {len(expected)} game dates -- treating as a FAILURE, not an "
                "empty month"
            )
        return "empty", 0

    # A month is written whole or not at all. mlb_statcast_search splits the
    # month into 7-day chunks and DROPS any chunk that came back empty, so one
    # 403'd week still yields a plausible, non-empty frame -- which the
    # file-exists resume would then freeze permanently.
    if expected:
        import polars as pl

        got = set(df.get_column("game_date").cast(pl.Utf8).to_list())
        missing = expected - got
        if missing:
            # A missing date is NOT automatically a dropped chunk. Savant
            # genuinely has no rows for some scheduled dates -- a game in
            # "Completed Early"/suspended state is finished on a later calendar
            # day and its pitches stay filed under the ORIGINAL date, so the
            # completion date has a Final game and zero Statcast rows.
            # Verified: 2023-10-02 (Marlins-Mets, "Completed Early") returns 0
            # rows on a direct single-date query, while 10-01 and 10-03 return
            # 4,402 and 1,184.
            #
            # Re-probe each missing date individually -- no chunking involved,
            # so a zero there is Savant's answer, not our fetch losing a week.
            # Missing dates are rare, so this costs a handful of requests.
            truly_absent, dropped = set(), set()
            for d in sorted(missing):
                probe = mlb_statcast_search(d, d)
                (truly_absent if probe is None or probe.height == 0 else dropped).add(d)
            if dropped:
                raise RuntimeError(
                    f"{season}-{month:02d}: partial month -- {len(dropped)} date(s) "
                    f"have Savant rows but are absent from the month pull "
                    f"(e.g. {sorted(dropped)[:3]}); a chunk was dropped, refusing to bank"
                )
            print(
                f"    {season}-{month:02d}: {len(truly_absent)} scheduled date(s) "
                f"have no Savant data at all (e.g. {sorted(truly_absent)[:2]}) -- "
                "accepted",
                flush=True,
            )

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(f".{os.getpid()}.tmp")
    df.write_parquet(tmp)
    tmp.rename(out)  # atomic: a killed run never leaves a half-written month
    return "captured", df.height


def capture_season(root: pathlib.Path, season: int, *, force: bool = False) -> dict:
    stats = {"captured": 0, "skipped": 0, "empty": 0, "failed": 0, "rows": 0}
    for month in MONTHS:
        try:
            r, rows = capture_month(root, season, month, force=force)
            stats[r] += 1
            if r == "captured":
                stats["rows"] += rows
        except Exception as exc:  # noqa: BLE001 - one month must not kill the season
            stats["failed"] += 1
            print(f"  {season}-{month:02d}: {type(exc).__name__}: {str(exc)[:160]}")
        # Pace after EVERY month, including failures: a struggling Savant must
        # not be hammered faster than a healthy one.
        if SLEEP:
            time.sleep(SLEEP)
    return stats


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int)
    ap.add_argument("--start", type=int)
    ap.add_argument("--end", type=int)
    ap.add_argument("--root", default=".")
    ap.add_argument("--force", action="store_true", help="refetch even if cached")
    a = ap.parse_args(argv)

    if a.season:
        seasons = [a.season]
    elif a.start and a.end:
        lo, hi = sorted((a.start, a.end))
        seasons = list(range(lo, hi + 1))
    else:
        ap.error("need --season, or --start and --end")

    root = pathlib.Path(a.root)
    rc = 0
    for s in seasons:
        if s < FLOOR:
            print(f"season {s}: before Statcast ({FLOOR}) -- nothing to fetch, skipping")
            continue
        st = capture_season(root, s, force=a.force)
        print(f"statcast season {s}: {st}", flush=True)
        if st["failed"]:
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
