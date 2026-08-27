"""CLI -- ``ncaa_baseball_data_build build --dataset {ds|all} --season YYYY [--publish|--dry-run]``.

Build maps this repo's tree onto the release layout
(``ncaa/{dataset}/parquet/{stem}_{season}.parquet`` via ``io.write_dataset``,
which also stages the csv.gz release asset and upserts the manifest).
``--publish`` uploads parquet + csv.gz + rds to the dataset's release on
sportsdataverse/sportsdataverse-data (``publish.py``, ported from
ncaa-mfb-football-data). ``--dataset all`` builds every payload-family dataset
from ONE sweep over the 100k+ parsed payloads (``builders.build_season``) and
writes the per-season finals-QA frame to ``ncaa/qa/`` (committed, never
released).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from ncaa_baseball_data_build._logging import get_logger
from ncaa_baseball_data_build.builders import PAYLOAD_DATASETS, RELEASE_DIVISIONS, build_season
from ncaa_baseball_data_build.config import REGISTRY, DatasetSpec, raw_root
from ncaa_baseball_data_build.io import write_dataset

log = get_logger()

_LEGACY_SCHEDULE_LAST = 2023  # 2012-2023 schedules come from the committed R-era frames


def _reference_frame(spec: DatasetSpec, season: int, raw: Path) -> pl.DataFrame:
    """teams / schedule / rosters from the raw tree's parquet (2024+), with the
    committed legacy R-era frame as the pre-2024 schedule fallback."""
    if spec.name == "teams":
        files = sorted((raw / "ncaa" / "teams" / "parquet").glob(f"{season}_d*.parquet"))
        if not files:
            raise FileNotFoundError(
                f"teams {season}: no {season}_d*.parquet under {raw / 'ncaa/teams/parquet'}"
            )
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
    if spec.name == "rosters":
        f = raw / "ncaa" / "rosters" / "parquet" / f"{season}.parquet"
        if not f.is_file():
            raise FileNotFoundError(f"rosters {season}: {f} not found")
        return pl.read_parquet(f)
    # schedule: capture-era schedule master when present, else the committed
    # legacy R-era frame re-emitted AS-IS (loaders depend on its columns --
    # never reshape it; the season stamp is the only addition).
    master = raw / "ncaa" / "schedule_master" / "parquet" / f"{season}.parquet"
    if master.is_file():
        return pl.read_parquet(master)
    legacy = raw / "ncaa" / "schedules" / "parquet" / f"{spec.stem}_{season}.parquet"
    if legacy.is_file():
        return pl.read_parquet(legacy)
    raise FileNotFoundError(f"schedule {season}: neither {master} nor {legacy} exists")


def _finish(
    df: pl.DataFrame, spec: DatasetSpec, season: int, base: Path, *, release: bool
) -> pl.DataFrame:
    # ALWAYS stamp -- never trust an upstream `season` to agree with the asset
    # name (an asset NAMED _2026 whose rows said 2025 once made sdv-db's
    # season-key check silently ingest 0 rows). Name and column can never drift.
    df = df.with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    write_dataset(df, spec, season, base=base, release=release)
    return df


def build_dataset(
    spec: DatasetSpec,
    season: int,
    base: Path,
    raw: Path,
    *,
    release: bool = False,
    divisions: "tuple[int, ...]" = RELEASE_DIVISIONS,
) -> pl.DataFrame:
    """Build ONE dataset for a season and write it via ``io``.

    Payload-family datasets run the full season sweep for just this dataset
    (per-dataset builds may re-scan); ``--dataset all`` goes through ``_build``
    which shares one sweep instead.
    """
    if spec.name in PAYLOAD_DATASETS:
        frames = build_season(season, raw, divisions)
        if not frames["games"].height:
            raise FileNotFoundError(
                f"{spec.name} {season}: no parsed payloads under {raw / 'ncaa' / 'json'}"
            )
        df = frames[spec.name]
    else:
        df = _reference_frame(spec, season, raw)
    return _finish(df, spec, season, base, release=release)


def _write_qa(qa: pl.DataFrame, season: int, base: Path) -> None:
    """Finals QA frame -> committed ``ncaa/qa/`` (small, never released)."""
    out = base / "ncaa" / "qa" / f"qa_pbp_finals_{season}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    qa.write_parquet(out)
    ok = (qa.get_column("finals_match") == True).sum()  # noqa: E712
    unv = qa.get_column("finals_match").null_count()
    log.info(
        "qa %s: %d/%d exact, %d unverifiable, %d flagged -> %s",
        season,
        ok,
        qa.height,
        unv,
        qa.height - ok - unv,
        out,
    )


def _publish(spec: DatasetSpec, season: int, base: Path, dry_run: bool) -> None:
    from ncaa_baseball_data_build import publish

    publish.publish_dataset(spec, season, base=base, dry_run=dry_run)


def _build(args: argparse.Namespace) -> int:
    raw = Path(args.raw_root) if args.raw_root else raw_root()
    base = Path(args.base)
    release = args.publish or args.dry_run
    # release scope: D-I unless explicitly widened (the raw/parsed trees keep
    # every captured division; publishing filters so a partially-captured
    # division never ships as if complete)
    divisions = () if getattr(args, "all_divisions", False) else RELEASE_DIVISIONS

    if args.dataset != "all":
        spec = REGISTRY[args.dataset]
        build_dataset(spec, args.season, base, raw, release=release, divisions=divisions)
        if release:
            _publish(spec, args.season, base, args.dry_run)
        return 0

    # --dataset all: reference frames individually, then EVERY payload-family
    # dataset (+ QA) from one sweep over the payloads.
    for name in ("teams", "schedule", "rosters"):
        spec = REGISTRY[name]
        try:
            build_dataset(spec, args.season, base, raw, release=release, divisions=divisions)
        except FileNotFoundError as exc:
            # pre-capture seasons have no teams/rosters source at all; only the
            # schedule has a legacy fallback, so its absence stays fatal.
            if name == "schedule":
                raise
            log.warning("skipping %s %s: %s", name, args.season, exc)
            continue
        if release:
            _publish(spec, args.season, base, args.dry_run)

    frames = build_season(args.season, raw, divisions)
    if not frames["games"].height:
        raise FileNotFoundError(
            f"season {args.season}: no parsed payloads under {raw / 'ncaa' / 'json'}"
        )
    for name in PAYLOAD_DATASETS:
        spec = REGISTRY[name]
        _finish(frames[name], spec, args.season, base, release=release)
        if release:
            _publish(spec, args.season, base, args.dry_run)
    _write_qa(frames["qa"], args.season, base)
    return 0


def _check(args: argparse.Namespace) -> int:
    """Compare each dataset's LOCALLY BUILT seasons against what the release holds.

    Semantics ported from ncaa-mfb-football-data: only ``built - live`` is
    fatal; ``GhUnavailable`` exits 2 (could-not-look is not there-are-gaps).
    """
    from ncaa_baseball_data_build.publish import (
        DEFAULT_REPO,
        GhUnavailable,
        published_seasons,
    )

    datasets = list(REGISTRY) if args.dataset == "all" else [args.dataset]
    base = Path(args.base)
    missing_total = 0
    for name in datasets:
        spec = REGISTRY[name]
        built = {
            int(p.stem.rsplit("_", 1)[1])
            for p in (base / "ncaa" / spec.name / "parquet").glob(f"{spec.stem}_*.parquet")
            if p.stem.rsplit("_", 1)[1].isdigit()
        }
        try:
            live = published_seasons(spec, repo=args.repo or DEFAULT_REPO)
        except GhUnavailable as exc:
            log.error("cannot audit %s: %s", name, exc)
            return 2
        if args.porcelain:
            for s in sorted(live):
                print(f"{name} {s}")
            continue
        missing = sorted(built - live)
        extra = sorted(live - built)
        missing_total += len(missing)
        status = "OK  " if not missing else "GAP "
        log.info(
            "%s %-18s built=%d published=%d%s%s",
            status,
            name,
            len(built),
            len(live),
            f" MISSING={missing}" if missing else "",
            f" PUBLISHED_ONLY={extra}" if extra else "",
        )
    if missing_total:
        log.error("%d built season(s) are NOT on their release", missing_total)
    return 1 if missing_total else 0


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(prog="ncaa_baseball_data_build", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="payloads/raw parquet -> ncaa/{dataset}/parquet/ [+ publish]")
    b.add_argument("--dataset", default="all", choices=["all", *REGISTRY])
    b.add_argument("--season", type=int, required=True, help="calendar year: 2024 = spring 2024")
    b.add_argument(
        "--base", default=str(Path(__file__).resolve().parents[2]), help="this repo's root"
    )
    b.add_argument(
        "--raw-root",
        default=None,
        help="override $NCAA_BASEBALL_ROOT / the repo root (this repo IS the raw repo)",
    )
    g = b.add_mutually_exclusive_group()
    g.add_argument("--publish", action="store_true", help="upload parquet+csv+rds to the release")
    g.add_argument(
        "--dry-run", action="store_true", help="stage release assets, log would-be uploads"
    )
    b.set_defaults(func=_build)

    c = sub.add_parser("check", help="audit built seasons against what each release actually holds")
    c.add_argument("--dataset", default="all", choices=["all", *REGISTRY])
    c.add_argument("--base", default=str(Path(__file__).resolve().parents[2]))
    c.add_argument("--repo", default=None)
    c.add_argument(
        "--porcelain",
        action="store_true",
        help="print '<dataset> <season>' per published unit (resume index)",
    )
    c.set_defaults(func=_check)

    args = ap.parse_args(argv)
    return args.func(args)
