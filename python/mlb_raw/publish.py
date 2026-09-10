"""Stage 05 -- publish: MLB pbp/pitches parquet -> sportsdataverse-data releases.

Reuses ``mlb_model_publish.artifacts.upload_artifacts`` rather than writing a
second uploader: it already does one ``gh release upload`` per file with
``--clobber`` (a multi-file glob silently drops large assets), creates the
release when absent, and takes an injectable runner so tests never shell out.

COMMITTED vs PUBLISHED, deliberately asymmetric:

  committed to git   parquet only -- ``mlb/pbp/*.parquet``
  published as assets parquet + csv.gz + rds

csv.gz/rds are built into ``mlb/_release_build/`` (gitignored, same contract as
``ncaa/_release_build`` and ``mlb/*/csv|rds``) and uploaded from there. They are
derivable staging: the parquet is the only durable copy, so a re-publish
regenerates them and nothing is lost by deleting the tree.

``--dry-run`` and ``--publish`` are mutually exclusive; a season is stamped
``.done_<season>`` only on a clean upload, never on a file merely existing.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

TAGS = {
    "pbp": ("mlb_pbp", "mlb_pbp_{season}.parquet"),
    "pitches": ("mlb_pitches", "mlb_pitches_{season}.parquet"),
}
REPO = "sportsdataverse/sportsdataverse-data"


def staging_dir(root: pathlib.Path) -> pathlib.Path:
    return root / "mlb" / "_release_build"


def build_side_formats(root: pathlib.Path, dataset: str, season: int) -> "list[pathlib.Path]":
    """Write csv.gz + rds beside the parquet, into gitignored staging.

    rds goes through sdv-py's ``write_rds`` so it carries baseballr's S3 class
    chain -- an R consumer reading a plain serialisation gets a bare data.frame
    and loses the class contract the loaders expect.
    """
    import polars as pl

    _, stem = TAGS[dataset]
    src = root / "mlb" / "pbp" / stem.format(season=season)
    if not src.exists():
        raise FileNotFoundError(f"{src} -- run stage 03 first")

    out = staging_dir(root) / dataset
    out.mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(src)
    base = src.stem  # mlb_pbp_2024

    made = []
    csv_gz = out / f"{base}.csv.gz"
    df.write_csv(csv_gz.with_suffix(""))  # polars writes plain csv
    import gzip
    import shutil

    with open(csv_gz.with_suffix(""), "rb") as fi, gzip.open(csv_gz, "wb") as fo:
        shutil.copyfileobj(fi, fo)
    csv_gz.with_suffix("").unlink()
    made.append(csv_gz)

    try:
        from datetime import datetime, timezone

        from sportsdataverse._rds import write_rds

        # Match mlb_model_publish/builders.py exactly: R has no unsigned integer
        # type, and the S3 class chain is what makes baseballr's loaders see a
        # baseballr_data tibble rather than a bare data.frame.
        rds_df = df.with_columns(
            pl.col(c).cast(pl.Int64)
            for c, t in df.schema.items()
            if t in (pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)
        )
        rds = out / f"{base}.rds"
        write_rds(
            rds_df,
            rds,
            cls=["baseballr_data", "tbl_df", "tbl", "data.table", "data.frame"],
            attributes={
                "baseballr_timestamp": datetime.now(timezone.utc),
                "baseballr_type": f"MLB {dataset} data",
            },
        )
        made.append(rds)
    except (ImportError, AttributeError) as exc:
        # Loud, not silent: an R consumer expecting .rds must not discover the
        # gap by finding the asset missing on the release.
        print(f"  WARN: no rds for {base} ({type(exc).__name__}: {exc})")
    return made


def publish_season(
    root: pathlib.Path, dataset: str, season: int, *, dry_run: bool = True, sides: bool = True
) -> dict:
    from mlb_model_publish.artifacts import upload_artifacts

    tag, stem = TAGS[dataset]
    parquet = root / "mlb" / "pbp" / stem.format(season=season)
    if not parquet.exists():
        raise FileNotFoundError(f"{parquet} -- run stage 03 first")

    files = [parquet]
    if sides:
        files += build_side_formats(root, dataset, season)

    # upload_artifacts globs a directory; give it each file explicitly by
    # pointing at the parent with an exact-name pattern, so nothing unrelated
    # in the directory is ever swept into a release.
    uploaded = 0
    for f in files:
        r = upload_artifacts(f.parent, tag, REPO, pattern=f.name, dry_run=dry_run)
        uploaded += r.get("uploaded", 0) if isinstance(r, dict) else 0

    if not dry_run and uploaded:
        (staging_dir(root) / f".done_{dataset}_{season}").write_text("")
    return {"dataset": dataset, "season": season, "files": len(files), "uploaded": uploaded}


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", choices=sorted(TAGS) + ["all"], default="all")
    ap.add_argument("--season", type=int)
    ap.add_argument("--start", type=int)
    ap.add_argument("--end", type=int)
    ap.add_argument("--root", default=".")
    ap.add_argument(
        "--no-sides", action="store_true", help="parquet only; skip the csv.gz/rds release assets"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--publish", action="store_true")
    a = ap.parse_args(argv)

    if a.season:
        seasons = [a.season]
    elif a.start and a.end:
        lo, hi = sorted((a.start, a.end))
        seasons = list(range(lo, hi + 1))
    else:
        ap.error("need --season, or --start and --end")

    datasets = sorted(TAGS) if a.dataset == "all" else [a.dataset]
    root = pathlib.Path(a.root)
    rc = 0
    for ds in datasets:
        for s in seasons:
            try:
                st = publish_season(root, ds, s, dry_run=not a.publish, sides=not a.no_sides)
                print(f"  {st}", flush=True)
            except Exception as exc:  # noqa: BLE001 - one season must not kill the range
                print(f"  {ds} {s}: FAILED {type(exc).__name__}: {exc}")
                rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
