"""Tests for ncaa_baseball_data_build.io -- parquet writer + csv staging + manifest.

Hermetic port of the ncaa-mfb-football-data io suite (league dir ``ncaa``,
``ncaa_baseball_*`` stems).
"""

import gzip
from pathlib import Path

import polars as pl
from ncaa_baseball_data_build.config import REGISTRY
from ncaa_baseball_data_build.io import CSV_SUFFIX, write_dataset

SPEC = REGISTRY["pbp"]


def _frame(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"game_key": [str(i) for i in range(n)], "value": [float(i) for i in range(n)]}
    )


def test_release_false_writes_only_parquet_no_csv_anywhere(tmp_path: Path):
    df = _frame(5)
    paths = write_dataset(df, SPEC, 2024, base=tmp_path)

    pq = tmp_path / "ncaa" / "pbp" / "parquet" / "ncaa_baseball_pbp_2024.parquet"
    assert paths == [pq]
    assert pq.exists()
    assert pl.read_parquet(pq).equals(df)

    release_dir = tmp_path / "ncaa" / "_release_build"
    # Both suffixes: asserting only *.csv.gz would let a regression that writes
    # a PLAIN csv when release=False pass unnoticed.
    staged = list(release_dir.rglob("*.csv")) + list(release_dir.rglob("*.csv.gz"))
    assert not release_dir.exists() or not staged


def test_release_true_writes_parquet_and_staged_csv(tmp_path: Path):
    df = _frame(5)
    paths = write_dataset(df, SPEC, 2024, base=tmp_path, release=True)

    assert len(paths) == 2
    csv = tmp_path / "ncaa" / "_release_build" / "pbp" / f"ncaa_baseball_pbp_2024{CSV_SUFFIX}"
    assert csv in paths
    assert csv.exists()
    # Gzipped, and still a readable csv once decompressed -- the asset a
    # consumer downloads must survive the round trip, not merely exist.
    assert csv.read_bytes()[:2] == bytes.fromhex("1f8b"), "release csv is not gzip-compressed"
    with gzip.open(csv, "rb") as fh:
        read_back = pl.read_csv(fh)
    assert read_back.height == df.height
    assert read_back.width == df.width


def test_release_csv_flattens_list_columns(tmp_path: Path):
    """PBP carries List(Utf8) columns json can't put in a csv cell -- they must
    be joined to strings, not crash the writer."""
    df = pl.DataFrame(
        {"game_key": ["1"], "scoring_runners": [["A. Runner", "B. Runner"]]},
        schema={"game_key": pl.Utf8, "scoring_runners": pl.List(pl.Utf8)},
    )
    write_dataset(df, SPEC, 2024, base=tmp_path, release=True)
    csv = tmp_path / "ncaa" / "_release_build" / "pbp" / f"ncaa_baseball_pbp_2024{CSV_SUFFIX}"
    with gzip.open(csv, "rt") as fh:
        text = fh.read()
    assert "A. Runner; B. Runner" in text
    # the committed parquet keeps the real list dtype
    pq = tmp_path / "ncaa" / "pbp" / "parquet" / "ncaa_baseball_pbp_2024.parquet"
    assert pl.read_parquet(pq).schema["scoring_runners"] == pl.List(pl.Utf8)


def test_manifest_upserts_by_dataset_and_season(tmp_path: Path):
    df = _frame(5)
    write_dataset(df, SPEC, 2024, base=tmp_path)

    mf = tmp_path / "ncaa" / "pbp" / "manifest.csv"
    assert mf.exists()
    rows = pl.read_csv(mf)
    pbp_2024 = rows.filter((pl.col("dataset") == "pbp") & (pl.col("season") == 2024))
    assert pbp_2024.height == 1
    assert pbp_2024["row_count"][0] == 5

    # Re-write same (dataset, season) with a different height -> upsert, not append-dup.
    df2 = _frame(9)
    write_dataset(df2, SPEC, 2024, base=tmp_path)
    rows = pl.read_csv(mf)
    pbp_2024 = rows.filter((pl.col("dataset") == "pbp") & (pl.col("season") == 2024))
    assert pbp_2024.height == 1
    assert pbp_2024["row_count"][0] == 9

    # A second season adds a second row.
    write_dataset(_frame(3), SPEC, 2023, base=tmp_path)
    rows = pl.read_csv(mf)
    assert rows.height == 2


def test_schedule_quirk_singular_stem(tmp_path: Path):
    """The schedules release keeps the R-era plural-tag/singular-stem split:
    files are ncaa_baseball_schedule_{year}.* under ncaa/schedule/."""
    spec = REGISTRY["schedule"]
    assert spec.tag == "ncaa_baseball_schedules"  # plural: baseballr loader compat
    assert spec.stem == "ncaa_baseball_schedule"  # singular asset stem
    paths = write_dataset(_frame(2), spec, 2015, base=tmp_path, release=True)
    assert paths[0].name == "ncaa_baseball_schedule_2015.parquet"
    assert paths[1].name == f"ncaa_baseball_schedule_2015{CSV_SUFFIX}"
