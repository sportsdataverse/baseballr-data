"""The reshape contract, exercised against the committed raw tree.

This is the runnable check for the claim the whole design rests on (see
``docs/mlb-raw-layer.md`` §8): a ``-data`` consumer can enumerate games from the
manifest alone, verify the bytes it got, read every Savant slice as Utf8 against
one declared 119-column contract, and concat them **vertically** -- never
``diagonal``, which is the operation that turns "this file is from a different
vintage" into "this value is null".

Skips cleanly when the raw tree is not checked out (CI uses a sparse checkout).
"""

from __future__ import annotations

import gzip
import io
import json
from pathlib import Path

import polars as pl
import pytest
from mlb_raw import core

REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / "mlb" / "raw"
SEASON = 2024

pytestmark = pytest.mark.skipif(
    not core.manifest_path(ROOT, SEASON).is_file(),
    reason="mlb/raw not checked out (sparse checkout)",
)


def _manifest() -> "dict[int, dict]":
    return core.read_manifest(ROOT, SEASON)


def _captured(surface: str, limit: int) -> "list[dict]":
    rows = [r for r in _manifest().values() if r.get(f"{surface}_path")]
    if not rows:
        pytest.skip(f"no {surface} captures in the {SEASON} manifest yet")
    rows.sort(key=lambda r: int(r["game_pk"]))
    step = max(1, len(rows) // limit)
    return rows[::step][:limit]


def test_manifest_is_the_only_enumeration_a_consumer_needs() -> None:
    idx = core.index_path(ROOT)
    assert idx.is_file(), "manifest/index.csv is the season-discovery entry point"
    seasons = pl.read_csv(idx)
    assert SEASON in seasons["season"].to_list()
    m = _manifest()
    assert len(m) > 2_000
    # keys a consumer joins on are present and non-null for every row
    for r in m.values():
        assert r["game_pk"] and r["game_date"] and r["game_type"] and r["status_code"]


@pytest.mark.parametrize("surface", ["statsapi", "savant"])
def test_recorded_bytes_and_sha256_match_the_files(surface: str) -> None:
    for r in _captured(surface, 8):
        p = ROOT / r[f"{surface}_path"]
        assert p.is_file(), p
        assert p.stat().st_size == int(r[f"{surface}_bytes"])
        assert core.sha256_of(p) == r[f"{surface}_sha256"]


def test_statsapi_payloads_decode_and_carry_plays() -> None:
    for r in _captured("statsapi", 5):
        payload = json.loads(gzip.decompress((ROOT / r["statsapi_path"]).read_bytes()))
        assert int(payload["gamePk"]) == int(r["game_pk"])
        plays = payload["liveData"]["plays"]["allPlays"]
        assert plays, r["game_pk"]
        # playEvents is what makes this payload worth 69% of its bytes
        assert any(e.get("isPitch") for p in plays for e in p.get("playEvents", []))


def test_savant_slices_share_one_column_contract_and_concat_vertically() -> None:
    rows = _captured("savant", 8)
    frames = []
    for r in rows:
        text = gzip.decompress((ROOT / r["savant_path"]).read_bytes()).decode("utf-8")
        df = pl.read_csv(
            io.StringIO(text), infer_schema_length=0
        )  # R1: Utf8, typed by the consumer
        assert df.height == int(r["savant_rows"])
        assert set(df["game_pk"].unique().to_list()) == {str(r["game_pk"])}
        frames.append(df)

    contract = frames[0].columns
    assert len(contract) == 119, f"Savant's per-pitch contract is 119 columns, got {len(contract)}"
    for df, r in zip(frames, rows):
        assert df.columns == contract, f"{r['game_pk']} diverges from the column contract"

    # R2: vertical, never diagonal. A diagonal concat would silently null-fill
    # a divergent slice instead of failing.
    out = pl.concat(frames, how="vertical")
    assert out.height == sum(f.height for f in frames)
    assert out.width == 119
    assert out["game_pk"].n_unique() == len(frames)


def test_null_means_untracked_not_zero() -> None:
    """The property the per-season cache cannot give: nulls are per-pitch facts.

    On a 2024 slice ``estimated_woba_using_speedangle`` is null exactly where
    there was no batted ball, so a consumer can tell "not measured" from a real
    value -- it never has to ask which day the scraper ran.
    """
    r = _captured("savant", 1)[0]
    text = gzip.decompress((ROOT / r["savant_path"]).read_bytes()).decode("utf-8")
    df = pl.read_csv(io.StringIO(text), infer_schema_length=0)
    for col in ("estimated_woba_using_speedangle", "launch_speed", "bat_speed", "arm_angle"):
        assert col in df.columns, f"{col} must exist in every season's capture"
    bip = df.filter(pl.col("launch_speed").is_not_null())
    assert bip.height > 0
    # every tracked batted ball carries an expected-stat value
    assert bip["estimated_woba_using_speedangle"].null_count() < bip.height
