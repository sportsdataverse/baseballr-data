"""The release sidecars R's sportsdataverse_save() attaches to every tag.

The model publisher hand-rolled ``gh release upload`` and never wrote them, so
its tags carry no timestamp -- a consumer has no way to tell whether the
artifacts moved since they last downloaded.
"""

import json
from pathlib import Path

from mlb_model_publish.artifacts import _PRODUCER, PKG_FUNCTION, upload_artifacts

SIDECAR_NAMES = [
    "timestamp.txt",
    "timestamp.json",
    "package_function.txt",
    "package_function.json",
]
TAG = "mlb_hitting_models"


def _seed(tmp_path):
    (tmp_path / "mlb_expected_stats_2025.parquet").write_bytes(b"x")
    return tmp_path


def test_upload_stamps_the_tag_last(tmp_path):
    calls: list[list[str]] = []

    upload_artifacts(
        _seed(tmp_path),
        TAG,
        "sportsdataverse/sportsdataverse-data",
        pattern="mlb_expected_stats_*.*",
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )

    names = [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]]
    assert names[-4:] == SIDECAR_NAMES
    assert len(names) > 4, "the data asset itself must still upload"
    assert all(c[2] == TAG and c[-1] == "--clobber" for c in calls)


def test_nothing_uploaded_means_no_stamp(tmp_path):
    """A run that published nothing must not move the timestamp."""
    calls: list[list[str]] = []

    upload_artifacts(
        tmp_path,
        TAG,
        "r/r",
        pattern="mlb_expected_stats_*.*",
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )

    assert not any(c[:2] == ["release", "upload"] for c in calls)


def test_sidecars_carry_a_name_and_a_timestamp(tmp_path):
    seen: dict[str, str] = {}

    def _runner(argv: list[str]) -> None:
        # read inside the runner: the temp dir is cleaned up behind the upload
        path = Path(argv[3])
        if path.name.startswith(("timestamp.", "package_function.")):
            seen[path.name] = path.read_text()

    upload_artifacts(
        _seed(tmp_path),
        TAG,
        "r/r",
        pattern="mlb_expected_stats_*.*",
        runner=_runner,
        exists_check=lambda tag, repo: True,
    )

    expected = PKG_FUNCTION.get(TAG, _PRODUCER)
    assert seen["package_function.txt"].strip() == expected
    assert json.loads(seen["package_function.json"])["package_function"] == expected
    assert json.loads(seen["timestamp.json"])["last_updated"].strip()


def test_unchanged_assets_skip_and_parquet_uploads_last(tmp_path, monkeypatch):
    """One listing per call; digest-equal files cost nothing; parquet lands last."""
    from mlb_model_publish import artifacts

    d = tmp_path / "artifacts"
    d.mkdir()
    for n, body in (("m_2025.parquet", b"p"), ("m_2025.csv.gz", b"c"), ("m_2025.rds", b"r")):
        (d / n).write_bytes(body)
    listed: list[str] = []

    def fake_digests(tag, repo):
        listed.append(tag)
        return {"m_2025.csv.gz": artifacts.file_sha256(d / "m_2025.csv.gz")}

    monkeypatch.setattr(artifacts, "release_digests", fake_digests)
    calls: list[list[str]] = []
    res = upload_artifacts(d, "t", "r/r", pattern="*", runner=lambda a: calls.append(a))

    assert listed == ["t"]
    data = [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]][:2]
    assert data == ["m_2025.rds", "m_2025.parquet"]
    assert res["uploaded"] == 2 and res["skipped"] == 1


def test_stage05_skips_a_season_whose_parquet_is_already_published(tmp_path, monkeypatch):
    from mlb_model_publish import artifacts
    from mlb_raw import publish

    pq = tmp_path / "mlb" / "pbp" / "mlb_pbp_2026.parquet"
    pq.parent.mkdir(parents=True)
    pq.write_bytes(b"pbp")
    digest = artifacts.file_sha256(pq)
    remote = {"mlb_pbp_2026.parquet": digest, "mlb_pbp_2026.csv.gz": "x", "mlb_pbp_2026.rds": "x"}
    monkeypatch.setattr(artifacts, "release_digests", lambda tag, repo: remote)

    def boom(*a, **k):
        raise AssertionError("an unchanged season must not upload")

    monkeypatch.setattr(artifacts, "upload_artifacts", boom)
    res = publish.publish_season(tmp_path, "pbp", 2026, dry_run=False)
    assert res["uploaded"] == 0 and res["unchanged"]


def test_stem_with_unchanged_parquet_skips_its_restamped_rds(tmp_path, monkeypatch):
    """Model families write parquet/csv/rds into format subdirs; rds bytes change every
    build, so a stem whose parquet matches (and whose siblings exist) is skipped whole."""
    from mlb_model_publish import artifacts

    for sub, n, body in (
        ("parquet", "mlb_wpa_2026.parquet", b"p"),
        ("csv", "mlb_wpa_2026.csv", b"c"),
        ("rds", "mlb_wpa_2026.rds", b"fresh-timestamp"),
        ("parquet", "mlb_wpa_2025.parquet", b"old"),
        ("parquet", "mlb_re24_matrix_2026.parquet", b"changed"),
        ("rds", "mlb_re24_matrix_2026.rds", b"r"),
    ):
        (tmp_path / sub).mkdir(exist_ok=True)
        (tmp_path / sub / n).write_bytes(body)
    (tmp_path / "mlb_game_state_card.json").write_text("{}")
    sha = artifacts.file_sha256
    remote = {
        "mlb_wpa_2026.parquet": sha(tmp_path / "parquet" / "mlb_wpa_2026.parquet"),
        "mlb_wpa_2026.csv": sha(tmp_path / "csv" / "mlb_wpa_2026.csv"),
        "mlb_wpa_2026.rds": "sha256:yesterday",
        "mlb_wpa_2025.parquet": sha(tmp_path / "parquet" / "mlb_wpa_2025.parquet"),
        "mlb_re24_matrix_2026.parquet": "sha256:stale",
        "mlb_re24_matrix_2026.rds": "sha256:stale",
        "mlb_game_state_card.json": sha(tmp_path / "mlb_game_state_card.json"),
    }
    monkeypatch.setattr(artifacts, "release_digests", lambda tag, repo: remote)
    calls: list[list[str]] = []
    res = upload_artifacts(tmp_path, "t", "r/r", pattern="**/mlb_*.*", runner=calls.append)

    data = [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]][:2]
    assert data == ["mlb_re24_matrix_2026.rds", "mlb_re24_matrix_2026.parquet"]
    assert res["uploaded"] == 2 and res["skipped"] == 5


def test_unchanged_stems_upload_nothing_and_stamp_nothing(tmp_path, monkeypatch):
    from mlb_model_publish import artifacts

    for sub, n in (("parquet", "mlb_x_2026.parquet"), ("rds", "mlb_x_2026.rds")):
        (tmp_path / sub).mkdir()
        (tmp_path / sub / n).write_bytes(n.encode())
    pq = tmp_path / "parquet" / "mlb_x_2026.parquet"
    remote = {pq.name: artifacts.file_sha256(pq), "mlb_x_2026.rds": "sha256:other"}
    monkeypatch.setattr(artifacts, "release_digests", lambda tag, repo: remote)
    calls: list[list[str]] = []
    res = upload_artifacts(tmp_path, "t", "r/r", pattern="**/mlb_*.*", runner=calls.append)
    assert calls == [] and res["uploaded"] == 0
