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
