"""Tests for ncaa_baseball_data_build.publish -- per-file ``gh release upload --clobber``.

Hermetic port of the ncaa-mfb-football-data publish suite: no real ``gh`` or R
calls. Files are pre-staged; ``runner``/``exists_check`` are fakes that just
record/answer.
"""

from __future__ import annotations

from pathlib import Path

from ncaa_baseball_data_build.config import REGISTRY
from ncaa_baseball_data_build.io import CSV_SUFFIX
from ncaa_baseball_data_build.publish import DEFAULT_REPO, publish_dataset

_SPEC = REGISTRY["pbp"]


#: release metadata sidecars -- asserted separately, not a data asset
SIDECARS = ("timestamp.", "package_function.")


def _stage(tmp_path: Path) -> None:
    pq_dir = tmp_path / "ncaa" / "pbp" / "parquet"
    pq_dir.mkdir(parents=True)
    (pq_dir / "ncaa_baseball_pbp_2024.parquet").write_bytes(b"parquet-bytes")

    rel_dir = tmp_path / "ncaa" / "_release_build" / "pbp"
    rel_dir.mkdir(parents=True)
    (rel_dir / f"ncaa_baseball_pbp_2024{CSV_SUFFIX}").write_bytes(b"csv-bytes")
    (rel_dir / "ncaa_baseball_pbp_2024.rds").write_bytes(b"rds-bytes")


def test_publish_creates_release_when_absent(tmp_path: Path):
    _stage(tmp_path)
    calls: list[list[str]] = []

    result = publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: False,
        make_rds=False,
    )

    creates = [c for c in calls if c[:2] == ["release", "create"]]
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(creates) == 1
    assert creates[0][2] == "ncaa_baseball_pbp"
    assert "--repo" in creates[0] and DEFAULT_REPO in creates[0]

    assert len(uploads) == 3
    for c in uploads:
        assert c[2] == "ncaa_baseball_pbp"
        assert c[4:6] == ["--repo", DEFAULT_REPO]
        assert c[-1] == "--clobber"

    assert result["uploaded"] == 3
    assert result["tag"] == "ncaa_baseball_pbp"


def test_publish_schedule_uses_plural_tag_singular_files(tmp_path: Path):
    """The R-era compat quirk end-to-end: assets named ncaa_baseball_schedule_*
    upload to the ncaa_baseball_schedules (plural) release."""
    spec = REGISTRY["schedule"]
    pq_dir = tmp_path / "ncaa" / "schedule" / "parquet"
    pq_dir.mkdir(parents=True)
    (pq_dir / "ncaa_baseball_schedule_2015.parquet").write_bytes(b"pq")
    calls: list[list[str]] = []

    result = publish_dataset(
        spec,
        2015,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: True,
        make_rds=False,
    )

    (upload,) = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert upload[2] == "ncaa_baseball_schedules"
    assert upload[3].endswith("ncaa_baseball_schedule_2015.parquet")
    assert result["tag"] == "ncaa_baseball_schedules"


def test_publish_skips_create_when_release_present(tmp_path: Path):
    _stage(tmp_path)
    calls: list[list[str]] = []

    publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: True,
        make_rds=False,
    )

    assert not any(c[:2] == ["release", "create"] for c in calls)
    data_uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(data_uploads) == 3


def test_publish_dry_run_makes_no_calls(tmp_path: Path):
    _stage(tmp_path)
    calls: list[list[str]] = []

    result = publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        dry_run=True,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: False,
        make_rds=False,
    )

    assert calls == []
    assert result["uploaded"] == 0


def _stage_parquet_only(tmp_path: Path) -> None:
    pq_dir = tmp_path / "ncaa" / "pbp" / "parquet"
    pq_dir.mkdir(parents=True)
    (pq_dir / "ncaa_baseball_pbp_2024.parquet").write_bytes(b"parquet-bytes")


def test_publish_only_parquet_staged_uploads_one_file(tmp_path: Path):
    _stage_parquet_only(tmp_path)
    calls: list[list[str]] = []

    result = publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: True,
        make_rds=False,
    )

    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(uploads) == 1
    assert result["uploaded"] == 1


def test_publish_make_rds_stages_and_uploads_rds(tmp_path: Path, monkeypatch):
    """Only a parquet is pre-staged; a stubbed rds.to_rds should get called and
    its output picked up as a second uploaded asset."""
    _stage_parquet_only(tmp_path)

    def _fake_to_rds(parquet_path, rds_path, **kwargs):
        rds_path = Path(rds_path)
        rds_path.parent.mkdir(parents=True, exist_ok=True)
        rds_path.write_bytes(b"fake-rds-bytes")
        return rds_path

    monkeypatch.setattr("ncaa_baseball_data_build.rds.to_rds", _fake_to_rds)
    calls: list[list[str]] = []

    result = publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: True,
        make_rds=True,
    )

    rds_path = tmp_path / "ncaa" / "_release_build" / "pbp" / "ncaa_baseball_pbp_2024.rds"
    assert rds_path.exists()
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(uploads) == 2
    assert result["uploaded"] == 2


def test_publish_make_rds_failure_still_uploads_parquet(tmp_path: Path, monkeypatch):
    """rds.to_rds raising must not block the parquet(+csv) upload -- swallowed, not fatal."""
    _stage_parquet_only(tmp_path)

    def _raising_to_rds(parquet_path, rds_path, **kwargs):
        raise RuntimeError("Rscript not found")

    monkeypatch.setattr("ncaa_baseball_data_build.rds.to_rds", _raising_to_rds)
    calls: list[list[str]] = []

    result = publish_dataset(
        _SPEC,
        2024,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda t, r: True,
        make_rds=True,
    )

    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(uploads) == 1
    assert result["uploaded"] == 1


# --- release-state resolution ------------------------------------------------
#
# The distinction under test: "this tag has nothing" vs "I could not look".
# Absence must be proven by gh's `release not found` stderr, never inferred
# from an exit code -- gh also exits non-zero for auth, rate-limit, and
# bad-repo errors.


class _Proc:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_published_assets_parses_names(monkeypatch):
    from ncaa_baseball_data_build import publish as P

    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: _Proc(0, "a.parquet\na.csv.gz\n"))
    assert P.published_assets("t") == {"a.parquet", "a.csv.gz"}


def test_published_assets_empty_only_for_confirmed_missing(monkeypatch):
    from ncaa_baseball_data_build import publish as P

    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: _Proc(1, "", "release not found"))
    assert P.published_assets("t") == set()


def test_published_assets_raises_on_ambiguous_exit(monkeypatch):
    """A DLL-init / auth / rate-limit failure must NOT look like an empty tag."""
    import pytest
    from ncaa_baseball_data_build import publish as P

    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: _Proc(3221225794))
    with pytest.raises(P.GhUnavailable):
        P.published_assets("t")


def test_published_assets_raises_on_launch_failure_and_timeout(monkeypatch):
    """These arrive as EXCEPTIONS, not exit codes -- an rc-only check misses them."""
    import pytest
    from ncaa_baseball_data_build import publish as P

    def _boom(*a, **k):
        raise OSError("gh not found")

    monkeypatch.setattr(P.subprocess, "run", _boom)
    with pytest.raises(P.GhUnavailable):
        P.published_assets("t")

    def _hang(*a, **k):
        raise P.subprocess.TimeoutExpired("gh", 1)

    monkeypatch.setattr(P.subprocess, "run", _hang)
    with pytest.raises(P.GhUnavailable):
        P.published_assets("t")


def test_release_exists_assumes_present_when_gh_cannot_answer(monkeypatch):
    """Safe default: a bogus `release create` over a live tag fails the unit."""
    from ncaa_baseball_data_build import publish as P

    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: _Proc(3221225794))
    assert P._gh_release_exists("t", DEFAULT_REPO) is True

    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: _Proc(1, "", "release not found"))
    assert P._gh_release_exists("t", DEFAULT_REPO) is False


def test_published_seasons_requires_parquet_and_csv():
    from ncaa_baseball_data_build.publish import published_seasons

    assets = {
        "ncaa_baseball_pbp_2023.parquet",
        f"ncaa_baseball_pbp_2023{CSV_SUFFIX}",
        "ncaa_baseball_pbp_2024.parquet",  # csv missing -> not published
    }
    assert published_seasons(_SPEC, assets=assets) == {2023}


# --- gh timeouts -------------------------------------------------------------
#
# Metadata calls and uploads are different workloads; a shared short bound
# once aborted a merely-slow multi-MB upload mid-transfer in the MFB repo.


def test_metadata_and_upload_timeouts_are_separate():
    from ncaa_baseball_data_build import publish as P

    assert P._GH_TIMEOUT == 180
    assert P._GH_UPLOAD_TIMEOUT > P._GH_TIMEOUT
    # Big enough for a large asset on a slow link.
    assert P._GH_UPLOAD_TIMEOUT >= 900


def test_uploads_use_the_long_timeout(tmp_path: Path, monkeypatch):
    """The upload shell-out must not inherit the short metadata bound."""
    from ncaa_baseball_data_build import publish as P

    _stage(tmp_path)
    seen: "list[int]" = []

    def _fake_run(argv, **kw):
        # the long bound is about a 100MB+ data asset on a slow link; the
        # ~50-byte release sidecars run on the short metadata bound and would
        # otherwise read as a violation of it
        if "upload" in argv and not any(Path(a).name.startswith(SIDECARS) for a in argv):
            seen.append(kw.get("timeout"))

        class _R:
            returncode = 0

        return _R()

    monkeypatch.setattr(P.subprocess, "run", _fake_run)
    P.publish_dataset(_SPEC, 2024, base=tmp_path, exists_check=lambda *_: True, make_rds=False)
    assert seen, "no upload was attempted"
    assert all(t == P._GH_UPLOAD_TIMEOUT for t in seen), seen
