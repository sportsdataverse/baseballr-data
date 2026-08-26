"""Release publishing -- per-file ``gh release upload --clobber`` (create-if-missing).

Ported from ncaa-mfb-football-data. Multi-asset globs silently drop large
files, so upload one file at a time -- and uploads never delete-then-upload,
they overwrite in place via ``--clobber``. ``runner``/``exists_check`` are
injectable for hermetic tests.

Assets are parquet (in-repo, always present) + csv + rds (release staging,
gitignored, written by ``io.write_dataset(release=True)`` / ``rds.to_rds``).
Filenames use ``spec.stem`` while the release itself is ``spec.tag`` -- the
schedules dataset keeps the R-era plural-tag/singular-stem quirk.
"""

from __future__ import annotations

import subprocess
from functools import partial
from pathlib import Path
from typing import Callable

from ncaa_baseball_data_build._logging import get_logger, human_size
from ncaa_baseball_data_build.config import DatasetSpec
from ncaa_baseball_data_build.io import CSV_SUFFIX  # single source of the csv extension

_LEAGUE = "ncaa"

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

log = get_logger()


# Bound each `gh` shell-out so a network stall / hung invocation can't block the
# whole publish run indefinitely (a failed upload is safe to re-run -- every
# upload is idempotent via --clobber). Metadata calls (release view/create)
# answer in seconds; UPLOADS move tens of MB and need their own far longer
# bound -- one shared timeout cannot serve both workloads (an MFB run once
# aborted a merely-slow upload mid-transfer on the short bound).
_GH_TIMEOUT = 180
_GH_UPLOAD_TIMEOUT = 1800


def _gh(args: list[str], *, timeout: int = _GH_TIMEOUT) -> None:
    subprocess.run(["gh", *args], check=True, timeout=timeout)


class GhUnavailable(RuntimeError):
    """``gh`` could not answer -- callers must NOT infer release state from it.

    The whole point of this type is that "I don't know" is not "it's empty".
    Collapsing the two is what makes a resume gate silently re-upload a full
    history.
    """


# `gh release view` prints exactly this to stderr when the tag does not exist.
# It is the ONLY confirmation of absence: a nonzero exit alone is not, because
# gh also exits nonzero for auth failures, rate limits, and bad repos.
_NOT_FOUND = "release not found"


def _gh_release_assets(tag: str, repo: str) -> "set[str] | None":
    """Asset names for ``tag``, or ``None`` when the release is CONFIRMED absent.

    Raises:
        GhUnavailable: gh could not answer -- ambiguous exit, launch failure, or
            timeout, each retried once first.

    Absence is proven by the ``release not found`` stderr line, never by an
    exit code (a transient DLL-init failure once read as "tag missing" and
    killed a live unit on a redundant ``release create``).
    """
    last: str = "unknown"
    for attempt in (1, 2):
        try:
            proc = subprocess.run(
                # fmt: off
                [
                    "gh",
                    "release",
                    "view",
                    tag,
                    "--repo",
                    repo,
                    "--json",
                    "assets",
                    "--jq",
                    ".assets[].name",
                ],
                # fmt: on
                capture_output=True,
                text=True,
                timeout=_GH_TIMEOUT,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            # gh failed to LAUNCH or hung -- never an exit code, so an
            # rc-only check would not see these at all.
            last = f"{type(exc).__name__}: {exc}"
            log.warning("gh release view %s: %s (attempt %d/2)", tag, last, attempt)
            continue
        if proc.returncode == 0:
            return {ln.strip() for ln in proc.stdout.splitlines() if ln.strip()}
        err = (proc.stderr or "").strip()
        if _NOT_FOUND in err.lower():
            return None  # confirmed absent
        last = f"exit {proc.returncode}: {err[:200]}"
        log.warning("gh release view %s: %s (attempt %d/2)", tag, last, attempt)
    raise GhUnavailable(f"cannot resolve release {tag} on {repo} -- {last}")


def _gh_release_exists(tag: str, repo: str) -> bool:
    """True when ``tag`` exists. An unanswerable gh is NOT reported as absent.

    When gh cannot answer, assume the release EXISTS: if it really is missing,
    the upload that follows fails loudly and the unit retries, whereas assuming
    absence makes the caller ``release create`` over a live tag and takes an
    otherwise-good unit down.
    """
    try:
        return _gh_release_assets(tag, repo) is not None
    except GhUnavailable as exc:
        log.warning(
            "%s -- assuming the release exists, so a real upload error surfaces "
            "instead of a bogus 'release create'",
            exc,
        )
        return True


def published_assets(tag: str, repo: str = DEFAULT_REPO) -> set[str]:
    """Asset names attached to ``tag``; empty set only when it is CONFIRMED absent.

    Raises:
        GhUnavailable: gh could not answer. This MUST propagate -- it is the
            difference between "this tag has nothing" and "I could not look".
            Returning an empty set on failure would make ``check --porcelain``
            emit an empty resume index and exit 0, so a resume driver would
            rebuild and re-upload the entire history while reporting a clean run.

    ONE ``gh`` call per TAG, not per (dataset, season): a sweep costs 9 calls
    instead of ~135, and per-unit polling has burned API quota into a 403
    partway through a publish before.
    """
    return _gh_release_assets(tag, repo) or set()


def published_seasons(
    spec: DatasetSpec,
    *,
    repo: str = DEFAULT_REPO,
    assets: "set[str] | None" = None,
) -> set[int]:
    """Seasons of ``spec`` whose REQUIRED assets are actually on the release.

    Required = parquet AND csv; ``.rds`` is best-effort (``publish_dataset``
    already degrades to a warning when no R install has arrow), so demanding it
    would make every season look unpublished on a machine without R.

    This exists because **a manifest row proves a BUILD, not a PUBLISH.**
    ``io.write_dataset`` upserts the manifest before ``publish_dataset`` runs,
    so a season whose upload failed still leaves a manifest row behind -- and a
    resume check keyed on the manifest skips it forever, silently. Ask the
    release what it actually has.
    """
    names = published_assets(spec.tag, repo) if assets is None else assets
    out: set[int] = set()
    prefix, suffix = f"{spec.stem}_", ".parquet"
    for n in names:
        if not (n.startswith(prefix) and n.endswith(suffix)):
            continue
        season = n[len(prefix) : -len(suffix)]
        if season.isdigit() and f"{prefix}{season}{CSV_SUFFIX}" in names:
            out.add(int(season))
    return out


def _dataset_files(spec: DatasetSpec, season: int, base: Path) -> list[Path]:
    release_dir = base / _LEAGUE / "_release_build" / spec.name
    cands = [
        base / _LEAGUE / spec.name / "parquet" / f"{spec.stem}_{season}.parquet",
        release_dir / f"{spec.stem}_{season}{CSV_SUFFIX}",
        release_dir / f"{spec.stem}_{season}.rds",
    ]
    return [f for f in cands if f.exists()]


def publish_dataset(
    spec: DatasetSpec,
    season: int,
    *,
    base: "str | Path",
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: "Callable[[list[str]], None] | None" = None,
    exists_check: "Callable[[str, str], bool] | None" = None,
    make_rds: bool = True,
) -> dict:
    """Upload a dataset/season's parquet + csv + rds to the release, creating it if missing.

    Args:
        spec: Dataset spec from ``config.REGISTRY``.
        season: Season year; must match the files already written by ``io.write_dataset``.
        base: Root directory containing ``ncaa/{dataset}/parquet`` + ``ncaa/_release_build/{dataset}``.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, skip all ``gh`` calls and log the would-be uploads.
        runner: Injectable ``gh`` arg-list executor; defaults to a real subprocess call.
        exists_check: Injectable ``(tag, repo) -> bool`` release-existence check.
        make_rds: If True, stage the rds asset from the parquet (via ``rds.to_rds``)
            when missing. RDS failure (e.g. no Rscript/arrow) only logs a warning --
            it never blocks the parquet+csv upload.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.

    Example:
        Quick start::

            from ncaa_baseball_data_build.config import REGISTRY
            from ncaa_baseball_data_build import publish
            publish.publish_dataset(REGISTRY["pbp"], 2024, base=".")
    """
    run = runner or _gh
    # Uploads move tens of MB and need a far longer bound than the metadata
    # calls (see _GH_UPLOAD_TIMEOUT). Bind it here rather than widening `_gh`'s
    # default, so release view/create keep a bound short enough to still detect
    # a hang. An INJECTED runner is passed through untouched -- its signature is
    # the documented `Callable[[list[str]], None]` and tests' fakes take no
    # timeout kwarg.
    upload = runner or partial(_gh, timeout=_GH_UPLOAD_TIMEOUT)
    exists = exists_check or _gh_release_exists
    base = Path(base)

    if make_rds:
        parquet = base / _LEAGUE / spec.name / "parquet" / f"{spec.stem}_{season}.parquet"
        rds_path = base / _LEAGUE / "_release_build" / spec.name / f"{spec.stem}_{season}.rds"
        # Regenerate the rds when it's missing OR stale (parquet rebuilt since):
        # a prior run's rds must never be uploaded against a freshly written parquet.
        if parquet.exists() and (
            not rds_path.exists() or rds_path.stat().st_mtime < parquet.stat().st_mtime
        ):
            from ncaa_baseball_data_build import rds

            try:
                rds.to_rds(parquet, rds_path)
            except Exception as e:  # noqa: BLE001 -- R may be absent in CI
                log.warning(
                    "%s %s: rds conversion failed, skipping rds asset: %s",
                    spec.name,
                    season,
                    e,
                )

    files = _dataset_files(spec, season, base)
    if not files:
        log.warning("%s %s: no files to publish under %s", spec.name, season, base)

    if not dry_run and not exists(spec.tag, repo):
        log.info("release %s missing on %s -- creating it", spec.tag, repo)
        run(
            [
                "release",
                "create",
                spec.tag,
                "--repo",
                repo,
                "--title",
                spec.tag,
                "--notes",
                f"{spec.tag} (NCAA baseball dataset, Python-built).",
            ]
        )

    count = 0
    for f in files:
        if dry_run:
            size = human_size(f.stat().st_size)
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, spec.tag)
            continue
        size = human_size(f.stat().st_size)
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, spec.tag)
        upload(["release", "upload", spec.tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, spec.tag, count, len(files))

    return {"tag": spec.tag, "files": [str(f) for f in files], "uploaded": count}
