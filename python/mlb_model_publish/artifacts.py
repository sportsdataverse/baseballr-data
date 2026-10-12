"""Release-asset uploads via the gh CLI (pattern path, family template)."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Optional

from sportsdataverse.release import upload_release_sidecars

GH_TIMEOUT_SECONDS = 300

# Release-notes body used when auto-creating a missing release. Keyed by tag;
# falls back to a generic note for any other tag.
_RELEASE_BODY = {
    "mlb_game_state": (
        "MLB game-state tables per season: the empirical RE24 run-expectancy "
        "matrix (within 0.05 of the Tango reference), the win-expectancy state "
        "table, and per-play WPA/leverage (spearman >= 0.95 vs statsapi win "
        "probability, per-game WPA telescoping identity within 0.02). Built by "
        "sdv-py's mlb_run_expectancy / mlb_win_expectancy over statsapi pbp."
    ),
    "mlb_hitting_models": (
        "MLB expected hitting stats per batter-season: xwOBA/xBA/xSLG "
        "(spearman >= 0.95 vs Savant same-input), expected home runs "
        "(full-season spearman >= 0.90), and as-of batter projections with "
        "aging curves. Built by sdv-py's mlb_expected_stats / "
        "mlb_expected_home_runs / mlb_batter_projection over Baseball Savant."
    ),
    "mlb_fielding_models": (
        "MLB fielding models per season: outs above average per (fielder, "
        "position) (full-season Pearson 0.605 vs Savant OAA) and catcher "
        "framing (0.468 vs Savant). Catcher throwing/blocking and baserunning "
        "are deliberately EXCLUDED -- the public per-pitch feed lacks the "
        "tracking data (documented data ceiling). Built by sdv-py's "
        "mlb_fielding_oaa / mlb_catcher_framing over Baseball Savant."
    ),
    "mlb_pitching_models": (
        "MLB pitching models per season: xERA (MAE <= 0.30 vs Savant xERA), "
        "arsenal-level Stuff+ (rank gate >= 0.20) and pitcher-level Command+ "
        "(directional gate only -- see the model card). SIERA-like and pitch "
        "tunneling are deliberately EXCLUDED (unfitted coefficients / no "
        "public oracle). Built by sdv-py's pitching suite over Baseball Savant."
    ),
}


#: Release sidecar metadata: the loader a consumer reads each tag through.
#: R's sportsdataverse_save() writes this as package_function.txt/.json beside
#: every published asset; this publisher dropped it along with the timestamp
#: pair. Model tags with no loader fall back to naming this producer -- the
#: convention the ncaa_*_rapm tags already carry on their published sidecars.
PKG_FUNCTION: dict[str, str] = {
    "mlb_fielding_models": "sportsdataverse.mlb.load_mlb_oaa()",
    "mlb_game_state": "sportsdataverse.mlb.load_mlb_re24_matrix()",
    "mlb_hitting_models": "sportsdataverse.mlb.load_mlb_expected_stats()",
    "mlb_pitching_models": "sportsdataverse.mlb.load_mlb_xera()",
}
_PRODUCER = "python/mlb_model_publish/artifacts.py"


def _gh_runner(args: list) -> None:
    subprocess.run(["gh", *args], check=True, timeout=GH_TIMEOUT_SECONDS)


def release_digests(tag: str, repo: str) -> Optional[dict[str, str]]:
    """``{asset name: "sha256:<hex>"}`` for ``tag`` in ONE core API call; ``None`` = no release.

    The release object embeds its whole asset list with a server-computed
    ``digest`` per asset, so one call answers both "does the release exist" and
    "which assets changed". The droplet shares one 5,000/h quota; the old
    per-file ``gh release view`` + ``gh release upload`` + per-file 4-sidecar
    stamp cost 66 HTTP requests for one 3-file season (measured 2026-10-12).
    """
    r = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/releases/tags/{tag}",
            "--jq",
            r'.assets[] | "\(.name)\t\(.digest)"',
        ],
        capture_output=True,
        text=True,
        timeout=GH_TIMEOUT_SECONDS,
    )
    if r.returncode:
        if "404" in r.stderr:
            return None  # first publish of this tag
        # auth / rate limit must surface (the stage-05 retry keys on the text)
        raise RuntimeError(f"gh api releases/tags/{tag}: {r.stderr.strip()}")
    return dict(ln.split("\t", 1) for ln in r.stdout.splitlines() if "\t" in ln)


def file_sha256(path) -> str:
    """``"sha256:<hex>"`` -- the format of a release asset's ``digest``."""
    with open(path, "rb") as fh:
        return "sha256:" + hashlib.file_digest(fh, "sha256").hexdigest()


def upload_artifacts(
    artifacts_dir,
    tag: str,
    repo: str,
    *,
    pattern: str,
    dry_run: bool = False,
    runner=None,
    exists_check=None,
) -> dict:
    """Upload ``artifacts_dir.glob(pattern)`` (sorted) to the ``tag`` release.

    The release is created if it does not already exist (``gh release upload``
    does not create one), so a single call is self-sufficient. Uploads are one
    ``gh release upload`` per file with ``--clobber`` -- never a multi-file
    glob, which silently drops large assets. A file whose sha256 already
    equals the release asset's ``digest`` is skipped (one listing per call).
    ``.parquet`` files go LAST: stage 05 treats an unchanged parquet as "this
    season is already fully published", so it must land after its csv.gz/rds.
    ``runner`` and ``exists_check`` are injectable for hermetic testing; an
    injected ``exists_check`` skips the listing and uploads everything.
    """
    run = runner or _gh_runner
    files = sorted(Path(artifacts_dir).glob(pattern), key=lambda f: (f.suffix == ".parquet", f))
    created_release = False
    remote: Optional[dict[str, str]] = {}
    if dry_run:
        print(f"[dry-run] would ensure release {repo}:{tag} exists")
    elif exists_check is not None:
        remote = {} if exists_check(tag, repo) else None
    else:
        remote = release_digests(tag, repo)
    if remote is None:
        body = _RELEASE_BODY.get(tag, f"{tag} (auto-created by mlb_model_publish).")
        run(["release", "create", tag, "--repo", repo, "--title", tag, "--notes", body])
        created_release = True
        remote = {}
    uploaded = skipped = 0
    for f in files:
        if dry_run:
            print(f"[dry-run] would upload {f} -> {repo}:{tag}")
            continue
        if remote.get(f.name) == file_sha256(f):
            skipped += 1
            continue
        run(["release", "upload", tag, str(f), "--repo", repo, "--clobber"])
        uploaded += 1
    if skipped:
        print(f"{tag}: {skipped} unchanged asset(s) skipped, {uploaded} uploaded")
    # stamp LAST so the timestamp describes a finished upload, and only when
    # something actually uploaded -- a stamp on a no-op run would claim data
    # moved when it did not
    if uploaded:
        upload_release_sidecars(
            tag, runner=run, pkg_function=PKG_FUNCTION.get(tag, _PRODUCER), repo=repo
        )
    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "files": [str(f) for f in files],
        "tag": tag,
        "created_release": created_release,
    }
