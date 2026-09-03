"""Shared plumbing for the MLB raw stages: paths, persist guards, manifest I/O.

Design invariants this module owns:

1. **Never persist an empty or error payload.** Every write goes through
   :func:`persist_json` / :func:`persist_text`, which take a validator. A
   payload that fails validation is not written and the caller counts it as a
   miss -- so a zero-byte or ``{"messageNumber": 10}`` error body can never
   masquerade as a captured game.
2. **Presence-based resume.** :func:`already_captured` is the only skip test;
   it checks the file exists and is non-trivial. Re-running a stage is always
   safe and always cheap.
3. **Deterministic bytes.** JSON is stored as canonical, key-order-preserving,
   separator-minimal UTF-8 (``json.dumps(..., separators=(",", ":"))``) then
   gzipped at ``mtime=0``. Re-capturing an unchanged payload reproduces the
   same file byte-for-byte, so git sees no diff and a re-run costs nothing.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

#: One row per captured game. This file IS the manifest, the integrity index
#: and the resume checkpoint -- a consumer reads it over raw.githubusercontent
#: to enumerate game ids without listing a directory.
MANIFEST_COLUMNS = (
    "game_pk",
    "game_date",
    "season",
    "game_type",
    "status_code",
    "doubleheader",
    "game_number",
    "home_id",
    "home_abbr",
    "away_id",
    "away_abbr",
    "venue_id",
    "statsapi_path",
    "statsapi_bytes",
    "statsapi_sha256",
    "savant_path",
    "savant_rows",
    "savant_bytes",
    "savant_sha256",
)

#: Captured by default. ``S`` (spring) and ``E``/``A`` (exhibition/all-star)
#: are enumerated in the manifest but not captured unless asked for.
DEFAULT_GAME_TYPES = ("R", "F", "D", "L", "W")

#: A ``feed/live`` payload smaller than this is structurally impossible for a
#: completed game (the smallest real one measured is 443KB raw / 34KB gz, a
#: 2000-season game); anything under it is an error body.
MIN_STATSAPI_BYTES = 20_000

#: Savant caps ``/statcast_search/csv`` at 25,000 rows with no pagination. The
#: busiest single day measured is 4,777 rows (2008-06-15, 16 games), so a
#: one-day window has ~5x headroom -- but assert it rather than assume it.
SAVANT_ROW_CAP = 25_000


# --------------------------------------------------------------------- paths


def raw_root(explicit: Optional[str] = None) -> Path:
    """Resolve the capture root: ``--root`` > ``$SDV_MLB_RAW_ROOT`` > repo default."""
    if explicit:
        return Path(explicit)
    env = os.environ.get("SDV_MLB_RAW_ROOT")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2] / "mlb" / "raw"


def statsapi_path(root: Path, season: int, game_pk: int) -> Path:
    return root / "statsapi" / str(season) / f"{game_pk}.json.gz"


def savant_path(root: Path, season: int, game_pk: int) -> Path:
    return root / "savant" / str(season) / f"{game_pk}.csv.gz"


def schedule_path(root: Path, season: int) -> Path:
    return root / "schedule" / f"{season}.json.gz"


def manifest_path(root: Path, season: int) -> Path:
    return root / "manifest" / f"{season}.csv"


def index_path(root: Path) -> Path:
    return root / "manifest" / "index.csv"


def rel(root: Path, p: Path) -> str:
    """Root-relative posix path, as recorded in the manifest."""
    return p.relative_to(root).as_posix()


# ------------------------------------------------------------------- persist


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def already_captured(path: Path, min_bytes: int = 512) -> bool:
    """Presence-based resume test -- the only skip condition any stage uses."""
    try:
        return path.is_file() and path.stat().st_size >= min_bytes
    except OSError:
        return False


def outstanding(root: Path, row: "dict", surface: str, path: Path) -> bool:
    """True when ``row``'s ``surface`` still needs capturing.

    A manifest entry alone is NOT proof of capture -- the file it names can be
    deleted, truncated, or never have reached disk. Presence on disk is the
    authority (matching :func:`already_captured`, which is what the per-item
    skip uses), so a work list built from the manifest requeues any game whose
    recorded file is gone instead of silently treating it as done.
    """
    return not (row.get(f"{surface}_path") and already_captured(path))


def head(items: "list", limit: Optional[int]) -> "list":
    """``items[:limit]`` with ``limit=0`` meaning zero, not "no limit"."""
    if limit is None:
        return items
    if limit < 0:
        raise ValueError(f"--limit must be >= 0, got {limit}")
    return items[:limit]


def nonneg_int(value: str) -> int:
    """argparse type for ``--limit``: reject a negative up front, not at slice time."""
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {n}")
    return n


def _write_gz(path: Path, payload: bytes) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    # mtime=0 AND filename="" so an unchanged payload re-gzips to identical
    # bytes (git no-op). Without filename="", GzipFile stamps the temp file's
    # own name into the gzip header and two identical payloads differ on disk.
    with open(tmp, "wb") as fh:
        with gzip.GzipFile(filename="", fileobj=fh, mode="wb", compresslevel=6, mtime=0) as gz:
            gz.write(payload)
    tmp.replace(path)
    return path.stat().st_size


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def persist_json(path: Path, obj: Any, validate: Callable[[Any], Optional[str]]) -> Optional[int]:
    """Validate then write ``obj`` as canonical gzipped JSON.

    Returns the on-disk byte count, or ``None`` (writing nothing) when
    ``validate`` returns a rejection reason.
    """
    if validate(obj):
        return None
    return _write_gz(path, canonical_json(obj))


def persist_text(path: Path, text: str, validate: Callable[[str], Optional[str]]) -> Optional[int]:
    """Same contract as :func:`persist_json` for a text (CSV) payload."""
    if validate(text):
        return None
    return _write_gz(path, text.encode("utf-8"))


def read_gz_text(path: Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.read()


# ------------------------------------------------------------------ manifest


def read_manifest(root: Path, season: int) -> "dict[int, dict]":
    p = manifest_path(root, season)
    if not p.is_file():
        return {}
    with open(p, newline="", encoding="utf-8") as fh:
        return {int(r["game_pk"]): r for r in csv.DictReader(fh)}


def write_manifest(root: Path, season: int, rows: "Iterable[dict]") -> Path:
    p = manifest_path(root, season)
    p.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows, key=lambda r: (str(r.get("game_date") or ""), int(r["game_pk"])))
    with open(p, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(MANIFEST_COLUMNS), extrasaction="ignore")
        w.writeheader()
        for r in ordered:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in MANIFEST_COLUMNS})
    return p


INDEX_COLUMNS = (
    "season",
    "games",
    "game_types",
    "statsapi_captured",
    "statsapi_bytes",
    "savant_captured",
    "savant_bytes",
    "manifest",
)


def refresh_index(root: Path) -> Path:
    """Rebuild ``manifest/index.csv`` -- one row per season, the discovery entry point."""
    mdir = root / "manifest"
    mdir.mkdir(parents=True, exist_ok=True)
    rows = []
    for p in sorted(mdir.glob("[0-9][0-9][0-9][0-9].csv")):
        season = int(p.stem)
        games = statsapi_n = savant_n = 0
        sa_bytes = sv_bytes = 0
        types: "dict[str, int]" = {}
        with open(p, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                games += 1
                types[r["game_type"]] = types.get(r["game_type"], 0) + 1
                if r.get("statsapi_path"):
                    statsapi_n += 1
                    sa_bytes += int(r.get("statsapi_bytes") or 0)
                if r.get("savant_path"):
                    savant_n += 1
                    sv_bytes += int(r.get("savant_bytes") or 0)
        rows.append(
            {
                "season": season,
                "games": games,
                "game_types": "|".join(f"{k}:{v}" for k, v in sorted(types.items())),
                "statsapi_captured": statsapi_n,
                "statsapi_bytes": sa_bytes,
                "savant_captured": savant_n,
                "savant_bytes": sv_bytes,
                "manifest": f"manifest/{season}.csv",
            }
        )
    out = index_path(root)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(INDEX_COLUMNS))
        w.writeheader()
        w.writerows(rows)
    return out


# ------------------------------------------------------------------ plumbing


def pace(env_var: str, default: float) -> None:
    """Sleep between requests. Rate pacing is env-only -- never hardcoded."""
    try:
        secs = float(os.environ.get(env_var, default))
    except ValueError:
        secs = default
    if secs > 0:
        time.sleep(secs)


def git_commit(root: Path, paths: "list[Path]", message: str) -> bool:
    """Commit-as-you-go. Returns False (quietly) outside a git worktree."""
    repo = root
    while repo != repo.parent and not (repo / ".git").exists():
        repo = repo.parent
    if not (repo / ".git").exists():
        return False
    rels = [str(p.relative_to(repo)) for p in paths if p.exists()]
    if not rels:
        return False
    subprocess.run(["git", "-C", str(repo), "add", "--", *rels], check=True)
    if (
        subprocess.run(
            ["git", "-C", str(repo), "diff", "--cached", "--quiet"], check=False
        ).returncode
        == 0
    ):
        return False  # nothing staged -> nothing changed
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", message], check=True)
    return True
