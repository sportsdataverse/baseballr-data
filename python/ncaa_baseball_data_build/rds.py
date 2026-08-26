"""Parquet -> ``.rds`` conversion for release assets, via a shelled-out ``Rscript``.

``.rds`` is R-native; there is no reliable pure-Python writer for it (``pyreadr``
cannot read parquet and is not a dependency here). This module shells out to
``Rscript`` and uses R's ``arrow`` package (``arrow::read_parquet`` ->
``saveRDS``) to do the conversion.

Rscript resolution order: env ``SDV_RSCRIPT``, then env ``RSCRIPT``, then
``shutil.which("Rscript")`` (the PATH R), then a newest-first scan of
``C:/Program Files/R/R-*/bin/Rscript.exe``. Not every R install here has
``arrow`` -- if the resolved Rscript lacks it, ``to_rds`` raises with the
captured stderr. Override with ``SDV_RSCRIPT=/path/to/Rscript.exe`` to pick a
specific install.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

_R_ONELINER = (
    "a <- commandArgs(trailingOnly=TRUE); saveRDS(as.data.frame(arrow::read_parquet(a[1])), a[2])"
)


def _find_rscript() -> "str | None":
    """Resolve an ``Rscript`` executable: env overrides, then PATH, then a scan."""
    for env_var in ("SDV_RSCRIPT", "RSCRIPT"):
        val = os.environ.get(env_var)
        if val and Path(val).exists():
            return val

    on_path = shutil.which("Rscript")
    if on_path:
        return on_path

    candidates = sorted(Path("C:/Program Files/R").glob("R-*/bin/Rscript.exe"), reverse=True)
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return None


def to_rds(
    parquet_path: "str | Path", rds_path: "str | Path", *, rscript: "str | None" = None
) -> Path:
    """Convert a parquet file to ``.rds`` via ``Rscript`` + R's ``arrow`` package.

    Raises ``RuntimeError`` if no Rscript is found, or if the conversion fails
    (e.g. R's ``arrow`` package is missing) -- the R stderr is included.
    """
    resolved = rscript or _find_rscript()
    if resolved is None:
        raise RuntimeError("Rscript not found; set SDV_RSCRIPT")

    rds_path = Path(rds_path)
    rds_path.parent.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            resolved,
            "-e",
            _R_ONELINER,
            Path(parquet_path).as_posix(),
            rds_path.as_posix(),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Rscript conversion failed (exit {result.returncode}): {result.stderr}")
    return rds_path
