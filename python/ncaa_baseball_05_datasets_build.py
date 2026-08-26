"""Stage 05 -- datasets: persisted html -> reference parquet frames. OFFLINE.

Thin shim over :func:`ncaa_pbp.datasets.main`: builds
``ncaa/teams/parquet/{season}_d{division}.parquet``,
``ncaa/schedule_master/parquet/{season}.parquet`` and
``ncaa/rosters/parquet/{season}.parquet`` via the sdv-py
``scrape.ncaa.reference`` parsers. Pure function of the tree; re-run overwrites.
"""

from __future__ import annotations

import sys

from ncaa_pbp import datasets


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return datasets.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
