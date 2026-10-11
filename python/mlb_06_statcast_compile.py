"""Stage 06 -- compile Statcast monthly raws into one parquet per season.

Thin shim over :func:`mlb_raw.statcast_compile.main`.
"""

from __future__ import annotations

import sys

from mlb_raw import statcast_compile


def main(argv: "list[str] | None" = None) -> int:
    return statcast_compile.main(list(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
