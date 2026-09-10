"""Stage 05 -- publish MLB pbp/pitches to sportsdataverse-data releases.

Thin shim over :func:`mlb_raw.publish.main`.
"""

from __future__ import annotations

import sys

from mlb_raw import publish


def main(argv: "list[str] | None" = None) -> int:
    return publish.main(list(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
