"""Stage 03 -- parse: committed raw bundles -> tidy pbp + pitches parquet (OFFLINE).

Thin shim over :func:`mlb_raw.parse.main` -- the durable logic lives in the
package, per the placement rule (numbered file = a runnable stage with a CLI).
"""

from __future__ import annotations

import sys

from mlb_raw import parse


def main(argv: "list[str] | None" = None) -> int:
    return parse.main(list(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
