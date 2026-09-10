"""Stage 04 -- Statcast raw: Savant pitch-level tracking -> committed parquet.

Thin shim over :func:`mlb_raw.statcast.main` -- durable logic lives in the
package, per the placement rule (numbered file = a runnable stage with a CLI).
"""

from __future__ import annotations

import sys

from mlb_raw import statcast


def main(argv: "list[str] | None" = None) -> int:
    return statcast.main(list(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
