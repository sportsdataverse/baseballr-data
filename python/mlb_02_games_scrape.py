"""Stage 02 -- capture: feed/live bundle per completed game -> mlb/raw/{season}/.

Thin shim over :func:`mlb_raw.capture.main` -- the durable logic lives in the
package, per the placement rule (numbered file = a runnable stage with a CLI).
"""

from __future__ import annotations

import sys

from mlb_raw import capture


def main(argv: "list[str] | None" = None) -> int:
    return capture.main(list(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
