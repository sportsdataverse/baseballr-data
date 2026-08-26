"""Stage 03 -- parse: bundles + legacy trees -> ncaa/json/ payloads. OFFLINE.

Thin shim over :func:`ncaa_pbp.parse.main`: capture-era bundles
(``--season N``) and/or the legacy R-era trees (``--legacy [--year Y]``)
resolve into one parsed+enriched payload per game under
``ncaa/json/{game_key}.json.gz`` -- the pbp of both eras runs through the
same sdv-py decomposition engine.
"""

from __future__ import annotations

import sys

from ncaa_pbp import parse


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return parse.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
