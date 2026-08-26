"""Stage 04 -- rosters: ``teams/{id}/roster`` -> ``ncaa/rosters_html/{season}/``.

Thin shim over :func:`ncaa_pbp.rosters.main`. Team ids come from the persisted
stage-01 team lists; teams whose roster html is on disk are skipped (resumable).
"""

from __future__ import annotations

import sys

from ncaa_pbp import rosters


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return rosters.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
