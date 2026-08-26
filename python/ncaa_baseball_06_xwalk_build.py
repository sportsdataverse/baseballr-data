"""Stage 06 -- xwalk: NCAA <-> ESPN game crosswalk -> ``ncaa/xwalk/espn_game_id/{season}.json``.

Thin shim over :func:`ncaa_pbp.xwalk.main`: score-tier matching (exact /
date-window / unordered pair / +team-name disambiguation) between the stage-01
schedule master and a cached ``espn_college_baseball_scoreboard`` date sweep
(``ncaa/xwalk/espn_scoreboard/{season}/{date}.json`` -- re-runs are offline).
"""

from __future__ import annotations

import sys

from ncaa_pbp import xwalk


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return xwalk.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
