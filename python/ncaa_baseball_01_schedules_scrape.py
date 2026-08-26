"""Stage 01 -- schedules: team lists + team pages -> persisted html + schedule master.

Thin shim over :func:`ncaa_pbp.schedules.main`. Sweeps ``ncaa/teams_html/`` and
``ncaa/schedules_html/{season}/`` (file-exists resumable -- persisted html is
re-read, never re-fetched) and builds
``ncaa/schedule_master/parquet/{season}.parquet``.

Stage numbers mirror the NCAA raw twins (ncaa-mfb-football-raw /
ncaa-mbb-hoops-raw): 01 schedules, 02 games, 04 rosters, 05 datasets, 06 xwalk.
03 (parse) is a deliberate HOLE -- built separately on feat/ncaa-baseball-parse.
"""

from __future__ import annotations

import sys

from ncaa_pbp import schedules


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return schedules.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
