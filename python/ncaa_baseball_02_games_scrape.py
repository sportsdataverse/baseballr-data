"""Stage 02 -- games: capture the 5-tab bundle for every not-yet-captured contest.

Thin shim over :func:`ncaa_pbp.games.main`: contest ids come from the stage-01
schedule master parquet (NOT rediscovery), bundles land in
``ncaa/raw/{season}/{contest_id}.json.gz``. Chunk with ``--max N``, fan out with
``--shard i/N`` (one PROCESS per shard). Resumable: captured contests are
skipped; a ban trips the failure breaker and hard-stops the run with rc=1.
"""

from __future__ import annotations

import sys

from ncaa_pbp import games


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return games.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
