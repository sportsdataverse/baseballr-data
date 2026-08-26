"""Stage 07 -- season datasets: parsed payloads + reference parquet ->
release layout + publish. OFFLINE except ``gh``.

Thin shim over :func:`ncaa_baseball_data_build.cli.main`: builds
``ncaa/{dataset}/parquet/{stem}_{season}.parquet`` (+ the ``ncaa/qa/`` finals
frame on ``--dataset all``) and, with ``build ... --publish``, uploads
parquet + csv.gz + rds to the ``ncaa_baseball_*`` releases on
sportsdataverse/sportsdataverse-data. ``check`` audits built vs published.
"""

from __future__ import annotations

import sys

from ncaa_baseball_data_build import cli


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return cli.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
