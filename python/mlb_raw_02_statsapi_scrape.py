"""Stage 02 shim -- statsapi feed/live per-game capture.

Thin argparse wrapper; the working code lives in ``mlb_raw.statsapi``.
"""

from __future__ import annotations

import sys

from mlb_raw import statsapi


def main(argv=None) -> int:
    return statsapi.main(argv)


if __name__ == "__main__":
    sys.exit(main())
