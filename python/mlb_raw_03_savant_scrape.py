"""Stage 03 shim -- Baseball Savant per-pitch capture.

Thin argparse wrapper; the working code lives in ``mlb_raw.savant``.
"""

from __future__ import annotations

import sys

from mlb_raw import savant


def main(argv=None) -> int:
    return savant.main(argv)


if __name__ == "__main__":
    sys.exit(main())
