"""Stage 01 shim -- MLB season schedule + game manifest.

Thin argparse wrapper; the working code lives in ``mlb_raw.schedule``.
"""

from __future__ import annotations

import sys

from mlb_raw import schedule


def main(argv=None) -> int:
    return schedule.main(argv)


if __name__ == "__main__":
    sys.exit(main())
