"""Stage 02 — MLB hitting models (expected stats / xHR / projection).

Thin numbered entry over ``mlb_model_publish hitting`` (build + gate + publish
fused; gates live in `_CARD_META`). Compute-on-demand family: no fingerprint
skip (daily recompute — a skip is silent staleness), no committed artifacts;
the generated card on the release tag is the per-publish ledger.

Usage::

    python -m mlb_model_02_hitting --seasons 2026 [--dry-run]
    scripts/mlb_models.sh 02
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from mlb_model_publish.cli import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(["hitting", *argv])


if __name__ == "__main__":
    raise SystemExit(main())
