"""Stage 01 — MLB game-state model datasets (RE24 / WE / WPA).

Thin numbered entry over ``mlb_model_publish game-state`` (build + gate + publish
fused; gates live in `_CARD_META`). Compute-on-demand family: no fingerprint
skip (daily recompute — a skip is silent staleness), no committed artifacts;
the generated card on the release tag is the per-publish ledger.

Usage::

    python -m mlb_model_01_game_state --seasons 2026 [--dry-run]
    scripts/mlb_models.sh 01
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from mlb_model_publish.cli import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(["game-state", *argv])


if __name__ == "__main__":
    raise SystemExit(main())
