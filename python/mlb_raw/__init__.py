"""MLB raw-capture layer — statsapi ``feed/live`` + Baseball Savant per-pitch.

Capture once, commit the payload, reshape deterministically. Three numbered
stages (see ``RUNBOOK-MLB.md``):

* ``schedule`` — the season manifest (stage 01)
* ``statsapi`` — one ``feed/live`` payload per game (stage 02)
* ``savant``   — the per-pitch Statcast search, day-fetched, game-sliced (stage 03)

Everything writes under one configurable root (``--root`` /
``$SDV_MLB_RAW_ROOT``, default ``<repo>/mlb/raw``) so the tree lifts into a
dedicated ``mlb-raw`` repo with a ``git mv`` and no code change.
"""

from __future__ import annotations

__all__ = ["core", "savant", "schedule", "statsapi"]
