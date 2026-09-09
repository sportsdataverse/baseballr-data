"""MLB raw producer: statsapi feed/live capture -> tidy pbp + pitches.

Floor is 1988 -- the first season with COMPLETE pitch-by-pitch. Measured
2026-09-09: 1986-87 carry ~113-117 pitches/game (partial), 1988-89 carry
241-300 (complete). Schedule data exists back to 1901 and allPlays back to
1950, but 1950-87 coverage is uneven WITHIN a season, so that range needs a
completeness audit before it can be published.

Stage numbers mirror the NCAA producers in this repo: 01 schedules,
02 games capture, 03 parse.
"""

from __future__ import annotations

FLOOR_SEASON = 1988

__all__ = ["FLOOR_SEASON", "capture", "parse", "schedules", "transport"]
