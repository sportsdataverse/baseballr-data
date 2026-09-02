"""Dataset registry -- one row per released NCAA baseball dataset.

Datasets are built from THIS repo's own tree (this repo IS the raw repo):
game-grain families from the parsed payloads ``ncaa/json/{game_key}.json.gz``
(stage 03), reference frames from the stage-01/05 parquet
(``ncaa/schedule_master``, ``ncaa/teams``, ``ncaa/rosters``) with the
committed legacy R-era schedules as the pre-2024 schedule fallback.

``season`` is a single **calendar year** throughout -- baseball convention:
``season = 2024`` is the spring-2024 season. No academic-year offsets.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

#: Root of the raw tree. This repo is its own raw repo, so the default is the
#: repo root itself (three levels up from this file).
RAW_ROOT_ENV = "NCAA_BASEBALL_ROOT"
DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[2]

#: Release-tag/filename prefix; a downloaded asset keeps its provenance.
TAG_PREFIX = "ncaa_baseball_"

#: Covered seasons (calendar years, inclusive): legacy R era 2012-2023,
#: capture era 2024+.
FIRST_SEASON = 2012
LAST_SEASON = 2026


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    description: str
    #: Release tag override. Only ``schedule`` sets it -- see the REGISTRY note.
    tag_override: "str | None" = None

    @property
    def tag(self) -> str:
        return self.tag_override or TAG_PREFIX + self.name

    @property
    def stem(self) -> str:
        """Asset filename stem: ``{stem}_{season}.parquet`` etc."""
        return TAG_PREFIX + self.name


# Insertion order is the build order for ``--dataset all``: reference frames
# first, then per-game extracts. No dataset reads another dataset's output.
#
# COMPAT QUIRK (load-bearing, do not "fix"): the R-era releases that
# ``baseballr::load_ncaa_*`` reads use the PLURAL tag ``ncaa_baseball_schedules``
# holding SINGULAR-stemmed assets ``ncaa_baseball_schedule_{year}.*``. The
# ``schedule`` spec preserves exactly that via ``tag_override``; every other
# dataset has tag == stem.
REGISTRY: dict[str, DatasetSpec] = {
    "teams": DatasetSpec("teams", "team ids/names per division (capture era, 2024+)"),
    "schedule": DatasetSpec(
        "schedule",
        "season schedule: capture-era schedule master (2024+), legacy R-era frames as-is before",
        tag_override=TAG_PREFIX + "schedules",  # plural tag, singular stem -- R-era compat
    ),
    "rosters": DatasetSpec(
        "rosters", "per-team season rosters with stats.ncaa.org player ids (2024+)"
    ),
    "pbp": DatasetSpec("pbp", "30-col decomposed play-by-play, both eras"),
    "linescore": DatasetSpec("linescore", "per-inning linescore + game info (capture era)"),
    "team_stats": DatasetSpec("team_stats", "team box stat lines (capture era)"),
    "player_stats": DatasetSpec(
        "player_stats", "individual box, all categories (`category` column; capture era)"
    ),
    "situational_stats": DatasetSpec(
        "situational_stats", "situational splits, all categories (`category` column; capture era)"
    ),
    "games": DatasetSpec("games", "game-level index: one row per parsed payload"),
}


def raw_root() -> Path:
    return Path(os.environ.get(RAW_ROOT_ENV) or DEFAULT_RAW_ROOT)


# --- release sidecar metadata -------------------------------------------------
# Every published tag carries package_function.txt/.json naming the loader a
# consumer reaches the data through -- the half of R's sportsdataverse_save()
# the Python publisher used to drop. Values are NOT invented: where the R
# producer already published a package_function to the tag, that exact string
# is reused, so re-stamping from Python does not change what a consumer sees.
# Python-only tags that never had one name the sdv-py loader instead.
#
# Keyed by tag, not dataset -- several datasets can share one tag.
# The publish tests assert every REGISTRY tag has an entry, so a new dataset
# cannot ship an unnamed tag.
PKG_FUNCTION: dict[str, str] = {
    "ncaa_baseball_games": "sportsdataverse.mlb.load_ncaa_baseball_games()",
    "ncaa_baseball_linescore": "sportsdataverse.mlb.load_ncaa_baseball_linescore()",
    "ncaa_baseball_pbp": "sportsdataverse.mlb.load_ncaa_baseball_pbp()",
    "ncaa_baseball_player_stats": "sportsdataverse.mlb.load_ncaa_baseball_player_stats()",
    "ncaa_baseball_rosters": "sportsdataverse.mlb.load_ncaa_baseball_rosters()",
    "ncaa_baseball_schedules": "sportsdataverse.mlb.load_ncaa_baseball_schedule()",
    "ncaa_baseball_situational_stats": "sportsdataverse.mlb.load_ncaa_baseball_situational_stats()",
    "ncaa_baseball_team_stats": "sportsdataverse.mlb.load_ncaa_baseball_team_stats()",
    "ncaa_baseball_teams": "sportsdataverse.mlb.load_ncaa_baseball_teams()",
}
