"""Game-grain dataset builders -- parsed payloads -> season frames, ONE pass.

Every game-grain dataset builds from this repo's parsed + enriched
``ncaa/json/{game_key}.json.gz`` payloads (stage 03), never from raw HTML.
Season membership is ``payload["season"]`` (a single calendar year); with
100k+ payloads in one flat directory, :func:`build_season` accumulates ALL
payload-family frames in a single sweep -- ``cli build --dataset all`` reuses
that one pass rather than re-scanning per dataset.

``pbp`` frames are reconstructed against sdv-py's ``PBP_SCHEMA`` (the payloads
were produced by that parser, so columns agree by construction; json's
stringified values are cast back per schema -- ``scoring_runners`` /
``runners_advanced`` round-trip as python lists). Legacy R-era payloads
(2012-2023) carry empty linescore/team/player/situational families; ``source``
distinguishes the eras downstream.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator

import polars as pl

from ncaa_baseball_data_build._logging import get_logger

log = get_logger()

#: Datasets built from the payload sweep (everything except teams/schedule/rosters).
PAYLOAD_DATASETS = (
    "pbp",
    "linescore",
    "team_stats",
    "player_stats",
    "situational_stats",
    "games",
)

GAMES_SCHEMA: "dict[str, pl.DataType]" = {
    "game_key": pl.Utf8,
    "contest_id": pl.Utf8,
    "game_pbp_id": pl.Int64,
    "season": pl.Int64,
    "source": pl.Utf8,
    "espn_game_id": pl.Utf8,
    "away_team": pl.Utf8,
    "away_final": pl.Int64,
    "home_team": pl.Utf8,
    "home_final": pl.Int64,
}

QA_SCHEMA: "dict[str, pl.DataType]" = {
    "game_key": pl.Utf8,
    "source": pl.Utf8,
    "final_away": pl.Int64,
    "final_home": pl.Int64,
    "pbp_away": pl.Int64,
    "pbp_home": pl.Int64,
    "finals_match": pl.Boolean,
}


def iter_payloads(root: Path, season: int) -> "Iterator[dict[str, Any]]":
    """Payloads whose ``season`` matches; a corrupt file logs and skips.

    The json tree is flat (both eras share it), so this scans every payload
    and filters on the stamped ``season`` -- the one pass ``build_season``
    amortises across all datasets.
    """
    json_dir = root / "ncaa" / "json"
    for p in sorted(json_dir.glob("*.json.gz")):
        try:
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception as exc:  # noqa: BLE001 -- one bad payload must not sink the season
            log.warning("unreadable payload %s: %s", p.name, exc)
            continue
        if payload.get("season") == season:
            yield payload


def _frame(rows: "list[dict]", schema: "dict[str, pl.DataType]") -> pl.DataFrame:
    """Rows (json round-tripped) -> frame with the parser's schema and order."""
    if not rows:
        return pl.DataFrame(schema=schema)
    df = pl.DataFrame(rows, infer_schema_length=None)
    casts = {k: v for k, v in schema.items() if k in df.columns}
    return df.cast(casts).select([k for k in schema if k in df.columns])


def _stamp(df: pl.DataFrame, payload: "dict[str, Any]") -> pl.DataFrame:
    """Per-payload provenance + game-meta columns.

    ``game_date``/``location``/``attendance`` were per-row columns in the
    R-era released pbp -- ``baseballr::load_ncaa_baseball_pbp()`` consumers
    read them -- so the superseding build keeps them on every game-grain row.
    """
    return df.with_columns(
        pl.lit(payload.get("source"), dtype=pl.Utf8).alias("source"),
        pl.lit(payload.get("espn_game_id"), dtype=pl.Utf8).alias("espn_game_id"),
        pl.lit(str(payload.get("game_key")), dtype=pl.Utf8).alias("game_key"),
        pl.lit(payload.get("game_date"), dtype=pl.Utf8).alias("game_date"),
        pl.lit(payload.get("location"), dtype=pl.Utf8).alias("location"),
        pl.lit(_int(payload.get("attendance")), dtype=pl.Int64).alias("attendance"),
    )


def _team(payload: "dict[str, Any]", side: str) -> "dict[str, Any]":
    return next(
        (t for t in payload.get("teams") or [] if t.get("home_away") == side),
        {},
    )


def _int(v: Any) -> "int | None":
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _game_row(payload: "dict[str, Any]") -> "dict[str, Any]":
    away, home = _team(payload, "away"), _team(payload, "home")
    return {
        "game_key": str(payload.get("game_key")),
        "contest_id": payload.get("contest_id"),
        "game_pbp_id": _int(payload.get("game_pbp_id")),
        "season": _int(payload.get("season")),
        "source": payload.get("source"),
        "espn_game_id": payload.get("espn_game_id"),
        "away_team": away.get("team"),
        "away_final": _int(away.get("final")),
        "home_team": home.get("team"),
        "home_final": _int(home.get("final")),
    }


def _qa_row(payload: "dict[str, Any]") -> "dict[str, Any]":
    """Payload teams[].final vs the last pbp row's running score."""
    fa = _int(_team(payload, "away").get("final"))
    fh = _int(_team(payload, "home").get("final"))
    pbp = payload.get("pbp") or []
    pa = next((r["score_away"] for r in reversed(pbp) if r.get("score_away") is not None), None)
    ph = next((r["score_home"] for r in reversed(pbp) if r.get("score_home") is not None), None)
    comparable = None not in (fa, fh, pa, ph)
    return {
        "game_key": str(payload.get("game_key")),
        "source": payload.get("source"),
        "final_away": fa,
        "final_home": fh,
        "pbp_away": _int(pa),
        "pbp_home": _int(ph),
        "finals_match": (fa == pa and fh == ph) if comparable else None,
    }


def _cat_frames(families: Any, payload: "dict[str, Any]") -> "list[pl.DataFrame]":
    """Dict-of-category -> stamped frames with a ``category`` column.

    Legacy payloads store these families as ``[]``/``{}`` -- both are falsy and
    yield nothing.
    """
    out = []
    for cat, rows in (families or {}).items():
        if not rows:
            continue
        df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
            pl.lit(cat, dtype=pl.Utf8).alias("category")
        )
        out.append(_stamp(df, payload))
    return out


_EMPTY_CAT_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "category": pl.Utf8,
    "source": pl.Utf8,
    "espn_game_id": pl.Utf8,
    "game_key": pl.Utf8,
}


def _concat(frames: "list[pl.DataFrame]", empty_schema: "dict[str, pl.DataType]") -> pl.DataFrame:
    if not frames:
        return pl.DataFrame(schema=empty_schema)
    return pl.concat(frames, how="diagonal_relaxed")


def build_season(season: int, root: Path) -> "dict[str, pl.DataFrame]":
    """ALL payload-family frames for a season from ONE payload sweep.

    Returns ``{name: frame}`` for every :data:`PAYLOAD_DATASETS` entry plus
    ``"qa"`` (the finals-QA frame, committed under ``ncaa/qa/``, never
    released). Payload-less seasons return all-empty frames -- the caller
    decides whether that is fatal.
    """
    from sportsdataverse.baseball.college_baseball import (
        LINESCORE_SCHEMA,
        PBP_SCHEMA,
        TEAM_STATS_SCHEMA,
    )

    acc: "dict[str, list[pl.DataFrame]]" = {
        n: [] for n in ("pbp", "linescore", "team_stats", "player_stats", "situational_stats")
    }
    games: "list[dict]" = []
    qa: "list[dict]" = []
    n_payloads = 0
    for payload in iter_payloads(root, season):
        n_payloads += 1
        for name, schema in (
            ("pbp", PBP_SCHEMA),
            ("linescore", LINESCORE_SCHEMA),
            ("team_stats", TEAM_STATS_SCHEMA),
        ):
            df = _frame(payload.get(name) or [], schema)
            if df.height:
                acc[name].append(_stamp(df, payload))
        acc["player_stats"].extend(_cat_frames(payload.get("player_stats"), payload))
        acc["situational_stats"].extend(_cat_frames(payload.get("situational_stats"), payload))
        games.append(_game_row(payload))
        qa.append(_qa_row(payload))

    stamped = {"source": pl.Utf8, "espn_game_id": pl.Utf8, "game_key": pl.Utf8}
    out = {
        "pbp": _concat(acc["pbp"], {**PBP_SCHEMA, **stamped}),
        "linescore": _concat(acc["linescore"], {**LINESCORE_SCHEMA, **stamped}),
        "team_stats": _concat(acc["team_stats"], {**TEAM_STATS_SCHEMA, **stamped}),
        "player_stats": _concat(acc["player_stats"], _EMPTY_CAT_SCHEMA),
        "situational_stats": _concat(acc["situational_stats"], _EMPTY_CAT_SCHEMA),
        "games": pl.DataFrame(games, schema=GAMES_SCHEMA),
        "qa": pl.DataFrame(qa, schema=QA_SCHEMA),
    }
    log.info(
        "season %d: %d payloads -> %s",
        season,
        n_payloads,
        {k: v.height for k, v in out.items()},
    )
    return out
