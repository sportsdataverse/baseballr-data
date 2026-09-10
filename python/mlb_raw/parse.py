"""Stage 03 -- parse: committed raw bundles -> tidy pbp + pitches parquet.

OFFLINE. Reads only ``mlb/raw/{season}/``, so a parser fix reprocesses the whole
corpus without touching the network -- which is the entire point of committing
raw.

Measured 45.5 games/s single-core (13,141 pitch-rows/s), so the full
1988-2026 corpus parses in ~34 min on one core. The scrape is the cost, not this.

ID discipline (the recurring cross-language bug class): every id is written as
Int64, never float and never a stringified float. A missing id stays null rather
than becoming 0 or "None".
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib

import polars as pl

# Explicit dtypes, never inferred. polars infers from the first N rows, and
# spin_rate arrives as an int for some pitches and a float for others -- which
# raised "could not append value: 2395 of type: i64" mid-season. An explicit
# schema also keeps every season's parquet identical even where an era carries
# none of the tracking fields (1988-2007 have no pitchData at all), so the
# columns do not silently change shape across the corpus.
_I, _F, _S, _B = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean

PLAY_SCHEMA = {
    "game_pk": _I, "at_bat_index": _I, "inning": _I, "half_inning": _S,
    "batter_id": _I, "pitcher_id": _I,
    "event_type": _S, "event": _S, "description": _S,
    "rbi": _I, "away_score": _I, "home_score": _I,
    "is_scoring_play": _B, "outs": _I, "start_time": _S, "end_time": _S,
}
RUNNER_SCHEMA = {
    "game_pk": _I, "at_bat_index": _I, "runner_id": _I,
    "origin_base": _S, "start_base": _S, "end_base": _S, "out_base": _S,
    "is_out": _B, "out_number": _I,
    "event": _S, "event_type": _S, "movement_reason": _S,
    "is_scoring_event": _B, "rbi": _B, "earned": _B,
    "responsible_pitcher_id": _I,
}
PITCH_SCHEMA = {
    "game_pk": _I, "at_bat_index": _I, "pitch_number": _I,
    "batter_id": _I, "pitcher_id": _I,
    "pitch_type": _S, "pitch_name": _S, "call_code": _S, "call_description": _S,
    "balls": _I, "strikes": _I, "outs": _I,
    "start_speed": _F, "end_speed": _F, "spin_rate": _F, "extension": _F,
    "px": _F, "pz": _F, "sz_top": _F, "sz_bot": _F,
    "launch_speed": _F, "launch_angle": _F, "total_distance": _F,
    "trajectory": _S, "hardness": _S,
}


def _iid(v):
    """Int64 or None -- never a float, never '123.0'."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse_bundle(payload: dict) -> "tuple[list[dict], list[dict], list[dict]]":
    """plays, pitches, runners.

    Runners are extracted as the RELATIONAL FACT statsapi ships -- one row per
    runner movement -- not as reconstructed base-occupancy. RE24 needs
    pre_1/2/3 base state, which is derivable from these rows by walking a
    half-inning; doing that reconstruction here would bake one interpretation
    of ambiguous movements into the raw corpus. Ship the facts, derive the
    state downstream where it can be corrected without a re-parse.
    """
    game_pk = _iid(payload.get("gamePk"))
    plays_out, pitches_out, runners_out = [], [], []
    for p in ((payload.get("liveData") or {}).get("plays") or {}).get("allPlays") or []:
        about, res, ma = p.get("about") or {}, p.get("result") or {}, p.get("matchup") or {}
        abi = _iid(about.get("atBatIndex"))
        bat, pit = (
            _iid((ma.get("batter") or {}).get("id")),
            _iid((ma.get("pitcher") or {}).get("id")),
        )
        plays_out.append(
            {
                "game_pk": game_pk,
                "at_bat_index": abi,
                "inning": _iid(about.get("inning")),
                "half_inning": about.get("halfInning"),
                "batter_id": bat,
                "pitcher_id": pit,
                "event_type": res.get("eventType"),
                "event": res.get("event"),
                "description": res.get("description"),
                "rbi": _iid(res.get("rbi")),
                "away_score": _iid(res.get("awayScore")),
                "home_score": _iid(res.get("homeScore")),
                "is_scoring_play": bool(about.get("isScoringPlay")),
                "outs": _iid(p.get("count", {}).get("outs")),
                "start_time": about.get("startTime"),
                "end_time": about.get("endTime"),
            }
        )
        for r in p.get("runners") or []:
            mv, de = r.get("movement") or {}, r.get("details") or {}
            runners_out.append({
                "game_pk": game_pk, "at_bat_index": abi,
                "runner_id": _iid((de.get("runner") or {}).get("id")),
                "origin_base": mv.get("originBase"), "start_base": mv.get("start"),
                "end_base": mv.get("end"), "out_base": mv.get("outBase"),
                "is_out": bool(mv.get("isOut")), "out_number": _iid(mv.get("outNumber")),
                "event": de.get("event"), "event_type": de.get("eventType"),
                "movement_reason": de.get("movementReason"),
                "is_scoring_event": bool(de.get("isScoringEvent")),
                "rbi": bool(de.get("rbi")), "earned": bool(de.get("earned")),
                "responsible_pitcher_id": _iid((de.get("responsiblePitcher") or {}).get("id")),
            })
        for e in p.get("playEvents") or []:
            if not e.get("isPitch"):
                continue
            det, cnt = e.get("details") or {}, e.get("count") or {}
            pd, co = e.get("pitchData") or {}, (e.get("pitchData") or {}).get("coordinates") or {}
            hd, br = e.get("hitData") or {}, (e.get("pitchData") or {}).get("breaks") or {}
            pitches_out.append(
                {
                    "game_pk": game_pk,
                    "at_bat_index": abi,
                    "pitch_number": _iid(e.get("pitchNumber")),
                    "batter_id": bat,
                    "pitcher_id": pit,
                    "pitch_type": (det.get("type") or {}).get("code"),
                    "pitch_name": (det.get("type") or {}).get("description"),
                    "call_code": (det.get("call") or {}).get("code"),
                    "call_description": (det.get("call") or {}).get("description"),
                    "balls": _iid(cnt.get("balls")),
                    "strikes": _iid(cnt.get("strikes")),
                    "outs": _iid(cnt.get("outs")),
                    "start_speed": pd.get("startSpeed"),
                    "end_speed": pd.get("endSpeed"),
                    "spin_rate": br.get("spinRate"),
                    "extension": pd.get("extension"),
                    "px": co.get("pX"),
                    "pz": co.get("pZ"),
                    "sz_top": pd.get("strikeZoneTop"),
                    "sz_bot": pd.get("strikeZoneBottom"),
                    "launch_speed": hd.get("launchSpeed"),
                    "launch_angle": hd.get("launchAngle"),
                    "total_distance": hd.get("totalDistance"),
                    "trajectory": hd.get("trajectory"),
                    "hardness": hd.get("hardness"),
                }
            )
    return plays_out, pitches_out, runners_out


def parse_season(root: pathlib.Path, season: int) -> dict:
    raw_dir = root / "mlb" / "raw" / str(season)
    files = sorted(raw_dir.glob("*.json.gz"))
    if not files:
        raise FileNotFoundError(f"no bundles in {raw_dir} -- run stage 02 first")

    plays, pitches, runners, bad = [], [], [], 0
    for f in files:
        try:
            a, b, c = parse_bundle(json.loads(gzip.open(f).read()))
            plays.extend(a)
            pitches.extend(b)
            runners.extend(c)
        except Exception as exc:  # noqa: BLE001 - a corrupt bundle must be named, not silent
            bad += 1
            print(f"  {f.name}: {type(exc).__name__}: {str(exc)[:80]}")

    if not plays:
        raise ValueError(f"season {season}: parsed 0 plays from {len(files)} bundles")

    out = root / "mlb" / "pbp"
    out.mkdir(parents=True, exist_ok=True)
    # Explicit column order so the schema is stable across seasons even when an
    # era carries none of the tracking fields (1988-2007 have no pitchData).
    pl.DataFrame(plays, schema=PLAY_SCHEMA).write_parquet(out / f"mlb_pbp_{season}.parquet")
    pl.DataFrame(pitches, schema=PITCH_SCHEMA).write_parquet(
        out / f"mlb_pitches_{season}.parquet"
    )
    pl.DataFrame(runners, schema=RUNNER_SCHEMA).write_parquet(
        out / f"mlb_runners_{season}.parquet"
    )
    return {
        "games": len(files), "plays": len(plays), "pitches": len(pitches),
        "runners": len(runners), "unparsed": bad,
    }


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--root", default=".")
    a = ap.parse_args(argv)
    try:
        st = parse_season(pathlib.Path(a.root), a.season)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 2
    print(f"parse season {a.season}: {st}")
    return 1 if st["unparsed"] else 0
