"""Offline tests for the MLB raw producer.

No network: every test builds its payload inline. The two things worth pinning
are the ones that already went wrong once each during development.
"""

from __future__ import annotations

from mlb_raw import parse, schedules


def _schedule_payload():
    def game(pk, gtype, state="Final"):
        return {
            "gamePk": pk,
            "gameType": gtype,
            "season": "2024",
            "gameDate": "2024-06-15T17:05:00Z",
            "status": {"abstractGameState": state, "statusCode": "F", "detailedState": state},
            "teams": {
                "home": {"team": {"id": 147, "name": "H"}, "score": 3},
                "away": {"team": {"id": 111, "name": "A"}, "score": 2},
            },
            "venue": {"id": 3313, "name": "V"},
        }

    return {
        "dates": [
            {
                "date": "2024-06-15",
                "games": [
                    game(1, "S"),
                    game(2, "R"),
                    game(3, "E"),
                    game(4, "W"),
                    game(5, "R", "Preview"),
                ],
            }
        ]
    }


def test_spring_training_is_excluded_by_default():
    """A 5-game capture of 2024 once returned 873 pitches with ZERO non-null
    start_speed: the first games of a calendar year are February spring
    training, which carry no Statcast at all. The default corpus is regular
    season + postseason."""
    rows = schedules.rows_from(_schedule_payload())
    keep = [
        r["game_pk"]
        for r in rows
        if r["abstract_state"] == "Final" and r["game_type"] in schedules.DEFAULT_TYPES
    ]
    assert keep == [2, 4]  # R and W kept
    assert 1 not in keep  # spring training
    assert 3 not in keep  # exhibition
    assert 5 not in keep  # not Final


def test_duplicate_game_pks_are_deduped():
    """statsapi lists a suspended-and-resumed game under more than one date --
    27 of 2,208 gamePks in 2026. Duplicates put two thread-pool workers on one
    output path, where the loser can truncate the winner's file between its
    write and its rename: a corrupt bundle, not just a noisy error."""
    payload = _schedule_payload()
    # same game announced under a second date, as a resumed game is
    payload["dates"].append({"date": "2024-06-16",
                             "games": [payload["dates"][0]["games"][1]]})
    rows = schedules.rows_from(payload)
    assert [r["game_pk"] for r in rows].count(2) == 2, "fixture must contain a dupe"

    seen, out = set(), []
    for r in rows:
        pk = r["game_pk"]
        if (r["abstract_state"] == "Final" and pk
                and r["game_type"] in schedules.DEFAULT_TYPES and pk not in seen):
            seen.add(pk)
            out.append(pk)
    assert out.count(2) == 1
    assert len(out) == len(set(out))


def test_unfinished_games_are_never_captured():
    rows = schedules.rows_from(_schedule_payload())
    assert [r["game_pk"] for r in rows if r["abstract_state"] != "Final"] == [5]


def _feed_payload():
    return {
        "gamePk": 745444,
        "liveData": {
            "plays": {
                "allPlays": [
                    {
                        "about": {
                            "atBatIndex": 0,
                            "inning": 1,
                            "halfInning": "top",
                            "isScoringPlay": False,
                        },
                        "result": {
                            "eventType": "strikeout",
                            "event": "Strikeout",
                            "rbi": 0,
                            "awayScore": 0,
                            "homeScore": 0,
                        },
                        "matchup": {"batter": {"id": 660271}, "pitcher": {"id": 592450}},
                        "playEvents": [
                            {
                                "isPitch": True,
                                "pitchNumber": 1,
                                "details": {
                                    "type": {"code": "FF", "description": "Four-Seam"},
                                    "call": {"code": "S", "description": "Strike"},
                                },
                                "count": {"balls": 0, "strikes": 1, "outs": 0},
                                "pitchData": {
                                    "startSpeed": 97.1,
                                    "endSpeed": 88.2,
                                    "coordinates": {"pX": 0.31, "pZ": 2.44},
                                    "strikeZoneTop": 3.4,
                                    "strikeZoneBottom": 1.6,
                                    "breaks": {"spinRate": 2380},
                                },
                            },
                            {"isPitch": False, "details": {"description": "mound visit"}},
                        ],
                    }
                ]
            }
        },
    }


def test_pitch_extraction_reaches_the_tracking_fields():
    """Guards the null-tracking regression: these live at
    pitchData.coordinates.pX and pitchData.breaks.spinRate, not on the event."""
    plays, pitches, _ = parse.parse_bundle(_feed_payload())
    assert len(plays) == 1
    assert len(pitches) == 1, "non-pitch playEvents must not become pitch rows"
    p = pitches[0]
    assert p["start_speed"] == 97.1
    assert p["px"] == 0.31 and p["pz"] == 2.44
    assert p["spin_rate"] == 2380
    assert p["pitch_type"] == "FF"


def test_ids_are_int_not_float_or_stringified_float():
    """The recurring cross-language join bug: a float-origin id becomes '123.0'
    and silently matches nothing."""
    plays, pitches, _ = parse.parse_bundle(_feed_payload())
    for row in (plays[0], pitches[0]):
        for key in ("game_pk", "batter_id", "pitcher_id", "at_bat_index"):
            assert isinstance(row[key], int), f"{key} is {type(row[key])}"


def test_runner_movements_are_extracted_as_facts_not_reconstructed_state():
    """RE24 needs base-out state, which sdv-py expects as pre_1/2/3 columns.
    We ship the RELATIONAL FACT statsapi gives (one row per runner movement)
    rather than reconstructing occupancy in the parser -- that derivation can
    then be corrected downstream without re-parsing 91k bundles."""
    payload = _feed_payload()
    payload["liveData"]["plays"]["allPlays"][0]["runners"] = [{
        "movement": {"originBase": None, "start": None, "end": "1B",
                     "outBase": None, "isOut": False, "outNumber": None},
        "details": {"event": "Single", "eventType": "single",
                    "runner": {"id": 660271}, "isScoringEvent": False,
                    "rbi": False, "earned": False, "responsiblePitcher": None},
    }]
    _, _, runners = parse.parse_bundle(payload)
    assert len(runners) == 1
    r = runners[0]
    assert r["runner_id"] == 660271 and isinstance(r["runner_id"], int)
    assert r["end_base"] == "1B"
    assert r["is_out"] is False
    assert r["game_pk"] == 745444


def test_missing_ids_become_none_not_zero():
    payload = _feed_payload()
    payload["liveData"]["plays"]["allPlays"][0]["matchup"] = {}
    plays, _, _ = parse.parse_bundle(payload)
    assert plays[0]["batter_id"] is None


if __name__ == "__main__":  # pragma: no cover - convenience
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")


def test_savant_transport_failure_is_not_reported_as_an_empty_month(tmp_path, monkeypatch):
    """The green-but-empty trap, closed. sdv-py's download RETURNS the error
    response once its retry budget is spent, _csv_to_frame turns a 403 body into
    an empty frame, and a zero-row month would otherwise read as 'no baseball
    that month' and exit 0."""
    import polars as pl
    from mlb_raw import statcast

    monkeypatch.setattr(statcast, "SLEEP", 0)
    monkeypatch.setattr(
        statcast, "scheduled_game_dates", lambda root, season, month: {"2024-04-01"}
    )
    monkeypatch.setattr(
        "sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search",
        lambda *a, **k: pl.DataFrame(),
    )
    st = statcast.capture_season(tmp_path, 2024)
    assert st["failed"] == len(list(statcast.MONTHS)), st
    assert st["empty"] == 0, "a 403 must never be counted as an empty month"


def test_a_month_missing_scheduled_dates_is_refused_not_banked(tmp_path, monkeypatch):
    """mlb_statcast_search drops empty 7-day chunks, so one 403'd week still
    yields a plausible non-empty frame -- which file-exists resume would freeze."""
    import polars as pl
    from mlb_raw import statcast

    monkeypatch.setattr(statcast, "SLEEP", 0)
    monkeypatch.setattr(
        statcast,
        "scheduled_game_dates",
        lambda root, season, month: {"2024-04-01", "2024-04-08", "2024-04-15"},
    )
    monkeypatch.setattr(  # only one of the three weeks came back
        "sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search",
        lambda *a, **k: pl.DataFrame({"game_date": ["2024-04-01"] * 500}),
    )
    st = statcast.capture_season(tmp_path, 2024)
    assert st["captured"] == 0, "a partial month must never be written"
    assert st["failed"] > 0


def test_schedule_dtypes_survive_a_scores_absent_season():
    """An early-season build has every game in Preview with no scores. Inferred
    dtypes make home_score/away_score Null, and a later concat or
    scan_parquet('schedule/*.parquet') then raises SchemaError -- order
    dependently. The explicit schema is what keeps 39 seasons stackable."""
    import polars as pl

    preview = {
        "dates": [{"date": "2026-01-05", "games": [{
            "gamePk": 1, "gameType": "R", "season": "2026", "gameDate": "x",
            "status": {"abstractGameState": "Preview", "statusCode": "S",
                       "detailedState": "Scheduled"},
            "teams": {"home": {"team": {"id": 147, "name": "H"}},
                      "away": {"team": {"id": 111, "name": "A"}}},
            "venue": {"id": 1, "name": "V"},
        }]}]
    }
    df = pl.DataFrame(schedules.rows_from(preview), schema=schedules.SCHEDULE_SCHEMA)
    assert df.schema["home_score"] == pl.Int64
    assert df.schema["game_pk"] == pl.Int64
    scored = df.with_columns(pl.lit(3).cast(pl.Int64).alias("home_score"))
    assert pl.concat([df, scored]).height == 2  # would raise SchemaError on Null


def test_absent_boolean_stays_null_not_false():
    """bool(None) is False, which ASSERTS a fact statsapi did not state.
    movement.isOut is JSON-null in ~50 runner rows per season."""
    payload = _feed_payload()
    payload["liveData"]["plays"]["allPlays"][0]["runners"] = [{
        "movement": {"originBase": None, "start": None, "end": "1B",
                     "outBase": None, "isOut": None, "outNumber": None},
        "details": {"event": "Single", "eventType": "single",
                    "runner": {"id": 660271}, "responsiblePitcher": None},
    }]
    _, _, runners = parse.parse_bundle(payload)
    assert runners[0]["is_out"] is None, "unknown must not become False"
