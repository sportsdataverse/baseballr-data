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
