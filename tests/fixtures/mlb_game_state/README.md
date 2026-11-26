# mlb_game_state fixtures

| File | Source | Notes |
|---|---|---|
| `pbp_extra_innings_2023.parquet` | statsapi `https://statsapi.mlb.com/api/v1/game/{718735,718511}/playByPlay`, parsed by sdv-py `mlb_play_by_play`, captured 2026-11-24 (copied from sdv-py `tests/fixtures/mlb_game_state/`) | Games 718735 (2023-04-02, 10 innings) and 718511 (11 innings), 176 PAs, 6 extra half-innings. Columns: `game_id`, `about_*`, `count_outs`, `result_*_score`, `matchup_post_on_*_id`, `matchup_batter_id`, `runners`, `result_event`; ids are Utf8. 718511's bottom-11 automatic runner scores on the half's first PA, so only `runners` shows him. Used by the automatic-runner gate tests in `test_mlb_model_publish.py`. |
