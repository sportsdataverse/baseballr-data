# mlb_game_state fixtures

| File | Source | Notes |
|---|---|---|
| `pbp_extra_innings_2023.parquet` | statsapi `https://statsapi.mlb.com/api/v1/game/{718735,718511}/playByPlay`, parsed by sdv-py `mlb_play_by_play`, captured 2026-11-24 (copied from sdv-py `tests/fixtures/mlb_game_state/`) | Games 718735 (2023-04-02, 10 innings) and 718511 (11 innings), 176 PAs, 6 extra half-innings. Columns: `game_id`, `about_*`, `count_outs`, `result_*_score`, `matchup_post_on_*_id`, `matchup_batter_id`, `runners`, `result_event`; ids are Utf8. 718511's bottom-11 automatic runner scores on the half's first PA, so only `runners` shows him. Used by the automatic-runner gate tests in `test_mlb_model_publish.py`. |
| `leverage_index_2024_published_2026_11_26.parquet` | `mlb_game_state` release asset `mlb_leverage_index_2024.parquet` as published 2026-11-26 | 4,945 state buckets from the backward-looking estimator (first PA 0.414): the table the leverage gate must refuse. |
| `leverage_index_2024_rebuilt_2026_11_27.parquet` | Rebuilt 2024 leverage (forward swing, WE pooled over 2015-2026), the frame `compute_game_state` hands the gate | 4,945 buckets, first PA 0.931: the table the gate must pass. |
| `we_table_2023.parquet`, `we_table_2024.parquet` | `mlb_game_state` release assets `mlb_we_table_2023/2024.parquet` (2026-11-26 rebuild) | Real per-season WE tables for the pooled-WE tests. |
