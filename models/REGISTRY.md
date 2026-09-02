# Model registry

One row per model family this repo publishes (Track C step 1). These are
**compute-on-demand model datasets** — no fitted booster artifacts; the
"model" is the sdv-py compute recipe plus the published per-season frames,
and each tag carries a generated `*_card.json` with the full metadata.

**Authority for grain / source / gates is `_CARD_META` in
`python/mlb_model_publish/builders.py`** — the gate values below are cited
from it, and `tests/test_model_registry.py` fails the build if a family or a
gate name in `_CARD_META` has no row here (so the two cannot drift apart
silently).

"Last publish" = the daily in-season rebuild (`mlb_models_cron.yml`, cron
`30 10 * 4-10 *`, Apr–Oct + dispatch); these recompute rather than retrain.

| model family | artifact stems (per season: parquet + csv + rds) | release tag | training data | fitting script | gates at publish (`_CARD_META`) | last publish | cadence |
|---|---|---|---|---|---|---|---|
| Game state (RE24 / WE / LI / WPA) | `mlb_re24_matrix`, `mlb_we_table`, `mlb_leverage_index`, `mlb_wpa` (+ `mlb_game_state_card.json`) | `mlb_game_state` | statsapi.mlb.com regular-season pbp via sdv-py `mlb_run_expectancy` / `mlb_win_expectancy` | `mlb_model_publish game-state` | `re24_vs_tango_max_abs_diff` ≤ 0.05; `wpa_spearman_vs_statsapi_wp` ≥ 0.95; `per_game_wpa_sum_identity_tol` ≤ 0.02; LI is PA-weighted-mean 1.0 by construction (observed 1.0000, 2024); `thin` = bucket `n` < 50 (threshold measured — see note below) | 2026-08-31 | daily in-season |
| Hitting (expected + observed stats / xHR / projection) | `mlb_expected_stats` (now also observed `woba`/`ba`), `mlb_expected_hr`, `mlb_batter_projection` (+ card) | `mlb_hitting_models` | Baseball Savant via sdv-py `mlb_expected_stats` / `mlb_expected_home_runs` / `mlb_batter_projection`; projection for season S trains only on seasons < S (as-of enforced) | `mlb_model_publish hitting` | `xwoba_spearman_same_input` ≥ 0.95; `xba_spearman_same_input` ≥ 0.95; `xhr_full_season_spearman_live` ≥ 0.90; PLUS absolute scale bands (the Spearman gates are rank-based and scale-blind — that is how the mis-scaled seasons shipped): `qualified_league_mean_xwoba_band` .26–.38, `qualified_league_mean_xba_band` .21–.29, `qualified_league_mean_expected_vs_observed_max_gap` ≤ 0.02 | 2026-08-31 | daily in-season |
| Pitching (xERA / Stuff+ / Command+) | `mlb_xera`, `mlb_stuff_plus`, `mlb_command_plus` (+ card) | `mlb_pitching_models` | pitch_features substrate (Baseball Savant) via sdv-py `x_era` / `mlb_stuff_plus` / `mlb_command_plus` | `mlb_model_publish pitching` | `xera_mae_vs_savant_xera` ≤ 0.30; `stuff_plus_spearman_vs_run_value` ≥ 0.20; `command_plus_spearman_vs_run_value` ≥ 0.04 (DIRECTIONAL only — weak ordinal signal) | 2026-08-31 | daily in-season |
| Fielding (OAA / directional OAA / catcher framing) | `mlb_oaa`, `mlb_oaa_direction`, `mlb_catcher_framing` (+ card) | `mlb_fielding_models` | Baseball Savant balls-in-play via sdv-py `mlb_fielding_oaa` / `mlb_catcher_framing` | `mlb_model_publish fielding` | `oaa_full_season_pearson_live` ≥ 0.55; `framing_full_season_pearson_live` ≥ 0.40 (feature-capped ceilings — public feed lacks fielder start coordinates); `mlb_oaa_direction` rows sum exactly to `mlb_oaa` (partition, not a re-fit — observed max drift 3.6e-15 on 2021) | 2026-08-31 | daily in-season |

## Deliberately NOT published (recorded so nobody "finds" the gap)

- **SIERA-like** — coefficients are unfitted literature placeholders.
- **Pitch tunneling / sequence run value** — no public oracle to gate against.
- **Catcher throwing/blocking, baserunning, SB value** — data-ceiling-limited
  (live floors 0.03–0.073 vs 0.80+ design targets; only ~23% of SB/CS
  attempts are narrated in the public feed).

## Operability (Track C steps 2–6)

- `models/manifest.yaml` — single home for the family/stage list (guarded by `tests/test_model_manifest.py`, in lockstep with `_CARD_META`).
- One family = one numbered pipeline at `python/mlb_model_NN_<family>.py` (thin entries over `mlb_model_publish`); run subsets with `scripts/mlb_models.sh` or the `families` dispatch input on `mlb_models_cron.yml`. The cron stays ONE job by design — the Savant families share a cached season pull and one tree commit; a matrix would re-pull 3× and race the commit.
- Fingerprints / ledger.jsonl / committed artifacts: honest N/As — compute-on-demand daily recompute; the published `*_card.json` per tag is the per-publish ledger.

## Derived thresholds (measured, never chosen)

- **`thin` bucket flag, `n` < 50** (`computes.THIN_BUCKET_N`). Pooling every
  adjacent pair of full seasons (2020 excluded as the anomaly under test), the
  same state bucket's WE estimate disagrees across seasons by mean |dWE| .1260
  (n<10), .0823 (10-25), .0586 (25-50), .0429 (50-100), .0312 (100-200), .0296
  (200+) — roughly a doubling from the large-bucket floor as size falls through
  50, recomputed on every render of the game-state writeup. 2020 is the motivating
  case: median bucket n=5 and 93.1% thin, against median 11 and ~81.5% in every
  full season — previously carried with no flag at all.
- **Hitting scale bands.** `xwoba` .26–.38 and the expected-vs-observed gap
  <= 0.02 were set at the 2026-09-01 incident. `xba`'s band was re-derived
  2026-09-02 from .18–.30 to .21–.29 after the untracked-batted-ball fix in
  sdv-py: rebuilt on the real 2015-2021 seasons, qualified league-mean xBA is
  .2400–.2532 against an observed BA of .2410–.2575 (it previously ran
  .2026–.2229 because balls in play with no launch data counted in `ab` with a
  zero numerator). This is a re-derivation against a newly-measured quantity,
  not a loosening — the band tightened.
