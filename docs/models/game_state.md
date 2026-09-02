# MLB game state — RE24, Win Expectancy, WPA


The game-state family publishes the three classical baseball state-value
surfaces on the `mlb_game_state` release tag: the **RE24 matrix**
(expected runs to the end of the inning for each of the 24 base-out
states), a **win expectancy table** over inning / score-differential /
base-out buckets, and per-plate-appearance **WPA** (the change in win
expectancy attributed to each plate appearance). They are computed by
sdv-py’s `mlb_run_expectancy` / `mlb_win_expectancy` over
statsapi.mlb.com regular-season play-by-play and committed per season
under `mlb/game_state/`.

These are **empirical conditional means over observed states** — the
“model” is the state definition and the estimator. That design choice
buys two things: the surfaces are assumption-free (no parametric form to
misfit), and their internal accounting is *exactly* checkable. This
document runs those checks at render time from the committed parquet —
the WPA zero-sum identity, the RE24 monotonicity structure, and the
stability of the run environment across seasons.

There is deliberately no feature-importance or SHAP section here: with
no fitted coefficients or trees, attribution reduces to the state
definition itself. The nearest analogue — how much each dimension of the
state moves the estimate — is exactly what the heatmaps below show.

## Training data

<div id="btlcwawpgr" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#btlcwawpgr table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#btlcwawpgr thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#btlcwawpgr p { margin: 0; padding: 0; }
 #btlcwawpgr .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #btlcwawpgr .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #btlcwawpgr .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #btlcwawpgr .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #btlcwawpgr .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #btlcwawpgr .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #btlcwawpgr .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #btlcwawpgr .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #btlcwawpgr .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #btlcwawpgr .gt_column_spanner_outer:first-child { padding-left: 0; }
 #btlcwawpgr .gt_column_spanner_outer:last-child { padding-right: 0; }
 #btlcwawpgr .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #btlcwawpgr .gt_spanner_row { border-bottom-style: hidden; }
 #btlcwawpgr .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #btlcwawpgr .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #btlcwawpgr .gt_from_md> :first-child { margin-top: 0; }
 #btlcwawpgr .gt_from_md> :last-child { margin-bottom: 0; }
 #btlcwawpgr .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #btlcwawpgr .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #btlcwawpgr .gt_indent_1 { text-indent: 5px; }
 #btlcwawpgr .gt_indent_2 { text-indent: calc(5px * 2); }
 #btlcwawpgr .gt_indent_3 { text-indent: calc(5px * 3); }
 #btlcwawpgr .gt_indent_4 { text-indent: calc(5px * 4); }
 #btlcwawpgr .gt_indent_5 { text-indent: calc(5px * 5); }
 #btlcwawpgr .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #btlcwawpgr .gt_row_group_first td { border-top-width: 2px; }
 #btlcwawpgr .gt_row_group_first th { border-top-width: 2px; }
 #btlcwawpgr .gt_striped { color: #333333; background-color: #F4F4F4; }
 #btlcwawpgr .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #btlcwawpgr .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #btlcwawpgr .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #btlcwawpgr .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #btlcwawpgr .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #btlcwawpgr .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #btlcwawpgr .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #btlcwawpgr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #btlcwawpgr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #btlcwawpgr .gt_left { text-align: left; }
 #btlcwawpgr .gt_center { text-align: center; }
 #btlcwawpgr .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #btlcwawpgr .gt_font_normal { font-weight: normal; }
 #btlcwawpgr .gt_font_bold { font-weight: bold; }
 #btlcwawpgr .gt_font_italic { font-style: italic; }
 #btlcwawpgr .gt_super { font-size: 65%; }
 #btlcwawpgr .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #btlcwawpgr .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #btlcwawpgr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #btlcwawpgr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #btlcwawpgr .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #btlcwawpgr .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed game-state assets, by season |  |  |  |  |
|----|----|----|----|----|
| statsapi.mlb.com regular-season play-by-play; computed at render time |  |  |  |  |
| season | games | plate_appearances | re24_states | we_buckets |
| 2020 | 898 | 67,701 | 24 | 4434 |
| 2026 | 2069 | 159,111 | 24 | 4923 |
| 2018 | 2431 | 188,172 | 24 | 4978 |
| 2016 | 2428 | 187,517 | 24 | 4997 |
| 2021 | 2429 | 185,215 | 24 | 5079 |
| 2023 | 2430 | 187,451 | 24 | 5062 |
| 2022 | 2430 | 184,985 | 24 | 4943 |
| 2017 | 2430 | 188,126 | 24 | 4974 |
| 2019 | 2429 | 189,617 | 24 | 5055 |
| 2025 | 2430 | 186,115 | 24 | 5003 |
| 2024 | 2429 | 185,540 | 24 | 4985 |
| 2015 | 2429 | 186,878 | 24 | 5023 |

&#10;</div>

Every season from the first committed year to the present is rebuilt by
the daily in-season cron. 2020’s short season is visibly thinner and its
WE buckets are correspondingly noisier — and as of 2026-09-02 that is
**carried in the data** rather than left for the reader to infer: both
state-bucket tables ship the bucket count `n` and a `thin` flag.

## How thin is thin? (the threshold is measured, not chosen)

A bucket flagged `thin` is one whose win-expectancy estimate does not
reproduce across seasons. The cut is derived by asking exactly that: for
every state bucket, how far apart are the same bucket’s WE estimates in
adjacent full seasons?

<div id="ndgmesmbjv" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ndgmesmbjv table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ndgmesmbjv thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ndgmesmbjv p { margin: 0; padding: 0; }
 #ndgmesmbjv .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ndgmesmbjv .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ndgmesmbjv .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ndgmesmbjv .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ndgmesmbjv .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ndgmesmbjv .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndgmesmbjv .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ndgmesmbjv .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ndgmesmbjv .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ndgmesmbjv .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ndgmesmbjv .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ndgmesmbjv .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ndgmesmbjv .gt_spanner_row { border-bottom-style: hidden; }
 #ndgmesmbjv .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ndgmesmbjv .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ndgmesmbjv .gt_from_md> :first-child { margin-top: 0; }
 #ndgmesmbjv .gt_from_md> :last-child { margin-bottom: 0; }
 #ndgmesmbjv .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ndgmesmbjv .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ndgmesmbjv .gt_indent_1 { text-indent: 5px; }
 #ndgmesmbjv .gt_indent_2 { text-indent: calc(5px * 2); }
 #ndgmesmbjv .gt_indent_3 { text-indent: calc(5px * 3); }
 #ndgmesmbjv .gt_indent_4 { text-indent: calc(5px * 4); }
 #ndgmesmbjv .gt_indent_5 { text-indent: calc(5px * 5); }
 #ndgmesmbjv .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ndgmesmbjv .gt_row_group_first td { border-top-width: 2px; }
 #ndgmesmbjv .gt_row_group_first th { border-top-width: 2px; }
 #ndgmesmbjv .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ndgmesmbjv .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndgmesmbjv .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ndgmesmbjv .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ndgmesmbjv .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndgmesmbjv .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ndgmesmbjv .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ndgmesmbjv .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ndgmesmbjv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndgmesmbjv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ndgmesmbjv .gt_left { text-align: left; }
 #ndgmesmbjv .gt_center { text-align: center; }
 #ndgmesmbjv .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ndgmesmbjv .gt_font_normal { font-weight: normal; }
 #ndgmesmbjv .gt_font_bold { font-weight: bold; }
 #ndgmesmbjv .gt_font_italic { font-style: italic; }
 #ndgmesmbjv .gt_super { font-size: 65%; }
 #ndgmesmbjv .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndgmesmbjv .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ndgmesmbjv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndgmesmbjv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ndgmesmbjv .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ndgmesmbjv .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Why the thin cut sits at n = 50 |  |  |
|----|----|----|
| same bucket, adjacent full seasons — disagreement more than doubles below n=50 |  |  |
| bucket size | buckets | mean \|dWE\| vs next season |
| \<10 | 22066 | 0.1260 |
| 10-25 | 8717 | 0.0823 |
| 25-50 | 4686 | 0.0586 |
| 50-100 | 3791 | 0.0429 |
| 100-200 | 2468 | 0.0312 |
| 200+ | 1336 | 0.0296 |

&#10;</div>

As bucket size falls through 50, a bucket’s disagreement with its own
next-season value roughly doubles off the large-bucket floor (~.03), so
50 is where the estimate stops being reusable. Applying it per season
shows how far outside the norm 2020 sits:

<div id="zthzrleqsw" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zthzrleqsw table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zthzrleqsw thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zthzrleqsw p { margin: 0; padding: 0; }
 #zthzrleqsw .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zthzrleqsw .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zthzrleqsw .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zthzrleqsw .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zthzrleqsw .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zthzrleqsw .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zthzrleqsw .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zthzrleqsw .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zthzrleqsw .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zthzrleqsw .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zthzrleqsw .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zthzrleqsw .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zthzrleqsw .gt_spanner_row { border-bottom-style: hidden; }
 #zthzrleqsw .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zthzrleqsw .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zthzrleqsw .gt_from_md> :first-child { margin-top: 0; }
 #zthzrleqsw .gt_from_md> :last-child { margin-bottom: 0; }
 #zthzrleqsw .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zthzrleqsw .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zthzrleqsw .gt_indent_1 { text-indent: 5px; }
 #zthzrleqsw .gt_indent_2 { text-indent: calc(5px * 2); }
 #zthzrleqsw .gt_indent_3 { text-indent: calc(5px * 3); }
 #zthzrleqsw .gt_indent_4 { text-indent: calc(5px * 4); }
 #zthzrleqsw .gt_indent_5 { text-indent: calc(5px * 5); }
 #zthzrleqsw .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zthzrleqsw .gt_row_group_first td { border-top-width: 2px; }
 #zthzrleqsw .gt_row_group_first th { border-top-width: 2px; }
 #zthzrleqsw .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zthzrleqsw .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zthzrleqsw .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zthzrleqsw .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zthzrleqsw .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zthzrleqsw .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zthzrleqsw .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zthzrleqsw .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zthzrleqsw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zthzrleqsw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zthzrleqsw .gt_left { text-align: left; }
 #zthzrleqsw .gt_center { text-align: center; }
 #zthzrleqsw .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zthzrleqsw .gt_font_normal { font-weight: normal; }
 #zthzrleqsw .gt_font_bold { font-weight: bold; }
 #zthzrleqsw .gt_font_italic { font-style: italic; }
 #zthzrleqsw .gt_super { font-size: 65%; }
 #zthzrleqsw .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zthzrleqsw .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zthzrleqsw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zthzrleqsw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zthzrleqsw .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zthzrleqsw .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Thin state buckets by season (n \< 50) |  |  |  |
|----|----|----|----|
| 2020's 60-game season is the outlier the flag exists to mark |  |  |  |
| season | buckets | median n | share thin |
| 2015 | 5023 | 11 | 82.2% |
| 2016 | 4997 | 11 | 81.8% |
| 2017 | 4974 | 12 | 81.2% |
| 2018 | 4978 | 12 | 81.4% |
| 2019 | 5055 | 11 | 81.2% |
| 2020 | 4434 | 5 | 93.1% |
| 2021 | 5079 | 11 | 82.1% |
| 2022 | 4943 | 11 | 81.7% |
| 2023 | 5062 | 11 | 81.6% |
| 2024 | 4985 | 11 | 81.8% |
| 2025 | 5003 | 11 | 81.5% |
| 2026 | 4923 | 10 | 83.2% |

&#10;</div>

2020 carries a median bucket of 5 plate appearances against 11 in a full
season, and ~93% of its buckets thin against ~82%. The flag does not
repair 2020 — nothing can, the games were not played — it makes the
difference visible to a consumer who would otherwise average the two
together.

## The RE24 surface

<img src="game_state_files/figure-commonmark/cell-6-output-1.png"
width="420" height="300"
alt="RE24 matrix, latest season — expected runs to end of inning by base-out state." />

<img src="game_state_files/figure-commonmark/cell-7-output-1.png"
width="420" height="300"
alt="Run environment over time: RE24 for three anchor states, by season." />

The matrix reproduces the canonical structure — monotone in baserunners,
decreasing in outs — and the anchor-state series tracks the league run
environment (the publish gate anchors the matrix against the Tango
run-expectancy tables at max abs diff ≤ 0.05).

<div id="zxukhzzjtr" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zxukhzzjtr table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zxukhzzjtr thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zxukhzzjtr p { margin: 0; padding: 0; }
 #zxukhzzjtr .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zxukhzzjtr .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zxukhzzjtr .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zxukhzzjtr .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zxukhzzjtr .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zxukhzzjtr .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zxukhzzjtr .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zxukhzzjtr .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zxukhzzjtr .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zxukhzzjtr .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zxukhzzjtr .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zxukhzzjtr .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zxukhzzjtr .gt_spanner_row { border-bottom-style: hidden; }
 #zxukhzzjtr .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zxukhzzjtr .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zxukhzzjtr .gt_from_md> :first-child { margin-top: 0; }
 #zxukhzzjtr .gt_from_md> :last-child { margin-bottom: 0; }
 #zxukhzzjtr .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zxukhzzjtr .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zxukhzzjtr .gt_indent_1 { text-indent: 5px; }
 #zxukhzzjtr .gt_indent_2 { text-indent: calc(5px * 2); }
 #zxukhzzjtr .gt_indent_3 { text-indent: calc(5px * 3); }
 #zxukhzzjtr .gt_indent_4 { text-indent: calc(5px * 4); }
 #zxukhzzjtr .gt_indent_5 { text-indent: calc(5px * 5); }
 #zxukhzzjtr .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zxukhzzjtr .gt_row_group_first td { border-top-width: 2px; }
 #zxukhzzjtr .gt_row_group_first th { border-top-width: 2px; }
 #zxukhzzjtr .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zxukhzzjtr .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zxukhzzjtr .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zxukhzzjtr .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zxukhzzjtr .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zxukhzzjtr .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zxukhzzjtr .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zxukhzzjtr .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zxukhzzjtr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zxukhzzjtr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zxukhzzjtr .gt_left { text-align: left; }
 #zxukhzzjtr .gt_center { text-align: center; }
 #zxukhzzjtr .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zxukhzzjtr .gt_font_normal { font-weight: normal; }
 #zxukhzzjtr .gt_font_bold { font-weight: bold; }
 #zxukhzzjtr .gt_font_italic { font-style: italic; }
 #zxukhzzjtr .gt_super { font-size: 65%; }
 #zxukhzzjtr .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zxukhzzjtr .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zxukhzzjtr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zxukhzzjtr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zxukhzzjtr .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zxukhzzjtr .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| RE24 structural sanity, all committed seasons   |               |
|-------------------------------------------------|---------------|
| structural check                                | share passing |
| RE decreasing in outs (per base state × season) | 100.0%        |

&#10;</div>

## Win expectancy

<img src="game_state_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="Home win expectancy by inning and score differential (bases empty, 0 outs, latest season)." />

The surface fans out with innings exactly as it should: a one-run lead
in the first is worth ~60%, the same lead in the ninth ~85%+. The
publish gate validates the derived per-PA WPA against statsapi’s own win
probability at Spearman ≥ 0.95.

## Leverage index

Leverage index answers “how much is this state capable of swinging the
game?” — the expected absolute win-expectancy change over the outcomes
that actually follow this state, normalized so the league-average plate
appearance is 1.0. It ships as `mlb_leverage_index` on the WE table’s
own state-bucket key (and with the same `n` / `thin` columns), so WPA
and LI join without a second derivation.

<p><strong>Pending republish.</strong> the <code>mlb_leverage_index</code> stem lands with the next rebuild. Validated on real 2024 statsapi play-by-play at build time: <strong>PA-weighted mean LI = 1.0000</strong> (1.0 by construction), range 0.00&ndash;13.87, and late-and-close states (inning &ge; 8, margin &le; 1 run, non-thin) average <strong>1.52</strong> against <strong>0.80</strong> for early lopsided ones.</p>

The normalization is **PA-weighted**: the average *plate appearance* has
LI 1.0, not the average *bucket*. The unweighted mean across buckets is
far higher (~1.8 in 2024) because high-leverage states are numerous but
seldom occur — so a naive `mean(leverage_index) == 1` check looks like a
bug and is not one. Bucket-level LI is also noisier than the WE it
derives from (it is a second moment of the same counts), which is
exactly why the `thin` flag travels with it.

## Evaluation — the WPA accounting identity

WPA’s defining property is that it is a **zero-sum credit ledger**:
summed over a game, home-perspective WPA must equal ±0.5 (start at 50%,
end at 0 or 100%). This is checked here over every committed season:

<div id="ofywkeqfsv" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ofywkeqfsv table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ofywkeqfsv thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ofywkeqfsv p { margin: 0; padding: 0; }
 #ofywkeqfsv .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ofywkeqfsv .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ofywkeqfsv .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ofywkeqfsv .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ofywkeqfsv .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ofywkeqfsv .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ofywkeqfsv .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ofywkeqfsv .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ofywkeqfsv .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ofywkeqfsv .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ofywkeqfsv .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ofywkeqfsv .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ofywkeqfsv .gt_spanner_row { border-bottom-style: hidden; }
 #ofywkeqfsv .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ofywkeqfsv .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ofywkeqfsv .gt_from_md> :first-child { margin-top: 0; }
 #ofywkeqfsv .gt_from_md> :last-child { margin-bottom: 0; }
 #ofywkeqfsv .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ofywkeqfsv .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ofywkeqfsv .gt_indent_1 { text-indent: 5px; }
 #ofywkeqfsv .gt_indent_2 { text-indent: calc(5px * 2); }
 #ofywkeqfsv .gt_indent_3 { text-indent: calc(5px * 3); }
 #ofywkeqfsv .gt_indent_4 { text-indent: calc(5px * 4); }
 #ofywkeqfsv .gt_indent_5 { text-indent: calc(5px * 5); }
 #ofywkeqfsv .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ofywkeqfsv .gt_row_group_first td { border-top-width: 2px; }
 #ofywkeqfsv .gt_row_group_first th { border-top-width: 2px; }
 #ofywkeqfsv .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ofywkeqfsv .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ofywkeqfsv .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ofywkeqfsv .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ofywkeqfsv .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ofywkeqfsv .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ofywkeqfsv .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ofywkeqfsv .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ofywkeqfsv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ofywkeqfsv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ofywkeqfsv .gt_left { text-align: left; }
 #ofywkeqfsv .gt_center { text-align: center; }
 #ofywkeqfsv .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ofywkeqfsv .gt_font_normal { font-weight: normal; }
 #ofywkeqfsv .gt_font_bold { font-weight: bold; }
 #ofywkeqfsv .gt_font_italic { font-style: italic; }
 #ofywkeqfsv .gt_super { font-size: 65%; }
 #ofywkeqfsv .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ofywkeqfsv .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ofywkeqfsv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ofywkeqfsv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ofywkeqfsv .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ofywkeqfsv .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Per-game WPA sum identity — \|Σ wpa\| vs 0.5 |  |  |  |
|----|----|----|----|
| exact accounting check over every committed season |  |  |  |
| season | games | identity_MAE | identity_max |
| 2015 | 2429 | 0.000000 | 0.000000 |
| 2016 | 2428 | 0.000000 | 0.000000 |
| 2017 | 2430 | 0.000000 | 0.000000 |
| 2018 | 2431 | 0.000000 | 0.000000 |
| 2019 | 2429 | 0.000000 | 0.000000 |
| 2020 | 898 | 0.000000 | 0.000000 |
| 2021 | 2429 | 0.000000 | 0.000000 |
| 2022 | 2430 | 0.000000 | 0.000000 |
| 2023 | 2430 | 0.000000 | 0.000000 |
| 2024 | 2429 | 0.000000 | 0.000000 |
| 2025 | 2430 | 0.000000 | 0.000000 |
| 2026 | 2069 | 0.000000 | 0.000000 |

&#10;</div>

<img src="game_state_files/figure-commonmark/cell-12-output-1.png"
width="420" height="300"
alt="Distribution of per-PA WPA, latest season — heavy mass near zero, long leverage tails." />

A mean identity error at (or numerically indistinguishable from) zero in
every season is the strongest possible statement about internal
consistency: the ledger balances game by game, not just on average. The
long tails of the per-PA distribution are the leverage plays — walk-offs
and late-inning homers — exactly the events WPA exists to weight.

## Provenance & reproducibility

- **Built from:** statsapi.mlb.com regular-season play-by-play, seasons
  in the table above, by sdv-py’s `mlb_run_expectancy` /
  `mlb_win_expectancy`.
- **Committed at:** `mlb/game_state/parquet/` (this document reads only
  those files); published to the `mlb_game_state` release tag;
  per-publish metadata in
  [`../../mlb/game_state/mlb_game_state_card.json`](../../mlb/game_state/mlb_game_state_card.json).
- **Pipeline:** `scripts/mlb_models.sh 01` → stage
  `python/mlb_model_01_game_state.py` (`mlb_models_cron.yml`, daily
  in-season). Single home: `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; this repo’s `.venv` via `QUARTO_PYTHON`; `uv sync --group docs`).

## Avenues for improvement & open issues

- **Era/park variants** — league-average tables hide park and era
  structure a consumer may want; a park-adjusted variant is cheap from
  the same substrate. Scoped but deliberately not built here: per-park
  state buckets are ~1/30th the count of these, so the `thin` discipline
  introduced below would dominate the result. It needs a park-factor
  input and a bucket-collapsing scheme (park x base-out, pooling
  inning/score) before it is publishable.
- **Resolved (2026-09-02, PR \#13):** leverage index is published as
  `mlb_leverage_index`, on the WE table’s own state-bucket key, carrying
  `n` and `thin`. Validated on real 2024 play-by-play: PA-weighted mean
  LI 1.0000, late-and-close states 1.52 against 0.80 for early lopsided
  ones.
- **Resolved (2026-09-02, PR \#13):** the WE and leverage tables carry
  the bucket count `n` and a `thin` flag (`n < 50`), so 2020’s short
  season is marked in the data. The threshold is measured —
  adjacent-season WE disagreement is .0586 at n=25-50 against .0296 at
  n\>=200 — and 2020 shows 93.1% thin buckets (median n=5) against
  ~81.5% (median n=11) in a full season. Both are recomputed at render
  time above.
