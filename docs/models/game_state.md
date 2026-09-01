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

<div id="dihbqicscn" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#dihbqicscn table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#dihbqicscn thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#dihbqicscn p { margin: 0; padding: 0; }
 #dihbqicscn .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #dihbqicscn .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #dihbqicscn .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #dihbqicscn .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #dihbqicscn .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #dihbqicscn .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dihbqicscn .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #dihbqicscn .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #dihbqicscn .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #dihbqicscn .gt_column_spanner_outer:first-child { padding-left: 0; }
 #dihbqicscn .gt_column_spanner_outer:last-child { padding-right: 0; }
 #dihbqicscn .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #dihbqicscn .gt_spanner_row { border-bottom-style: hidden; }
 #dihbqicscn .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #dihbqicscn .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #dihbqicscn .gt_from_md> :first-child { margin-top: 0; }
 #dihbqicscn .gt_from_md> :last-child { margin-bottom: 0; }
 #dihbqicscn .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #dihbqicscn .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #dihbqicscn .gt_indent_1 { text-indent: 5px; }
 #dihbqicscn .gt_indent_2 { text-indent: calc(5px * 2); }
 #dihbqicscn .gt_indent_3 { text-indent: calc(5px * 3); }
 #dihbqicscn .gt_indent_4 { text-indent: calc(5px * 4); }
 #dihbqicscn .gt_indent_5 { text-indent: calc(5px * 5); }
 #dihbqicscn .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #dihbqicscn .gt_row_group_first td { border-top-width: 2px; }
 #dihbqicscn .gt_row_group_first th { border-top-width: 2px; }
 #dihbqicscn .gt_striped { color: #333333; background-color: #F4F4F4; }
 #dihbqicscn .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dihbqicscn .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #dihbqicscn .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #dihbqicscn .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dihbqicscn .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #dihbqicscn .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #dihbqicscn .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #dihbqicscn .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dihbqicscn .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #dihbqicscn .gt_left { text-align: left; }
 #dihbqicscn .gt_center { text-align: center; }
 #dihbqicscn .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #dihbqicscn .gt_font_normal { font-weight: normal; }
 #dihbqicscn .gt_font_bold { font-weight: bold; }
 #dihbqicscn .gt_font_italic { font-style: italic; }
 #dihbqicscn .gt_super { font-size: 65%; }
 #dihbqicscn .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dihbqicscn .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #dihbqicscn .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dihbqicscn .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #dihbqicscn .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #dihbqicscn .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed game-state assets, by season |  |  |  |  |
|----|----|----|----|----|
| statsapi.mlb.com regular-season play-by-play; computed at render time |  |  |  |  |
| season | games | plate_appearances | re24_states | we_buckets |
| 2016 | 2428 | 187,517 | 24 | 4997 |
| 2022 | 2430 | 184,985 | 24 | 4943 |
| 2025 | 2430 | 186,115 | 24 | 5003 |
| 2024 | 2429 | 185,540 | 24 | 4985 |
| 2023 | 2430 | 187,451 | 24 | 5062 |
| 2017 | 2430 | 188,126 | 24 | 4974 |
| 2018 | 2431 | 188,172 | 24 | 4978 |
| 2026 | 2069 | 159,111 | 24 | 4923 |
| 2015 | 2429 | 186,878 | 24 | 5023 |
| 2021 | 2429 | 185,215 | 24 | 5079 |
| 2019 | 2429 | 189,617 | 24 | 5055 |
| 2020 | 898 | 67,701 | 24 | 4434 |

&#10;</div>

Every season from the first committed year to the present is rebuilt by
the daily in-season cron; 2020’s short season is visibly thinner and its
WE buckets are correspondingly noisier (a known limitation carried
without a flag in the data itself).

## The RE24 surface

<img src="game_state_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="RE24 matrix, latest season — expected runs to end of inning by base-out state." />

<img src="game_state_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Run environment over time: RE24 for three anchor states, by season." />

The matrix reproduces the canonical structure — monotone in baserunners,
decreasing in outs — and the anchor-state series tracks the league run
environment (the publish gate anchors the matrix against the Tango
run-expectancy tables at max abs diff ≤ 0.05).

<div id="rjkncaxmnv" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#rjkncaxmnv table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#rjkncaxmnv thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#rjkncaxmnv p { margin: 0; padding: 0; }
 #rjkncaxmnv .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #rjkncaxmnv .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #rjkncaxmnv .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #rjkncaxmnv .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #rjkncaxmnv .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rjkncaxmnv .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rjkncaxmnv .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rjkncaxmnv .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #rjkncaxmnv .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #rjkncaxmnv .gt_column_spanner_outer:first-child { padding-left: 0; }
 #rjkncaxmnv .gt_column_spanner_outer:last-child { padding-right: 0; }
 #rjkncaxmnv .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #rjkncaxmnv .gt_spanner_row { border-bottom-style: hidden; }
 #rjkncaxmnv .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #rjkncaxmnv .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #rjkncaxmnv .gt_from_md> :first-child { margin-top: 0; }
 #rjkncaxmnv .gt_from_md> :last-child { margin-bottom: 0; }
 #rjkncaxmnv .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #rjkncaxmnv .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #rjkncaxmnv .gt_indent_1 { text-indent: 5px; }
 #rjkncaxmnv .gt_indent_2 { text-indent: calc(5px * 2); }
 #rjkncaxmnv .gt_indent_3 { text-indent: calc(5px * 3); }
 #rjkncaxmnv .gt_indent_4 { text-indent: calc(5px * 4); }
 #rjkncaxmnv .gt_indent_5 { text-indent: calc(5px * 5); }
 #rjkncaxmnv .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #rjkncaxmnv .gt_row_group_first td { border-top-width: 2px; }
 #rjkncaxmnv .gt_row_group_first th { border-top-width: 2px; }
 #rjkncaxmnv .gt_striped { color: #333333; background-color: #F4F4F4; }
 #rjkncaxmnv .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rjkncaxmnv .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rjkncaxmnv .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #rjkncaxmnv .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rjkncaxmnv .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rjkncaxmnv .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #rjkncaxmnv .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #rjkncaxmnv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rjkncaxmnv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rjkncaxmnv .gt_left { text-align: left; }
 #rjkncaxmnv .gt_center { text-align: center; }
 #rjkncaxmnv .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #rjkncaxmnv .gt_font_normal { font-weight: normal; }
 #rjkncaxmnv .gt_font_bold { font-weight: bold; }
 #rjkncaxmnv .gt_font_italic { font-style: italic; }
 #rjkncaxmnv .gt_super { font-size: 65%; }
 #rjkncaxmnv .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rjkncaxmnv .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #rjkncaxmnv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rjkncaxmnv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rjkncaxmnv .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #rjkncaxmnv .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| RE24 structural sanity, all committed seasons   |               |
|-------------------------------------------------|---------------|
| structural check                                | share passing |
| RE decreasing in outs (per base state × season) | 100.0%        |

&#10;</div>

## Win expectancy

<img src="game_state_files/figure-commonmark/cell-7-output-1.png"
width="420" height="300"
alt="Home win expectancy by inning and score differential (bases empty, 0 outs, latest season)." />

The surface fans out with innings exactly as it should: a one-run lead
in the first is worth ~60%, the same lead in the ninth ~85%+. The
publish gate validates the derived per-PA WPA against statsapi’s own win
probability at Spearman ≥ 0.95.

## Evaluation — the WPA accounting identity

WPA’s defining property is that it is a **zero-sum credit ledger**:
summed over a game, home-perspective WPA must equal ±0.5 (start at 50%,
end at 0 or 100%). This is checked here over every committed season:

<div id="gayurzwvpy" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#gayurzwvpy table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#gayurzwvpy thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#gayurzwvpy p { margin: 0; padding: 0; }
 #gayurzwvpy .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #gayurzwvpy .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #gayurzwvpy .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #gayurzwvpy .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #gayurzwvpy .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #gayurzwvpy .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #gayurzwvpy .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #gayurzwvpy .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #gayurzwvpy .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #gayurzwvpy .gt_column_spanner_outer:first-child { padding-left: 0; }
 #gayurzwvpy .gt_column_spanner_outer:last-child { padding-right: 0; }
 #gayurzwvpy .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #gayurzwvpy .gt_spanner_row { border-bottom-style: hidden; }
 #gayurzwvpy .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #gayurzwvpy .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #gayurzwvpy .gt_from_md> :first-child { margin-top: 0; }
 #gayurzwvpy .gt_from_md> :last-child { margin-bottom: 0; }
 #gayurzwvpy .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #gayurzwvpy .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #gayurzwvpy .gt_indent_1 { text-indent: 5px; }
 #gayurzwvpy .gt_indent_2 { text-indent: calc(5px * 2); }
 #gayurzwvpy .gt_indent_3 { text-indent: calc(5px * 3); }
 #gayurzwvpy .gt_indent_4 { text-indent: calc(5px * 4); }
 #gayurzwvpy .gt_indent_5 { text-indent: calc(5px * 5); }
 #gayurzwvpy .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #gayurzwvpy .gt_row_group_first td { border-top-width: 2px; }
 #gayurzwvpy .gt_row_group_first th { border-top-width: 2px; }
 #gayurzwvpy .gt_striped { color: #333333; background-color: #F4F4F4; }
 #gayurzwvpy .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #gayurzwvpy .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #gayurzwvpy .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #gayurzwvpy .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #gayurzwvpy .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #gayurzwvpy .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #gayurzwvpy .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #gayurzwvpy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #gayurzwvpy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #gayurzwvpy .gt_left { text-align: left; }
 #gayurzwvpy .gt_center { text-align: center; }
 #gayurzwvpy .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #gayurzwvpy .gt_font_normal { font-weight: normal; }
 #gayurzwvpy .gt_font_bold { font-weight: bold; }
 #gayurzwvpy .gt_font_italic { font-style: italic; }
 #gayurzwvpy .gt_super { font-size: 65%; }
 #gayurzwvpy .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #gayurzwvpy .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #gayurzwvpy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #gayurzwvpy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #gayurzwvpy .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #gayurzwvpy .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<img src="game_state_files/figure-commonmark/cell-9-output-1.png"
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
  the same substrate.
- **Leverage index** — the WE table already contains everything needed
  to publish LI alongside WPA; it is the most-requested missing column.
- **Known issue:** 2020’s short season produces visibly thinner WE
  buckets; the table carries it without a flag.
