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

<div id="zynewzjuzz" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zynewzjuzz table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zynewzjuzz thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zynewzjuzz p { margin: 0; padding: 0; }
 #zynewzjuzz .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zynewzjuzz .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zynewzjuzz .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zynewzjuzz .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zynewzjuzz .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zynewzjuzz .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zynewzjuzz .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zynewzjuzz .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zynewzjuzz .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zynewzjuzz .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zynewzjuzz .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zynewzjuzz .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zynewzjuzz .gt_spanner_row { border-bottom-style: hidden; }
 #zynewzjuzz .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zynewzjuzz .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zynewzjuzz .gt_from_md> :first-child { margin-top: 0; }
 #zynewzjuzz .gt_from_md> :last-child { margin-bottom: 0; }
 #zynewzjuzz .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zynewzjuzz .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zynewzjuzz .gt_indent_1 { text-indent: 5px; }
 #zynewzjuzz .gt_indent_2 { text-indent: calc(5px * 2); }
 #zynewzjuzz .gt_indent_3 { text-indent: calc(5px * 3); }
 #zynewzjuzz .gt_indent_4 { text-indent: calc(5px * 4); }
 #zynewzjuzz .gt_indent_5 { text-indent: calc(5px * 5); }
 #zynewzjuzz .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zynewzjuzz .gt_row_group_first td { border-top-width: 2px; }
 #zynewzjuzz .gt_row_group_first th { border-top-width: 2px; }
 #zynewzjuzz .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zynewzjuzz .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zynewzjuzz .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zynewzjuzz .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zynewzjuzz .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zynewzjuzz .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zynewzjuzz .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zynewzjuzz .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zynewzjuzz .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zynewzjuzz .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zynewzjuzz .gt_left { text-align: left; }
 #zynewzjuzz .gt_center { text-align: center; }
 #zynewzjuzz .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zynewzjuzz .gt_font_normal { font-weight: normal; }
 #zynewzjuzz .gt_font_bold { font-weight: bold; }
 #zynewzjuzz .gt_font_italic { font-style: italic; }
 #zynewzjuzz .gt_super { font-size: 65%; }
 #zynewzjuzz .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zynewzjuzz .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zynewzjuzz .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zynewzjuzz .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zynewzjuzz .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zynewzjuzz .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed game-state assets, by season |  |  |  |  |
|----|----|----|----|----|
| statsapi.mlb.com regular-season play-by-play; computed at render time |  |  |  |  |
| season | games | plate_appearances | re24_states | we_buckets |
| 2019 | 2429 | 189,617 | 24 | 5055 |
| 2015 | 2429 | 186,878 | 24 | 5023 |
| 2017 | 2430 | 188,126 | 24 | 4974 |
| 2021 | 2429 | 185,215 | 24 | 5079 |
| 2026 | 2069 | 159,111 | 24 | 4923 |
| 2016 | 2428 | 187,517 | 24 | 4997 |
| 2024 | 2429 | 185,540 | 24 | 4985 |
| 2025 | 2430 | 186,115 | 24 | 5003 |
| 2018 | 2431 | 188,172 | 24 | 4978 |
| 2022 | 2430 | 184,985 | 24 | 4943 |
| 2023 | 2430 | 187,451 | 24 | 5062 |
| 2020 | 898 | 67,701 | 24 | 4434 |

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

<div id="vwbnpomioc" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#vwbnpomioc table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#vwbnpomioc thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#vwbnpomioc p { margin: 0; padding: 0; }
 #vwbnpomioc .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #vwbnpomioc .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #vwbnpomioc .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #vwbnpomioc .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #vwbnpomioc .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vwbnpomioc .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vwbnpomioc .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vwbnpomioc .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #vwbnpomioc .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #vwbnpomioc .gt_column_spanner_outer:first-child { padding-left: 0; }
 #vwbnpomioc .gt_column_spanner_outer:last-child { padding-right: 0; }
 #vwbnpomioc .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #vwbnpomioc .gt_spanner_row { border-bottom-style: hidden; }
 #vwbnpomioc .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #vwbnpomioc .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #vwbnpomioc .gt_from_md> :first-child { margin-top: 0; }
 #vwbnpomioc .gt_from_md> :last-child { margin-bottom: 0; }
 #vwbnpomioc .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #vwbnpomioc .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #vwbnpomioc .gt_indent_1 { text-indent: 5px; }
 #vwbnpomioc .gt_indent_2 { text-indent: calc(5px * 2); }
 #vwbnpomioc .gt_indent_3 { text-indent: calc(5px * 3); }
 #vwbnpomioc .gt_indent_4 { text-indent: calc(5px * 4); }
 #vwbnpomioc .gt_indent_5 { text-indent: calc(5px * 5); }
 #vwbnpomioc .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #vwbnpomioc .gt_row_group_first td { border-top-width: 2px; }
 #vwbnpomioc .gt_row_group_first th { border-top-width: 2px; }
 #vwbnpomioc .gt_striped { color: #333333; background-color: #F4F4F4; }
 #vwbnpomioc .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vwbnpomioc .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vwbnpomioc .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #vwbnpomioc .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vwbnpomioc .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vwbnpomioc .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #vwbnpomioc .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #vwbnpomioc .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vwbnpomioc .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vwbnpomioc .gt_left { text-align: left; }
 #vwbnpomioc .gt_center { text-align: center; }
 #vwbnpomioc .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #vwbnpomioc .gt_font_normal { font-weight: normal; }
 #vwbnpomioc .gt_font_bold { font-weight: bold; }
 #vwbnpomioc .gt_font_italic { font-style: italic; }
 #vwbnpomioc .gt_super { font-size: 65%; }
 #vwbnpomioc .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vwbnpomioc .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #vwbnpomioc .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vwbnpomioc .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vwbnpomioc .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #vwbnpomioc .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="ixuwvxxqxf" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ixuwvxxqxf table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ixuwvxxqxf thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ixuwvxxqxf p { margin: 0; padding: 0; }
 #ixuwvxxqxf .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ixuwvxxqxf .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ixuwvxxqxf .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ixuwvxxqxf .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ixuwvxxqxf .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ixuwvxxqxf .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ixuwvxxqxf .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ixuwvxxqxf .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ixuwvxxqxf .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ixuwvxxqxf .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ixuwvxxqxf .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ixuwvxxqxf .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ixuwvxxqxf .gt_spanner_row { border-bottom-style: hidden; }
 #ixuwvxxqxf .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ixuwvxxqxf .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ixuwvxxqxf .gt_from_md> :first-child { margin-top: 0; }
 #ixuwvxxqxf .gt_from_md> :last-child { margin-bottom: 0; }
 #ixuwvxxqxf .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ixuwvxxqxf .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ixuwvxxqxf .gt_indent_1 { text-indent: 5px; }
 #ixuwvxxqxf .gt_indent_2 { text-indent: calc(5px * 2); }
 #ixuwvxxqxf .gt_indent_3 { text-indent: calc(5px * 3); }
 #ixuwvxxqxf .gt_indent_4 { text-indent: calc(5px * 4); }
 #ixuwvxxqxf .gt_indent_5 { text-indent: calc(5px * 5); }
 #ixuwvxxqxf .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ixuwvxxqxf .gt_row_group_first td { border-top-width: 2px; }
 #ixuwvxxqxf .gt_row_group_first th { border-top-width: 2px; }
 #ixuwvxxqxf .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ixuwvxxqxf .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ixuwvxxqxf .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ixuwvxxqxf .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ixuwvxxqxf .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ixuwvxxqxf .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ixuwvxxqxf .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ixuwvxxqxf .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ixuwvxxqxf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ixuwvxxqxf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ixuwvxxqxf .gt_left { text-align: left; }
 #ixuwvxxqxf .gt_center { text-align: center; }
 #ixuwvxxqxf .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ixuwvxxqxf .gt_font_normal { font-weight: normal; }
 #ixuwvxxqxf .gt_font_bold { font-weight: bold; }
 #ixuwvxxqxf .gt_font_italic { font-style: italic; }
 #ixuwvxxqxf .gt_super { font-size: 65%; }
 #ixuwvxxqxf .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ixuwvxxqxf .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ixuwvxxqxf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ixuwvxxqxf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ixuwvxxqxf .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ixuwvxxqxf .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="tnbzhlwnho" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#tnbzhlwnho table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#tnbzhlwnho thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#tnbzhlwnho p { margin: 0; padding: 0; }
 #tnbzhlwnho .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #tnbzhlwnho .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #tnbzhlwnho .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #tnbzhlwnho .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #tnbzhlwnho .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #tnbzhlwnho .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnbzhlwnho .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #tnbzhlwnho .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #tnbzhlwnho .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #tnbzhlwnho .gt_column_spanner_outer:first-child { padding-left: 0; }
 #tnbzhlwnho .gt_column_spanner_outer:last-child { padding-right: 0; }
 #tnbzhlwnho .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #tnbzhlwnho .gt_spanner_row { border-bottom-style: hidden; }
 #tnbzhlwnho .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #tnbzhlwnho .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #tnbzhlwnho .gt_from_md> :first-child { margin-top: 0; }
 #tnbzhlwnho .gt_from_md> :last-child { margin-bottom: 0; }
 #tnbzhlwnho .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #tnbzhlwnho .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #tnbzhlwnho .gt_indent_1 { text-indent: 5px; }
 #tnbzhlwnho .gt_indent_2 { text-indent: calc(5px * 2); }
 #tnbzhlwnho .gt_indent_3 { text-indent: calc(5px * 3); }
 #tnbzhlwnho .gt_indent_4 { text-indent: calc(5px * 4); }
 #tnbzhlwnho .gt_indent_5 { text-indent: calc(5px * 5); }
 #tnbzhlwnho .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #tnbzhlwnho .gt_row_group_first td { border-top-width: 2px; }
 #tnbzhlwnho .gt_row_group_first th { border-top-width: 2px; }
 #tnbzhlwnho .gt_striped { color: #333333; background-color: #F4F4F4; }
 #tnbzhlwnho .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnbzhlwnho .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #tnbzhlwnho .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #tnbzhlwnho .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnbzhlwnho .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #tnbzhlwnho .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #tnbzhlwnho .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #tnbzhlwnho .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnbzhlwnho .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #tnbzhlwnho .gt_left { text-align: left; }
 #tnbzhlwnho .gt_center { text-align: center; }
 #tnbzhlwnho .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #tnbzhlwnho .gt_font_normal { font-weight: normal; }
 #tnbzhlwnho .gt_font_bold { font-weight: bold; }
 #tnbzhlwnho .gt_font_italic { font-style: italic; }
 #tnbzhlwnho .gt_super { font-size: 65%; }
 #tnbzhlwnho .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnbzhlwnho .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #tnbzhlwnho .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnbzhlwnho .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #tnbzhlwnho .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #tnbzhlwnho .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="abahgvoklc" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#abahgvoklc table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#abahgvoklc thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#abahgvoklc p { margin: 0; padding: 0; }
 #abahgvoklc .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #abahgvoklc .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #abahgvoklc .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #abahgvoklc .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #abahgvoklc .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #abahgvoklc .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #abahgvoklc .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #abahgvoklc .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #abahgvoklc .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #abahgvoklc .gt_column_spanner_outer:first-child { padding-left: 0; }
 #abahgvoklc .gt_column_spanner_outer:last-child { padding-right: 0; }
 #abahgvoklc .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #abahgvoklc .gt_spanner_row { border-bottom-style: hidden; }
 #abahgvoklc .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #abahgvoklc .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #abahgvoklc .gt_from_md> :first-child { margin-top: 0; }
 #abahgvoklc .gt_from_md> :last-child { margin-bottom: 0; }
 #abahgvoklc .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #abahgvoklc .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #abahgvoklc .gt_indent_1 { text-indent: 5px; }
 #abahgvoklc .gt_indent_2 { text-indent: calc(5px * 2); }
 #abahgvoklc .gt_indent_3 { text-indent: calc(5px * 3); }
 #abahgvoklc .gt_indent_4 { text-indent: calc(5px * 4); }
 #abahgvoklc .gt_indent_5 { text-indent: calc(5px * 5); }
 #abahgvoklc .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #abahgvoklc .gt_row_group_first td { border-top-width: 2px; }
 #abahgvoklc .gt_row_group_first th { border-top-width: 2px; }
 #abahgvoklc .gt_striped { color: #333333; background-color: #F4F4F4; }
 #abahgvoklc .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #abahgvoklc .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #abahgvoklc .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #abahgvoklc .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #abahgvoklc .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #abahgvoklc .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #abahgvoklc .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #abahgvoklc .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #abahgvoklc .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #abahgvoklc .gt_left { text-align: left; }
 #abahgvoklc .gt_center { text-align: center; }
 #abahgvoklc .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #abahgvoklc .gt_font_normal { font-weight: normal; }
 #abahgvoklc .gt_font_bold { font-weight: bold; }
 #abahgvoklc .gt_font_italic { font-style: italic; }
 #abahgvoklc .gt_super { font-size: 65%; }
 #abahgvoklc .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #abahgvoklc .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #abahgvoklc .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #abahgvoklc .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #abahgvoklc .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #abahgvoklc .gt_asterisk { font-size: 100%; vertical-align: 0; }
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
- **Resolved (2026-09-02, PR \#14):** leverage index is published as
  `mlb_leverage_index`, on the WE table’s own state-bucket key, carrying
  `n` and `thin`. Validated on real 2024 play-by-play: PA-weighted mean
  LI 1.0000, late-and-close states 1.52 against 0.80 for early lopsided
  ones.
- **Resolved (2026-09-02, PR \#14):** the WE and leverage tables carry
  the bucket count `n` and a `thin` flag (`n < 50`), so 2020’s short
  season is marked in the data. The threshold is measured —
  adjacent-season WE disagreement is .0586 at n=25-50 against .0296 at
  n\>=200 — and 2020 shows 93.1% thin buckets (median n=5) against
  ~81.5% (median n=11) in a full season. Both are recomputed at render
  time above.
