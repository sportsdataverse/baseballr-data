# MLB hitting models — expected stats, xHR, batter projection


Three surfaces ship on the `mlb_hitting_models` release tag: **expected
stats** (xwOBA / xBA / xSLG from Statcast contact quality), **expected
home runs** (neutral and park-adjusted xHR from launch conditions), and
a **batter projection** whose fit for season S trains only on seasons \<
S — the as-of discipline is enforced by construction, so the projection
can be forward-validated honestly, which this document does below.

The modeling idea is the standard one stated plainly: replace outcome
with contact quality. A batter controls exit velocity, launch angle and
spray far more than they control where fielders stand or which park they
hit in, so expected stats strip defense, park and sequencing luck out of
the batter’s ledger. The builders live in sdv-py (`mlb_expected_stats`,
`mlb_expected_home_runs`, `mlb_batter_projection`) over Baseball Savant
batted-ball data; this repository commits the per-season outputs under
`mlb/hitting_models/` and publishes them daily in-season. Everything
below is computed at render time from those committed files.

## Training data

<div id="nfluiiayqk" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#nfluiiayqk table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#nfluiiayqk thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#nfluiiayqk p { margin: 0; padding: 0; }
 #nfluiiayqk .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #nfluiiayqk .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #nfluiiayqk .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #nfluiiayqk .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #nfluiiayqk .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nfluiiayqk .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nfluiiayqk .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nfluiiayqk .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #nfluiiayqk .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #nfluiiayqk .gt_column_spanner_outer:first-child { padding-left: 0; }
 #nfluiiayqk .gt_column_spanner_outer:last-child { padding-right: 0; }
 #nfluiiayqk .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #nfluiiayqk .gt_spanner_row { border-bottom-style: hidden; }
 #nfluiiayqk .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #nfluiiayqk .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #nfluiiayqk .gt_from_md> :first-child { margin-top: 0; }
 #nfluiiayqk .gt_from_md> :last-child { margin-bottom: 0; }
 #nfluiiayqk .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #nfluiiayqk .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #nfluiiayqk .gt_indent_1 { text-indent: 5px; }
 #nfluiiayqk .gt_indent_2 { text-indent: calc(5px * 2); }
 #nfluiiayqk .gt_indent_3 { text-indent: calc(5px * 3); }
 #nfluiiayqk .gt_indent_4 { text-indent: calc(5px * 4); }
 #nfluiiayqk .gt_indent_5 { text-indent: calc(5px * 5); }
 #nfluiiayqk .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #nfluiiayqk .gt_row_group_first td { border-top-width: 2px; }
 #nfluiiayqk .gt_row_group_first th { border-top-width: 2px; }
 #nfluiiayqk .gt_striped { color: #333333; background-color: #F4F4F4; }
 #nfluiiayqk .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nfluiiayqk .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nfluiiayqk .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #nfluiiayqk .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nfluiiayqk .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nfluiiayqk .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #nfluiiayqk .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #nfluiiayqk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nfluiiayqk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nfluiiayqk .gt_left { text-align: left; }
 #nfluiiayqk .gt_center { text-align: center; }
 #nfluiiayqk .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #nfluiiayqk .gt_font_normal { font-weight: normal; }
 #nfluiiayqk .gt_font_bold { font-weight: bold; }
 #nfluiiayqk .gt_font_italic { font-style: italic; }
 #nfluiiayqk .gt_super { font-size: 65%; }
 #nfluiiayqk .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nfluiiayqk .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #nfluiiayqk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nfluiiayqk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nfluiiayqk .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #nfluiiayqk .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed hitting-model assets, by season |  |  |  |  |
|----|----|----|----|----|
| from mlb/hitting_models/parquet/; computed at render time |  |  |  |  |
| season | batters_xstats | total_pa | batters_xhr | batters_projected |
| 2015 | 2040 | 795,019 | 947 | <na> |
| 2016 | 2050 | 809,743 | 1021 | 2035.0 |
| 2017 | 2054 | 828,584 | 1071 | 2688.0 |
| 2018 | 2111 | 811,193 | 1076 | 3238.0 |
| 2019 | 2097 | 821,241 | 1088 | 3295.0 |
| 2020 | 1470 | 336,648 | 806 | 3342.0 |
| 2021 | 1416 | 802,587 | 1188 | 3031.0 |
| 2022 | 1655 | 775,629 | 1091 | 2672.0 |
| 2023 | 1820 | 834,954 | 1319 | 2601.0 |
| 2024 | 1777 | 826,758 | 1280 | 2766.0 |
| 2025 | 1752 | 868,045 | 1489 | 2659.0 |
| 2026 | 1930 | 746,898 | 1781 | 2731.0 |

&#10;</div>

## Exploratory data analysis

<img src="hitting_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="xwOBA distribution among qualified batters (PA ≥ 100), latest season." />

<img src="hitting_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="xBA vs xSLG, qualified batters — the power axis separates from the contact axis." />

## Known anomalies, reproduced live

**Root cause identified and fixed upstream (2026-09-01).** The published
`pa`/`ab` columns counted **every pitch** rather than
plate-appearance-ending events (the assets carry `max_pa` ≈ 3,400 per
batter-season), deflating xBA/xSLG; and on cache vintages with
degenerate `woba_value`/`woba_denom` semantics the xwOBA scale corrupted
per season. The fix — PA-ender discipline plus an events-derived wOBA
denominator — landed in sdv-py (`fix/mlb-expected-stats-pa-enders`), and
the publisher now carries a publish-blocking absolute scale gate
(qualified league-mean xwOBA .26–.38, xBA .18–.30). **The tables below
keep showing the corrupted published assets until the full-history
republish runs** — the flags stay live by design so this page proves the
republish when it happens.

**Scale drift in the published xwOBA.** League-mean xwOBA should sit
near .320 in every season; the committed assets do not. This table
recomputes the per-season mean on every render and flags seasons outside
a generous .280–.360 band — several seasons fail, which corrupts every
cross-column and cross-season comparison below and is the leading
suspect for the other two flags in this document:

<div id="nruxnxpxiy" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#nruxnxpxiy table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#nruxnxpxiy thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#nruxnxpxiy p { margin: 0; padding: 0; }
 #nruxnxpxiy .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #nruxnxpxiy .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #nruxnxpxiy .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #nruxnxpxiy .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #nruxnxpxiy .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nruxnxpxiy .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nruxnxpxiy .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nruxnxpxiy .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #nruxnxpxiy .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #nruxnxpxiy .gt_column_spanner_outer:first-child { padding-left: 0; }
 #nruxnxpxiy .gt_column_spanner_outer:last-child { padding-right: 0; }
 #nruxnxpxiy .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #nruxnxpxiy .gt_spanner_row { border-bottom-style: hidden; }
 #nruxnxpxiy .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #nruxnxpxiy .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #nruxnxpxiy .gt_from_md> :first-child { margin-top: 0; }
 #nruxnxpxiy .gt_from_md> :last-child { margin-bottom: 0; }
 #nruxnxpxiy .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #nruxnxpxiy .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #nruxnxpxiy .gt_indent_1 { text-indent: 5px; }
 #nruxnxpxiy .gt_indent_2 { text-indent: calc(5px * 2); }
 #nruxnxpxiy .gt_indent_3 { text-indent: calc(5px * 3); }
 #nruxnxpxiy .gt_indent_4 { text-indent: calc(5px * 4); }
 #nruxnxpxiy .gt_indent_5 { text-indent: calc(5px * 5); }
 #nruxnxpxiy .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #nruxnxpxiy .gt_row_group_first td { border-top-width: 2px; }
 #nruxnxpxiy .gt_row_group_first th { border-top-width: 2px; }
 #nruxnxpxiy .gt_striped { color: #333333; background-color: #F4F4F4; }
 #nruxnxpxiy .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nruxnxpxiy .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nruxnxpxiy .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #nruxnxpxiy .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nruxnxpxiy .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nruxnxpxiy .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #nruxnxpxiy .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #nruxnxpxiy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nruxnxpxiy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nruxnxpxiy .gt_left { text-align: left; }
 #nruxnxpxiy .gt_center { text-align: center; }
 #nruxnxpxiy .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #nruxnxpxiy .gt_font_normal { font-weight: normal; }
 #nruxnxpxiy .gt_font_bold { font-weight: bold; }
 #nruxnxpxiy .gt_font_italic { font-style: italic; }
 #nruxnxpxiy .gt_super { font-size: 65%; }
 #nruxnxpxiy .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nruxnxpxiy .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #nruxnxpxiy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nruxnxpxiy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nruxnxpxiy .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #nruxnxpxiy .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| FLAGGED: league-mean xwOBA by season vs plausible band (.280–.360) |  |  |  |  |
|----|----|----|----|----|
| means of .44–.73 are impossible on the wOBA scale — the builder mis-scales some seasons |  |  |  |  |
| season | mean_xwoba | mean_xba | n | OUT_OF_BAND |
| 2015 | 0.534 | 0.047 | 682 | True |
| 2016 | 0.672 | 0.046 | 700 | True |
| 2017 | 0.724 | 0.045 | 720 | True |
| 2018 | 0.632 | 0.047 | 695 | True |
| 2019 | 0.556 | 0.048 | 691 | True |
| 2020 | 0.444 | 0.049 | 523 | True |
| 2021 | 0.389 | 0.049 | 699 | True |
| 2022 | 0.345 | 0.052 | 622 | False |
| 2023 | 0.466 | 0.051 | 694 | True |
| 2024 | 0.481 | 0.050 | 655 | True |
| 2025 | 0.448 | 0.049 | 734 | True |
| 2026 | 0.387 | 0.055 | 768 | True |

&#10;</div>

The 2026-09-01 audit flagged that **xwOBA and xBA correlate negatively**
on the published asset — two expected-stat columns that should agree in
sign. This document recomputes that correlation on every render so the
anomaly stays visible until the builder is fixed, rather than being
quietly forgotten:

<div id="remeueuhda" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#remeueuhda table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#remeueuhda thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#remeueuhda p { margin: 0; padding: 0; }
 #remeueuhda .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #remeueuhda .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #remeueuhda .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #remeueuhda .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #remeueuhda .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #remeueuhda .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #remeueuhda .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #remeueuhda .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #remeueuhda .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #remeueuhda .gt_column_spanner_outer:first-child { padding-left: 0; }
 #remeueuhda .gt_column_spanner_outer:last-child { padding-right: 0; }
 #remeueuhda .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #remeueuhda .gt_spanner_row { border-bottom-style: hidden; }
 #remeueuhda .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #remeueuhda .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #remeueuhda .gt_from_md> :first-child { margin-top: 0; }
 #remeueuhda .gt_from_md> :last-child { margin-bottom: 0; }
 #remeueuhda .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #remeueuhda .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #remeueuhda .gt_indent_1 { text-indent: 5px; }
 #remeueuhda .gt_indent_2 { text-indent: calc(5px * 2); }
 #remeueuhda .gt_indent_3 { text-indent: calc(5px * 3); }
 #remeueuhda .gt_indent_4 { text-indent: calc(5px * 4); }
 #remeueuhda .gt_indent_5 { text-indent: calc(5px * 5); }
 #remeueuhda .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #remeueuhda .gt_row_group_first td { border-top-width: 2px; }
 #remeueuhda .gt_row_group_first th { border-top-width: 2px; }
 #remeueuhda .gt_striped { color: #333333; background-color: #F4F4F4; }
 #remeueuhda .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #remeueuhda .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #remeueuhda .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #remeueuhda .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #remeueuhda .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #remeueuhda .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #remeueuhda .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #remeueuhda .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #remeueuhda .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #remeueuhda .gt_left { text-align: left; }
 #remeueuhda .gt_center { text-align: center; }
 #remeueuhda .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #remeueuhda .gt_font_normal { font-weight: normal; }
 #remeueuhda .gt_font_bold { font-weight: bold; }
 #remeueuhda .gt_font_italic { font-style: italic; }
 #remeueuhda .gt_super { font-size: 65%; }
 #remeueuhda .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #remeueuhda .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #remeueuhda .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #remeueuhda .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #remeueuhda .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #remeueuhda .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| FLAGGED: Pearson r between xwOBA and xBA (PA ≥ 100), by season |  |  |
|----|----|----|
| expected-stat columns should correlate strongly positive; investigate builder scaling/join before trusting cross-column comparisons |  |  |
| season | pearson_xwoba_xba | n |
| 2015 | −0.330 | 682 |
| 2016 | −0.430 | 700 |
| 2017 | −0.415 | 720 |
| 2018 | −0.313 | 695 |
| 2019 | −0.320 | 691 |
| 2020 | −0.361 | 523 |
| 2021 | −0.195 | 699 |
| 2022 | −0.033 | 622 |
| 2023 | −0.453 | 694 |
| 2024 | −0.392 | 655 |
| 2025 | −0.467 | 734 |
| 2026 | −0.136 | 768 |

&#10;</div>

## Expected home runs

<img src="hitting_files/figure-commonmark/cell-8-output-1.png"
width="420" height="300"
alt="Observed HR vs park-adjusted xHR — the diagonal is neutral luck." />

<div id="sjzpfpwdox" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#sjzpfpwdox table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#sjzpfpwdox thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#sjzpfpwdox p { margin: 0; padding: 0; }
 #sjzpfpwdox .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #sjzpfpwdox .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #sjzpfpwdox .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #sjzpfpwdox .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #sjzpfpwdox .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #sjzpfpwdox .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #sjzpfpwdox .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #sjzpfpwdox .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #sjzpfpwdox .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #sjzpfpwdox .gt_column_spanner_outer:first-child { padding-left: 0; }
 #sjzpfpwdox .gt_column_spanner_outer:last-child { padding-right: 0; }
 #sjzpfpwdox .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #sjzpfpwdox .gt_spanner_row { border-bottom-style: hidden; }
 #sjzpfpwdox .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #sjzpfpwdox .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #sjzpfpwdox .gt_from_md> :first-child { margin-top: 0; }
 #sjzpfpwdox .gt_from_md> :last-child { margin-bottom: 0; }
 #sjzpfpwdox .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #sjzpfpwdox .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #sjzpfpwdox .gt_indent_1 { text-indent: 5px; }
 #sjzpfpwdox .gt_indent_2 { text-indent: calc(5px * 2); }
 #sjzpfpwdox .gt_indent_3 { text-indent: calc(5px * 3); }
 #sjzpfpwdox .gt_indent_4 { text-indent: calc(5px * 4); }
 #sjzpfpwdox .gt_indent_5 { text-indent: calc(5px * 5); }
 #sjzpfpwdox .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #sjzpfpwdox .gt_row_group_first td { border-top-width: 2px; }
 #sjzpfpwdox .gt_row_group_first th { border-top-width: 2px; }
 #sjzpfpwdox .gt_striped { color: #333333; background-color: #F4F4F4; }
 #sjzpfpwdox .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #sjzpfpwdox .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #sjzpfpwdox .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #sjzpfpwdox .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #sjzpfpwdox .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #sjzpfpwdox .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #sjzpfpwdox .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #sjzpfpwdox .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #sjzpfpwdox .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #sjzpfpwdox .gt_left { text-align: left; }
 #sjzpfpwdox .gt_center { text-align: center; }
 #sjzpfpwdox .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #sjzpfpwdox .gt_font_normal { font-weight: normal; }
 #sjzpfpwdox .gt_font_bold { font-weight: bold; }
 #sjzpfpwdox .gt_font_italic { font-style: italic; }
 #sjzpfpwdox .gt_super { font-size: 65%; }
 #sjzpfpwdox .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #sjzpfpwdox .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #sjzpfpwdox .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #sjzpfpwdox .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #sjzpfpwdox .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #sjzpfpwdox .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Largest HR over-performers vs xHR — 2026 |  |  |  |  |  |
|----|----|----|----|----|----|
| hr_above_expected = observed HR − park-adjusted xHR |  |  |  |  |  |
|  | Player | HR | xHR (neutral) | xHR (park) | HR − xHR |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/696100/headshot/67/current"
height="42" /> | Hunter Goodman | 38 | 29.5 | 30.8 | 8.5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/592518/headshot/67/current"
height="42" /> | Manny Machado | 27 | 19.3 | 19.8 | 7.7 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/701398/headshot/67/current"
height="42" /> | Sal Stewart | 35 | 27.6 | 29.2 | 7.4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/691406/headshot/67/current"
height="42" /> | Junior Caminero | 37 | 30.1 | 30.9 | 6.9 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/605141/headshot/67/current"
height="42" /> | Mookie Betts | 17 | 10.8 | 12.2 | 6.2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/592626/headshot/67/current"
height="42" /> | Joc Pederson | 24 | 18.2 | 17.5 | 5.8 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/621020/headshot/67/current"
height="42" /> | Dansby Swanson | 21 | 15.2 | 16.1 | 5.8 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/694192/headshot/67/current"
height="42" /> | Jackson Chourio | 21 | 15.3 | 15.1 | 5.7 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/678882/headshot/67/current"
height="42" /> | Ceddanne Rafaela | 21 | 15.5 | 15.5 | 5.5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/553993/headshot/67/current"
height="42" /> | Eugenio Suárez | 20 | 14.6 | 15.2 | 5.4 |

&#10;</div>

## Evaluation — forward validation of the batter projection

Because the projection for season S trains only on seasons \< S, joining
each projection to the *realized* xwOBA of the same batter in season S
is a true out-of-sample test. Computed across every committed pair of
adjacent seasons:

<div id="icrpoqddbx" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#icrpoqddbx table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#icrpoqddbx thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#icrpoqddbx p { margin: 0; padding: 0; }
 #icrpoqddbx .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #icrpoqddbx .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #icrpoqddbx .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #icrpoqddbx .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #icrpoqddbx .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #icrpoqddbx .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #icrpoqddbx .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #icrpoqddbx .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #icrpoqddbx .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #icrpoqddbx .gt_column_spanner_outer:first-child { padding-left: 0; }
 #icrpoqddbx .gt_column_spanner_outer:last-child { padding-right: 0; }
 #icrpoqddbx .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #icrpoqddbx .gt_spanner_row { border-bottom-style: hidden; }
 #icrpoqddbx .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #icrpoqddbx .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #icrpoqddbx .gt_from_md> :first-child { margin-top: 0; }
 #icrpoqddbx .gt_from_md> :last-child { margin-bottom: 0; }
 #icrpoqddbx .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #icrpoqddbx .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #icrpoqddbx .gt_indent_1 { text-indent: 5px; }
 #icrpoqddbx .gt_indent_2 { text-indent: calc(5px * 2); }
 #icrpoqddbx .gt_indent_3 { text-indent: calc(5px * 3); }
 #icrpoqddbx .gt_indent_4 { text-indent: calc(5px * 4); }
 #icrpoqddbx .gt_indent_5 { text-indent: calc(5px * 5); }
 #icrpoqddbx .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #icrpoqddbx .gt_row_group_first td { border-top-width: 2px; }
 #icrpoqddbx .gt_row_group_first th { border-top-width: 2px; }
 #icrpoqddbx .gt_striped { color: #333333; background-color: #F4F4F4; }
 #icrpoqddbx .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #icrpoqddbx .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #icrpoqddbx .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #icrpoqddbx .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #icrpoqddbx .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #icrpoqddbx .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #icrpoqddbx .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #icrpoqddbx .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #icrpoqddbx .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #icrpoqddbx .gt_left { text-align: left; }
 #icrpoqddbx .gt_center { text-align: center; }
 #icrpoqddbx .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #icrpoqddbx .gt_font_normal { font-weight: normal; }
 #icrpoqddbx .gt_font_bold { font-weight: bold; }
 #icrpoqddbx .gt_font_italic { font-style: italic; }
 #icrpoqddbx .gt_super { font-size: 65%; }
 #icrpoqddbx .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #icrpoqddbx .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #icrpoqddbx .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #icrpoqddbx .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #icrpoqddbx .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #icrpoqddbx .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Projection forward validation — proj_xwoba vs realized xwOBA (PA ≥ 100) |  |  |  |
|----|----|----|----|
| the projection for season S trained only on \< S; this join is out-of-sample by construction |  |  |  |
| season | pearson | MAE | batters |
| 2016 | 0.084 | 0.368 | 662 |
| 2017 | 0.090 | 0.427 | 700 |
| 2018 | 0.023 | 0.337 | 682 |
| 2019 | 0.043 | 0.319 | 678 |
| 2020 | 0.083 | 0.118 | 521 |
| 2021 | 0.094 | 0.189 | 687 |
| 2022 | 0.107 | 0.119 | 601 |
| 2023 | 0.066 | 0.156 | 678 |
| 2024 | 0.098 | 0.178 | 647 |
| 2025 | 0.105 | 0.160 | 722 |
| 2026 | 0.198 | 0.099 | 757 |
| pooled | 0.053 | 0.226 | 7335 |

&#10;</div>

<img src="hitting_files/figure-commonmark/cell-11-output-1.png"
width="420" height="300"
alt="Projected vs realized xwOBA, all forward-validated seasons pooled." />

A sane aging-curve projection lands in the 0.5–0.7 correlation range
with next-season xwOBA; the observed values in the table above are **far
below that**, hovering near zero. Given the scale-drift flag reproduced
earlier — realized league-mean “xwOBA” of .44–.73 in several seasons —
the honest reading is that the *published expected-stats columns are
corrupted*, not that the projection carries no signal: a forward
validation against a mis-scaled target cannot exonerate or condemn the
model. The builder fix comes first; this table then becomes the
projection’s real gate. The publish gates additionally anchor the
expected stats themselves: xwOBA/xBA Spearman vs Savant’s own published
expected stats ≥ 0.95 on identical inputs, and xHR full-season Spearman
vs live ≥ 0.90 — gates that plainly did not catch the scale drift, which
is itself a finding about the gates.

## Provenance & reproducibility

- **Trained on:** Baseball Savant batted-ball data (exit velocity,
  launch angle, spray), seasons in the table above; the projection for
  season S uses seasons \< S only.
- **Committed at:** `mlb/hitting_models/parquet/`; published to
  `mlb_hitting_models`; per-publish metadata in
  [`../../mlb/hitting_models/mlb_hitting_models_card.json`](../../mlb/hitting_models/mlb_hitting_models_card.json).
- **Pipeline:** `scripts/mlb_models.sh 02` → stage
  `python/mlb_model_02_hitting.py` (`mlb_models_cron.yml`). Single home:
  `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Player names/headshots resolve via one
  batched statsapi call; offline renders fall back to MLBAM ids.

## Avenues for improvement & open issues

- **Publish observed stats alongside** — the asset carries expected
  stats only; adding observed wOBA/BA would make luck-vs-skill deltas a
  one-liner for consumers (today it needs a second source).
- **Known issue:** the projection’s validated window starts at 2018;
  earlier seasons ship but sit outside the gates.
- **FLAGGED ANOMALY (2026-09-01, reproduced live above):** xwOBA vs xBA
  on the published assets correlates NEGATIVELY — expected-stat columns
  should agree in sign; investigate column scaling / join keys in the
  builder before trusting cross-column comparisons. This document
  recomputes the correlation on every render so the flag cannot silently
  go stale.
- **FLAGGED ANOMALY (2026-09-01, reproduced live above):** league-mean
  “xwOBA” of .44–.73 in several published seasons is impossible on the
  wOBA scale — per-season scale drift in the builder is the leading
  suspect for BOTH the negative xwOBA↔xBA correlation and the near-zero
  projection forward validation. The publish gates (Spearman vs Savant
  on identical inputs) are rank-based and therefore scale-blind — add an
  absolute league-mean band gate so this class cannot ship again.
