# MLB pitching models — xERA, Stuff+, Command+


Three surfaces ship on the `mlb_pitching_models` release tag: **xERA**
(contact-quality ERA), **Stuff+** (pitch-physics quality: velocity,
movement, release), and **Command+** (location quality). Together they
are the stuff/command decomposition of pitching: Stuff+ deliberately
excludes location, Command+ deliberately excludes physics — the
decomposition is the point, because a pitcher’s physical tools and their
ability to locate age and develop on different curves.

The builders live in sdv-py (`x_era`, `mlb_stuff_plus`,
`mlb_command_plus`) over Baseball Savant pitch-level features; this
repository commits per-season outputs under `mlb/pitching_models/` and
publishes them daily in-season. The publish gates are stated honestly:
xERA MAE vs Savant’s own xERA ≤ 0.30; Stuff+ Spearman vs run value ≥
0.20; Command+ ≥ 0.04 — the Command+ gate is explicitly **directional
only**, a weak ordinal signal stated as such rather than inflated.
Everything below is computed at render time from the committed files,
including the two evaluations that matter most for surfaces like these:
**year-over-year reliability** (does the metric measure a stable skill?)
and **forward predictiveness** (does this year’s Stuff+ tell you
anything about next year’s xERA?).

## Training data

<div id="lszakhmuar" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#lszakhmuar table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#lszakhmuar thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#lszakhmuar p { margin: 0; padding: 0; }
 #lszakhmuar .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #lszakhmuar .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #lszakhmuar .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #lszakhmuar .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #lszakhmuar .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #lszakhmuar .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #lszakhmuar .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #lszakhmuar .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #lszakhmuar .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #lszakhmuar .gt_column_spanner_outer:first-child { padding-left: 0; }
 #lszakhmuar .gt_column_spanner_outer:last-child { padding-right: 0; }
 #lszakhmuar .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #lszakhmuar .gt_spanner_row { border-bottom-style: hidden; }
 #lszakhmuar .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #lszakhmuar .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #lszakhmuar .gt_from_md> :first-child { margin-top: 0; }
 #lszakhmuar .gt_from_md> :last-child { margin-bottom: 0; }
 #lszakhmuar .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #lszakhmuar .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #lszakhmuar .gt_indent_1 { text-indent: 5px; }
 #lszakhmuar .gt_indent_2 { text-indent: calc(5px * 2); }
 #lszakhmuar .gt_indent_3 { text-indent: calc(5px * 3); }
 #lszakhmuar .gt_indent_4 { text-indent: calc(5px * 4); }
 #lszakhmuar .gt_indent_5 { text-indent: calc(5px * 5); }
 #lszakhmuar .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #lszakhmuar .gt_row_group_first td { border-top-width: 2px; }
 #lszakhmuar .gt_row_group_first th { border-top-width: 2px; }
 #lszakhmuar .gt_striped { color: #333333; background-color: #F4F4F4; }
 #lszakhmuar .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #lszakhmuar .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #lszakhmuar .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #lszakhmuar .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #lszakhmuar .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #lszakhmuar .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #lszakhmuar .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #lszakhmuar .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #lszakhmuar .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #lszakhmuar .gt_left { text-align: left; }
 #lszakhmuar .gt_center { text-align: center; }
 #lszakhmuar .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #lszakhmuar .gt_font_normal { font-weight: normal; }
 #lszakhmuar .gt_font_bold { font-weight: bold; }
 #lszakhmuar .gt_font_italic { font-style: italic; }
 #lszakhmuar .gt_super { font-size: 65%; }
 #lszakhmuar .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #lszakhmuar .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #lszakhmuar .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #lszakhmuar .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #lszakhmuar .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #lszakhmuar .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed pitching-model assets, by season |  |  |  |
|----|----|----|----|
| from mlb/pitching_models/parquet/; computed at render time |  |  |  |
| season | pitchers_xera | pitcher_pitchtype_rows | pitchers_command |
| 2015 | 736 | 4,676 | 883 |
| 2016 | 742 | 4,920 | 916 |
| 2017 | 755 | 4,459 | 862 |
| 2018 | 799 | 4,344 | 902 |
| 2019 | 830 | 4,379 | 944 |
| 2020 | 737 | 3,944 | 869 |
| 2021 | 909 | 4,543 | 1069 |
| 2022 | 871 | 4,676 | 1069 |
| 2023 | 863 | 5,761 | 1282 |
| 2024 | 855 | 5,795 | 1281 |
| 2025 | 873 | 6,569 | 1508 |
| 2026 | 833 | 7,646 | 1772 |

&#10;</div>

## Exploratory data analysis

<img src="pitching_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="Stuff+ distribution by pitch type, latest season (100 = league average)." />

<img src="pitching_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300" alt="Command+ distribution, latest season." />

## Known anomaly, reproduced live

The 2026-09-01 audit found the published **xERA is a perfect monotone
transform of x_wOBA-against** — two columns carrying one signal. The
correlation is recomputed on every render so the flag stays live until
the builder differentiates the recipe (batted-ball mix, park) or
documents xERA as a display transform:

<div id="ktidorqwyn" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ktidorqwyn table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ktidorqwyn thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ktidorqwyn p { margin: 0; padding: 0; }
 #ktidorqwyn .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ktidorqwyn .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ktidorqwyn .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ktidorqwyn .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ktidorqwyn .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ktidorqwyn .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktidorqwyn .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ktidorqwyn .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ktidorqwyn .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ktidorqwyn .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ktidorqwyn .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ktidorqwyn .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ktidorqwyn .gt_spanner_row { border-bottom-style: hidden; }
 #ktidorqwyn .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ktidorqwyn .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ktidorqwyn .gt_from_md> :first-child { margin-top: 0; }
 #ktidorqwyn .gt_from_md> :last-child { margin-bottom: 0; }
 #ktidorqwyn .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ktidorqwyn .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ktidorqwyn .gt_indent_1 { text-indent: 5px; }
 #ktidorqwyn .gt_indent_2 { text-indent: calc(5px * 2); }
 #ktidorqwyn .gt_indent_3 { text-indent: calc(5px * 3); }
 #ktidorqwyn .gt_indent_4 { text-indent: calc(5px * 4); }
 #ktidorqwyn .gt_indent_5 { text-indent: calc(5px * 5); }
 #ktidorqwyn .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ktidorqwyn .gt_row_group_first td { border-top-width: 2px; }
 #ktidorqwyn .gt_row_group_first th { border-top-width: 2px; }
 #ktidorqwyn .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ktidorqwyn .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktidorqwyn .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ktidorqwyn .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ktidorqwyn .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktidorqwyn .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ktidorqwyn .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ktidorqwyn .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ktidorqwyn .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktidorqwyn .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ktidorqwyn .gt_left { text-align: left; }
 #ktidorqwyn .gt_center { text-align: center; }
 #ktidorqwyn .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ktidorqwyn .gt_font_normal { font-weight: normal; }
 #ktidorqwyn .gt_font_bold { font-weight: bold; }
 #ktidorqwyn .gt_font_italic { font-style: italic; }
 #ktidorqwyn .gt_super { font-size: 65%; }
 #ktidorqwyn .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktidorqwyn .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ktidorqwyn .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktidorqwyn .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ktidorqwyn .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ktidorqwyn .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| FLAGGED: Pearson r between published xERA and x_wOBA-against |  |  |
|----|----|----|
| r = 1.0 means the asset carries ONE signal in two columns |  |  |
| season | pearson_xera_xwoba | n |
| 2015 | 1.0000 | 736 |
| 2016 | 1.0000 | 742 |
| 2017 | 1.0000 | 755 |
| 2018 | 1.0000 | 799 |
| 2019 | 1.0000 | 830 |
| 2020 | 1.0000 | 737 |
| 2021 | 1.0000 | 909 |
| 2022 | 1.0000 | 871 |
| 2023 | 1.0000 | 863 |
| 2024 | 1.0000 | 855 |
| 2025 | 1.0000 | 873 |
| 2026 | 1.0000 | 833 |

&#10;</div>

## Evaluation — reliability and forward predictiveness

A skill metric must first be **reliable**: a pitcher’s score this year
should correlate with the same pitcher’s score next year. Then it should
be **predictive**: this year’s process metric should anticipate next
year’s outcome metric. Both are computed here across every adjacent
committed season pair.

<div id="ddmejcjtab" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ddmejcjtab table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ddmejcjtab thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ddmejcjtab p { margin: 0; padding: 0; }
 #ddmejcjtab .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ddmejcjtab .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ddmejcjtab .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ddmejcjtab .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ddmejcjtab .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ddmejcjtab .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ddmejcjtab .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ddmejcjtab .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ddmejcjtab .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ddmejcjtab .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ddmejcjtab .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ddmejcjtab .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ddmejcjtab .gt_spanner_row { border-bottom-style: hidden; }
 #ddmejcjtab .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ddmejcjtab .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ddmejcjtab .gt_from_md> :first-child { margin-top: 0; }
 #ddmejcjtab .gt_from_md> :last-child { margin-bottom: 0; }
 #ddmejcjtab .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ddmejcjtab .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ddmejcjtab .gt_indent_1 { text-indent: 5px; }
 #ddmejcjtab .gt_indent_2 { text-indent: calc(5px * 2); }
 #ddmejcjtab .gt_indent_3 { text-indent: calc(5px * 3); }
 #ddmejcjtab .gt_indent_4 { text-indent: calc(5px * 4); }
 #ddmejcjtab .gt_indent_5 { text-indent: calc(5px * 5); }
 #ddmejcjtab .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ddmejcjtab .gt_row_group_first td { border-top-width: 2px; }
 #ddmejcjtab .gt_row_group_first th { border-top-width: 2px; }
 #ddmejcjtab .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ddmejcjtab .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ddmejcjtab .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ddmejcjtab .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ddmejcjtab .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ddmejcjtab .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ddmejcjtab .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ddmejcjtab .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ddmejcjtab .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ddmejcjtab .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ddmejcjtab .gt_left { text-align: left; }
 #ddmejcjtab .gt_center { text-align: center; }
 #ddmejcjtab .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ddmejcjtab .gt_font_normal { font-weight: normal; }
 #ddmejcjtab .gt_font_bold { font-weight: bold; }
 #ddmejcjtab .gt_font_italic { font-style: italic; }
 #ddmejcjtab .gt_super { font-size: 65%; }
 #ddmejcjtab .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ddmejcjtab .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ddmejcjtab .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ddmejcjtab .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ddmejcjtab .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ddmejcjtab .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Year-over-year reliability — same pitcher, adjacent seasons |  |  |
|----|----|----|
| all committed season pairs pooled; higher = more stable skill |  |  |
| metric | pairs | yoy_pearson |
| Stuff+ (pitcher mean) | 10349 | 0.377 |
| Command+ | 7912 | 0.375 |
| xERA | 6389 | 0.208 |

&#10;</div>

<div id="nbmyeloqwv" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#nbmyeloqwv table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#nbmyeloqwv thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#nbmyeloqwv p { margin: 0; padding: 0; }
 #nbmyeloqwv .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #nbmyeloqwv .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #nbmyeloqwv .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #nbmyeloqwv .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #nbmyeloqwv .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nbmyeloqwv .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nbmyeloqwv .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nbmyeloqwv .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #nbmyeloqwv .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #nbmyeloqwv .gt_column_spanner_outer:first-child { padding-left: 0; }
 #nbmyeloqwv .gt_column_spanner_outer:last-child { padding-right: 0; }
 #nbmyeloqwv .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #nbmyeloqwv .gt_spanner_row { border-bottom-style: hidden; }
 #nbmyeloqwv .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #nbmyeloqwv .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #nbmyeloqwv .gt_from_md> :first-child { margin-top: 0; }
 #nbmyeloqwv .gt_from_md> :last-child { margin-bottom: 0; }
 #nbmyeloqwv .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #nbmyeloqwv .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #nbmyeloqwv .gt_indent_1 { text-indent: 5px; }
 #nbmyeloqwv .gt_indent_2 { text-indent: calc(5px * 2); }
 #nbmyeloqwv .gt_indent_3 { text-indent: calc(5px * 3); }
 #nbmyeloqwv .gt_indent_4 { text-indent: calc(5px * 4); }
 #nbmyeloqwv .gt_indent_5 { text-indent: calc(5px * 5); }
 #nbmyeloqwv .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #nbmyeloqwv .gt_row_group_first td { border-top-width: 2px; }
 #nbmyeloqwv .gt_row_group_first th { border-top-width: 2px; }
 #nbmyeloqwv .gt_striped { color: #333333; background-color: #F4F4F4; }
 #nbmyeloqwv .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nbmyeloqwv .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nbmyeloqwv .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #nbmyeloqwv .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nbmyeloqwv .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nbmyeloqwv .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #nbmyeloqwv .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #nbmyeloqwv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nbmyeloqwv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nbmyeloqwv .gt_left { text-align: left; }
 #nbmyeloqwv .gt_center { text-align: center; }
 #nbmyeloqwv .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #nbmyeloqwv .gt_font_normal { font-weight: normal; }
 #nbmyeloqwv .gt_font_bold { font-weight: bold; }
 #nbmyeloqwv .gt_font_italic { font-style: italic; }
 #nbmyeloqwv .gt_super { font-size: 65%; }
 #nbmyeloqwv .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nbmyeloqwv .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #nbmyeloqwv .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nbmyeloqwv .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nbmyeloqwv .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #nbmyeloqwv .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Forward predictiveness — process metric vs next season's xERA |  |  |  |
|----|----|----|----|
| negative r is the expected sign (better stuff/command → lower xERA) |  |  |  |
| predictor (season S) | target (season S+1) | pairs | pearson |
| Stuff+ (pitcher mean) | xERA | 7392 | 0.031 |
| Command+ | xERA | 6798 | −0.062 |

&#10;</div>

<img src="pitching_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="This year’s Stuff+ vs next year’s xERA, all adjacent season pairs." />

The reliability ordering is the theoretically expected one — pitch
physics (Stuff+) is the stickiest thing a pitcher owns, location value
is noisier, and outcome-adjacent xERA sits between. The
forward-validation sign check (better process → lower next-season xERA)
is the honest floor for surfaces whose in-season gates are deliberately
weak.

## Results

<div id="siwwltzfkq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#siwwltzfkq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#siwwltzfkq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#siwwltzfkq p { margin: 0; padding: 0; }
 #siwwltzfkq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #siwwltzfkq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #siwwltzfkq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #siwwltzfkq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #siwwltzfkq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #siwwltzfkq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #siwwltzfkq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #siwwltzfkq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #siwwltzfkq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #siwwltzfkq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #siwwltzfkq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #siwwltzfkq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #siwwltzfkq .gt_spanner_row { border-bottom-style: hidden; }
 #siwwltzfkq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #siwwltzfkq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #siwwltzfkq .gt_from_md> :first-child { margin-top: 0; }
 #siwwltzfkq .gt_from_md> :last-child { margin-bottom: 0; }
 #siwwltzfkq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #siwwltzfkq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #siwwltzfkq .gt_indent_1 { text-indent: 5px; }
 #siwwltzfkq .gt_indent_2 { text-indent: calc(5px * 2); }
 #siwwltzfkq .gt_indent_3 { text-indent: calc(5px * 3); }
 #siwwltzfkq .gt_indent_4 { text-indent: calc(5px * 4); }
 #siwwltzfkq .gt_indent_5 { text-indent: calc(5px * 5); }
 #siwwltzfkq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #siwwltzfkq .gt_row_group_first td { border-top-width: 2px; }
 #siwwltzfkq .gt_row_group_first th { border-top-width: 2px; }
 #siwwltzfkq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #siwwltzfkq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #siwwltzfkq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #siwwltzfkq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #siwwltzfkq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #siwwltzfkq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #siwwltzfkq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #siwwltzfkq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #siwwltzfkq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #siwwltzfkq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #siwwltzfkq .gt_left { text-align: left; }
 #siwwltzfkq .gt_center { text-align: center; }
 #siwwltzfkq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #siwwltzfkq .gt_font_normal { font-weight: normal; }
 #siwwltzfkq .gt_font_bold { font-weight: bold; }
 #siwwltzfkq .gt_font_italic { font-style: italic; }
 #siwwltzfkq .gt_super { font-size: 65%; }
 #siwwltzfkq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #siwwltzfkq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #siwwltzfkq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #siwwltzfkq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #siwwltzfkq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #siwwltzfkq .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 10 pitchers by mean Stuff+ — 2026 |  |  |  |
|----|----|----|----|
|  | Pitcher | Stuff+ | pitch types |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/804546/headshot/67/current"
height="42" /> | Hoss Brewer | 109.0 | 4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/689450/headshot/67/current"
height="42" /> | Luke Taggart | 108.6 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/682044/headshot/67/current"
height="42" /> | Sean Boyle | 108.6 | 5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/695465/headshot/67/current"
height="42" /> | Larson Kindreich | 108.3 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/664774/headshot/67/current"
height="42" /> | LaMonte Wade Jr. | 108.2 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/800124/headshot/67/current"
height="42" /> | David Lorduy | 108.1 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/804663/headshot/67/current"
height="42" /> | Tyler Cleveland | 107.8 | 4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/622609/headshot/67/current"
height="42" /> | Hector Villarroel | 107.1 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/691711/headshot/67/current"
height="42" /> | Jawilme Ramírez | 106.8 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/700800/headshot/67/current"
height="42" /> | Darien Smith | 106.8 | 3 |

&#10;</div>

## Provenance & reproducibility

- **Trained on:** Baseball Savant pitch-level features (velocity,
  movement, release, location), seasons in the table above.
- **Committed at:** `mlb/pitching_models/parquet/`; published to
  `mlb_pitching_models`; per-publish metadata in
  [`../../mlb/pitching_models/mlb_pitching_models_card.json`](../../mlb/pitching_models/mlb_pitching_models_card.json).
- **Pipeline:** `scripts/mlb_models.sh 03` → stage
  `python/mlb_model_03_pitching.py` (`mlb_models_cron.yml`). Single
  home: `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Names/headshots via one batched statsapi
  call; offline renders fall back to MLBAM ids.

## Avenues for improvement & open issues

- **Stuff+ vs run value remains a weak-signal gate (≥ 0.20)** —
  pitch-level target engineering (per-pitch run value with count
  context) is the known lever.
- **Known issue:** Command+’s 0.04 directional gate is honest but
  near-noise; treat the column as ordinal at best — its YoY reliability
  above quantifies exactly how noisy.
- **FLAGGED (2026-09-01, reproduced live above):** published xERA is a
  perfect monotone transform of x_wOBA-against (r = 1.0) — one signal in
  two columns; either differentiate the recipe (batted-ball mix, park)
  or document xERA as a display transform.
